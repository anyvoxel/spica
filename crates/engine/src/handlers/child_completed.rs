//! The **one-hop** reaction to a child reaching a terminal state — no recursion up the owner chain.
//!
//! When a child settles under a `parent`, the parent reacts in a single hop by its own kind:
//! a drained-and-finishing parent (`Completing`/`Terminating` with no remaining children) is handed
//! a `ContinueComplete`/`ContinueTerminate` command (see `handlers::continue_`) whose own drain
//! happens on a **later round**; a `Running` container (a `Parallel`/`Map` with an open slot)
//! replenishes inline via [`dispatch_child_completed`]. Either way the old deep recursion is broken:
//! each drained ancestor occupies its own round, so convergence is a worklist of commands rather
//! than a call stack (Zeebe's `COMPLETE_ELEMENT` decoupling — see `crates/engine/src/types/command.rs`).
//!
//! **Why this is clean:** the collector folds each emitted `Event` eagerly into the working overlay at
//! `emit_event` time, so `parent`'s `active_children` is the real post-drain projection when this
//! reactor reads it — no deferred flush, no snapshot arithmetic, and the guard "only issue a Continue
//! when the owner is provably drained-and-finishing" means every issued command does real work on its
//! round (never a silent no-op that would need a marker event).

use crate::handler::{ActivityCtx, Collector, CtxKind, HandlerContext};
use crate::types::command::Command;
use crate::types::meta::{ObjectKind, ObjectReference};
use crate::types::thread::ThreadStatus;
use crate::{ActivityStatus, ExecutionStatus};

/// A direct child `child` has settled under `parent`. Single hop: react at the `parent` node itself,
/// routing by its own kind to that kind's hook. Each hook re-reads `parent` through the working
/// overlay and either issues a Continue command (drained-and-finishing) or replenishes (Running
/// container) — it never descends further itself.
pub async fn child_settled(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    parent: ObjectReference,
    child: ObjectReference,
) {
    match parent.kind {
        ObjectKind::Activity => Box::pin(activity_child_settled(ctx, out, parent, child)).await,
        ObjectKind::Execution => Box::pin(execution_child_settled(ctx, out, parent)).await,
        ObjectKind::Thread => Box::pin(thread_child_settled(ctx, out, parent)).await,
        // A Flow / FlowVersion / Timer / Task parent owns no children in this path.
        _ => {}
    }
}

/// The one-hop **scope drain** hook for a [`crate::Execution`]: as soon as its last child settles and
/// it is `Completing`/`Terminating`, hand it a Continue command so it emits its own terminal and, in
/// turn, notifies its (outer) owner — on the next round, never inline.
async fn execution_child_settled(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    parent: ObjectReference,
) {
    let Some(exec) = ctx.storage.get_execution(&parent).await.ok().flatten() else {
        return; // gone already; nothing to drain.
    };
    if !exec.active_children.is_empty() {
        return; // not drained yet — some other child owns the finish.
    }
    match &exec.status {
        ExecutionStatus::Completing => {
            out.emit_command(Command::ContinueComplete { owner: parent })
        }
        ExecutionStatus::Terminating(_) => {
            out.emit_command(Command::ContinueTerminate { owner: parent })
        }
        // Running + children: no state-specific replenish hook for an Execution yet.
        // TODO(Map/Parallel): dispatch replenish via the state table for Executions too.
        _ => {}
    }
}

/// The one-hop **scope drain** hook for a fan-out [`crate::Thread`] (a `Parallel` branch / `Map`
/// item): once its own children are gone and it is `Completing`/`Terminating`, hand it a Continue
/// command so its container `Activity` converges in a later round.
async fn thread_child_settled(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    parent: ObjectReference,
) {
    let Some(thread) = ctx.storage.get_thread(&parent).await.ok().flatten() else {
        return; // gone already; nothing to drain.
    };
    if !thread.active_children.is_empty() {
        return; // not drained yet — some other child owns the thread's finish.
    }
    match &thread.status {
        ThreadStatus::Completing => out.emit_command(Command::ContinueComplete { owner: parent }),
        ThreadStatus::Terminating(_) => {
            out.emit_command(Command::ContinueTerminate { owner: parent })
        }
        _ => {}
    }
}

/// The one-hop **activity** child-settled hook — the only one with a state-specific half.
/// - `Completing` / `Terminating` + no remaining children → hand the drained activity a Continue
///   command (its terminal is emitted on a later round).
/// - `Running` → a container state's **replenish** moment: dispatch its `child_completed` hook (see
///   [`dispatch_child_completed`]).
async fn activity_child_settled(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    parent: ObjectReference,
    child: ObjectReference,
) {
    let Some(act) = ctx.storage.get_activity(&parent).await.ok().flatten() else {
        return;
    };
    match act.value.status {
        ActivityStatus::Completing if act.active_children.is_empty() => {
            out.emit_command(Command::ContinueComplete { owner: parent });
        }
        ActivityStatus::Terminating(_) if act.active_children.is_empty() => {
            out.emit_command(Command::ContinueTerminate { owner: parent });
        }
        // Running + children: the **replenish** half (state-specific). Dispatched on *every*
        // settled child (not just when `active_children` is empty), so a `Map` refills a
        // freed `MaxConcurrency` slot one item at a time; the matching
        // `StateHandler::child_completed` decides replenish-vs-converge-vs-fail.
        ActivityStatus::Running => {
            dispatch_child_completed(ctx, out, parent, &act, child).await;
        }
        _ => {}
    }
}

/// The **replenish** half of a `Running` activity whose child settled: build an `ActivityCtx` for
/// the activity and dispatch to its `StateHandler::child_completed` so the state itself decides the
/// next move (for a Map: refill a `MaxConcurrency` slot or converge/fail; for a Parallel: aggregate
/// and complete or fail). Mirrors how `CompleteStateHandler` constructs a ctx for `complete`.
async fn dispatch_child_completed(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    activity: ObjectReference,
    act: &crate::storage::ActivityRecord,
    child: ObjectReference,
) {
    let scope_ref = act
        .value
        .meta
        .owner
        .clone()
        .expect("an owned activity has an owner");
    let scope = match crate::storage::load_scope_ref(ctx.storage, &scope_ref).await {
        Ok(Some(s)) => s,
        _ => return, // owning scope gone — nothing to replenish into.
    };
    let sm = match ctx.machine_for_scope(&scope).await {
        Ok(s) => s,
        Err(_) => return, // definition gone — nothing to decide.
    };
    let state_def = match super::resolve_state_for(
        &sm,
        &scope,
        &crate::handlers::state_name_from_path(act.value.state_path.as_ptr()),
    )
    .await
    {
        Ok(s) => s,
        Err(_) => return, // definition gone — nothing to decide.
    };
    let actx = ActivityCtx {
        activity: act.value(),
        execution_state_path: scope.state_path().cloned(),
        exec_input: scope.input().clone(),
        variables: scope.variables().clone(),
        kind: CtxKind::Complete,
    };
    match ctx.state_handlers.get(&std::mem::discriminant(state_def)) {
        Some(handler) => {
            handler
                .child_completed(ctx, out, activity, Some(&actx), state_def, child)
                .await;
        }
        None => {
            // A non-container state owning children while Running is an internal fault; nothing to
            // do (the activity stays Running, but no container logic runs).
            tracing::warn!(activity = %activity, "no container child_completed for state");
        }
    }
}
