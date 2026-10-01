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
//! `append_event` time, so `parent`'s `active_children` is the real post-drain projection when this
//! reactor reads it — no deferred flush, no snapshot arithmetic, and the guard "only issue a Continue
//! when the owner is provably drained-and-finishing" means every issued command does real work on its
//! round (never a silent no-op that would need a marker event).

use crate::handler::{Collector, HandlerContext};
use crate::types::activity::ActivityKind;
use crate::types::command::Command;
use crate::types::execution::ExecutionKind;
use crate::types::meta::{HasRawObjectRef, ObjectKind, ObjectRef, RawObjectRef};
use crate::types::thread::{ThreadKind, ThreadStatus};
use crate::{ActivityStatus, ExecutionStatus};

/// A direct child `child` has settled under `parent`. Single hop: react at the `parent` node itself,
/// routing by its own kind to that kind's hook. Each hook re-reads `parent` through the working
/// overlay and either issues a Continue command (drained-and-finishing) or replenishes (Running
/// container) — it never descends further itself.
pub async fn child_settled(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    parent: RawObjectRef,
    child: RawObjectRef,
) {
    match parent.kind {
        ObjectKind::Activity => {
            Box::pin(activity_child_settled(
                ctx,
                out,
                parent.clone().typed::<ActivityKind>(),
                child,
            ))
            .await
        }
        ObjectKind::Execution => {
            Box::pin(execution_child_settled(
                ctx,
                out,
                parent.clone().typed::<ExecutionKind>(),
            ))
            .await
        }
        ObjectKind::Thread => {
            Box::pin(thread_child_settled(
                ctx,
                out,
                parent.clone().typed::<ThreadKind>(),
            ))
            .await
        }
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
    parent: ObjectRef<ExecutionKind>,
) {
    let Some(exec) = ctx.storage.get_execution(&parent).await.ok().flatten() else {
        return; // gone already; nothing to drain.
    };
    if !exec.active_children.is_empty() {
        return; // not drained yet — some other child owns the finish.
    }
    // The Continue commands address their owner as a flat reference, so the erasure happens at this
    // seam — one value, both arms.
    let owner = parent.into_raw_object_ref();
    match &exec.status {
        ExecutionStatus::Completing => out.append_command(Command::ContinueComplete { owner }),
        ExecutionStatus::Terminating(_) => out.append_command(Command::ContinueTerminate { owner }),
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
    parent: ObjectRef<ThreadKind>,
) {
    let Some(thread) = ctx.storage.get_thread(&parent).await.ok().flatten() else {
        return; // gone already; nothing to drain.
    };
    if !thread.active_children.is_empty() {
        return; // not drained yet — some other child owns the thread's finish.
    }
    // See `execution_child_settled`: the Continue command's owner is flat, so the erasure is here.
    let owner = parent.into_raw_object_ref();
    match &thread.status {
        ThreadStatus::Completing => out.append_command(Command::ContinueComplete { owner }),
        ThreadStatus::Terminating(_) => out.append_command(Command::ContinueTerminate { owner }),
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
    parent: ObjectRef<ActivityKind>,
    child: RawObjectRef,
) {
    let Some(act) = ctx.storage.get_activity(&parent).await.ok().flatten() else {
        return;
    };
    // See `execution_child_settled`: the Continue command's owner is flat, so the erasure is here.
    let owner = parent.clone().into_raw_object_ref();
    match act.value.status {
        ActivityStatus::Completing if act.active_children.is_empty() => {
            out.append_command(Command::ContinueComplete { owner });
        }
        ActivityStatus::Terminating(_) if act.active_children.is_empty() => {
            out.append_command(Command::ContinueTerminate { owner });
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

/// The **replenish** half of a `Running` activity whose child settled: pass the activity value and
/// its scope's variables to the state's `StateHandler::child_completed` so the state itself decides
/// the next move (for a Map: refill a `MaxConcurrency` slot or converge/fail; for a Parallel:
/// aggregate and complete or fail).
async fn dispatch_child_completed(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    activity: ObjectRef<ActivityKind>,
    act: &crate::storage::ActivityRecord,
    child: RawObjectRef,
) {
    // An activity's owner is always a `Thread` (see `emit_transition`), so the row is read directly.
    let scope_ref = act.value.meta.owner.clone();
    let Some(thread) = ctx.storage.get_thread(&scope_ref).await.ok().flatten() else {
        return; // owning scope gone — nothing to replenish into.
    };
    let sm = match ctx.machine_for_thread(&thread).await {
        Ok(s) => s,
        Err(_) => return, // definition gone — nothing to decide.
    };
    // The state to replenish is the one this activity names: its own `state_path` locates the
    // definition inside the machine the owning thread binds to.
    let state_def = match sm.state_at(&act.value.state_path) {
        Ok(s) => s,
        Err(_) => return, // definition gone — nothing to decide.
    };
    let activity_value = act.value();
    let variables = thread.variables.clone();
    // Every `State` variant has a registered factory (see `build_state_handlers` + the
    // `registry.len() == 8` coverage test), so a miss here is an engine regression — fail loud
    // rather than leave the container stuck Running without its logic.
    let handler = ctx
        .state_handlers
        .create(state_def)
        .expect("state type has no registered handler: engine regression, not a flow error");
    handler
        .child_completed(ctx, out, activity, &activity_value, &variables, child)
        .await;
}
