//! Handles the deferred **drain-continuation** commands (`ContinueComplete` / `ContinueTerminate`).
//!
//! A settled child no longer drains its owner inline (the old recursive cascade in `child_completed`);
//! the one-hop reactor instead issues one Continue command per drained-and-finishing owner, dispatched
//! on a **later round** (Zeebe's `COMPLETE_ELEMENT` decoupling). Each Continue handler re-folds the
//! per-kind drain the old cascade performed: read the owner, verify it is drained-and-finishing, emit
//! its terminal, then hand the settled owner up to *its* owner via a follow-up Continue — exactly one
//! hop, never recursion, so stack depth is independent of owner-chain depth.
//!
//! An **activity**'s terminal is emitted by its own state's `finish` rather than a kind-local close:
//! the state's `complete` step opens the finish (`StateCompleting`) but defers while children remain,
//! so this hop is where its projection and `Next`/`End` routing actually run (see
//! [`finish_activity_via_state`]).

use crate::handler::{Collector, HandlerContext};
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectReference};

/// Emit a drained-and-finishing `node`'s terminal (by kind and status), then deliver the settled
/// node up to its owner as one hop ([`child_completed::child_settled`]) — which issues the next
/// Continue command (or replenishes a Running container) rather than recursing. The Continue-issue
/// invariant guarantees `node` is drained-and-finishing here, so a real terminal is always emitted.
pub(crate) async fn finish_node(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    node: &ObjectReference,
) {
    match node.kind {
        ObjectKind::Activity => Box::pin(finish_activity(ctx, out, node)).await,
        ObjectKind::Thread => Box::pin(finish_thread(ctx, out, node)).await,
        ObjectKind::Execution => Box::pin(finish_execution(ctx, out, node)).await,
        _ => {}
    }
}

/// Outcome of routing a drained `Completing` activity through its own state handler.
enum DeferredFinish {
    /// The state's `finish` ran — the terminal, its projection and its routing are emitted, so the
    /// caller may relay the settled activity up to its owner.
    Handled,
    /// The state could no longer be resolved (owner scope or definition gone): the caller closes the
    /// activity generically so its parent still drains.
    Unresolvable,
    /// The projection failed and the activity was terminated here; the terminate's own drain owns the
    /// rest, so the caller must not relay.
    Terminated,
}

/// Finish a drained `Completing` activity through its own state handler, reproducing the projection
/// (`Assign`/`Output`) and the `Next`/`End` routing the base `complete` step would have run had the
/// children already drained. Without this the drain hop would close the activity with an unprojected
/// result and leave the execution parked on a completed state. A projection failure terminates the
/// activity here — the same policy as the base's `fail_or!`.
async fn finish_activity_via_state(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    node: &ObjectReference,
    act: &crate::storage::ActivityRecord,
) -> DeferredFinish {
    let activity_value = act.value();
    let owner = activity_value
        .meta
        .owner
        .clone()
        .expect("an owned activity has an owner");
    // An activity's owner is always a `Thread` (see `emit_transition`), so the row is read directly.
    let Ok(Some(thread)) = ctx.storage.get_thread(&owner).await else {
        return DeferredFinish::Unresolvable;
    };
    let Ok(sm) = ctx.machine_for_thread(&thread).await else {
        return DeferredFinish::Unresolvable;
    };
    let Ok(state_def) =
        super::resolve_state_for(&sm, &thread, &activity_value.state_path.state_name()).await
    else {
        return DeferredFinish::Unresolvable;
    };
    let Some(handler) = ctx.state_handlers.create(state_def) else {
        return DeferredFinish::Unresolvable; // no registered handler — engine regression.
    };
    let variables = thread.variables.clone();
    if let Err(e) = handler
        .finish(ctx.env, out, node.clone(), &activity_value, &variables)
        .await
    {
        tracing::warn!(
            activity = %node,
            error = %e,
            "deferred state finish failed; terminating the activity"
        );
        out.terminate(Some(node.clone()), activity_value.execution.clone(), e);
        return DeferredFinish::Terminated;
    }
    DeferredFinish::Handled
}

async fn finish_activity(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    node: &ObjectReference,
) {
    use crate::ActivityStatus;
    let Some(act) = ctx.storage.get_activity(node).await.ok().flatten() else {
        return;
    };
    if !act.active_children.is_empty() {
        return; // not drained — defers to whichever settle triggers the Continue instead.
    }
    match &act.value.status {
        ActivityStatus::Completing => {
            match finish_activity_via_state(ctx, out, node, &act).await {
                DeferredFinish::Handled => {}
                DeferredFinish::Terminated => return,
                DeferredFinish::Unresolvable => {
                    // Defensive fallback: use the activity's raw result when present, otherwise the
                    // state's processed input remains the default output.
                    let output = act
                        .value
                        .raw_output
                        .clone()
                        .or_else(|| act.value.input.clone())
                        .unwrap_or(serde_json::Value::Null);
                    let mut activity_value = act.value();
                    activity_value.status = ActivityStatus::Completed;
                    activity_value.output = Some(output.clone());
                    out.append_event(Event::StateCompleted {
                        activity: activity_value,
                    })
                    .await;
                }
            }
        }
        ActivityStatus::Terminating(reason) => {
            let mut activity_value = act.value();
            activity_value.status = ActivityStatus::Terminated(reason.clone());
            out.append_event(Event::StateTerminated {
                activity: activity_value,
            })
            .await;
        }
        _ => return, // not finishing — nothing to continue.
    }
    if let Some(owner) = act.value.meta.owner.clone() {
        Box::pin(super::child_completed::child_settled(
            ctx,
            out,
            owner,
            node.clone(),
        ))
        .await;
    }
}

async fn finish_thread(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    node: &ObjectReference,
) {
    use crate::types::thread::ThreadStatus;
    let Some(thread) = ctx.storage.get_thread(node).await.ok().flatten() else {
        return;
    };
    if !thread.active_children.is_empty() {
        return;
    }
    match &thread.status {
        ThreadStatus::Completing => {
            let output = thread.output.clone().unwrap_or(Default::default());
            let mut completed_thread = thread.value();
            completed_thread.status = ThreadStatus::Completed;
            completed_thread.output = Some(output);
            completed_thread.meta.with_update_at(ctx.now());
            out.append_event(Event::ThreadCompleted {
                thread: completed_thread,
            })
            .await;
        }
        ThreadStatus::Terminating(reason) => {
            let mut terminated_thread = thread.value();
            terminated_thread.status = ThreadStatus::Terminated(reason.clone());
            terminated_thread.meta.with_update_at(ctx.now());
            out.append_event(Event::ThreadTerminated {
                thread: terminated_thread,
            })
            .await;
        }
        _ => return,
    }
    if let Some(owner) = thread.value.meta.owner.clone() {
        Box::pin(super::child_completed::child_settled(
            ctx,
            out,
            owner,
            node.clone(),
        ))
        .await;
    }
}

async fn finish_execution(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    node: &ObjectReference,
) {
    use crate::ExecutionStatus;
    let Some(exec) = ctx.storage.get_execution(node).await.ok().flatten() else {
        return;
    };
    if !exec.active_children.is_empty() {
        return;
    }
    match &exec.status {
        ExecutionStatus::Completing => {
            let output = exec.output.clone().unwrap_or(Default::default());
            let mut completed_execution = exec.value();
            completed_execution.status = ExecutionStatus::Completed;
            completed_execution.output = Some(output.clone());
            completed_execution.meta.with_update_at(ctx.now());
            out.append_event(Event::ExecutionCompleted {
                execution: completed_execution,
            })
            .await;
        }
        ExecutionStatus::Terminating(reason) => {
            let mut terminated_execution = exec.value();
            terminated_execution.status = ExecutionStatus::Terminated(reason.clone());
            terminated_execution.meta.with_update_at(ctx.now());
            out.append_event(Event::ExecutionTerminated {
                execution: terminated_execution,
            })
            .await;
        }
        _ => return,
    }
    if let Some(owner) = exec.value.meta.owner.clone() {
        Box::pin(super::child_completed::child_settled(
            ctx,
            out,
            owner,
            node.clone(),
        ))
        .await;
    }
}

/// Handles `Command::ContinueComplete`: emits the drained owner's success terminal on this round,
/// then issues (via `child_completed`) a follow-up Continue for the owner's own owner — one hop.
#[derive(Default)]
pub struct ContinueCompleteHandler;

impl ContinueCompleteHandler {
    pub(crate) async fn handle(
        &self,
        owner: &ObjectReference,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) {
        finish_node(ctx, out, owner).await;
    }
}

/// Handles `Command::ContinueTerminate`: the failure-drain analogue of [`ContinueCompleteHandler`].
#[derive(Default)]
pub struct ContinueTerminateHandler;

impl ContinueTerminateHandler {
    pub(crate) async fn handle(
        &self,
        owner: &ObjectReference,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) {
        finish_node(ctx, out, owner).await;
    }
}
