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

use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::handlers::container::{ActivityContainer, Container, ExecutionContainer};
use crate::types::activity::ActivityKind;
use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::execution::ExecutionKind;
use crate::types::meta::{HasRawObjectRef, ObjectKind, ObjectRef, RawObjectRef, ThreadOwner};
use crate::types::thread::{ThreadKind, ThreadStatus};

/// Emit a drained-and-finishing `node`'s terminal (by kind and status), then deliver the settled
/// node up to its owner as one hop ([`child_completed::child_settled`]) — which issues the next
/// Continue command (or replenishes a Running container) rather than recursing. The Continue-issue
/// invariant guarantees `node` is drained-and-finishing here, so a real terminal is always emitted.
///
/// A hop that could not even read the row it was told to close produced no outcome for the `Continue`
/// command, so its fault is returned rather than swallowed (see [`ProcessingError`]).
pub(crate) async fn finish_node(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    node: &RawObjectRef,
) -> Result<(), ProcessingError> {
    match node.kind {
        ObjectKind::Activity => {
            Box::pin(finish_activity(
                ctx,
                out,
                &node.clone().typed::<ActivityKind>(),
            ))
            .await
        }
        ObjectKind::Thread => {
            Box::pin(finish_thread(ctx, out, &node.clone().typed::<ThreadKind>())).await
        }
        ObjectKind::Execution => {
            Box::pin(finish_execution(
                ctx,
                out,
                &node.clone().typed::<ExecutionKind>(),
            ))
            .await
        }
        _ => Ok(()),
    }
}

/// Outcome of routing a drained `Completing` activity through its own state handler.
enum DeferredFinish {
    /// The state's `after_completing` ran — the terminal and its routing are emitted, so the caller may
    /// relay the settled activity up to its owner.
    Handled,
    /// The state could no longer be resolved (owner scope or definition gone): the caller closes the
    /// activity generically so its parent still drains.
    Unresolvable,
}

/// Finish a drained `Completing` activity through its own state's complete step: the children have
/// already drained (see `finish_activity`'s guard), so `after_completing` runs the same terminal,
/// projection and `Next`/`End` routing the `complete` step would have run had they drained before it.
/// Without this hop the drained activity would be closed with an unprojected result and leave the
/// execution parked on a completed state.
async fn finish_activity_via_state(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    act: &crate::storage::ActivityRecord,
) -> Result<DeferredFinish, ProcessingError> {
    let activity_value = act.value();
    let owner = activity_value.meta.owner.clone();
    // An activity's owner slot admits only a `Thread`, so the row is read directly.
    // A **read** fault is the dispatch's, not the finish's — it is returned so the leader can retry the
    // hop — while a missing row, definition, or handler is a real unresolvable (closed generically by
    // the caller). Only the read separates the two; `machine_for_thread` mixes a missing definition
    // (domain) with the storage fault underneath it, so its `Infra` is split out explicitly.
    let Some(thread) = ctx.storage.get_thread(&owner).await? else {
        return Ok(DeferredFinish::Unresolvable);
    };
    let sm = match ctx.machine_for_thread(&thread).await {
        Ok(sm) => sm,
        Err(ExecutionError::Infra(e)) => return Err(ProcessingError::Unexpected(e.into())),
        Err(_) => return Ok(DeferredFinish::Unresolvable),
    };
    // The state to finish is the one this activity names — its own `state_path` locates the
    // definition inside the machine the owning thread binds to.
    let Ok(state_def) = sm.state_at(&activity_value.state_path) else {
        return Ok(DeferredFinish::Unresolvable);
    };
    let Some(handler) = ctx.state_handlers.create(state_def) else {
        return Ok(DeferredFinish::Unresolvable); // no registered handler — engine regression.
    };
    let variables = thread.variables.clone();
    // The drain runs the state's own complete step. The hop lands only on a drained-and-finishing
    // activity (see `finish_activity`'s guard), so `after_completing` takes its drained arm and emits
    // the terminal; it runs no projection on the container's defensive arm, and a leaf's projection
    // failure is turned into its own `TerminateState` inside the state — so a surfacing `Err` here is a
    // read fault that is the dispatch's, returned rather than swallowed.
    handler
        .after_completing(ctx, out, &activity_value, &variables)
        .await?;
    Ok(DeferredFinish::Handled)
}

async fn finish_activity(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    node: &ObjectRef<ActivityKind>,
) -> Result<(), ProcessingError> {
    use crate::ActivityStatus;
    // A read fault is the dispatch's — returned, so the leader owns retry/refusal; a missing row is
    // "already closed", which this hop answers with nothing.
    let Some(act) = ctx.storage.get_activity(node).await? else {
        return Ok(());
    };
    if !act.active_children.is_empty() {
        return Ok(()); // not drained — defers to whichever settle triggers the Continue instead.
    }
    match &act.value.status {
        ActivityStatus::Completing => {
            match finish_activity_via_state(ctx, out, &act).await? {
                DeferredFinish::Handled => {}
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
        _ => return Ok(()), // not finishing — nothing to continue.
    }
    Box::pin(super::child_completed::child_settled(
        ctx,
        out,
        act.value.meta.owner.clone().into_raw_object_ref(),
        node.as_raw_object_ref().clone(),
    ))
    .await;
    Ok(())
}

async fn finish_thread(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    node: &ObjectRef<ThreadKind>,
) -> Result<(), ProcessingError> {
    // A read fault is the dispatch's — returned, so the leader owns retry/refusal; a missing row is
    // "already closed", which this hop answers with nothing.
    let Some(thread) = ctx.storage.get_thread(node).await? else {
        return Ok(());
    };
    if !thread.active_children.is_empty() {
        return Ok(());
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
        _ => return Ok(()),
    }
    // The drained thread hands its settle to its owner's `Container` — the same reaction point the
    // inline path uses (see `complete_thread`/`terminate_thread`) — so a root thread that drains
    // *here*, after the last of its own children settled, starts its run's teardown exactly as one
    // that had nothing to wait for does. A kind-routed `child_settled` would not: it has no arm for
    // a still-`Running` run, which is where that teardown has to come from.
    let child = node.clone().into_raw_object_ref();
    let terminated = matches!(thread.status, ThreadStatus::Terminating(_));
    match thread.value.meta.owner.clone() {
        ThreadOwner::Execution(execution) => {
            // A missing owner row is "already closed", the same answer a missing thread gets above; a
            // fault reading it is the dispatch's, and is returned.
            if let Some(container) = ExecutionContainer::open(ctx.storage, execution).await? {
                if terminated {
                    container.after_child_terminated(ctx, out, &child).await;
                } else {
                    container.after_child_completed(ctx, out, &child).await;
                }
            }
        }
        ThreadOwner::Activity(activity) => {
            if let Some(container) = ActivityContainer::open(ctx.storage, activity).await? {
                if terminated {
                    container.after_child_terminated(ctx, out, &child).await;
                } else {
                    container.after_child_completed(ctx, out, &child).await;
                }
            }
        }
    }
    Ok(())
}

async fn finish_execution(
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
    node: &ObjectRef<ExecutionKind>,
) -> Result<(), ProcessingError> {
    use crate::ExecutionStatus;
    // A read fault is the dispatch's — returned, so the leader owns retry/refusal; a missing row is
    // "already closed", which this hop answers with nothing.
    let Some(exec) = ctx.storage.get_execution(node).await? else {
        return Ok(());
    };
    if !exec.active_children.is_empty() {
        return Ok(());
    }
    match &exec.status {
        ExecutionStatus::Completing => {
            // The row already carries what the finish fixed (`output` was written when it entered
            // `Completing`), so the terminal advances the stored value rather than re-deriving one.
            let mut completed_execution = exec.value();
            // The arm is the transition's own precondition, so this can only decline if the row moved
            // under the read above — a race this hop answers with nothing, exactly as a missing row.
            if completed_execution.mark_completed(ctx.now()).is_err() {
                return Ok(());
            }
            out.append_event(Event::ExecutionCompleted {
                execution: completed_execution,
            })
            .await;
        }
        ExecutionStatus::Terminating(_) => {
            // The reason was written when the teardown began (`mark_terminating`), so the terminal
            // advances the stored value rather than re-deriving one — same as the Completing arm above.
            let mut terminated_execution = exec.value();
            // The arm is the transition's own precondition, so this can only decline if the row moved
            // under the read above — a race this hop answers with nothing, exactly as a missing row.
            if terminated_execution.mark_terminated(ctx.now()).is_err() {
                return Ok(());
            }
            out.append_event(Event::ExecutionTerminated {
                execution: terminated_execution,
            })
            .await;
        }
        _ => return Ok(()),
    }
    // Nothing to relay: a run is the root of its object tree, so it has no owner to settle up to.
    Ok(())
}

/// Handles `Command::ContinueComplete`: emits the drained owner's success terminal on this round,
/// then issues (via `child_completed`) a follow-up Continue for the owner's own owner — one hop.
#[derive(Default)]
pub struct ContinueCompleteHandler;

impl ContinueCompleteHandler {
    pub(crate) async fn handle(
        &self,
        owner: &RawObjectRef,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        finish_node(ctx, out, owner).await
    }
}

/// Handles `Command::ContinueTerminate`: the failure-drain analogue of [`ContinueCompleteHandler`].
#[derive(Default)]
pub struct ContinueTerminateHandler;

impl ContinueTerminateHandler {
    pub(crate) async fn handle(
        &self,
        owner: &RawObjectRef,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        finish_node(ctx, out, owner).await
    }
}
