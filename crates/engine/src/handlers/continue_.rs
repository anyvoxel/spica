//! Handles the deferred **drain-continuation** commands (`ContinueComplete` / `ContinueTerminate`).
//!
//! A settled child no longer drains its owner inline (the old recursive cascade in `child_completed`);
//! the one-hop reactor instead issues one Continue command per drained-and-finishing owner, dispatched
//! on a **later round** (Zeebe's `COMPLETE_ELEMENT` decoupling). Each Continue handler re-folds the
//! per-kind drain the old cascade performed: read the owner, verify it is drained-and-finishing, emit
//! its terminal, then hand the settled owner up to *its* owner via a follow-up Continue — exactly one
//! hop, never recursion, so stack depth is independent of owner-chain depth.

use async_trait::async_trait;

use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::log::Timestamp;
use crate::types::command::Command;
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
            // Success-drain uses the activity's raw result when present; otherwise the state's
            // processed input remains the default output (defensive fallback — a container state
            // emits its own `StateCompleted` after projecting convergence).
            let output = act
                .value
                .raw_output
                .clone()
                .or_else(|| act.value.input.clone())
                .unwrap_or(serde_json::Value::Null);
            let mut activity_value = act.value();
            activity_value.status = ActivityStatus::Completed;
            activity_value.output = Some(output.clone());
            out.emit_event(Event::StateCompleted {
                activity: activity_value,
            })
            .await;
        }
        ActivityStatus::Terminating(reason) => {
            let mut activity_value = act.value();
            activity_value.status = ActivityStatus::Terminated(reason.clone());
            out.emit_event(Event::StateTerminated {
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
            completed_thread.meta.with_update_at(Timestamp::now());
            out.emit_event(Event::ThreadCompleted {
                thread: completed_thread,
            })
            .await;
        }
        ThreadStatus::Terminating(reason) => {
            let mut terminated_thread = thread.value();
            terminated_thread.status = ThreadStatus::Terminated(reason.clone());
            terminated_thread.meta.with_update_at(Timestamp::now());
            out.emit_event(Event::ThreadTerminated {
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
            completed_execution.meta.with_update_at(Timestamp::now());
            out.emit_event(Event::ExecutionCompleted {
                execution: completed_execution,
            })
            .await;
        }
        ExecutionStatus::Terminating(reason) => {
            let mut terminated_execution = exec.value();
            terminated_execution.status = ExecutionStatus::Terminated(reason.clone());
            terminated_execution.meta.with_update_at(Timestamp::now());
            out.emit_event(Event::ExecutionTerminated {
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

#[async_trait]
impl CommandHandler for ContinueCompleteHandler {
    fn command(&self) -> Command {
        Command::ContinueComplete {
            owner: ObjectReference::nil(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector<'_>) {
        let Command::ContinueComplete { owner } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        finish_node(ctx, out, owner).await;
    }
}

/// Handles `Command::ContinueTerminate`: the failure-drain analogue of [`ContinueCompleteHandler`].
#[derive(Default)]
pub struct ContinueTerminateHandler;

#[async_trait]
impl CommandHandler for ContinueTerminateHandler {
    fn command(&self) -> Command {
        Command::ContinueTerminate {
            owner: ObjectReference::nil(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector<'_>) {
        let Command::ContinueTerminate { owner } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        finish_node(ctx, out, owner).await;
    }
}
