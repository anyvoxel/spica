use async_trait::async_trait;

use crate::ThreadStatus;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::log::Timestamp;
use crate::storage::ScopeRecord;
use crate::types::command::Command;
use crate::types::event::Event;
use crate::types::meta::ObjectReference;

/// Handles `CompleteThread`: begins the success finish of a fan-out `Thread` (a `Parallel` branch's
/// or a `Map` item's terminal `Succeed`/`End` reached). Emits `ThreadCompleting`, which fixes its
/// output on its row, cancels any owned timers, and — once children drain (immediately if none) —
/// emits `ThreadCompleted` then runs the inline `child_completed::child_settled` so the owning
/// container Activity converges once its last branch/item drains.
///
/// Mirrors [`CompleteExecutionHandler`](super::complete_execution::CompleteExecutionHandler) but is
/// `Thread`-only: a top-level `Execution`'s success is driven by that handler, so the two verbs (and
/// the kinds they address) never cross. A `Thread` is completed **internally** by its own terminal
/// hop (via `emit_transition`), never by an external caller.
#[derive(Default)]
pub struct CompleteThreadHandler;

#[async_trait]
impl CommandHandler for CompleteThreadHandler {
    fn command(&self) -> Command {
        Command::CompleteThread {
            thread: ObjectReference::nil(),
            output: Default::default(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector<'_>) {
        let Command::CompleteThread { thread, output } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        // The addressed run is a fan-out `Thread`; any other kind is an internal fault (dispatch
        // routes top-level runs to `CompleteExecution`), so it is ignored rather than mis-completed.
        let scope = match crate::storage::load_scope_ref(ctx.storage, thread).await {
            Ok(Some(s)) => s,
            Ok(None) => return, // thread already gone — nothing to complete.
            Err(_) => return,   // storage fault — the fold errors out; no fabricated Reject.
        };
        let ScopeRecord::Thread(thread_row) = scope else {
            tracing::debug!(
                target = %thread,
                "complete_thread: addressed node is not a thread; ignition ignored"
            );
            return;
        };
        let thread_ref = thread_row.reference();
        if !thread_row.value.status.is_running() {
            return; // idempotency: already finishing or terminal.
        }

        let mut completing_thread = thread_row.value();
        completing_thread.status = ThreadStatus::Completing;
        completing_thread.output = Some(output.clone());
        // A new lifecycle transition — advance the domain `updated_at` (stemmed at event
        // construction, not from Entry metadata); `created_at` is carried forward unchanged.
        completing_thread.meta.with_update_at(Timestamp::now());
        out.emit_event(Event::ThreadCompleting {
            thread: completing_thread,
        })
        .await;

        let children = thread_row.active_children.clone();
        let pending_children = super::complete_execution::cancel_timers(out, children);
        if pending_children == 0 {
            let mut completed_thread = thread_row.value();
            completed_thread.status = ThreadStatus::Completed;
            completed_thread.output = Some(output.clone());
            completed_thread.meta.with_update_at(Timestamp::now());
            out.emit_event(Event::ThreadCompleted {
                thread: completed_thread,
            })
            .await;
            // A fan-out thread is always owned by its container Activity; run the inline reaction so
            // the parallel/map converges once its last branch/item drains.
            if let Some(owner) = thread_row.value.meta.owner.clone() {
                super::child_completed::child_settled(ctx, out, owner, thread_ref.clone()).await;
            }
        } else {
            tracing::debug!(
                thread = %thread_ref,
                pending = pending_children,
                "thread completing deferred: waiting on owned children"
            );
        }
    }
}
