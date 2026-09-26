use crate::ThreadStatus;
use crate::handler::{Collector, HandlerContext};
use crate::types::command::{Command, CompleteExecution, CompleteThread};
use crate::types::event::Event;
use crate::types::meta::ObjectKind;

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

impl CompleteThreadHandler {
    pub(crate) async fn handle(
        &self,
        p: &CompleteThread,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) {
        let CompleteThread { thread, output } = p;
        // Addressed by kind: `CompleteThread` is only ever dispatched for a `Thread`, so the row is
        // read directly.
        let Some(thread_row) = ctx.storage.get_thread(thread).await.ok().flatten() else {
            return; // gone, or unreadable (the fold errors out) — nothing to complete.
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
        completing_thread.meta.with_update_at(ctx.now());
        out.append_event(Event::ThreadCompleting {
            thread: completing_thread,
        })
        .await;

        let children = thread_row.active_children.clone();
        let pending_children = super::complete_execution::cancel_timers(out, children);
        if pending_children == 0 {
            let mut completed_thread = thread_row.value();
            completed_thread.status = ThreadStatus::Completed;
            completed_thread.output = Some(output.clone());
            completed_thread.meta.with_update_at(ctx.now());
            out.append_event(Event::ThreadCompleted {
                thread: completed_thread,
            })
            .await;
            if let Some(owner) = thread_row.value.meta.owner.clone() {
                if owner.kind == ObjectKind::Execution {
                    // Root thread (owned by the Execution): its success *is* the run's success. The
                    // `ThreadCompleted` applier has already drained the root thread from the
                    // execution's `active_children`, so `CompleteExecution` now closes the run.
                    out.append_command(Command::CompleteExecution(CompleteExecution {
                        execution: owner,
                        output: output.clone(),
                    }));
                } else {
                    // A fan-out thread is owned by its container Activity; run the inline reaction
                    // so the parallel/map converges once its last branch/item drains.
                    super::child_completed::child_settled(ctx, out, owner, thread_ref.clone())
                        .await;
                }
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
