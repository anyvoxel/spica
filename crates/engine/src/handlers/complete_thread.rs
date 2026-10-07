use crate::RejectionType;
use crate::ThreadStatus;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::handlers::container::{ActivityContainer, Container, ExecutionContainer};
use crate::types::command::CompleteThread;
use crate::types::event::Event;
use crate::types::meta::ThreadOwner;

/// Handles `CompleteThread`: begins the success finish of a fan-out `Thread` (a `Parallel` branch's
/// or a `Map` item's terminal `Succeed`/`End` reached). Emits `ThreadCompleting`, which fixes its
/// output on its row; nothing else can be pending — a thread owns activities only, and the activity
/// relaying this completion has already drained — so `ThreadCompleted` follows in the same batch and
/// the settle is handed to the owner's [`Container`], which lets the owning container Activity
/// converge once its last branch/item drains (or closes the run, for a root thread).
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
    ) -> Result<(), ProcessingError> {
        let CompleteThread { thread, output } = p;
        // Addressed by kind: `CompleteThread` is only ever dispatched for a `Thread`, so the row is
        // read directly. Nothing ever removes a row, and a thread row is written by the batch that
        // creates it — before anything could complete it — so a miss is the log and the projection
        // disagreeing (a forged command, or a corrupt store) rather than a thread that outlived its own
        // finish. A fault reading the row is neither, and is returned so the leader can retry it.
        let Some(thread_row) = ctx.storage.get_thread(thread).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("complete_thread: thread {thread} does not exist; completion refused"),
            ));
        };
        let thread_ref = thread_row.meta.raw_object_ref();
        // A thread that is already finishing or terminal: the success finish's intent is already
        // satisfied. The duplicate is a race rather than a bug — a branch thread reaching its terminal
        // hop drafts this command while its container's own sweep drafts `TerminateThread` for the same
        // thread, and the leader dispatches in append order, so whichever lands second finds the thread
        // past `Running`. Refused rather than dropped, so the durable log records that this finish was a
        // duplicate rather than leaving it indistinguishable from one that applied.
        if !thread_row.value.status.is_running() {
            let phase = thread_row.value.status.phase();
            tracing::warn!(
                thread = %thread_ref,
                status = ?thread_row.value.status,
                "completion arrived for a thread that is already past Running; refused"
            );
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "complete_thread: thread {thread_ref} is already {phase}; completion refused"
                ),
            ));
        }

        // TODO：应该指 thread 的一个函数
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

        // A thread owns **activities only** — a deadline belongs to the activity that armed it
        // (`TimerOwner` has no `Thread` variant) — and the activity relaying this completion has
        // already drained, so an empty set is the ordinary case: the thread finishes here and hands its
        // result up to its owner.
        if thread_row.active_children.is_empty() {
            let mut completed_thread = thread_row.value();
            completed_thread.status = ThreadStatus::Completed;
            completed_thread.output = Some(output.clone());
            completed_thread.meta.with_update_at(ctx.now());

            // Which parent the settled thread reports to is decided by the *type* of its owner — a
            // `ThreadOwner` sum, so each variant names its own container — never by comparing a
            // runtime kind. The container is resolved *before* `ThreadCompleted`: an ownerless settle
            // is refused while it can still be answered, rather than discovered as a no-op once the
            // terminal event is already on the log. The hook then lets the owner decide what the
            // settle *means* (a run closes; a `Parallel`/`Map` converges).
            match thread_row.value.meta.owner.clone() {
                ThreadOwner::Execution(execution) => {
                    let container =
                        ExecutionContainer::open(ctx.storage, execution.clone()).await?;
                    out.append_event(Event::ThreadCompleted {
                        thread: completed_thread,
                    })
                    .await;
                    container
                        .after_child_completed(ctx, out, &thread_ref)
                        .await?;
                }
                // A fan-out thread is owned by its container Activity; the hook runs the settle so the
                // parallel/map converges (or replenishes) through the state's own decision.
                ThreadOwner::Activity(activity) => {
                    let container = ActivityContainer::open(ctx.storage, activity.clone()).await?;
                    out.append_event(Event::ThreadCompleted {
                        thread: completed_thread,
                    })
                    .await;
                    container
                        .after_child_completed(ctx, out, &thread_ref)
                        .await?;
                }
            }
            return Ok(());
        }

        // Anything still attached is the row's `active_children` disagreeing with the lifecycle rather
        // than a child to wait on: there is no deadline here to cancel, and an activity a thread could
        // still be waiting on is the very one relaying this completion. Refused rather than closed
        // over, and a refusal strands nothing — the child's own settle drains the thread.
        Err(ProcessingError::Rejected(
            RejectionType::InvalidState,
            format!(
                "complete_thread: thread {thread_ref} still owns {} live child(ren); completion refused",
                thread_row.active_children.len()
            ),
        ))
    }
}
