use crate::RejectionType;
use crate::ThreadStatus;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::handlers::container::{ActivityContainer, Container, ExecutionContainer};
use crate::types::activity::ActivityKind;
use crate::types::command::{Command, TerminateState, TerminateThread, TerminationReason};
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ThreadOwner};

/// Handles `TerminateThread`: begins the abnormal finish of a fan-out `Thread` with `reason`.
/// Mirrors [`TerminateExecutionHandler`](super::terminate_execution::TerminateExecutionHandler) but
/// resolved against **thread** storage: a `Thread` is only terminated internally by its owning
/// container Activity (a `Parallel`/`Map` sweep tearing down a branch/item), never by the external
/// name-addressed root terminate. Emits `ThreadTerminating`; a thread with nothing left to sweep lands
/// `ThreadTerminated` in this same batch and hands the settle to its owner's `Container`, while one
/// whose child is still live sweeps it and finishes when that child settles back. A thread's only child
/// kind is `Activity`, so a nested fan-out is unwound one layer down — by the `TerminateState` that
/// child receives — rather than by a thread sweeping a thread.
#[derive(Default)]
pub struct TerminateThreadHandler;

impl TerminateThreadHandler {
    pub(crate) async fn handle(
        &self,
        p: &TerminateThread,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let TerminateThread { thread, reason } = p;
        // Addressed by kind: `TerminateThread` is only ever dispatched for a `Thread`, so the row is
        // read directly. Nothing ever removes a row, and a thread row is written by the batch that
        // creates it — before anything could name it in a sweep — so a miss is the log and the
        // projection disagreeing (a forged command, or a corrupt store) rather than a thread that
        // outlived its teardown. A fault reading the row is neither, and is returned so the leader can
        // retry it.
        let Some(thread_row) = ctx.storage.get_thread(thread).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("terminate_thread: thread {thread} does not exist; termination refused"),
            ));
        };
        let thread_ref = thread_row.meta.raw_object_ref();
        // The whole precondition — only a `Running` thread has a teardown to begin — lives in the
        // transition (`mark_terminating`), so no call site can honor part of it and forget the rest.
        //
        // A thread that is already finishing or terminal is *expected* to be swept twice — one branch
        // thread is reachable from its container's own sweep and from the run-level cascade, so two of
        // them can tear the same branch down. Refused rather than dropped, so the durable log records
        // that this sweep was a duplicate rather than leaving a bogus one indistinguishable from an
        // absorbed one.
        let mut terminating_thread = thread_row.value();
        if let Err(why) = terminating_thread.mark_terminating(reason.clone(), ctx.now()) {
            tracing::warn!(
                thread = %thread_ref,
                status = ?thread_row.value.status,
                reason = ?reason,
                "termination arrived for a thread that is already past Running; refused: {why}"
            );
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("terminate_thread: thread {thread_ref} cannot be terminated: {why}"),
            ));
        }
        out.append_event(Event::ThreadTerminating {
            thread: terminating_thread,
        })
        .await;

        // A thread's abnormal finish reaches its owner only **once the thread itself has cleaned up**
        // — an empty child set below, or the last sweep settle later — never at the moment its own
        // teardown opens. Reaching earlier (as the run-level relay here once did) starts a parent's
        // teardown while this thread still holds children, so the parent sweeps a subtree that is
        // already unwinding and every command it issues lands against a row past `Running`.
        let children = thread_row.active_children.clone();
        if children.is_empty() {
            let mut terminated_thread = thread_row.value();
            terminated_thread.status = ThreadStatus::Terminated(reason.clone());
            terminated_thread.meta.with_update_at(ctx.now());

            // Which parent the settled thread reports to is decided by the *type* of its owner — a
            // `ThreadOwner` sum, so each variant names its own container — never by comparing a runtime
            // kind. The container is resolved *before* `ThreadTerminated`: an ownerless settle is
            // refused while it can still be answered, rather than discovered as a no-op once the
            // terminal event is already on the log. What the settle *means* is the owner's business: a
            // root thread landing here is what starts its run's own teardown.
            match thread_row.value.meta.owner.clone() {
                ThreadOwner::Execution(execution) => {
                    let container =
                        ExecutionContainer::open(ctx.storage, execution.clone()).await?;
                    out.append_event(Event::ThreadTerminated {
                        thread: terminated_thread,
                    })
                    .await;
                    container
                        .after_child_terminated(ctx, out, &thread_ref)
                        .await?;
                }
                // A fan-out thread is owned by its container Activity; the hook runs the settle so the
                // `Parallel`/`Map` converges through the state's own decision.
                ThreadOwner::Activity(activity) => {
                    let container = ActivityContainer::open(ctx.storage, activity.clone()).await?;
                    out.append_event(Event::ThreadTerminated {
                        thread: terminated_thread,
                    })
                    .await;
                    container
                        .after_child_terminated(ctx, out, &thread_ref)
                        .await?;
                }
            }
            return Ok(());
        }

        // A thread's only child kind is `Activity`: `ActivityKind::OwnedBy` is `ObjectRef<ThreadKind>`
        // and so is the one edge that can parent an object to a thread, while `ThreadOwner` and
        // `TimerOwner` admit no thread at all and an `Execution`/`Task` is never owned by one. A nested
        // fan-out therefore reaches its teardown through the `TerminateState` below — that activity's
        // own sweep reaps its child threads, timers and tasks — never thread-sweeping-thread.
        //
        // Any other kind in `active_children` is thus an engine invariant broken — a corrupt projection
        // or a forged child edge. Refuse to guess and fail loud, rather than leave the teardown
        // deferred on a child no sweep can reap.
        for child in children.iter().cloned() {
            match child.kind {
                ObjectKind::Activity => {
                    out.append_command(Command::TerminateState(TerminateState {
                        activity: child.typed::<ActivityKind>(),
                        // Swept, not failing: this thread is going down, and its children are being
                        // taken with it — so they carry `Cancelled` rather than this thread's own
                        // `reason`. Passing the reason down would put the original error on a
                        // bystander's row and, for a state with a `Catch`, let it route a failure that
                        // was never its own.
                        reason: TerminationReason::Cancelled,
                    }));
                }
                other => panic!("engine regression: a Thread owns no {other:?} child"),
            }
        }
        tracing::debug!(
            thread = %thread_ref,
            children = children.len(),
            "thread terminating deferred: waiting on owned children"
        );

        Ok(())
    }
}
