use crate::RejectionType;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::handlers::container::{ActivityContainer, Container};
use crate::types::command::CompleteTask;
use crate::types::event::{Event, TaskCompleted};
use crate::types::meta::HasRawObjectRef;

/// Handles `CompleteTask`: a worker reported its claimed task **completed** (Zeebe `CompleteJob`).
///
/// A request/**response** boundary (mirroring `CreateFlow`/`CreateExecution`): the worker mints a
/// `request_id` and the outcome is echoed back through it — the applied [`Event::TaskCompleted`] on
/// success (an injected `Hook` observer wakes the awaiter), or a [`Reject`](crate::Reject) when the
/// settlement guard refuses (via `Collector::reject`). Without this a `CompleteTask` would be
/// fire-and-forget; with it, `TaskApi::complete` awaits and reports the *actual* processing result
/// to the completing worker.
///
/// Idempotent and lease-guarded: the authoritative settlement guard is that the task is currently
/// `Running` (leased) **to the reporting `worker_id`**. A late `CompleteTask` after a cancel, a
/// timeout, or a lease that already expired (and was re-leased to another worker) is rejected as a
/// `Reject`, so the state advances exactly once even under Zeebe's at-least-once re-claims — and the
/// rejected worker is told *why* instead of hitting a silent no-op.
///
/// On success it emits `TaskCompleted` and hands the settle to the owning activity's container, which
/// resumes the state via `CompleteState` (its `complete` runs the success projection and routes to
/// `Next`/`End`); that container is resolved *before* the event is emitted, so a task whose owning
/// activity is gone is refused rather than logged as a settle nothing can resume. A *failed*
/// settlement belongs to `FailTaskHandler` (`Command::FailTask`), which owns the
/// `Retry`/`Catch`/terminate policy.
#[derive(Default)]
pub struct CompleteTaskHandler;

impl CompleteTaskHandler {
    pub(crate) async fn handle(
        &self,
        p: &CompleteTask,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let CompleteTask {
            request_id,
            task,
            worker_id,
            output,
        } = p;

        let act = match ctx.storage.get_task(task).await {
            Ok(Some(t)) => t,
            // A task that never existed in scope is a genuine refusal, not a silent no-op: the
            // awaiting worker must learn it settled nothing rather than hang on an unmatchable ack.
            Ok(None) => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("task {task} not found or not activated"),
                ));
            }
            // A read fault is the engine's, not the command's: returned so the leader can retry it
            // rather than answering the awaiting worker with a silent nothing.
            Err(e) => return Err(e.into()),
        };
        // Settlement guard: only a `Running` (leased) task has a settlement to take, and it must be
        // leased to the reporting worker right now. The whole guard lives in the transition itself
        // (`mark_completed`), so the caller cannot honor one half and forget the other: a task not
        // currently Running (still Pending, or already settled) and one leased to a *different* worker
        // are both refusals of the request's precondition — the caller's handle on the task is not the
        // live one — so both are `InvalidState`, told apart by their reason. Both keep the state
        // advancing exactly once (duplicate/foreign settles are refused, never applied twice).
        let mut task_value = act.value();
        if let Err(reason) = task_value.mark_completed(worker_id, ctx.now()) {
            tracing::warn!(
                task = %act.value.meta.raw_object_ref(),
                reported = %worker_id,
                leased = ?act.worker_id,
                "task settlement refused: {reason}"
            );
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("task {task} cannot be completed: {reason}"),
            ));
        }

        let activity_id = act.meta.owner.clone();
        // The container is resolved *before* anything is emitted: a task's settle has no meaning apart
        // from the activity it resumes, so an ownerless settle is refused by the container's own `open`
        // (`NotFound`, naming the gone owner) — while the worker is still waiting on an answer — rather
        // than discovered as a no-op after `TaskCompleted` is already on the log, which would strand the
        // owning state with nothing left to resume it. Deliberately neither `ProcessingError` — which
        // the log reserves for a command that outlived its retry budget — nor `Unexpected`, whose retry
        // could only repeat this conclusion later: a row is never removed, so the owning activity cannot
        // come back, and the worker is blocked on the ack.
        let container = ActivityContainer::open(ctx.storage, activity_id.clone()).await?;

        // Emit the completed task entity (lease cleared, status terminal) — the very value the
        // transition above advanced — and resume the owning Task state's `complete`. The concrete
        // output travels alongside (feeds the activity's raw_output). The success settlement echoes
        // the worker's own request id back on the event; the `AckHook` observer wakes the awaiting
        // `TaskApi::complete` with it (the request/response contract that lets it report this
        // settlement was applied).
        out.append_event(Event::TaskCompleted(TaskCompleted {
            request_id: *request_id,
            task: task_value,
            output: output.clone(),
        }))
        .await;
        // The activity's own `TimeoutSeconds` child needs no sweep here: the Task state declares it
        // supervisory, so the base complete step cancels it before it finishes. (A claim leaves no
        // child at all — the delivery lease is a field on the task, not a timer.)
        //
        // The settle is handed to the activity that owns the task rather than acted on here: what a
        // settled task means for its owner is the owner's business, so the child only names its owner
        // and the owner's container decides.
        container
            .after_child_completed(ctx, out, task.as_raw_object_ref())
            .await?;

        Ok(())
    }
}
