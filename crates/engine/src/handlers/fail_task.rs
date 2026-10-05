use crate::RejectionType;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::{Command, FailTask, TerminateState, TerminationReason};
use crate::types::event::{Event, TaskFailed};
use crate::types::task::FailureOutcome;

/// Handles `FailTask`: a claimed task was reported **failed** (Zeebe `FailJob`). A state's own
/// `TimeoutSeconds` does not take this route — a deadline is the state's failure, not the task's, so it
/// terminates the state itself (`TriggerTimerHandler`).
///
/// The handler is only orchestration: the task decides its own next life
/// ([`Task::handle_failure`](crate::Task::handle_failure) — re-queued for a retry, or terminally
/// failed, with the lease guard that admits the report), and a terminal failure is then handed to the
/// owning activity's **own** terminate flow, where the state's `Catch` policy gets its say
/// ([`StateHandler::on_failed`](super::state_handler::StateHandler::on_failed)). A retry notifies
/// nothing: the task is simply claimable again past its backoff gate.
#[derive(Default)]
pub struct FailTaskHandler;

impl FailTaskHandler {
    pub(crate) async fn handle(
        &self,
        p: &FailTask,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let FailTask {
            task,
            worker_id,
            error,
        } = p;

        let act = match ctx.storage.get_task(task).await? {
            Some(t) => t,
            // The task a report names is born in the batch that activates it and nothing ever removes a
            // row, so a miss means the report was forged into the log or the projection is corrupt — the
            // command's own precondition. A task that never activated leaves nothing to fail, and the
            // refusal is the entry this command owes either way.
            None => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("fail_task: task {task} does not exist; failure refused"),
                ));
            }
        };

        // The one transition this command performs, with its whole precondition (settled once, and
        // leased to the reporter) inside it — so a refused report writes nothing.
        let mut task_value = act.value();
        let outcome = match task_value.handle_failure(worker_id, error, ctx.now()) {
            Ok(outcome) => outcome,
            Err(why) => {
                tracing::warn!(
                    task = %act.value.meta.raw_object_ref(),
                    reported = %worker_id,
                    status = ?act.status,
                    "failure reported against a task that cannot take it; refused: {why}"
                );
                return Err(ProcessingError::Rejected(
                    RejectionType::InvalidState,
                    format!(
                        "fail_task: task {} cannot take the failure reported by {worker_id}: {why}",
                        act.value.meta.raw_object_ref()
                    ),
                ));
            }
        };
        out.append_event(Event::TaskFailed(TaskFailed {
            task: task_value,
            error: error.clone(),
        }))
        .await;

        // A retry needs nothing more: the task is claimable again past its backoff gate, and whoever
        // re-claims it runs the next attempt. Only an exhausted failure has to reach the state, whose
        // own terminate decides `Catch`-versus-fail.
        if outcome == FailureOutcome::Terminal {
            // The slot is an `ObjectRef<ActivityKind>`, so "the owner is an activity" is a type fact,
            // not a runtime check — the task's own owner names the activity it fails.
            out.append_command(Command::TerminateState(TerminateState {
                activity: act.meta.owner.clone(),
                reason: TerminationReason::Failed {
                    error: error.clone(),
                },
            }));
        }

        Ok(())
    }
}
