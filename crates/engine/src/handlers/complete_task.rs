use async_trait::async_trait;
use serde_json::Value;

use crate::TaskStatus;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::types::command::Command;
use crate::types::event::Event;
use crate::types::id::RequestId;
use crate::types::meta::ObjectKind;
use crate::types::reject::RejectionType;

/// Handles `CompleteTask`: a worker reported its claimed task **completed** (Zeebe `CompleteJob`).
///
/// A request/**response** boundary (mirroring `CreateFlow`/`CreateExecution`): the worker mints a
/// `request_id` and the outcome is echoed back through it — the applied [`Event::TaskCompleted`] on
/// success (via `ack_request`), or a [`Reject`](crate::Reject) when the settlement guard refuses
/// (via `Collector::reject`). Without this a `CompleteTask` would be fire-and-forget; with it,
/// `TaskApi::complete` awaits and reports the *actual* processing result to the completing worker.
///
/// Idempotent and lease-guarded: the authoritative settlement guard is that the task is currently
/// `Running` (leased) **to the reporting `worker_id`**. A late `CompleteTask` after a cancel, a
/// timeout, or a lease that already expired (and was re-leased to another worker) is rejected as a
/// `Reject`, so the state advances exactly once even under Zeebe's at-least-once re-claims — and the
/// rejected worker is told *why* instead of hitting a silent no-op.
///
/// On success it emits `TaskCompleted` and resumes the owning state via `CompleteState` (its
/// `complete` runs the success projection and routes to `Next`/`End`). A *failed* settlement belongs
/// to `FailTaskHandler` (`Command::FailTask`), which owns the `Retry`/`Catch`/terminate policy.
#[derive(Default)]
pub struct CompleteTaskHandler;

#[async_trait]
impl CommandHandler for CompleteTaskHandler {
    fn command(&self) -> Command {
        Command::CompleteTask {
            request_id: RequestId::nil(),
            task: crate::types::meta::ObjectReference::nil(),
            worker_id: String::new(),
            output: Value::Null,
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::CompleteTask {
            request_id,
            task,
            worker_id,
            output,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };

        let act = match ctx.storage.get_task(task).await {
            Ok(Some(t)) => t,
            // A task that never existed in scope is a genuine refusal, not a silent no-op: the
            // awaiting worker must learn it settled nothing rather than hang on an unmatchable ack.
            Ok(None) => {
                out.reject(
                    *request_id,
                    RejectionType::NotFound,
                    format!("task {task} not found or not activated"),
                );
                return;
            }
            Err(_) => return, // storage fault — not a domain decision; surface nothing.
        };
        // Settlement guard: must be leased to the reporting worker right now. A task not currently
        // Running (still Pending, or already settled) is a wrong-state refusal; one leased to a
        // *different* worker is a concurrent-conflict refusal. Both become a `Reject` so the worker
        // is told why, and both keep the state advancing exactly once (duplicate/foreign settles are
        // refused, never applied twice).
        if !act.status.is_running() {
            out.reject(
                *request_id,
                RejectionType::InvalidState,
                format!(
                    "task {} is not currently Running (status {:?}); settlement refused",
                    act.value.reference(),
                    act.status
                ),
            );
            return;
        }
        if act.worker_id.as_deref() != Some(worker_id.as_str()) || worker_id.is_empty() {
            tracing::warn!(
                task = %act.value.reference(),
                reported = %worker_id,
                leased = ?act.worker_id,
                "worker tried to complete a task it does not lease; report rejected"
            );
            out.reject(
                *request_id,
                RejectionType::StateConflict,
                format!(
                    "task {} is leased to {:?}, not {worker_id}; settlement refused",
                    act.value.reference(),
                    act.worker_id,
                ),
            );
            return;
        }

        let activity_id = act
            .meta
            .owner
            .clone()
            .expect("a completed task is always owned by an activity");
        if activity_id.kind != ObjectKind::Activity {
            out.reject(
                *request_id,
                RejectionType::ProcessingError,
                format!("task {} has no activity owner; internal fault", task),
            );
            return;
        }

        // Emit the completed task entity (lease cleared, status terminal) and resume the owning Task
        // state's `complete`. The concrete output travels alongside (feeds the activity's raw_output).
        let mut task_value = act.value();
        task_value.status = TaskStatus::Completed;
        task_value.worker_id = None;
        task_value.lease_until = None;
        // Stamp the completion moment; `created_at` is already carried on `task_value`.
        task_value.meta.touch(crate::log::Timestamp::now());
        let completed = Event::TaskCompleted {
            request_id: *request_id,
            task: task_value,
            output: output.clone(),
        };
        // The success settlement echoes the worker's own request id back as the ack — the request/response
        // contract that lets the awaiting `TaskApi::complete` report that this settlement was applied.
        out.ack_request(*request_id, completed.clone());
        out.emit_event(completed);
        // Sweep the activity's task timers (the `TaskLease` armed on assign, and any `TaskTimeout`)
        // before `CompleteState`: the M1 activity-completion guard refuses to finish an activity that
        // still has live children, and a settled task must leave none behind. Fired-since timers are
        // no longer active children and are simply absent.
        super::cancel_activity_timers(ctx, out, activity_id.clone()).await;
        out.emit_command(Command::CompleteState {
            activity: activity_id,
            // The Task's raw result is the worker's payload (its `raw_output` / `$states.result`).
            // Carrying it on the command makes the complete step self-contained and the log
            // self-describing, independent of the `TaskCompleted` projection fold.
            output: output.clone(),
        });
    }
}
