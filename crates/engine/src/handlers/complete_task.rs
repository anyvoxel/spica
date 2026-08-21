use async_trait::async_trait;
use serde_json::Value;

use crate::TaskStatus;
use crate::command::Command;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::id::NodeId;

/// Handles `CompleteTask`: a worker reported its claimed task **completed** (Zeebe `CompleteJob`).
///
/// Idempotent and lease-guarded: a no-op unless the task is currently `Running` (leased) **to the
/// reporting `worker_id`**. This is the authoritative settlement guard — a late `CompleteTask` after
/// a cancel, a timeout, or a lease that already expired (and was re-leased to another worker) is
/// rejected, so the state advances exactly once even under Zeebe's at-least-once re-claims.
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
            task: crate::id::TaskId::nil(),
            worker_id: String::new(),
            output: Value::Null,
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::CompleteTask {
            task,
            worker_id,
            output,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };

        let act = match ctx.storage.get_task(*task).await {
            Ok(Some(t)) => t,
            Ok(None) | Err(_) => return, // task never activated; nothing to do.
        };
        // Settlement guard: must be leased to the reporting worker right now. Anything else — a task
        // not yet claimed, one re-leased after expiry, or already settled — is a duplicate/foreign
        // report and drops (no-op).
        if !act.status.is_running() {
            return;
        }
        if act.worker_id.as_deref() != Some(worker_id.as_str()) || worker_id.is_empty() {
            tracing::warn!(
                task = %act.value.id,
                reported = %worker_id,
                leased = ?act.worker_id,
                "worker tried to complete a task it does not lease; report rejected"
            );
            return;
        }

        let activity_id = match act.parent {
            NodeId::Activity(a) => a,
            _ => return, // a task without an activity owner is an internal fault.
        };

        // Emit the completed task entity (lease cleared, status terminal) and resume the owning Task
        // state's `complete`. The concrete output travels alongside (feeds the activity's raw_output).
        let mut task_value = act.value();
        task_value.status = TaskStatus::Completed;
        task_value.worker_id = None;
        task_value.lease_until = None;
        out.emit_event(Event::TaskCompleted {
            task: task_value,
            output: output.clone(),
        });
        // Sweep the activity's task timers (the `TaskLease` armed on assign, and any `TaskTimeout`)
        // before `CompleteState`: the M1 activity-completion guard refuses to finish an activity that
        // still has live children, and a settled task must leave none behind. Fired-since timers are
        // no longer active children and are simply absent.
        super::cancel_activity_timers(ctx, out, activity_id).await;
        out.emit_command(Command::CompleteState {
            activity: activity_id,
        });
    }
}
