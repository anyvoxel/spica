use async_trait::async_trait;

use crate::RetryState;
use crate::Task;
use crate::TaskStatus;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::types::command::Command;
use crate::types::event::Event;

/// The side-effect handler that invokes a `Task` state's `Resource`: emits only `TaskActivated`,
/// recording the logical invocation (owning `parent` activity, `resource` URI, and projected
/// `arguments`) in the stream. The follow-on `CompleteTask` is synthesized later by a worker's
/// [`TaskApi::poll_tasks`](crate::task_api::TaskApi::poll_tasks) when the external call settles — so the
/// stream advances while the task "runs" and an in-dispatch external await inside this handler
/// can't block the cascade's serial dispatch (e.g. an external `TerminateExecution` from
/// [`crate::Engine::cancel_execution`] must be able to reach the stream while a `Task` is in flight).
///
/// The task is simply made claimable (`Pending`): a worker that serves `resource` will pull it via
/// `TaskApi::poll_tasks`; an unserved task stays queued (Zeebe semantics) and, if the state sets
/// `TimeoutSeconds`, is eventually failed by the `TaskTimeout` backstop. No worker-presence
/// prediction happens here — a worker may appear at any time and claim the task.
#[derive(Default)]
pub struct ActivateTaskHandler;

#[async_trait]
impl CommandHandler for ActivateTaskHandler {
    fn command(&self) -> Command {
        Command::ActivateTask {
            execution: crate::types::meta::ObjectReference::nil(),
            owner: crate::types::meta::ObjectReference::nil(),
            task: crate::types::meta::ObjectReference::nil(),
            resource: String::new(),
            arguments: Default::default(),
            retry_plan: Vec::new(),
        }
    }

    async fn handle(&self, cmd: &Command, _ctx: &mut HandlerContext<'_>, out: &mut Collector<'_>) {
        let Command::ActivateTask {
            execution,
            owner,
            task,
            resource,
            arguments,
            retry_plan,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        out.emit_event(Event::TaskActivated {
            task: Task {
                // The execution anchor is carried from the command (finding #13), so the task is
                // traceable to / nameable from its root run even in a branch.
                execution: execution.clone(),
                resource: resource.clone(),
                arguments: arguments.clone(),
                status: TaskStatus::Pending,
                deadline: None,
                worker_id: None,
                lease_until: None,
                retry_plan: retry_plan.clone(),
                retry_state: RetryState::default(),
                // Birth: `created_at == updated_at == now` (invocation moment). The `meta.name` is
                // carried from the command's task reference (already execution-based per finding
                // #13), not re-derived as `child-<uid>`; the owner is the invoking activity carried
                // on the command, converted to the meta owner reference.
                meta: crate::types::meta::ObjectMeta::builder(
                    crate::types::meta::ObjectKind::Task,
                    task.uid,
                )
                .name(task.name.clone())
                .at(crate::log::Timestamp::now())
                .build()
                .with_owner(owner.clone()),
            },
        })
        .await;
    }
}
