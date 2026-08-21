use async_trait::async_trait;

use crate::TaskStatus;
use crate::TaskValue;
use crate::command::Command;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::id::{NodeId, TaskId};

/// The side-effect handler that invokes a `Task` state's `Resource`: emits only `TaskActivated`,
/// recording the logical invocation (owning `parent` activity, `resource` URI, and projected
/// `arguments`) in the stream. The follow-on `CompleteTask` is synthesized later by a worker's
/// [`TaskApi::activate`](crate::job_api::TaskApi::activate) when the external call settles — so the
/// stream advances while the task "runs" and an in-dispatch external await inside this handler
/// can't block the cascade's serial dispatch (e.g. an external `TerminateExecution` from
/// [`crate::Engine::cancel_execution`] must be able to reach the stream while a `Task` is in flight).
///
/// The task is simply made claimable (`Pending`): a worker that serves `resource` will pull it via
/// `TaskApi::activate`; an unserved task stays queued (Zeebe semantics) and, if the state sets
/// `TimeoutSeconds`, is eventually failed by the `TaskTimeout` backstop. No worker-presence
/// prediction happens here — a worker may appear at any time and claim the task.
#[derive(Default)]
pub struct ActivateTaskHandler;

#[async_trait]
impl CommandHandler for ActivateTaskHandler {
    fn command(&self) -> Command {
        Command::ActivateTask {
            parent: NodeId::Activity(crate::id::ActivityId::nil()),
            task: TaskId::nil(),
            resource: String::new(),
            arguments: Default::default(),
        }
    }

    async fn handle(&self, cmd: &Command, _ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::ActivateTask {
            parent,
            task,
            resource,
            arguments,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        out.emit_event(Event::TaskActivated {
            task: TaskValue {
                id: *task,
                parent: *parent,
                resource: resource.clone(),
                arguments: arguments.clone(),
                status: TaskStatus::Pending,
                deadline: None,
                worker_id: None,
                lease_until: None,
            },
        });
    }
}
