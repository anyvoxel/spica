use async_trait::async_trait;

use crate::command::Command;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};

/// The side-effect handler that invokes a `Task` state's `Resource`: emits only `TaskActivated`,
/// recording the logical invocation (owning `parent` activity, `resource` URI, and projected
/// `arguments`) in the stream. The follow-on `CompleteTask` is synthesized later by the
/// [`TaskServiceHandle`](crate::task_service::TaskServiceHandle) when the external call settles
/// (driven by the persisted invocation, not a re-dispatch) — so the stream advances while the task
/// "runs" and an in-dispatch external await inside this handler can't block the cascade's serial
/// dispatch (e.g. an external `TerminateExecution` from [`crate::Engine::terminate`] must be able
/// to reach the stream while a `Task` is in flight).
#[derive(Default)]
pub struct ActivateTaskHandler;

#[async_trait]
impl CommandHandler for ActivateTaskHandler {
    fn command(&self) -> Command {
        Command::ActivateTask {
            parent: crate::id::NodeId::Activity(crate::id::ActivityId::nil()),
            task: crate::id::TaskId::nil(),
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
            parent: *parent,
            task: *task,
            resource: resource.clone(),
            arguments: arguments.clone(),
        });
    }
}
