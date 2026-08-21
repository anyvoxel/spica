use async_trait::async_trait;

use crate::TaskStatus;
use crate::command::Command;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};

/// Handles `CancelTask`: an in-flight `Task` is cancelled because its owning activity/execution is
/// being torn down. Emits only `TaskCancelled`, which marks the task `Cancelled` in storage and
/// drains it from its parent. The physical call is deliberately left running (see the
/// [`TaskService`](crate::task_service::TaskService) docs); a later `CompleteTask` for
/// this task is swallowed by the `CompleteTaskHandler`'s non-`Running` guard — exactly the race
/// guard a `CancelTimer` + late `TriggerTimer` already uses.
#[derive(Default)]
pub struct CancelTaskHandler;

#[async_trait]
impl CommandHandler for CancelTaskHandler {
    fn command(&self) -> Command {
        Command::CancelTask {
            task: crate::id::TaskId::nil(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::CancelTask { task } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        let Some(task_value) = ctx
            .storage
            .get_task(*task)
            .await
            .ok()
            .flatten()
            .map(|task| task.value())
        else {
            return;
        };
        out.emit_event(Event::TaskCancelled {
            task: crate::TaskValue {
                status: TaskStatus::Cancelled,
                ..task_value
            },
        });
    }
}
