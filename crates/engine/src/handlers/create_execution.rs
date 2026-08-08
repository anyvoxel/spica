use async_trait::async_trait;

use crate::command::{Command, TimerPurpose};
use crate::error::ExecutionError;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::id::NodeId;
use crate::log::Timestamp;

/// Handles `CreateExecution`: records the execution (via `ExecutionCreated`) and starts it. Also
/// arms the state-machine `TimeoutSeconds` timer if configured. Immediately enters the start state
/// via `ActivateState`.
#[derive(Default)]
pub struct CreateExecutionHandler;

#[async_trait]
impl CommandHandler for CreateExecutionHandler {
    fn command(&self) -> Command {
        Command::CreateExecution {
            id: crate::id::ExecutionId::nil(),
            input: Default::default(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::CreateExecution { id, input } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        out.emit_event(Event::ExecutionCreated {
            id: *id,
            input: input.clone(),
        });

        if let Some(secs) = ctx.sm.timeout_seconds
            && secs > 0
        {
            let timer = out.next_timer();
            // Normalize the relative TimeoutSeconds into an absolute deadline at activation, so the
            // persisted TimerActivated fact carries the wall-clock moment the execution must finish by.
            let deadline =
                Timestamp::now().checked_add(std::time::Duration::from_secs(secs as u64));
            let Some(deadline) = deadline else {
                out.fail_execution(
                    *id,
                    ExecutionError::InvalidDefinition(
                        "TimeoutSeconds overflows the absolute deadline".into(),
                    ),
                );
                return;
            };
            out.emit_command(Command::ActivateTimer {
                parent: NodeId::Execution(*id),
                timer,
                purpose: TimerPurpose::ExecutionTimeout,
                deadline,
            });
        }

        let start = ctx.sm.start_at.clone();
        let activity = out.next_activity();
        out.emit_command(Command::ActivateState {
            execution: *id,
            activity,
            state: start,
            input: input.clone(),
        });
    }
}
