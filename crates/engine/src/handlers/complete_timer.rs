use async_trait::async_trait;

use crate::command::{Command, TerminationReason, TimerPurpose};
use crate::error::ExecutionError;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::id::NodeId;
use crate::storage::TimerStatus;

/// Handles `CompleteTimer`: a timer's deadline elapsed. Idempotent (a no-op if the timer is gone or
/// already terminal). Dispatches by `purpose`: `WaitResume` fires the owning state;
/// `ExecutionTimeout` terminates the owning execution `TimedOut`.
#[derive(Default)]
pub struct CompleteTimerHandler;

#[async_trait]
impl CommandHandler for CompleteTimerHandler {
    fn command(&self) -> Command {
        Command::CompleteTimer {
            timer: crate::id::TimerId::nil(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::CompleteTimer { timer } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };

        let act = match ctx.storage.get_timer(*timer).await {
            Ok(Some(t)) => t,
            Ok(None) | Err(_) => return, // timer never armed; nothing to do.
        };
        if act.status != TimerStatus::Active {
            return; // already completed/cancelled — a duplicate fire is a no-op.
        }

        out.emit_event(Event::TimerCompleted { timer: *timer });

        match act.purpose {
            TimerPurpose::WaitResume => {
                // Resume the owning state: the activity's `CompleteState` runs its `complete`.
                let activity_id = match act.parent {
                    NodeId::Activity(a) => a,
                    _ => return, // a Wait timer without an activity owner is an internal fault.
                };
                out.emit_command(Command::CompleteState {
                    activity: activity_id,
                });
            }
            TimerPurpose::ExecutionTimeout => {
                // The execution ran past its `TimeoutSeconds` deadline. Drive it to a `TimedOut`
                // termination; any in-flight children drain via the cascade started by
                // `TerminateExecution`.
                let execution_id = match act.parent {
                    NodeId::Execution(e) => e,
                    _ => return,
                };
                out.emit_command(Command::TerminateExecution {
                    id: execution_id,
                    reason: TerminationReason::Failed {
                        error: ExecutionError::TimedOut {
                            message: format!(
                                "execution ran past its TimeoutSeconds deadline ({})",
                                act.deadline.as_millis()
                            ),
                        },
                    },
                });
            }
        }
    }
}
