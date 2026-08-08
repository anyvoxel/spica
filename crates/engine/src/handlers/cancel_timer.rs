use async_trait::async_trait;

use crate::command::Command;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::storage::TimerStatus;

/// Handles `CancelTimer`: marks an armed timer cancelled. Idempotent — a no-op for a timer that
/// already fired or was already cancelled. After recording the timer's terminal state, notifies
/// the owner: a cancel is often the last thing draining a Completing/Terminating owner, so the
/// [`ChildFinalized`] notice lets the owner's own handler emit its deferred ed.
#[derive(Default)]
pub struct CancelTimerHandler;

#[async_trait]
impl CommandHandler for CancelTimerHandler {
    fn command(&self) -> Command {
        Command::CancelTimer {
            timer: crate::id::TimerId::nil(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::CancelTimer { timer } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        let act = match ctx.storage.get_timer(*timer).await {
            Ok(Some(t)) => t,
            Ok(None) | Err(_) => return,
        };
        if act.status != TimerStatus::Active {
            return; // already finished; duplicate cancel is a no-op.
        }
        out.emit_event(Event::TimerCancelled { timer: *timer });
        out.emit_command(Command::ChildFinalized {
            parent: act.parent,
            child: crate::id::NodeId::Timer(*timer),
        });
    }
}
