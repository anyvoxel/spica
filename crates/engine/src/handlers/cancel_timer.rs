use async_trait::async_trait;

use crate::TimerStatus;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::types::command::Command;
use crate::types::event::Event;

/// Handles `CancelTimer`: marks an armed timer cancelled. Idempotent — a no-op for a timer that
/// already fired or was already cancelled. After recording the timer's terminal state, notifies
/// the owner: a cancel is often the last thing draining a Completing/Terminating owner, so the
/// [`ProcessChildCompleted`] notice lets the owner's own handler emit its deferred ed.
#[derive(Default)]
pub struct CancelTimerHandler;

#[async_trait]
impl CommandHandler for CancelTimerHandler {
    fn command(&self) -> Command {
        Command::CancelTimer {
            timer: crate::types::meta::ObjectReference::nil(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::CancelTimer { timer } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        let act = match ctx.storage.get_timer(timer).await {
            Ok(Some(t)) => t,
            Ok(None) | Err(_) => return,
        };
        if act.value.status != TimerStatus::Active {
            return; // already finished; duplicate cancel is a no-op.
        }
        out.emit_event(Event::TimerCancelled {
            timer: crate::Timer {
                execution: act.value.execution.clone(),
                purpose: act.value.purpose,
                status: crate::TimerStatus::Cancelled,
                deadline: act.value.deadline,
                // Carry the timer's full meta (name/uid/created_at/owner) forward. A timer may be
                // custom-named (`{execution.name}-{suffix}`); reconstructing it via
                // `placeholder_with_times` would re-derive `obj-<uid>` and break the child-edge
                // removal. Stamp the cancel moment as `updated_at`.
                meta: {
                    let mut m = act.value.meta.clone();
                    m.touch(crate::log::Timestamp::now());
                    m
                },
            },
        });
        out.emit_command(Command::ProcessChildCompleted {
            parent: act
                .value
                .meta
                .owner
                .clone()
                .expect("a live timer is always owned"),
            child: timer.clone(),
        });
    }
}
