use async_trait::async_trait;

use crate::command::Command;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};

/// The side-effect handler that arms a timer: emits only `TimerActivated`, recording the logical
/// arm (with its **absolute `deadline`**) in the stream. The follow-on `CompleteTimer` is
/// synthesized later by the [`SchedulerHandle`](crate::scheduler::SchedulerHandle) when the
/// deadline passes (driven by the persisted `deadline`, not a fresh relative count) — so the
/// stream advances while the timer "runs" and an in-line sleep inside this handler can't block the
/// cascade's serial dispatch (e.g. an external `TerminateExecution` from
/// [`crate::Engine::terminate`] must be able to reach the stream while a `Wait`'s timer is armed).
#[derive(Default)]
pub struct ActivateTimerHandler;

#[async_trait]
impl CommandHandler for ActivateTimerHandler {
    fn command(&self) -> Command {
        Command::ActivateTimer {
            parent: crate::id::NodeId::Execution(crate::id::ExecutionId::nil()),
            timer: crate::id::TimerId::nil(),
            purpose: crate::command::TimerPurpose::WaitResume,
            deadline: crate::log::Timestamp::from_millis(0),
        }
    }

    async fn handle(&self, cmd: &Command, _ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::ActivateTimer {
            parent,
            timer,
            purpose,
            deadline,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        out.emit_event(Event::TimerActivated {
            parent: *parent,
            timer: *timer,
            purpose: *purpose,
            deadline: *deadline,
        });
    }
}
