use crate::TimerStatus;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::event::Event;
use crate::types::meta::{ErasedOwner, ObjectReference};

/// Handles `CancelTimer`: marks an armed timer cancelled. Idempotent — a no-op for a timer that
/// already fired or was already cancelled. After recording the timer's terminal state, runs the
/// inline child-settled reaction: a cancel is often the last thing draining a Completing/Terminating
/// owner, so [`child_completed::child_settled`] lets the owner converge in the same batch.
#[derive(Default)]
pub struct CancelTimerHandler;

impl CancelTimerHandler {
    pub(crate) async fn handle(
        &self,
        timer: &ObjectReference,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let act = match ctx.storage.get_timer(timer).await? {
            Some(t) => t,
            None => return Ok(()), // never armed — nothing to cancel.
        };
        if act.value.status != TimerStatus::Active {
            // TODO：应该返回一个 Reject，而不是直接静默掉，或者再次重试
            return Ok(()); // already finished; duplicate cancel is a no-op.
        }
        let mut timer_value = act.value.clone();
        timer_value.cancel(ctx.now());
        out.append_event(Event::TimerCancelled { timer: timer_value })
            .await;
        super::child_completed::child_settled(
            ctx,
            out,
            act.value.meta.owner.clone().into_erased(),
            timer.clone(),
        )
        .await;

        Ok(())
    }
}
