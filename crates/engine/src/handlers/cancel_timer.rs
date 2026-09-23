use crate::TimerStatus;
use crate::handler::{Collector, HandlerContext};
use crate::types::event::Event;
use crate::types::meta::ObjectReference;

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
    ) {
        let act = match ctx.storage.get_timer(timer).await {
            Ok(Some(t)) => t,
            Ok(None) | Err(_) => return,
        };
        if act.value.status != TimerStatus::Active {
            return; // already finished; duplicate cancel is a no-op.
        }
        out.append_event(Event::TimerCancelled {
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
                    m.with_update_at(crate::log::Timestamp::now());
                    m
                },
            },
        })
        .await;
        super::child_completed::child_settled(
            ctx,
            out,
            act.value
                .meta
                .owner
                .clone()
                .expect("a live timer is always owned"),
            timer.clone(),
        )
        .await;
    }
}
