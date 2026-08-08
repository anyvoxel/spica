//! `TimerActivated` event projection: folds the `Event::TimerActivated` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::command::TimerPurpose;
use crate::id::{ExecutionId, NodeId, TimerId};
use crate::log::Timestamp;
use crate::storage::TimerStatus;

/// `TimerActivated` folds the timer row into Storage **and** arms the physical deadline in the
/// scheduler. The durable stream carries the logical "armed" fact plus its absolute `deadline`;
/// the scheduler derives the wall-clock wait (`deadline - now`) from it. A replayed projection
/// re-derives the same wait from the persisted absolute moment rather than a fresh relative count.
#[derive(Default)]
pub(crate) struct TimerActivatedApplier;
#[async_trait]
impl EventApplier for TimerActivatedApplier {
    fn event(&self) -> Event {
        Event::TimerActivated {
            parent: NodeId::Execution(ExecutionId::nil()),
            timer: TimerId::nil(),
            purpose: TimerPurpose::WaitResume,
            deadline: Timestamp::from_millis(0),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::TimerActivated {
            parent,
            timer,
            purpose,
            deadline,
        } = event
        else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        ctx.storage
            .put_timer(crate::storage::Timer {
                id: *timer,
                parent: *parent,
                purpose: *purpose,
                status: TimerStatus::Active,
                deadline: *deadline,
            })
            .await?;
        ctx.storage
            .add_child(*parent, NodeId::Timer(*timer))
            .await?;
        // Schedule the physical deadline (the storage fold is pure; this is the side effect).
        // The scheduler needs the owning entry's stream/cause identity to re-envelope the
        // `CompleteTimer` it fires on expiry; the Processor supplies these via the context.
        // The wait duration is derived from the persisted absolute `deadline`: already-past
        // fire immediately (saturating to zero).
        ctx.scheduler
            .schedule(*timer, *deadline, ctx.stream_id, ctx.cause_id);
        Ok(())
    }
}
