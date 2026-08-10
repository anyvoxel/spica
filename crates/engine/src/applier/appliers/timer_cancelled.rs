//! `TimerCancelled` event projection: folds the `Event::TimerCancelled` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{NodeId, TimerId};
use crate::storage::TimerStatus;

/// `TimerCancelled` folds the terminal status into Storage and deschedules the pending deadline.
#[derive(Default)]
pub(crate) struct TimerCancelledApplier;
#[async_trait]
impl EventApplier for TimerCancelledApplier {
    fn event(&self) -> Event {
        Event::TimerCancelled {
            timer: TimerId::nil(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::TimerCancelled { timer } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        ctx.scheduler.cancel(*timer);
        if let Some(mut t) = ctx.storage.get_timer(*timer).await? {
            let parent = t.parent;
            t.status = TimerStatus::Cancelled;
            ctx.storage.put_timer(t).await?;
            ctx.storage
                .remove_child(parent, NodeId::Timer(*timer))
                .await?;
        }
        Ok(())
    }
}
