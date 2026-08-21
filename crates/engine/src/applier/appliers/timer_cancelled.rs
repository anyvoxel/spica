//! `TimerCancelled` event projection: folds the `Event::TimerCancelled` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::TimerStatus;
use crate::id::{NodeId, TimerId};

/// `TimerCancelled` folds the terminal status into Storage and deschedules the pending deadline.
#[derive(Default)]
pub(crate) struct TimerCancelledApplier;
#[async_trait]
impl EventApplier for TimerCancelledApplier {
    fn event(&self) -> Event {
        Event::TimerCancelled {
            timer: crate::TimerValue {
                id: TimerId::nil(),
                parent: NodeId::Execution(crate::ExecutionId::nil()),
                purpose: crate::TimerPurpose::WaitResume,
                status: TimerStatus::Cancelled,
                deadline: crate::Timestamp::from_millis(0),
            },
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
        ctx.scheduler.cancel(timer.id);
        if let Some(mut t) = ctx.storage.get_timer(timer.id).await? {
            let parent = t.value.parent;
            t.value.status = TimerStatus::Cancelled;
            t.touch(ctx.timestamp);
            ctx.storage.put_timer(t).await?;
            ctx.storage
                .remove_child(parent, NodeId::Timer(timer.id))
                .await?;
        }
        Ok(())
    }
}
