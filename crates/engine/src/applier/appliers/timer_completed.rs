//! `TimerCompleted` event projection: folds the `Event::TimerCompleted` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{NodeId, TimerId};
use crate::storage::TimerStatus;

#[derive(Default)]
pub(crate) struct TimerCompletedApplier;
#[async_trait]
impl EventApplier for TimerCompletedApplier {
    fn event(&self) -> Event {
        Event::TimerCompleted {
            timer: TimerId::nil(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::TimerCompleted { timer } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut t) = ctx.storage.get_timer(*timer).await? {
            let parent = t.parent;
            t.status = TimerStatus::Completed;
            ctx.storage.put_timer(t).await?;
            ctx.storage
                .remove_child(parent, NodeId::Timer(*timer))
                .await?;
        }
        Ok(())
    }
}
