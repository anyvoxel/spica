//! `TimerTriggered` event projection: folds the `Event::TimerTriggered` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::TimerStatus;
use crate::id::{NodeId, TimerId};

#[derive(Default)]
pub(crate) struct TimerTriggeredApplier;
#[async_trait]
impl EventApplier for TimerTriggeredApplier {
    fn event(&self) -> Event {
        Event::TimerTriggered {
            timer: crate::TimerValue {
                id: TimerId::nil(),
                parent: NodeId::Execution(crate::ExecutionId::nil()),
                purpose: crate::TimerPurpose::WaitResume,
                status: TimerStatus::Completed,
                deadline: crate::Timestamp::from_millis(0),
            },
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::TimerTriggered { timer } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut t) = ctx.storage.get_timer(timer.id).await? {
            let parent = t.value.parent;
            t.value.status = TimerStatus::Completed;
            t.touch(ctx.timestamp);
            ctx.storage.put_timer(t).await?;
            ctx.storage
                .remove_child(parent, NodeId::Timer(timer.id))
                .await?;
        }
        Ok(())
    }
}
