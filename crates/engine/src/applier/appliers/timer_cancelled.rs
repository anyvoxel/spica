//! `TimerCancelled` event projection: folds the `Event::TimerCancelled` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::TimerStatus;

/// `TimerCancelled` folds the terminal status into Storage and deschedules the pending deadline.
#[derive(Default)]
pub(crate) struct TimerCancelledApplier;
#[async_trait]
impl EventApplier for TimerCancelledApplier {
    fn event(&self) -> Event {
        Event::TimerCancelled {
            timer: crate::Timer {
                execution: crate::types::meta::ObjectReference::nil(),
                purpose: crate::TimerPurpose::WaitResume,
                status: TimerStatus::Cancelled,
                deadline: crate::Timestamp::from_millis(0),
                meta: crate::types::meta::ObjectMeta::placeholder_with_times(
                    crate::types::meta::ObjectKind::Timer,
                    ulid::Ulid::nil(),
                    crate::Timestamp::from_millis(0),
                    crate::Timestamp::from_millis(0),
                ),
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
        ctx.scheduler.cancel(&timer.reference());
        if let Some(mut t) = ctx.storage.get_timer(&timer.reference()).await? {
            let parent = t
                .value
                .meta
                .owner
                .clone()
                .expect("an owned timer always has an owner");
            t.value.status = TimerStatus::Cancelled;
            // Sync the domain value's transition stamp from the event (see timer_triggered.rs).
            t.value.meta.touch(timer.meta.updated_at);
            t.touch(ctx.timestamp);
            ctx.storage.put_timer(t).await?;
            ctx.storage.remove_child(parent, timer.reference()).await?;
        }
        Ok(())
    }
}
