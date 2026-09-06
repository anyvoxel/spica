//! `TimerCancelled` event projection: folds the `Event::TimerCancelled` into Storage. The physical
//! descheduling is not folded here — a consumer re-derives `cancel` from the durable event via the
//! injected `Hook`.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::TimerStatus;

/// `TimerCancelled` folds the terminal status into Storage.
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
                meta: crate::types::meta::ObjectMeta::builder(
                    crate::types::meta::ObjectKind::Timer,
                    ulid::Ulid::nil(),
                )
                .timestamps(
                    crate::Timestamp::from_millis(0),
                    crate::Timestamp::from_millis(0),
                )
                .build(),
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
        if let Some(mut t) = ctx.storage.get_timer(&timer.reference()).await? {
            let parent = t
                .value
                .meta
                .owner
                .clone()
                .expect("an owned timer always has an owner");
            t.value.status = TimerStatus::Cancelled;
            // Sync the domain value's transition stamp from the event (see timer_triggered.rs).
            t.value.meta.with_update_at(timer.meta.updated_at);
            t.with_update_at(ctx.timestamp);
            ctx.storage.put_timer(t).await?;
            ctx.storage.remove_child(parent, timer.reference()).await?;
        }
        // The physical descheduling is not folded here — a consumer re-derives `cancel` from the
        // durable `TimerCancelled` event once it is committed.
        Ok(())
    }
}
