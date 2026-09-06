//! `TimerTriggered` event projection: folds the `Event::TimerTriggered` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::TimerStatus;

#[derive(Default)]
pub(crate) struct TimerTriggeredApplier;
#[async_trait]
impl EventApplier for TimerTriggeredApplier {
    fn event(&self) -> Event {
        Event::TimerTriggered {
            timer: crate::Timer {
                execution: crate::types::meta::ObjectReference::nil(),
                purpose: crate::TimerPurpose::WaitResume,
                status: TimerStatus::Completed,
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
        let Event::TimerTriggered { timer } = event else {
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
            t.value.status = TimerStatus::Completed;
            // Sync the domain value's transition stamp from the event (the row's own `updated_at` is
            // the entry timestamp via `with_update_at`, a separate concept).
            t.value.meta.with_update_at(timer.meta.updated_at);
            t.with_update_at(ctx.timestamp);
            ctx.storage.put_timer(t).await?;
            ctx.storage.remove_child(parent, timer.reference()).await?;
        }
        Ok(())
    }
}
