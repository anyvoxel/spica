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
            // the entry timestamp via `touch`, a separate concept).
            t.value.meta.touch(timer.meta.updated_at);
            t.touch(ctx.timestamp);
            ctx.storage.put_timer(t).await?;
            ctx.storage.remove_child(parent, timer.reference()).await?;
        }
        Ok(())
    }
}
