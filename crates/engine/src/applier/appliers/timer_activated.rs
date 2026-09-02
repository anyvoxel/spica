//! `TimerActivated` event projection: folds the `Event::TimerActivated` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::TimerStatus;
use crate::log::Timestamp;
use crate::types::command::TimerPurpose;

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
            timer: crate::Timer {
                execution: crate::types::meta::ObjectReference::nil(),
                purpose: TimerPurpose::WaitResume,
                status: TimerStatus::Active,
                deadline: Timestamp::from_millis(0),
                meta: crate::types::meta::ObjectMeta::placeholder_with_times(
                    crate::types::meta::ObjectKind::Timer,
                    ulid::Ulid::nil(),
                    Timestamp::from_millis(0),
                    Timestamp::from_millis(0),
                ),
            },
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::TimerActivated { timer } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        let mut row = crate::storage::TimerRecord::from_value(timer.clone());
        // Birth: `created_at`/`updated_at` stamped with the `TimerActivated` entry's moment.
        row.born(ctx.timestamp);
        ctx.storage.put_timer(row).await?;
        ctx.storage
            .add_child(
                timer
                    .meta
                    .owner
                    .clone()
                    .expect("an armed timer is always owned"),
                timer.reference(),
            )
            .await?;
        // Schedule the physical deadline (the storage fold is pure; this is the side effect).
        // The scheduler needs the owning entry's causal identity to re-envelope the `TriggerTimer`
        // it fires on expiry; the StreamProcessor supplies it via the context. (There is no per-execution
        // stream — a LogStream is one stream, so stream identity lives on the log, not here.)
        // The wait duration is derived from the persisted absolute `deadline`: already-past
        // fire immediately (saturating to zero).
        ctx.scheduler
            .schedule(&timer.reference(), timer.deadline, ctx.cause_id);
        Ok(())
    }
}
