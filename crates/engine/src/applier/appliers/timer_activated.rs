//! `TimerActivated` event projection: folds the `Event::TimerActivated` into Storage. The durable
//! event carries the timer's absolute `deadline`; a consumer (via the injected `Hook`) re-derives the
//! physical deadline arm from that persisted moment — a replayed projection re-derives the same wait
//! rather than a fresh relative count.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::TimerStatus;
use crate::log::Timestamp;
use crate::types::command::TimerPurpose;

/// `TimerActivated` folds the timer row into Storage **and** declares the physical deadline arm as an
/// [`Effect`] for the caller to execute. The applier performs no external side effect itself — the
/// arm is applied against the real scheduler only once the producing fold's transaction is durable.
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
                meta: crate::types::meta::ObjectMeta::builder(
                    crate::types::meta::ObjectKind::Timer,
                    ulid::Ulid::nil(),
                )
                .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                .build(),
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
        super::bump_generated_seq(ctx.storage, &timer.reference().name).await?;
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
        // The physical deadline arm is not folded here — a consumer re-derives `schedule` from the
        // durable `TimerActivated` event (which carries the absolute `deadline`) once it is committed.
        Ok(())
    }
}
