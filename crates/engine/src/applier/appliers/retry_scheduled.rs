//! `RetryScheduled` event projection: folds the `Event::RetryScheduled` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::log::Timestamp;
use crate::{ApplierContext, EventApplier};

use crate::id::ActivityId;
use crate::storage::RetrierAttemptState;

/// `RetryScheduled` updates the owning activity's retry bookkeeping so the re-invoked task's
/// `$states.context.State.RetryCount` and each retrier's independent attempt budget are durable and
/// replayable. The event carries the fully-decided bookkeeping facts (`retrier_index`,
/// `retrier_attempt`, `retry_count`, `scheduled_at`) so the applier folds them directly rather than
/// re-deriving any retry policy from local handler state.
#[derive(Default)]
pub(crate) struct RetryScheduledApplier;
#[async_trait]
impl EventApplier for RetryScheduledApplier {
    fn event(&self) -> Event {
        Event::RetryScheduled {
            activity: ActivityId::nil(),
            retrier_index: 0,
            retrier_attempt: 0,
            retry_count: 0,
            scheduled_at: Timestamp::from_millis(0),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::RetryScheduled {
            activity,
            retrier_index,
            retrier_attempt,
            retry_count,
            scheduled_at,
        } = event
        else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut act) = ctx.storage.get_activity(*activity).await? {
            // The event carries the authoritative retry facts: the activity-wide total count, the
            // matched retrier's own attempt counter, and when that retry decision was taken. Fold them
            // verbatim so replay rebuilds the exact same retry state without re-running the matching
            // logic.
            act.retry_state.retry_count = *retry_count;
            if act.retry_state.retrier_attempts.len() <= *retrier_index {
                act.retry_state
                    .retrier_attempts
                    .resize(*retrier_index + 1, RetrierAttemptState::default());
            }
            act.retry_state.retrier_attempts[*retrier_index] = RetrierAttemptState {
                attempt_count: *retrier_attempt,
                last_retry_at: Some(*scheduled_at),
            };
            ctx.storage.put_activity(act).await?;
        }
        Ok(())
    }
}
