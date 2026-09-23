//! `TimerCancelled` event projection: folds the `Event::TimerCancelled` into Storage. The physical
//! descheduling is not folded here — a consumer re-derives `cancel` from the durable event via the
//! injected `Hook`.

use crate::ApplierContext;
use crate::Timer;
use crate::types::error::ExecutionError;

use crate::TimerStatus;

/// `TimerCancelled` folds the terminal status into Storage.
#[derive(Default)]
pub(crate) struct TimerCancelledApplier;
impl TimerCancelledApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        timer: &Timer,
    ) -> Result<(), ExecutionError> {
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
