//! `TimerTriggered` event projection: folds the `Event::TimerTriggered` into Storage.

use crate::ApplierContext;
use crate::Timer;
use crate::types::error::ExecutionError;
use crate::types::meta::HasRawObjectRef;

use crate::TimerStatus;

#[derive(Default)]
pub(crate) struct TimerTriggeredApplier;
impl TimerTriggeredApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        timer: &Timer,
    ) -> Result<(), ExecutionError> {
        if let Some(mut t) = ctx.storage.get_timer(&timer.meta.object_ref()).await? {
            let parent = t.value.meta.owner.clone();
            t.value.status = TimerStatus::Completed;
            // Sync the domain value's transition stamp from the event (the row's own `updated_at` is
            // the entry timestamp via `with_update_at`, a separate concept).
            t.value.meta.with_update_at(timer.meta.updated_at);
            t.with_update_at(ctx.timestamp);
            ctx.storage.put_timer(t).await?;
            ctx.storage
                .remove_child(parent.into_raw_object_ref(), timer.meta.raw_object_ref())
                .await?;
        }
        Ok(())
    }
}
