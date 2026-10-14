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
            let parent = t.meta.owner.clone();
            t.status = TimerStatus::Completed;
            // The row's timing facts are the domain value's `meta`; the event carries the handler's
            // stamp for this transition, so it rides along with the status update.
            t.meta.with_update_at(timer.meta.updated_at);
            ctx.storage.put_timer(t).await?;
            ctx.storage
                .remove_child(parent.into_raw_object_ref(), timer.meta.raw_object_ref())
                .await?;
        }
        Ok(())
    }
}
