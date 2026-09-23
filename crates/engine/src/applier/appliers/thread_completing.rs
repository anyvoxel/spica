//! `ThreadCompleting` event projection: folds the `Event::ThreadCompleting` into Storage.

use crate::types::error::ExecutionError;
use crate::{ApplierContext, Thread, ThreadStatus};

#[derive(Default)]
pub(crate) struct ThreadCompletingApplier;
impl ThreadCompletingApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        thread: &Thread,
    ) -> Result<(), ExecutionError> {
        if let Some(mut row) = ctx.storage.get_thread(&thread.reference()).await? {
            row.status = ThreadStatus::Completing;
            row.output = thread.output.clone();
            // Keep the projected domain `updated_at` in step with the event's (handler-stamped).
            row.value.meta.updated_at = thread.meta.updated_at;
            row.with_update_at(ctx.timestamp);
            ctx.storage.put_thread(row).await?;
        }
        Ok(())
    }
}
