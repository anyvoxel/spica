//! `ThreadCompleted` event projection: folds the `Event::ThreadCompleted` into Storage.

use crate::types::error::ExecutionError;
use crate::types::meta::ErasedOwner;
use crate::{ApplierContext, Thread, ThreadStatus};

#[derive(Default)]
pub(crate) struct ThreadCompletedApplier;
impl ThreadCompletedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        thread: &Thread,
    ) -> Result<(), ExecutionError> {
        if let Some(mut row) = ctx.storage.get_thread(&thread.meta.reference()).await? {
            row.status = ThreadStatus::Completed;
            row.output = thread.output.clone();
            // Keep the projected domain `updated_at` in step with the event's (handler-stamped).
            row.value.meta.updated_at = thread.meta.updated_at;
            let parent = row.value.meta.owner.clone();
            row.with_update_at(ctx.timestamp);
            ctx.storage.put_thread(row).await?;
            ctx.storage
                .remove_child(parent.into_erased(), thread.meta.reference())
                .await?;
        }
        Ok(())
    }
}
