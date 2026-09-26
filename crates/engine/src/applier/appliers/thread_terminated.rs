//! `ThreadTerminated` event projection: folds the `Event::ThreadTerminated` into Storage.

use crate::types::error::ExecutionError;
use crate::{ApplierContext, Thread};

#[derive(Default)]
pub(crate) struct ThreadTerminatedApplier;
impl ThreadTerminatedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        thread: &Thread,
    ) -> Result<(), ExecutionError> {
        if let Some(mut row) = ctx.storage.get_thread(&thread.reference()).await? {
            row.status = thread.status.clone();
            row.value.meta.updated_at = thread.meta.updated_at;
            let parent = row.value.meta.owner.clone();
            row.with_update_at(ctx.timestamp);
            ctx.storage.put_thread(row).await?;
            if let Some(owner) = parent {
                ctx.storage.remove_child(owner, thread.reference()).await?;
            }
        }
        Ok(())
    }
}
