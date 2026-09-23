//! `ThreadTerminating` event projection: folds the `Event::ThreadTerminating` into Storage.

use crate::types::error::ExecutionError;
use crate::{ApplierContext, Thread};

#[derive(Default)]
pub(crate) struct ThreadTerminatingApplier;
impl ThreadTerminatingApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        thread: &Thread,
    ) -> Result<(), ExecutionError> {
        if let Some(mut row) = ctx.storage.get_thread(&thread.reference()).await? {
            row.status = thread.status.clone();
            row.value.meta.updated_at = thread.meta.updated_at;
            row.with_update_at(ctx.timestamp);
            ctx.storage.put_thread(row).await?;
        }
        Ok(())
    }
}
