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
        if let Some(mut row) = ctx.storage.get_thread(&thread.meta.object_ref()).await? {
            row.status = thread.status.clone();
            row.meta.updated_at = thread.meta.updated_at;
            ctx.storage.put_thread(row).await?;
        }
        Ok(())
    }
}
