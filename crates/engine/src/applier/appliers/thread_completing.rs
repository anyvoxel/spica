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
        if let Some(mut row) = ctx.storage.get_thread(&thread.meta.object_ref()).await? {
            row.status = ThreadStatus::Completing;
            row.output = thread.output.clone();
            // The row's timing facts are the domain value's `meta`; the event carries the handler's
            // stamp for this transition, so it rides along with the status/output update.
            row.meta.updated_at = thread.meta.updated_at;
            ctx.storage.put_thread(row).await?;
        }
        Ok(())
    }
}
