//! `ThreadCompleted` event projection: folds the `Event::ThreadCompleted` into Storage.

use crate::types::error::ExecutionError;
use crate::types::meta::HasRawObjectRef;
use crate::{ApplierContext, Thread, ThreadStatus};

#[derive(Default)]
pub(crate) struct ThreadCompletedApplier;
impl ThreadCompletedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        thread: &Thread,
    ) -> Result<(), ExecutionError> {
        if let Some(mut row) = ctx.storage.get_thread(&thread.meta.object_ref()).await? {
            row.status = ThreadStatus::Completed;
            row.output = thread.output.clone();
            // The row's timing facts are the domain value's `meta`; the event carries the handler's
            // stamp for this transition, so it rides along with the status/output update.
            row.meta.updated_at = thread.meta.updated_at;
            let parent = row.meta.owner.clone();
            ctx.storage.put_thread(row).await?;
            ctx.storage
                .remove_child(parent.into_raw_object_ref(), thread.meta.raw_object_ref())
                .await?;
        }
        Ok(())
    }
}
