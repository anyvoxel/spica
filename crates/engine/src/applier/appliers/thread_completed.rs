//! `ThreadCompleted` event projection: folds the `Event::ThreadCompleted` into Storage.

use crate::types::error::ExecutionError;
use crate::{ApplierContext, Thread, ThreadStatus};

#[derive(Default)]
pub(crate) struct ThreadCompletedApplier;
impl ThreadCompletedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        thread: &Thread,
    ) -> Result<(), ExecutionError> {
        if let Some(mut row) = ctx.storage.get_thread(&thread.reference()).await? {
            row.status = ThreadStatus::Completed;
            row.output = thread.output.clone();
            // Keep the projected domain `updated_at` in step with the event's (handler-stamped).
            row.value.meta.updated_at = thread.meta.updated_at;
            // A terminal thread cannot still hold an in-flight state activation cursor.
            row.current_activity = None;
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
