//! `TaskCancelled` event projection: folds the `Event::TaskCancelled` into Storage.

use crate::ApplierContext;
use crate::types::error::ExecutionError;
use crate::types::meta::HasRawObjectRef;

use crate::{Task, TaskStatus};

#[derive(Default)]
pub(crate) struct TaskCancelledApplier;
impl TaskCancelledApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        task: &Task,
    ) -> Result<(), ExecutionError> {
        // Mark the task Cancelled and drain it from its owning activity. The physical call is left
        // running; a later `CompleteTask` for this task is swallowed by the `TaskElementBehavior::complete`'s
        // non-`Running` guard.
        if let Some(mut t) = ctx.storage.get_task(&task.meta.object_ref()).await? {
            let parent = t
                .meta
                .owner
                .clone()
                // The slot is an `ObjectRef<ActivityKind>` and an ownership edge speaks flat addresses.
                .into_raw_object_ref();
            t.status = TaskStatus::Cancelled;
            // The row's timing facts are the domain value's `meta`; the event carries the handler's
            // stamp for this transition, so it rides along with the status update.
            t.meta.with_update_at(task.meta.updated_at);
            ctx.storage.put_task(t).await?;
            ctx.storage
                .remove_child(parent, task.meta.raw_object_ref())
                .await?;
        }
        Ok(())
    }
}
