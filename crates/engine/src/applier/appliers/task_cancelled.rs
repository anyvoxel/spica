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
        // running; a later `CompleteTask` for this task is swallowed by the `CompleteTaskHandler`'s
        // non-`Running` guard.
        if let Some(mut t) = ctx.storage.get_task(&task.meta.typed_reference()).await? {
            let parent = t
                .meta
                .owner
                .clone()
                // The slot is an `ObjectRef<ActivityKind>` and `active_children` speaks flat addresses.
                .into_raw_object_ref();
            t.status = TaskStatus::Cancelled;
            // Sync the domain value's transition stamp from the event (see task_completed.rs).
            t.value.meta.with_update_at(task.meta.updated_at);
            t.with_update_at(ctx.timestamp);
            ctx.storage.put_task(t).await?;
            ctx.storage
                .remove_child(parent, task.meta.reference())
                .await?;
        }
        Ok(())
    }
}
