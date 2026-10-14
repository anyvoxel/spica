//! `TaskCompleted` event projection: folds the `Event::TaskCompleted` into Storage.

use crate::ApplierContext;
use crate::types::error::ExecutionError;
use crate::types::event::TaskCompleted;

use crate::TaskStatus;
use crate::types::meta::HasRawObjectRef;

#[derive(Default)]
pub(crate) struct TaskCompletedApplier;
impl TaskCompletedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &TaskCompleted,
    ) -> Result<(), ExecutionError> {
        let TaskCompleted {
            request_id: _,
            task,
            output,
        } = event;
        // Mark the task Completed and drain it from its owning activity. The task's returned payload
        // is folded into the activity's `raw_output`: it is the state's raw result before the
        // complete step's `Output` projection, distinct from the immutable processed input recorded
        // during `StateActivated`.
        if let Some(mut t) = ctx.storage.get_task(&task.meta.object_ref()).await? {
            // An activity is the only thing that can own a task (the slot's own type), so the fold
            // needs no `kind` guard here.
            let parent = t.meta.owner.clone();
            t.status = TaskStatus::Completed;
            // The row's timing facts are the domain value's `meta`; the event carries the handler's
            // stamp for this transition, so it rides along with the status update.
            t.meta.with_update_at(task.meta.updated_at);
            ctx.storage.put_task(t).await?;
            if let Some(mut act) = ctx.storage.get_activity(&parent).await? {
                act.raw_output = Some(output.clone());
                // The event carries no activity value, so this fold's own write is stamped from the
                // entry's frozen moment — deterministic on replay, never a local clock.
                act.meta.with_update_at(ctx.timestamp);
                ctx.storage.put_activity(act).await?;
            }
            ctx.storage
                .remove_child(parent.into_raw_object_ref(), task.meta.raw_object_ref())
                .await?;
        }
        Ok(())
    }
}
