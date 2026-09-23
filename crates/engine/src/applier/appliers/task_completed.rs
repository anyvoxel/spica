//! `TaskCompleted` event projection: folds the `Event::TaskCompleted` into Storage.

use crate::ApplierContext;
use crate::types::error::ExecutionError;
use crate::types::event::TaskCompleted;

use crate::TaskStatus;
use crate::types::meta::ObjectKind;

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
        if let Some(mut t) = ctx.storage.get_task(&task.reference()).await? {
            let parent = t
                .meta
                .owner
                .clone()
                .expect("an owned task always has an owner");
            t.status = TaskStatus::Completed;
            // Sync the domain value's transition stamp from the event (the row's own `updated_at`
            // is the entry timestamp via `with_update_at`, a separate concept).
            t.value.meta.with_update_at(task.meta.updated_at);
            t.with_update_at(ctx.timestamp);
            ctx.storage.put_task(t).await?;
            if parent.kind == ObjectKind::Activity
                && let Some(mut act) = ctx.storage.get_activity(&parent).await?
            {
                act.value.raw_output = Some(output.clone());
                act.with_update_at(ctx.timestamp);
                ctx.storage.put_activity(act).await?;
            }
            ctx.storage.remove_child(parent, task.reference()).await?;
        }
        Ok(())
    }
}
