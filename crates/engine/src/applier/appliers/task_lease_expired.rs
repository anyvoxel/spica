//! `TaskLeaseExpired` event projection: folds the `Event::TaskLeaseExpired` into Storage, returning
//! the task to `Pending` (available again) and clearing the broken lease.

use crate::ApplierContext;
use crate::types::error::ExecutionError;

use crate::{Task, TaskStatus};

#[derive(Default)]
pub(crate) struct TaskLeaseExpiredApplier;
impl TaskLeaseExpiredApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        task: &Task,
    ) -> Result<(), ExecutionError> {
        // Clear the lease and return the task to `Pending`, so it can be claimed again (Zeebe's
        // activation-timeout re-queue). The physical handler may still be running from the old
        // lease — that's the at-least-once contract; its late settle is rejected because the task is
        // no longer `Running` to it.
        if let Some(mut t) = ctx.storage.get_task(&task.reference()).await? {
            t.status = TaskStatus::Pending;
            t.worker_id = None;
            t.lease_until = None;
            // Sync the domain value's transition stamp from the event (see task_completed.rs).
            t.value.meta.with_update_at(task.meta.updated_at);
            t.with_update_at(ctx.timestamp);
            ctx.storage.put_task(t).await?;
        }
        Ok(())
    }
}
