//! `TasksClaimed` event projection: folds the `Event::TasksClaimed` batch into Storage, marking
//! each task `Running` and recording its leasing worker and lease deadline.

use crate::ApplierContext;
use crate::types::error::ExecutionError;
use crate::types::event::TasksClaimed;

use crate::TaskStatus;

#[derive(Default)]
pub(crate) struct TasksClaimedApplier;
impl TasksClaimedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &TasksClaimed,
    ) -> Result<(), ExecutionError> {
        let TasksClaimed {
            request_id: _,
            tasks,
        } = event;
        // Fold each claim **only while the task is still claimable** — the conditional exactly-once
        // stake. The batch was decided at discovery time (see `ClaimTasksHandler`), so a racing pull
        // can learn of a task it no longer legitimately owns, or a stale/replayed `TasksClaimed` can
        // arrive after a cancel or a settlement; overwriting in those cases would wrongly hand the
        // task to a worker who doesn't own it. Skipping one entry leaves the task with whoever does
        // own it (or settled) while the rest still fold — the state thus advances at-most-once per
        // entry even if the caller's *work* is at-least-once.
        //
        // The decision is re-made here with the **entry's** timestamp, not a live clock reading: on
        // replay the same entry must fold the same way. It is the same predicate discovery used, so
        // what a poll grants is exactly what the fold accepts — including a steal, which lands as an
        // ordinary claim: the expired lease is simply claimable again.
        for task in tasks {
            if let Some(mut t) = ctx.storage.get_task(&task.reference()).await?
                && t.is_claimable_at(ctx.timestamp)
            {
                t.status = TaskStatus::Running;
                t.worker_id = task.worker_id.clone();
                t.lease_expires_at = task.lease_expires_at;
                // Claimed — clear the retry backoff gate (see `Task::next_available_at`).
                t.retry_state.next_available_at = task.retry_state.next_available_at;
                // Sync the domain value's transition stamp from the event (see task_completed.rs).
                t.value.meta.with_update_at(task.meta.updated_at);
                t.with_update_at(ctx.timestamp);
                ctx.storage.put_task(t).await?;
            }
        }
        Ok(())
    }
}
