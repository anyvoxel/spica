//! `TaskActivated` event projection: folds the `Event::TaskActivated` into Storage, making the task
//! **available** (`Pending`) for a worker to claim.

use crate::ApplierContext;
use crate::types::error::ExecutionError;

use crate::Task;

#[derive(Default)]
pub(crate) struct TaskActivatedApplier;
impl TaskActivatedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        task: &Task,
    ) -> Result<(), ExecutionError> {
        // Fold the invocation as a durable fact: a `Pending` (available) task row owned by the
        // invoking activity. `deadline` is the state's `TimeoutSeconds` instant (carried on the
        // command), and `worker_id`/`lease_expires_at` are `None` until a worker claims it.
        let mut row = crate::storage::TaskRecord::from_value(task.clone());
        // Birth: `created_at`/`updated_at` stamped with the `TaskActivated` entry's moment.
        row.born(ctx.timestamp);
        ctx.storage.put_task(row).await?;
        super::bump_generated_seq(ctx.storage, &task.reference().name).await?;
        ctx.storage
            .add_child(
                task.meta
                    .owner
                    .clone()
                    .expect("an activated task is always owned"),
                task.reference(),
            )
            .await?;
        // No handler is invoked here (M1 used to `invoke` as a post-commit side effect). The task is
        // now *claimable*: a worker pulls it via `TaskApi::poll_tasks` and performs the physical call.
        // Storage is the durable source of truth; the "queue" of `Pending` tasks is derived from it.
        Ok(())
    }
}
