//! `TaskFailed` event projection: folds the `Event::TaskFailed` into Storage.

use crate::ApplierContext;
use crate::types::error::ExecutionError;
use crate::types::event::TaskFailed;

use crate::TaskStatus;
use crate::types::meta::ObjectKind;

/// Applies `TaskFailed`, whose **task entity's `status` is the outcome**:
///
/// - `Pending` — a `Retry` was scheduled ([[task-retry-model]] stage 2): the *same* task entity
///   re-queues, claimable no earlier than `next_available_at` (the backoff gate, carried in the
///   entity along with the advanced per-retrier attempt counters). It stays a child of its owning
///   activity and is folded verbatim from the event (`status`, cleared worker/lease, `attempts`,
///   `retrier_attempts`, `next_available_at`). Its total attempt count is mirrored onto the owning
///   activity's `retry_state.attempts` (the shared `$states.context.State.RetryCount` every state
///   handler reads, including `Map`/`Parallel`).
/// - `Failed` — terminal: the retry budget is exhausted. The task is marked `Failed` and drained
///   from its owning activity (the sweep + duplicate guard); `error` is not folded into the task row
///   (it drives the state's `Catch`/`terminate` decision instead).
#[derive(Default)]
pub(crate) struct TaskFailedApplier;
impl TaskFailedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &TaskFailed,
    ) -> Result<(), ExecutionError> {
        let TaskFailed { task, .. } = event;
        let retryable = task.status == TaskStatus::Pending;
        if let Some(mut t) = ctx.storage.get_task(&task.reference()).await? {
            let parent = t
                .meta
                .owner
                .clone()
                .expect("an owned task always has an owner");
            // Fold the entity verbatim (status, cleared worker/lease, + the retry bookkeeping the
            // handler stamped: `attempts`, `retrier_attempts`, `next_available_at`).
            t.value = task.clone();
            t.with_update_at(ctx.timestamp);
            ctx.storage.put_task(t).await?;
            if retryable {
                // The reused task stays a child (claimable again after `next_available_at` lapses).
                // Mirror its total attempt count onto the owning activity's `$states` RetryCount so
                // the shared projection / Catch path sees the accumulated retries.
                if parent.kind == ObjectKind::Activity
                    && let Some(mut act) = ctx.storage.get_activity(&parent).await?
                {
                    act.retry_state.get_or_insert_default().attempts = task.retry_state.attempts;
                    act.with_update_at(ctx.timestamp);
                    ctx.storage.put_activity(act).await?;
                }
            } else {
                // Terminal failure: drain the task from its owning activity (the sweep + duplicate
                // guard). `error` is not folded — it drives the state's `Catch`/`TerminateState`.
                ctx.storage.remove_child(parent, task.reference()).await?;
            }
        }
        Ok(())
    }
}
