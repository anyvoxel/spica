//! `TaskFailed` event projection: folds the `Event::TaskFailed` into Storage.

use async_trait::async_trait;

use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::types::meta::ObjectKind;
use crate::{RetryState, Task, TaskStatus};

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
#[async_trait]
impl EventApplier for TaskFailedApplier {
    fn event(&self) -> Event {
        Event::TaskFailed {
            task: Task {
                execution: crate::types::meta::ObjectReference::nil(),
                resource: String::new(),
                arguments: Default::default(),
                status: TaskStatus::Failed,
                deadline: None,
                worker_id: None,
                lease_until: None,
                retry_plan: vec![],
                retry_state: RetryState::default(),
                meta: crate::types::meta::ObjectMeta::placeholder_with_times(
                    crate::types::meta::ObjectKind::Task,
                    ulid::Ulid::nil(),
                    crate::log::Timestamp::from_millis(0),
                    crate::log::Timestamp::from_millis(0),
                ),
            },
            error: ExecutionError::Runtime(RuntimeError::InvalidDefinition(String::new())),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::TaskFailed { task, .. } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
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
            t.touch(ctx.timestamp);
            ctx.storage.put_task(t).await?;
            if retryable {
                // The reused task stays a child (claimable again after `next_available_at` lapses).
                // Mirror its total attempt count onto the owning activity's `$states` RetryCount so
                // the shared projection / Catch path sees the accumulated retries.
                if parent.kind == ObjectKind::Activity
                    && let Some(mut act) = ctx.storage.get_activity(&parent).await?
                {
                    act.retry_state.attempts = task.retry_state.attempts;
                    act.touch(ctx.timestamp);
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
