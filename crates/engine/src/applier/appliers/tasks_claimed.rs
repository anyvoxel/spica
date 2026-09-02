//! `TasksClaimed` event projection: folds the `Event::TasksClaimed` batch into Storage, marking
//! each task `Running` and recording its leasing worker and lease deadline.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::{RetryState, Task, TaskStatus};

#[derive(Default)]
pub(crate) struct TasksClaimedApplier;
#[async_trait]
impl EventApplier for TasksClaimedApplier {
    fn event(&self) -> Event {
        Event::TasksClaimed {
            tasks: vec![Task {
                execution: crate::types::meta::ObjectReference::nil(),
                resource: String::new(),
                arguments: Default::default(),
                status: TaskStatus::Running,
                deadline: None,
                worker_id: Some(String::new()),
                lease_until: Some(crate::log::Timestamp::from_millis(0)),
                retry_plan: vec![],
                retry_state: RetryState::default(),
                meta: crate::types::meta::ObjectMeta::placeholder_with_times(
                    crate::types::meta::ObjectKind::Task,
                    ulid::Ulid::nil(),
                    crate::log::Timestamp::from_millis(0),
                    crate::log::Timestamp::from_millis(0),
                ),
            }],
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::TasksClaimed { tasks } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        // Fold each claim **only while the task is still available** (`Pending`) — the conditional
        // exactly-once stake. The batch was decided at discovery time (see `ClaimTasksHandler`), so a
        // racing pull can learn of a task it no longer legitimately owns, or a stale/replayed
        // `TasksClaimed` can arrive after a cancel or a settlement; overwriting in those cases would
        // wrongly hand the task to a worker who doesn't own it. Skipping one entry leaves the task
        // with whoever does own it (or settled) while the rest still fold — the state thus advances
        // at-most-once per entry even if the caller's *work* is at-least-once.
        for task in tasks {
            if let Some(mut t) = ctx.storage.get_task(&task.reference()).await?
                && t.status.is_pending()
            {
                t.status = TaskStatus::Running;
                t.worker_id = task.worker_id.clone();
                t.lease_until = task.lease_until;
                // Claimed — clear the retry backoff gate (see `Task::next_available_at`).
                t.retry_state.next_available_at = task.retry_state.next_available_at;
                // Sync the domain value's transition stamp from the event (see task_completed.rs).
                t.value.meta.touch(task.meta.updated_at);
                t.touch(ctx.timestamp);
                ctx.storage.put_task(t).await?;
            }
        }
        Ok(())
    }
}
