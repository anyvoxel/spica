//! `TaskActivated` event projection: folds the `Event::TaskActivated` into Storage, making the task
//! **available** (`Pending`) for a worker to claim.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::{RetryState, Task, TaskStatus};

#[derive(Default)]
pub(crate) struct TaskActivatedApplier;
#[async_trait]
impl EventApplier for TaskActivatedApplier {
    fn event(&self) -> Event {
        Event::TaskActivated {
            task: Task {
                execution: crate::types::meta::ObjectReference::nil(),
                resource: String::new(),
                arguments: Default::default(),
                status: TaskStatus::Pending,
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
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::TaskActivated { task } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        // Fold the invocation as a durable fact: an `Pending` (available) task row owned by the
        // invoking activity. `deadline` is `None` in M1 (no `TimeoutSeconds` support yet), and
        // `worker_id`/`lease_until` are `None` until a worker claims it.
        let mut row = crate::storage::TaskRecord::from_value(task.clone());
        // Birth: `created_at`/`updated_at` stamped with the `TaskActivated` entry's moment.
        row.born(ctx.timestamp);
        ctx.storage.put_task(row).await?;
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
