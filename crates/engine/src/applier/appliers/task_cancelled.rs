//! `TaskCancelled` event projection: folds the `Event::TaskCancelled` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::{RetryState, Task, TaskStatus};

#[derive(Default)]
pub(crate) struct TaskCancelledApplier;
#[async_trait]
impl EventApplier for TaskCancelledApplier {
    fn event(&self) -> Event {
        Event::TaskCancelled {
            task: Task {
                execution: crate::types::meta::ObjectReference::nil(),
                resource: String::new(),
                arguments: Default::default(),
                status: TaskStatus::Cancelled,
                deadline: None,
                worker_id: None,
                lease_until: None,
                retry_plan: vec![],
                retry_state: RetryState::default(),
                meta: crate::types::meta::ObjectMeta::builder(
                    crate::types::meta::ObjectKind::Task,
                    ulid::Ulid::nil(),
                )
                .timestamps(
                    crate::log::Timestamp::from_millis(0),
                    crate::log::Timestamp::from_millis(0),
                )
                .build(),
            },
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::TaskCancelled { task } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        // Mark the task Cancelled and drain it from its owning activity. The physical call is left
        // running; a later `CompleteTask` for this task is swallowed by the `CompleteTaskHandler`'s
        // non-`Running` guard.
        if let Some(mut t) = ctx.storage.get_task(&task.reference()).await? {
            let parent = t
                .meta
                .owner
                .clone()
                .expect("an owned task always has an owner");
            t.status = TaskStatus::Cancelled;
            // Sync the domain value's transition stamp from the event (see task_completed.rs).
            t.value.meta.with_update_at(task.meta.updated_at);
            t.with_update_at(ctx.timestamp);
            ctx.storage.put_task(t).await?;
            ctx.storage.remove_child(parent, task.reference()).await?;
        }
        Ok(())
    }
}
