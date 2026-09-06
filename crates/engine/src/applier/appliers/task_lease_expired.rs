//! `TaskLeaseExpired` event projection: folds the `Event::TaskLeaseExpired` into Storage, returning
//! the task to `Pending` (available again) and clearing the broken lease.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::{RetryState, Task, TaskStatus};

#[derive(Default)]
pub(crate) struct TaskLeaseExpiredApplier;
#[async_trait]
impl EventApplier for TaskLeaseExpiredApplier {
    fn event(&self) -> Event {
        Event::TaskLeaseExpired {
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
        let Event::TaskLeaseExpired { task } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
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
