//! `TaskLeased` event projection: folds the `Event::TaskLeased` into Storage, marking the task
//! `Running` and recording the leasing worker and its lease deadline.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{NodeId, TaskId};
use crate::{TaskStatus, TaskValue};

#[derive(Default)]
pub(crate) struct TaskLeasedApplier;
#[async_trait]
impl EventApplier for TaskLeasedApplier {
    fn event(&self) -> Event {
        Event::TaskLeased {
            task: TaskValue {
                id: TaskId::nil(),
                parent: NodeId::Activity(crate::id::ActivityId::nil()),
                resource: String::new(),
                arguments: Default::default(),
                status: TaskStatus::Running,
                deadline: None,
                worker_id: Some(String::new()),
                lease_until: Some(crate::log::Timestamp::from_millis(0)),
            },
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::TaskLeased { task } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        // Fold the lease **only while the task is still available** (`Pending`) — the conditional
        // exactly-once stake. The grant was decided at discovery time (see `PullTasksHandler`), so a
        // racing pull can learn of a task it no longer legitimately owns, or a stale/replayed
        // `TaskLeased` can arrive after a cancel or a settlement; overwriting in those cases would
        // wrongly hand the task to a worker who doesn't own it. Skipping the fold leaves the task
        // with whoever does own it (or settled). The state thus advances at-most-once even if the
        // caller's *work* is at-least-once.
        if let Some(mut t) = ctx.storage.get_task(task.id).await?
            && t.status.is_pending()
        {
            t.status = TaskStatus::Running;
            t.worker_id = task.worker_id.clone();
            t.lease_until = task.lease_until;
            t.touch(ctx.timestamp);
            ctx.storage.put_task(t).await?;
        }
        Ok(())
    }
}
