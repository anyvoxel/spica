//! `TaskCancelled` event projection: folds the `Event::TaskCancelled` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{NodeId, TaskId};
use crate::storage::TaskStatus;

#[derive(Default)]
pub(crate) struct TaskCancelledApplier;
#[async_trait]
impl EventApplier for TaskCancelledApplier {
    fn event(&self) -> Event {
        Event::TaskCancelled {
            task: TaskId::nil(),
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
        // non-`Active` guard.
        if let Some(mut t) = ctx.storage.get_task(*task).await? {
            let parent = t.parent;
            t.status = TaskStatus::Cancelled;
            ctx.storage.put_task(t).await?;
            ctx.storage
                .remove_child(parent, NodeId::Task(*task))
                .await?;
        }
        Ok(())
    }
}
