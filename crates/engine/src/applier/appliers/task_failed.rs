//! `TaskFailed` event projection: folds the `Event::TaskFailed` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{NodeId, TaskId};
use crate::storage::TaskStatus;

#[derive(Default)]
pub(crate) struct TaskFailedApplier;
#[async_trait]
impl EventApplier for TaskFailedApplier {
    fn event(&self) -> Event {
        Event::TaskFailed {
            task: TaskId::nil(),
            error: ExecutionError::InvalidDefinition(String::new()),
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
        // Mark the task Failed and drain it from its owning activity. `error` is not folded into the
        // task row — it drives the state's `TerminateState` (the `TaskFailed` event's real consumer
        // is the terminate path), so the terminal status here only serves the sweep + duplicate
        // guard.
        if let Some(mut t) = ctx.storage.get_task(*task).await? {
            let parent = t.parent;
            t.status = TaskStatus::Failed;
            ctx.storage.put_task(t).await?;
            ctx.storage
                .remove_child(parent, NodeId::Task(*task))
                .await?;
        }
        Ok(())
    }
}
