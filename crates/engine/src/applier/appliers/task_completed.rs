//! `TaskCompleted` event projection: folds the `Event::TaskCompleted` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{NodeId, TaskId};
use crate::storage::TaskStatus;

#[derive(Default)]
pub(crate) struct TaskCompletedApplier;
#[async_trait]
impl EventApplier for TaskCompletedApplier {
    fn event(&self) -> Event {
        Event::TaskCompleted {
            task: TaskId::nil(),
            output: Default::default(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::TaskCompleted { task, output } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        // Mark the task Completed and drain it from its owning activity. The task's returned payload
        // is folded into the activity's `raw_output`: it is the state's raw result before the
        // complete step's `Output` projection, distinct from the immutable processed input recorded
        // during `StateActivated`.
        if let Some(mut t) = ctx.storage.get_task(*task).await? {
            let parent = t.parent;
            t.status = TaskStatus::Completed;
            ctx.storage.put_task(t).await?;
            if let NodeId::Activity(activity_id) = parent
                && let Some(mut act) = ctx.storage.get_activity(activity_id).await?
            {
                act.raw_output = Some(output.clone());
                ctx.storage.put_activity(act).await?;
            }
            ctx.storage
                .remove_child(parent, NodeId::Task(*task))
                .await?;
        }
        Ok(())
    }
}
