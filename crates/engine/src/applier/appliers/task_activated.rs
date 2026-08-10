//! `TaskActivated` event projection: folds the `Event::TaskActivated` into Storage and feeds the
//! external call to the [`TaskServiceHandle`](crate::task_service::TaskServiceHandle).

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{NodeId, TaskId};
use crate::storage::TaskStatus;

#[derive(Default)]
pub(crate) struct TaskActivatedApplier;
#[async_trait]
impl EventApplier for TaskActivatedApplier {
    fn event(&self) -> Event {
        Event::TaskActivated {
            parent: NodeId::Activity(crate::id::ActivityId::nil()),
            task: TaskId::nil(),
            resource: String::new(),
            arguments: Default::default(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::TaskActivated {
            parent,
            task,
            resource,
            arguments,
        } = event
        else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        // Fold the invocation as a durable fact: an `Active` task row owned by the invoking
        // activity. `deadline` is `None` in M1 (no `TimeoutSeconds` support yet — a later milestone
        // arms a deadline-derived timeout).
        ctx.storage
            .put_task(crate::storage::Task {
                id: *task,
                parent: *parent,
                resource: resource.clone(),
                arguments: arguments.clone(),
                status: TaskStatus::Active,
                deadline: None,
            })
            .await?;
        ctx.storage.add_child(*parent, NodeId::Task(*task)).await?;
        // Feed the external call to the task service (the storage fold is pure; this is the side
        // effect). The service needs the owning entry's stream/cause identity to re-envelope the
        // `CompleteTask` it fires on settle; the Processor supplies these via the context.
        ctx.task_service.invoke(
            *task,
            resource.clone(),
            arguments.clone(),
            ctx.stream_id,
            ctx.cause_id,
        );
        Ok(())
    }
}
