//! `ExecutionCompleted` event projection: folds the `Event::ExecutionCompleted` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{ExecutionId, NodeId};
use crate::storage::Status;

#[derive(Default)]
pub(crate) struct ExecutionCompletedApplier;
#[async_trait]
impl EventApplier for ExecutionCompletedApplier {
    fn event(&self) -> Event {
        Event::ExecutionCompleted {
            id: ExecutionId::nil(),
            output: Default::default(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::ExecutionCompleted { id, output } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut exec) = ctx.storage.get_execution(*id).await? {
            exec.status = Status::Completed;
            exec.output = Some(output.clone());
            exec.current_activity = None;
            let parent = exec.parent;
            ctx.storage.put_execution(exec).await?;
            if let Some(parent) = parent {
                ctx.storage
                    .remove_child(parent, NodeId::Execution(*id))
                    .await?;
            }
        }
        Ok(())
    }
}
