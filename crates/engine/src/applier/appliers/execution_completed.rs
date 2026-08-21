//! `ExecutionCompleted` event projection: folds the `Event::ExecutionCompleted` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{ExecutionId, NodeId};
use crate::{ExecutionStatus, ExecutionValue};

#[derive(Default)]
pub(crate) struct ExecutionCompletedApplier;
#[async_trait]
impl EventApplier for ExecutionCompletedApplier {
    fn event(&self) -> Event {
        Event::ExecutionCompleted {
            execution: ExecutionValue {
                id: ExecutionId::nil(),
                flow_version_id: crate::id::FlowVersionId::nil(),
                root_execution: ExecutionId::nil(),
                parent: None,
                state_path: None,
                status: ExecutionStatus::Completed,
                input: Default::default(),
                output: Some(Default::default()),
            },
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::ExecutionCompleted { execution } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut exec) = ctx.storage.get_execution(execution.id).await? {
            exec.status = ExecutionStatus::Completed;
            exec.output = execution.output.clone();
            // A terminal execution cannot still own an in-flight state activation cursor.
            exec.current_activity = None;
            let parent = exec.parent;
            exec.touch(ctx.timestamp);
            ctx.storage.put_execution(exec).await?;
            if let Some(parent) = parent {
                ctx.storage
                    .remove_child(parent, NodeId::Execution(execution.id))
                    .await?;
            }
        }
        Ok(())
    }
}
