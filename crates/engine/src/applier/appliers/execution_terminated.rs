//! `ExecutionTerminated` event projection: folds the `Event::ExecutionTerminated` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{ExecutionId, NodeId};
use crate::{ExecutionStatus, ExecutionValue};

#[derive(Default)]
pub(crate) struct ExecutionTerminatedApplier;
#[async_trait]
impl EventApplier for ExecutionTerminatedApplier {
    fn event(&self) -> Event {
        Event::ExecutionTerminated {
            execution: ExecutionValue {
                id: ExecutionId::nil(),
                flow_version_id: crate::id::FlowVersionId::nil(),
                root_execution: ExecutionId::nil(),
                parent: None,
                state_path: None,
                status: ExecutionStatus::Terminated(crate::command::TerminationReason::Cancelled),
                input: Default::default(),
                output: None,
            },
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::ExecutionTerminated { execution } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut exec) = ctx.storage.get_execution(execution.id).await? {
            exec.status = execution.status.clone();
            // Termination also clears the projection-only active cursor; no state remains current
            // once the execution itself has reached a terminal abnormal finish.
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
