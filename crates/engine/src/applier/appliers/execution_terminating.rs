//! `ExecutionTerminating` event projection: folds the `Event::ExecutionTerminating` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::ExecutionId;
use crate::{ExecutionStatus, ExecutionValue};

#[derive(Default)]
pub(crate) struct ExecutionTerminatingApplier;
#[async_trait]
impl EventApplier for ExecutionTerminatingApplier {
    fn event(&self) -> Event {
        Event::ExecutionTerminating {
            execution: ExecutionValue {
                id: ExecutionId::nil(),
                flow_version_id: crate::id::FlowVersionId::nil(),
                root_execution: ExecutionId::nil(),
                parent: None,
                state_path: None,
                status: ExecutionStatus::Terminating(crate::command::TerminationReason::Cancelled),
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
        let Event::ExecutionTerminating { execution } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut exec) = ctx.storage.get_execution(execution.id).await? {
            exec.status = execution.status.clone();
            exec.touch(ctx.timestamp);
            ctx.storage.put_execution(exec).await?;
        }
        Ok(())
    }
}
