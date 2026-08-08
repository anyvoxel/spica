//! `ExecutionTerminated` event projection: folds the `Event::ExecutionTerminated` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{ExecutionId, NodeId};
use crate::storage::Status;

#[derive(Default)]
pub(crate) struct ExecutionTerminatedApplier;
#[async_trait]
impl EventApplier for ExecutionTerminatedApplier {
    fn event(&self) -> Event {
        Event::ExecutionTerminated {
            id: ExecutionId::nil(),
            reason: crate::command::TerminationReason::Cancelled,
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::ExecutionTerminated { id, reason } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut exec) = ctx.storage.get_execution(*id).await? {
            exec.status = Status::Terminated(reason.clone());
            exec.reason = Some(reason.clone());
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
