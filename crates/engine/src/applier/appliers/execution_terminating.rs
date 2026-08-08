//! `ExecutionTerminating` event projection: folds the `Event::ExecutionTerminating` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::ExecutionId;
use crate::storage::Status;

#[derive(Default)]
pub(crate) struct ExecutionTerminatingApplier;
#[async_trait]
impl EventApplier for ExecutionTerminatingApplier {
    fn event(&self) -> Event {
        Event::ExecutionTerminating {
            id: ExecutionId::nil(),
            reason: crate::command::TerminationReason::Cancelled,
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::ExecutionTerminating { id, reason } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut exec) = ctx.storage.get_execution(*id).await? {
            exec.status = Status::Terminating;
            exec.pending_reason = Some(reason.clone());
            ctx.storage.put_execution(exec).await?;
        }
        Ok(())
    }
}
