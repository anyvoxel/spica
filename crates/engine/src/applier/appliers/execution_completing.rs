//! `ExecutionCompleting` event projection: folds the `Event::ExecutionCompleting` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::ExecutionId;
use crate::storage::Status;

#[derive(Default)]
pub(crate) struct ExecutionCompletingApplier;
#[async_trait]
impl EventApplier for ExecutionCompletingApplier {
    fn event(&self) -> Event {
        Event::ExecutionCompleting {
            id: ExecutionId::nil(),
            output: Default::default(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::ExecutionCompleting { id, output } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut exec) = ctx.storage.get_execution(*id).await? {
            exec.status = Status::Completing;
            exec.pending_output = Some(output.clone());
            ctx.storage.put_execution(exec).await?;
        }
        Ok(())
    }
}
