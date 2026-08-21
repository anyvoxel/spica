//! `VariablesAssigned` event projection: folds the `Event::VariablesAssigned` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::ExecutionId;

#[derive(Default)]
pub(crate) struct VariablesAssignedApplier;
#[async_trait]
impl EventApplier for VariablesAssignedApplier {
    fn event(&self) -> Event {
        Event::VariablesAssigned {
            execution: ExecutionId::nil(),
            variables: Default::default(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::VariablesAssigned {
            execution,
            variables,
        } = event
        else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut exec) = ctx.storage.get_execution(*execution).await? {
            // Variable assignment is a projection concern in the C-lite model: execution lifecycle
            // events stay focused on identity/status, while the mutable execution variables are folded
            // here as a full snapshot for later JSONata evaluation and replay.
            exec.variables = variables.clone();
            exec.touch(ctx.timestamp);
            ctx.storage.put_execution(exec).await?;
        }
        Ok(())
    }
}
