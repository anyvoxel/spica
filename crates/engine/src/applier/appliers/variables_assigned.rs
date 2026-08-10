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
            assignments: Default::default(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::VariablesAssigned {
            execution,
            assignments,
        } = event
        else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut exec) = ctx.storage.get_execution(*execution).await? {
            for (k, v) in assignments {
                exec.scope.insert(k.clone(), v.clone());
            }
            ctx.storage.put_execution(exec).await?;
        }
        Ok(())
    }
}
