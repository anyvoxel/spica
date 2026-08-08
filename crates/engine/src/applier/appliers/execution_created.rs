//! `ExecutionCreated` event projection: folds the `Event::ExecutionCreated` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::ExecutionId;
use crate::scope::Scope;
use crate::storage::Status;

#[derive(Default)]
pub(crate) struct ExecutionCreatedApplier;
#[async_trait]
impl EventApplier for ExecutionCreatedApplier {
    fn event(&self) -> Event {
        Event::ExecutionCreated {
            id: ExecutionId::nil(),
            input: Default::default(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::ExecutionCreated { id, input } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        ctx.storage
            .put_execution(crate::storage::Execution {
                id: *id,
                parent: None,
                status: Status::Running,
                current_state: None,
                current_activity: None,
                scope: Scope::new(),
                input: input.clone(),
                output: None,
                reason: None,
                pending_output: None,
                pending_reason: None,
                active_children: std::collections::HashSet::new(),
            })
            .await
    }
}
