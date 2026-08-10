//! `ExecutionCreated` event projection: folds the `Event::ExecutionCreated` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{ExecutionId, NodeId};
use crate::scope::Scope;
use crate::storage::ExecutionStatus;

#[derive(Default)]
pub(crate) struct ExecutionCreatedApplier;
#[async_trait]
impl EventApplier for ExecutionCreatedApplier {
    fn event(&self) -> Event {
        Event::ExecutionCreated {
            id: ExecutionId::nil(),
            root_execution: ExecutionId::nil(),
            parent: None,
            state_path: None,
            input: Default::default(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::ExecutionCreated {
            id,
            root_execution,
            parent,
            state_path,
            input,
        } = event
        else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        ctx.storage
            .put_execution(crate::storage::Execution {
                id: *id,
                // A child execution inherits the top-level run's id (never its own) so the whole
                // tree shares one flat query anchor. The top-level run's `CreateExecution` sets
                // `root_execution = itself`.
                root_execution: *root_execution,
                parent: *parent,
                state_path: state_path.clone(),
                status: ExecutionStatus::Running,
                current_state: None,
                current_activity: None,
                scope: Scope::new(),
                input: input.clone(),
                output: None,
                active_children: std::collections::HashSet::new(),
            })
            .await?;
        // A child execution (a Parallel branch) is added to its owner's `active_children` so the
        // owner drains (Completing/Terminating) waits on it via the shared cascade, and the
        // `ProcessChildCompleted` drain notices it as in-flight. The top-level run (`parent: None`)
        // is owned by nothing and adds nothing.
        if let Some(parent) = parent {
            ctx.storage
                .add_child(*parent, NodeId::Execution(*id))
                .await?;
        }
        Ok(())
    }
}
