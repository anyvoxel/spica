//! `ExecutionCreated` event projection: folds the `Event::ExecutionCreated` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{ExecutionId, NodeId};
use crate::{ExecutionStatus, ExecutionValue};

#[derive(Default)]
pub(crate) struct ExecutionCreatedApplier;
#[async_trait]
impl EventApplier for ExecutionCreatedApplier {
    fn event(&self) -> Event {
        Event::ExecutionCreated {
            request_id: crate::id::RequestId::nil(),
            execution: ExecutionValue {
                id: ExecutionId::nil(),
                flow_version_id: crate::id::FlowVersionId::nil(),
                root_execution: ExecutionId::nil(),
                parent: None,
                state_path: None,
                status: ExecutionStatus::Running,
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
        let Event::ExecutionCreated { execution, .. } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        let mut exec = crate::storage::Execution::from_value(
            execution.clone(),
            std::collections::HashSet::new(),
        );
        // Birth: the row's `created_at`/`updated_at` are stamped with the `ExecutionCreated` entry's
        // moment (deterministic across replicas — see `ApplierContext::timestamp`).
        exec.born(ctx.timestamp);
        ctx.storage.put_execution(exec).await?;
        // A child execution (a Parallel branch) is added to its owner's `active_children` so the
        // owner drains (Completing/Terminating) waits on it via the shared cascade, and the
        // `ProcessChildCompleted` drain notices it as in-flight. The top-level run (`parent: None`)
        // is owned by nothing and adds nothing.
        if let Some(parent) = execution.parent {
            ctx.storage
                .add_child(parent, NodeId::Execution(execution.id))
                .await?;
        }
        Ok(())
    }
}
