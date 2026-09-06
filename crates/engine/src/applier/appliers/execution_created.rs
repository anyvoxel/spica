//! `ExecutionCreated` event projection: folds the `Event::ExecutionCreated` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::types::meta::ObjectReference;
use crate::{Execution, ExecutionStatus};

#[derive(Default)]
pub(crate) struct ExecutionCreatedApplier;
#[async_trait]
impl EventApplier for ExecutionCreatedApplier {
    fn event(&self) -> Event {
        Event::ExecutionCreated {
            request_id: crate::types::id::RequestId::nil(),
            execution: Execution {
                flow_version: ObjectReference::nil(),
                status: ExecutionStatus::Running,
                input: Default::default(),
                output: None,
                meta: crate::types::meta::ObjectMeta::builder(
                    crate::types::meta::ObjectKind::Execution,
                    ulid::Ulid::nil(),
                )
                .at(crate::log::Timestamp::from_millis(0))
                .build(),
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
        let mut exec = crate::storage::ExecutionRecord::from_value(
            execution.clone(),
            std::collections::HashSet::new(),
        );
        // Birth: the row's `created_at`/`updated_at` are stamped with the `ExecutionCreated` entry's
        // moment (deterministic across replicas — see `ApplierContext::timestamp`).
        exec.born(ctx.timestamp);
        ctx.storage.put_execution(exec).await?;
        // A child execution (a Parallel branch) is added to its owner's `active_children` so the
        // owner drains (Completing/Terminating) waits on it via the shared cascade, and the inline
        // drain reaction notices it as in-flight. The top-level run (no owner) is owned by nothing.
        if let Some(owner) = execution.meta.owner.clone() {
            ctx.storage.add_child(owner, execution.reference()).await?;
        }
        Ok(())
    }
}
