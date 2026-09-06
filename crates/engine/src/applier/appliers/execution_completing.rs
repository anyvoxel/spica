//! `ExecutionCompleting` event projection: folds the `Event::ExecutionCompleting` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::types::meta::ObjectReference;
use crate::{Execution, ExecutionStatus};

#[derive(Default)]
pub(crate) struct ExecutionCompletingApplier;
#[async_trait]
impl EventApplier for ExecutionCompletingApplier {
    fn event(&self) -> Event {
        Event::ExecutionCompleting {
            execution: Execution {
                flow_version: ObjectReference::nil(),
                status: ExecutionStatus::Completing,
                input: Default::default(),
                output: Some(Default::default()),
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
        let Event::ExecutionCompleting { execution } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut exec) = ctx.storage.get_execution(&execution.reference()).await? {
            exec.status = ExecutionStatus::Completing;
            exec.output = execution.output.clone();
            // Keep the projected domain value's `updated_at` in step with the event's (which the
            // handler stamped at construction); the record's own `updated_at` is separately touched
            // from the entry timestamp below.
            exec.value.meta.updated_at = execution.meta.updated_at;
            exec.with_update_at(ctx.timestamp);
            ctx.storage.put_execution(exec).await?;
        }
        Ok(())
    }
}
