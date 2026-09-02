//! `ExecutionTerminating` event projection: folds the `Event::ExecutionTerminating` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::{Execution, ExecutionStatus};

#[derive(Default)]
pub(crate) struct ExecutionTerminatingApplier;
#[async_trait]
impl EventApplier for ExecutionTerminatingApplier {
    fn event(&self) -> Event {
        Event::ExecutionTerminating {
            execution: Execution {
                flow_version: crate::types::meta::ObjectReference::nil(),
                status: ExecutionStatus::Terminating(
                    crate::types::command::TerminationReason::Cancelled,
                ),
                input: Default::default(),
                output: None,
                meta: crate::types::meta::ObjectMeta::born_placeholder(
                    crate::types::meta::ObjectKind::Execution,
                    ulid::Ulid::nil(),
                    crate::log::Timestamp::from_millis(0),
                ),
            },
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::ExecutionTerminating { execution } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut exec) = ctx.storage.get_execution(&execution.reference()).await? {
            exec.status = execution.status.clone();
            exec.value.meta.updated_at = execution.meta.updated_at;
            exec.touch(ctx.timestamp);
            ctx.storage.put_execution(exec).await?;
        }
        Ok(())
    }
}
