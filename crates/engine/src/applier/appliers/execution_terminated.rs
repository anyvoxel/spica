//! `ExecutionTerminated` event projection: folds the `Event::ExecutionTerminated` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::{Execution, ExecutionStatus};

#[derive(Default)]
pub(crate) struct ExecutionTerminatedApplier;
#[async_trait]
impl EventApplier for ExecutionTerminatedApplier {
    fn event(&self) -> Event {
        Event::ExecutionTerminated {
            execution: Execution {
                flow_version: crate::types::meta::ObjectReference::nil(),
                status: ExecutionStatus::Terminated(
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
        let Event::ExecutionTerminated { execution } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut exec) = ctx.storage.get_execution(&execution.reference()).await? {
            exec.status = execution.status.clone();
            exec.value.meta.updated_at = execution.meta.updated_at;
            // Termination also clears the projection-only active cursor; no state remains current
            // once the execution itself has reached a terminal abnormal finish.
            exec.current_activity = None;
            let parent = exec.value.meta.owner.clone();
            exec.touch(ctx.timestamp);
            ctx.storage.put_execution(exec).await?;
            if let Some(owner) = parent {
                ctx.storage
                    .remove_child(owner, execution.reference())
                    .await?;
            }
        }
        Ok(())
    }
}
