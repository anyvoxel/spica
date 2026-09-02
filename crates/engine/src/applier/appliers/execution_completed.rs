//! `ExecutionCompleted` event projection: folds the `Event::ExecutionCompleted` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::types::meta::ObjectReference;
use crate::{Execution, ExecutionStatus};

#[derive(Default)]
pub(crate) struct ExecutionCompletedApplier;
#[async_trait]
impl EventApplier for ExecutionCompletedApplier {
    fn event(&self) -> Event {
        Event::ExecutionCompleted {
            execution: Execution {
                flow_version: ObjectReference::nil(),
                status: ExecutionStatus::Completed,
                input: Default::default(),
                output: Some(Default::default()),
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
        let Event::ExecutionCompleted { execution } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut exec) = ctx.storage.get_execution(&execution.reference()).await? {
            exec.status = ExecutionStatus::Completed;
            exec.output = execution.output.clone();
            // Keep the projected domain `updated_at` in step with the event's (handler-stamped).
            exec.value.meta.updated_at = execution.meta.updated_at;
            // A terminal execution cannot still own an in-flight state activation cursor.
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
