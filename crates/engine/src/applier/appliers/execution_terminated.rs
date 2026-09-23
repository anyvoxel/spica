//! `ExecutionTerminated` event projection: folds the `Event::ExecutionTerminated` into Storage.

use crate::ApplierContext;
use crate::types::error::ExecutionError;

use crate::Execution;

#[derive(Default)]
pub(crate) struct ExecutionTerminatedApplier;
impl ExecutionTerminatedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        execution: &Execution,
    ) -> Result<(), ExecutionError> {
        if let Some(mut exec) = ctx.storage.get_execution(&execution.reference()).await? {
            exec.status = execution.status.clone();
            exec.value.meta.updated_at = execution.meta.updated_at;
            // Termination also clears the projection-only active cursor; no state remains current
            // once the execution itself has reached a terminal abnormal finish.
            exec.current_activity = None;
            let parent = exec.value.meta.owner.clone();
            exec.with_update_at(ctx.timestamp);
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
