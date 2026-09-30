//! `ExecutionCompleted` event projection: folds the `Event::ExecutionCompleted` into Storage.

use crate::ApplierContext;
use crate::types::error::ExecutionError;

use crate::{Execution, ExecutionStatus};

#[derive(Default)]
pub(crate) struct ExecutionCompletedApplier;
impl ExecutionCompletedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        execution: &Execution,
    ) -> Result<(), ExecutionError> {
        if let Some(mut exec) = ctx
            .storage
            .get_execution(&execution.meta.reference())
            .await?
        {
            exec.status = ExecutionStatus::Completed;
            exec.output = execution.output.clone();
            // Keep the projected domain `updated_at` in step with the event's (handler-stamped).
            exec.value.meta.updated_at = execution.meta.updated_at;
            exec.with_update_at(ctx.timestamp);
            // No parent edge to detach: a run has no owner (its slot is `NoOwner`).
            ctx.storage.put_execution(exec).await?;
        }
        Ok(())
    }
}
