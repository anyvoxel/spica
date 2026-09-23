//! `ExecutionCompleting` event projection: folds the `Event::ExecutionCompleting` into Storage.

use crate::ApplierContext;
use crate::types::error::ExecutionError;

use crate::{Execution, ExecutionStatus};

#[derive(Default)]
pub(crate) struct ExecutionCompletingApplier;
impl ExecutionCompletingApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        execution: &Execution,
    ) -> Result<(), ExecutionError> {
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
