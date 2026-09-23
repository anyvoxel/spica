//! `ExecutionTerminating` event projection: folds the `Event::ExecutionTerminating` into Storage.

use crate::ApplierContext;
use crate::types::error::ExecutionError;

use crate::Execution;

#[derive(Default)]
pub(crate) struct ExecutionTerminatingApplier;
impl ExecutionTerminatingApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        execution: &Execution,
    ) -> Result<(), ExecutionError> {
        if let Some(mut exec) = ctx.storage.get_execution(&execution.reference()).await? {
            exec.status = execution.status.clone();
            exec.value.meta.updated_at = execution.meta.updated_at;
            exec.with_update_at(ctx.timestamp);
            ctx.storage.put_execution(exec).await?;
        }
        Ok(())
    }
}
