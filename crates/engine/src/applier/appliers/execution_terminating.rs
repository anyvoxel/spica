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
        if let Some(mut exec) = ctx
            .storage
            .get_execution(&execution.meta.object_ref())
            .await?
        {
            exec.status = execution.status.clone();
            exec.meta.updated_at = execution.meta.updated_at;
            ctx.storage.put_execution(exec).await?;
        }
        Ok(())
    }
}
