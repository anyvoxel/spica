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
        if let Some(mut exec) = ctx
            .storage
            .get_execution(&execution.meta.object_ref())
            .await?
        {
            exec.status = ExecutionStatus::Completing;
            exec.output = execution.output.clone();
            // The row's timing facts are the domain value's `meta`; the event carries the handler's
            // stamp for this transition, so it rides along with the status/output update.
            exec.meta.updated_at = execution.meta.updated_at;
            ctx.storage.put_execution(exec).await?;
        }
        Ok(())
    }
}
