//! `ExecutionCreated` event projection: folds the `Event::ExecutionCreated` into Storage.

use crate::ApplierContext;
use crate::types::error::ExecutionError;
use crate::types::event::ExecutionCreated;

#[derive(Default)]
pub(crate) struct ExecutionCreatedApplier;
impl ExecutionCreatedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &ExecutionCreated,
    ) -> Result<(), ExecutionError> {
        let ExecutionCreated { execution, .. } = event;
        let mut exec = crate::storage::ExecutionRecord::from_value(
            execution.clone(),
            std::collections::HashSet::new(),
        );
        // Birth: the row's `created_at`/`updated_at` are stamped with the `ExecutionCreated` entry's
        // moment (deterministic across replicas — see `ApplierContext::timestamp`).
        exec.born(ctx.timestamp);
        ctx.storage.put_execution(exec).await?;
        // No child edge to add: a run is the root of its own tree (its owner slot is `NoOwner`), and
        // everything it owns — its root thread, activities and timers — hangs below it.
        Ok(())
    }
}
