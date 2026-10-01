//! `VariablesAssigned` event projection: folds the `Event::VariablesAssigned` into Storage.

use crate::ApplierContext;
use crate::types::error::ExecutionError;
use crate::types::event::VariablesAssigned;

#[derive(Default)]
pub(crate) struct VariablesAssignedApplier;
impl VariablesAssignedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &VariablesAssigned,
    ) -> Result<(), ExecutionError> {
        let VariablesAssigned { scope, variables } = event;
        // An `Assign` targets the scope the assigning activity lives in, and that scope is always a
        // Thread — the run's derived root thread for a top-level state, a branch Thread for a
        // `Parallel`/`Map` branch (see `ActivityKind::OwnedBy`). Assignment is a projection concern:
        // lifecycle events stay focused on identity/status, while the scope's mutable variables are
        // folded here as a full snapshot for later JSONata evaluation and replay.
        if let Some(mut thread) = ctx.storage.get_thread(scope).await? {
            thread.variables = variables.clone();
            thread.with_update_at(ctx.timestamp);
            ctx.storage.put_thread(thread).await?;
        }
        Ok(())
    }
}
