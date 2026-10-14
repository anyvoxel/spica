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
        // folded here as a full snapshot for later JSONata evaluation and replay. Because the payload
        // *is* that snapshot, the fold is a pure overwrite of the scope's own cell — it reads neither
        // the scope cell nor the thread row, so no read-modify-write can interleave and the row's
        // lifecycle writes stay untouched.
        ctx.storage.put_variables(scope, variables.clone()).await?;
        Ok(())
    }
}
