//! `VariablesAssigned` event projection: folds the `Event::VariablesAssigned` into Storage.

use crate::ApplierContext;
use crate::types::error::ExecutionError;
use crate::types::event::VariablesAssigned;
use crate::types::meta::ObjectKind;

#[derive(Default)]
pub(crate) struct VariablesAssignedApplier;
impl VariablesAssignedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &VariablesAssigned,
    ) -> Result<(), ExecutionError> {
        let VariablesAssigned { scope, variables } = event;
        // Assign targets a *scope* — an Execution or a fan-out Thread — named by a bare reference, so
        // this applier is where the kind must be branched on: a branch's Assign then lands on its
        // Thread's variable snapshot instead of being dropped. Variable assignment is a projection
        // concern in the C-lite model: lifecycle events stay focused on identity/status, while the
        // scope's mutable variables are folded here as a full snapshot for later JSONata evaluation
        // and replay.
        match scope.kind {
            ObjectKind::Execution => {
                if let Some(mut exec) = ctx.storage.get_execution(scope).await? {
                    exec.variables = variables.clone();
                    exec.with_update_at(ctx.timestamp);
                    ctx.storage.put_execution(exec).await?;
                }
            }
            ObjectKind::Thread => {
                if let Some(mut thread) = ctx.storage.get_thread(scope).await? {
                    thread.variables = variables.clone();
                    thread.with_update_at(ctx.timestamp);
                    ctx.storage.put_thread(thread).await?;
                }
            }
            // A non-scope kind carries no variable store; a late/odd Assign is a no-op.
            _ => {}
        }
        Ok(())
    }
}
