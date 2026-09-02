//! `VariablesAssigned` event projection: folds the `Event::VariablesAssigned` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::meta::ObjectKind;
use crate::{ApplierContext, EventApplier};

#[derive(Default)]
pub(crate) struct VariablesAssignedApplier;
#[async_trait]
impl EventApplier for VariablesAssignedApplier {
    fn event(&self) -> Event {
        Event::VariablesAssigned {
            scope: crate::types::meta::ObjectReference::nil(),
            variables: Default::default(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::VariablesAssigned { scope, variables } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        // Assign targets a *scope* — an Execution or a fan-out Thread. Dispatch on the reference's
        // structural kind so a branch's Assign lands on its Thread's variable snapshot instead of
        // being silently dropped (the former Execution-only path missed a Thread reference in the
        // execution table). The kind is the authoritative disambiguator here, exactly as `load_scope`
        // centralizes it for the handler path. Variable assignment is a projection concern in the
        // C-lite model: lifecycle events stay focused on identity/status, while the scope's mutable
        // variables are folded here as a full snapshot for later JSONata evaluation and replay.
        match scope.kind {
            ObjectKind::Execution => {
                if let Some(mut exec) = ctx.storage.get_execution(scope).await? {
                    exec.variables = variables.clone();
                    exec.touch(ctx.timestamp);
                    ctx.storage.put_execution(exec).await?;
                }
            }
            ObjectKind::Thread => {
                if let Some(mut thread) = ctx.storage.get_thread(scope).await? {
                    thread.variables = variables.clone();
                    thread.touch(ctx.timestamp);
                    ctx.storage.put_thread(thread).await?;
                }
            }
            // A non-scope kind carries no variable store; a late/odd Assign is a no-op.
            _ => {}
        }
        Ok(())
    }
}
