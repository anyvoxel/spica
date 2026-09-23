//! `StateTerminated` event projection: folds the `Event::StateTerminated` activity value into Storage.

use crate::types::error::ExecutionError;
use crate::{Activity, ApplierContext};

use crate::types::meta::ObjectKind;

#[derive(Default)]
pub(crate) struct StateTerminatedApplier;
impl StateTerminatedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        activity: &Activity,
    ) -> Result<(), ExecutionError> {
        if let Some(act) = ctx.storage.get_activity(&activity.reference()).await? {
            let parent = act
                .value
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner");
            // An update, not a birth: carry the row's `created_at` over and stamp `updated_at`.
            let mut row =
                crate::storage::ActivityRecord::from_value(activity.clone(), act.active_children);
            row.created_at = act.created_at;
            row.updated_at = ctx.timestamp;
            ctx.storage.put_activity(row).await?;
            ctx.storage
                .remove_child(parent.clone(), activity.reference())
                .await?;
            if parent.kind == ObjectKind::Execution
                && let Some(mut exec) = ctx.storage.get_execution(&parent).await?
            {
                // Mirror `StateCompleted`: once the owned activity terminates, the execution-level
                // projection cursor must be cleared so the unwind sees no stale "current" state.
                exec.current_activity = None;
                exec.with_update_at(ctx.timestamp);
                ctx.storage.put_execution(exec).await?;
            }
        }
        Ok(())
    }
}
