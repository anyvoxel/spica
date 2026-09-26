//! `StateCompleted` event projection: folds the `Event::StateCompleted` activity value into Storage.

use crate::types::error::ExecutionError;
use crate::{Activity, ApplierContext};

#[derive(Default)]
pub(crate) struct StateCompletedApplier;
impl StateCompletedApplier {
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
        }
        Ok(())
    }
}
