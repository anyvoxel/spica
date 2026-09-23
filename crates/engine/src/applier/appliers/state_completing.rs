//! `StateCompleting` event projection: folds the `Event::StateCompleting` activity value into Storage.

use crate::types::error::ExecutionError;
use crate::{Activity, ApplierContext};

#[derive(Default)]
pub(crate) struct StateCompletingApplier;
impl StateCompletingApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        activity: &Activity,
    ) -> Result<(), ExecutionError> {
        if let Some(act) = ctx.storage.get_activity(&activity.reference()).await? {
            // An update, not a birth: carry the row's `created_at` over and stamp `updated_at`.
            let mut row =
                crate::storage::ActivityRecord::from_value(activity.clone(), act.active_children);
            row.created_at = act.created_at;
            row.updated_at = ctx.timestamp;
            ctx.storage.put_activity(row).await?;
        }
        Ok(())
    }
}
