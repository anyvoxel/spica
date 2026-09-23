//! `StateActivated` event projection: folds the `Event::StateActivated` activity value onto Storage.

use crate::types::error::ExecutionError;
use crate::{Activity, ApplierContext};

#[derive(Default)]
pub(crate) struct StateActivatedApplier;
impl StateActivatedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        activity: &Activity,
    ) -> Result<(), ExecutionError> {
        let Some(act) = ctx.storage.get_activity(&activity.reference()).await? else {
            return Ok(());
        };
        // Activation mutates only the domain value itself (processed input and, for Map, its
        // activation product). The independently-maintained `active_children` set is preserved; the
        // row's `created_at` is carried over (this is an update, not a birth) and `updated_at` is
        // stamped with this entry's moment.
        let mut row =
            crate::storage::ActivityRecord::from_value(activity.clone(), act.active_children);
        row.created_at = act.created_at;
        row.updated_at = ctx.timestamp;
        ctx.storage.put_activity(row).await?;
        Ok(())
    }
}
