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
        // Only an existing row is updated — see `state_activated.rs`.
        if ctx
            .storage
            .get_activity(&activity.meta.object_ref())
            .await?
            .is_some()
        {
            ctx.storage.put_activity(activity.clone()).await?;
        }
        Ok(())
    }
}
