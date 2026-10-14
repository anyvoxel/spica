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
        // Only an existing row is updated: activation mutates the domain value it was born with
        // (processed input and, for Map, its activation product), it never creates one. The activity's
        // in-flight children are ownership edges, untouched by this row write.
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
