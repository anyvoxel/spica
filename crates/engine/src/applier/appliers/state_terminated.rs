//! `StateTerminated` event projection: folds the `Event::StateTerminated` activity value into Storage.

use crate::types::error::ExecutionError;
use crate::types::meta::HasRawObjectRef;
use crate::{Activity, ApplierContext};

#[derive(Default)]
pub(crate) struct StateTerminatedApplier;
impl StateTerminatedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        activity: &Activity,
    ) -> Result<(), ExecutionError> {
        if let Some(act) = ctx
            .storage
            .get_activity(&activity.meta.object_ref())
            .await?
        {
            let parent = act
                .meta
                .owner
                .clone()
                // A thread is the only thing that can own an activity, so the storage seam needs no
                // `kind` guard — and storage speaks flat addresses.
                .into_raw_object_ref();
            // An update, not a birth: the row keeps its identity and takes the event's value.
            ctx.storage.put_activity(activity.clone()).await?;
            ctx.storage
                .remove_child(parent, activity.meta.raw_object_ref())
                .await?;
        }
        Ok(())
    }
}
