//! `StateActivating` event projection: folds the `Event::StateActivating` activity value into Storage.

use crate::types::error::ExecutionError;
use crate::{Activity, ApplierContext};

use crate::types::meta::HasRawObjectRef;

#[derive(Default)]
pub(crate) struct StateActivatingApplier;
impl StateActivatingApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        activity: &Activity,
    ) -> Result<(), ExecutionError> {
        // `StateActivating` is the creation moment of the projection row: the event already carries
        // the canonical domain entity, so the row is that value verbatim.
        ctx.storage.put_activity(activity.clone()).await?;
        super::bump_generated_seq(ctx.storage, &activity.meta.raw_object_ref().name).await?;
        ctx.storage
            .add_child(
                activity
                    .meta
                    .owner
                    .clone()
                    // A thread is the only thing that can own an activity, so the storage seam needs
                    // no `kind` guard — and storage speaks flat addresses.
                    .into_raw_object_ref(),
                activity.meta.raw_object_ref(),
            )
            .await?;
        Ok(())
    }
}
