//! `StateActivating` event projection: folds the `Event::StateActivating` activity value into Storage.

use crate::types::error::ExecutionError;
use crate::{Activity, ApplierContext};

use crate::storage::ActivityRecord;

#[derive(Default)]
pub(crate) struct StateActivatingApplier;
impl StateActivatingApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        activity: &Activity,
    ) -> Result<(), ExecutionError> {
        // `StateActivating` is the creation moment of the projection row: the event already carries
        // the canonical domain entity, and storage only adds its projection-only `active_children`
        // bookkeeping alongside it.
        let mut row =
            ActivityRecord::from_value(activity.clone(), std::collections::HashSet::new());
        // Birth: `created_at`/`updated_at` stamped with the `StateActivating` entry's moment.
        row.born(ctx.timestamp);
        ctx.storage.put_activity(row).await?;
        super::bump_generated_seq(ctx.storage, &activity.reference().name).await?;
        ctx.storage
            .add_child(
                activity
                    .meta
                    .owner
                    .clone()
                    .expect("an owned activity has an owner"),
                activity.reference(),
            )
            .await?;
        Ok(())
    }
}
