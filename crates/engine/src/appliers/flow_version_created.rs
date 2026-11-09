//! `FlowVersionCreated` event projection: folds a new flow version into Storage and advances the
//! owning `Flow`'s `latest_version` counter.

use crate::ApplierContext;
use crate::types::error::ExecutionError;
use crate::types::event::FlowVersionCreated;
use crate::types::flow::Flow;
use crate::types::flow::FlowStatus;

#[derive(Default)]
pub(crate) struct FlowVersionCreatedApplier;
impl FlowVersionCreatedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &FlowVersionCreated,
    ) -> Result<(), ExecutionError> {
        // `request_id` is a routing-only correlation key (the awaiting caller's ack); the projection
        // only records the version + advances the flow pointer, so it is ignored here.
        let FlowVersionCreated { flow_version, .. } = event;

        // Persist the version under its canonical object name (`{flow_name}-{version}`, executions
        // bind to its reference); the name-keyed row makes `flow_version_of` / a prefix scan resolve
        // a flow's versions in ordinal order without a separate index.
        ctx.storage.put_flow_version(flow_version.clone()).await?;

        // Advance the owning `Flow`'s `latest_version` counter so the newest ordinal is an O(1)
        // point read. A brand-new flow's `FlowCreated` row is applied earlier in the same batch
        // (with its initial counter), so `get_flow_by_name` normally finds it; get-or-create is a
        // defensive fallback for a directly-applied (non-batch) stream where the birth event may not
        // have preceded — kept replay-safe by keying the fallback off the version's own name/ids.
        // A persisted version always carries its owning flow, so this read is the one place the owner
        // is taken: its name locates — or, as a defensive fallback, names the reconstructed — `Flow`
        // row, and its uid is that flow's incarnation.
        let owner = flow_version.flow_owner();
        // The owning flow's uid (post-`create_flow`) is the flow's real incarnation uid — every object
        // has its own uid, so a reconstructed row must use the same one, not a nil sentinel, to stay
        // consistent with the normal path.
        let mut flow = match ctx
            .storage
            .get_flow_by_name(flow_version.flow_name())
            .await?
        {
            Some(existing) => existing,
            None => Flow {
                meta: crate::types::meta::ObjectMeta::builder(owner.uid())
                    // The flow's name IS its owner's name (a plain user name), so the reconstructed row
                    // takes it verbatim rather than re-parsing it back out of the version's address.
                    .name(owner.name().clone())
                    .at(flow_version.meta.created_at)
                    // A flow is a root of its own tree: its owner slot is `NoOwner` by type.
                    .with_owner(crate::types::meta::NoOwner::new()),
                status: FlowStatus::Active,
                latest_version: flow_version.version,
            },
        };
        flow.latest_version = flow_version.version;
        // A new version is a flow *update*: advance the pointer and stamp the write moment.
        flow.meta.with_update_at(ctx.timestamp);
        ctx.storage.put_flow(flow).await?;
        Ok(())
    }
}
