//! `FlowCreated` event projection: folds the birth of a flow aggregate into Storage.

use crate::ApplierContext;
use crate::types::error::ExecutionError;
use crate::types::event::FlowCreated;

#[derive(Default)]
pub(crate) struct FlowCreatedApplier;
impl FlowCreatedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &FlowCreated,
    ) -> Result<(), ExecutionError> {
        // `request_id` is a routing-only correlation key (the awaiting caller's ack); the projection
        // only records the flow row, so it is ignored here.
        let FlowCreated { flow, .. } = event;
        // Upsert the authoritative flow row by its immutable name. `FlowCreated` fires only on a new
        // name (see the event docs), and the `FlowVersionCreatedApplier` co-applied in the same batch
        // later advances `latest_version` to this version — the flow row carries the initial
        // counter here, and the version applier reconciles it (a replay-safe ordering).
        ctx.storage.put_flow(flow.clone()).await?;
        Ok(())
    }
}
