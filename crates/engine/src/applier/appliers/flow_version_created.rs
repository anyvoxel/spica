//! `FlowVersionCreated` event projection: folds a new flow version into Storage and advances the
//! owning `Flow`'s `latest_flow_version_id` pointer.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::flow::Flow;
use crate::flow::FlowStatus;
use crate::{ApplierContext, EventApplier};

#[derive(Default)]
pub(crate) struct FlowVersionCreatedApplier;
#[async_trait]
impl EventApplier for FlowVersionCreatedApplier {
    fn event(&self) -> Event {
        Event::FlowVersionCreated {
            request_id: crate::id::RequestId::nil(),
            flow_version: crate::flow_version::FlowVersion {
                flow_version_id: crate::id::FlowVersionId::nil(),
                flow_id: crate::id::FlowId::nil(),
                name: crate::id::FlowName::new("default")
                    .expect("static placeholder name is valid"),
                version: 0,
                definition: String::new(),
                created_at: crate::log::Timestamp::from_millis(0),
            },
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        // `request_id` is a routing-only correlation key (the awaiting caller's ack); the projection
        // only records the version + advances the flow pointer, so it is ignored here.
        let Event::FlowVersionCreated { flow_version, .. } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };

        // Persist the version under its never-reused `flow_version_id` (executions bind to it); the
        // store also indexes `(flow_id, version)` so `flow_version_of` / `latest_flow_version` /
        // `list_flow_versions` resolve without a scan.
        ctx.storage.put_flow_version(flow_version.clone()).await?;

        // Advance the owning `Flow`'s `latest_flow_version_id` so the newest version is an O(1)
        // point read. A brand-new flow's `FlowCreated` row is applied earlier in the same batch
        // (with its initial pointer), so `get_flow_by_name` normally finds it; get-or-create is a
        // defensive fallback for a directly-applied (non-batch) stream where the birth event may not
        // have preceded — kept replay-safe by keying the fallback off the version's own audit ids.
        let mut flow = match ctx
            .storage
            .get_flow_by_name(flow_version.name.clone())
            .await?
        {
            Some(existing) => existing,
            None => Flow {
                flow_id: flow_version.flow_id,
                name: flow_version.name.clone(),
                created_at: flow_version.created_at,
                // A flows' `updated_at` under a get-or-create fallback is stamped with this entry's
                // moment (the birth event normally sets it via `FlowCreated`; this is a replay-fallback).
                updated_at: ctx.timestamp,
                status: FlowStatus::Active,
                latest_flow_version_id: flow_version.flow_version_id,
            },
        };
        flow.latest_flow_version_id = flow_version.flow_version_id;
        // A new version is a flow *update*: advance the pointer and stamp the write moment.
        flow.updated_at = ctx.timestamp;
        ctx.storage.put_flow(flow).await?;
        Ok(())
    }
}
