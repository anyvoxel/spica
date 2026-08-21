//! `FlowCreated` event projection: folds the birth of a flow aggregate into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::id::FlowName;
use crate::{ApplierContext, EventApplier};

#[derive(Default)]
pub(crate) struct FlowCreatedApplier;
#[async_trait]
impl EventApplier for FlowCreatedApplier {
    fn event(&self) -> Event {
        Event::FlowCreated {
            request_id: crate::id::RequestId::nil(),
            flow: crate::flow::Flow {
                flow_id: crate::id::FlowId::nil(),
                name: FlowName::new("default").expect("static placeholder name is valid"),
                created_at: crate::log::Timestamp::from_millis(0),
                updated_at: crate::log::Timestamp::from_millis(0),
                status: crate::flow::FlowStatus::Active,
                latest_flow_version_id: crate::id::FlowVersionId::nil(),
            },
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        // `request_id` is a routing-only correlation key (the awaiting caller's ack); the projection
        // only records the flow row, so it is ignored here.
        let Event::FlowCreated { flow, .. } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        // Upsert the authoritative flow row by its immutable name. `FlowCreated` fires only on a new
        // name (see the event docs), and the `FlowVersionCreatedApplier` co-applied in the same batch
        // later advances `latest_flow_version_id` to this version — the flow row carries the initial
        // pointer here, and the version applier reconciles it (a replay-safe ordering).
        ctx.storage.put_flow(flow.clone()).await
    }
}
