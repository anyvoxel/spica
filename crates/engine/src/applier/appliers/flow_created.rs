//! `FlowCreated` event projection: folds the birth of a flow aggregate into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

#[derive(Default)]
pub(crate) struct FlowCreatedApplier;
#[async_trait]
impl EventApplier for FlowCreatedApplier {
    fn event(&self) -> Event {
        Event::FlowCreated {
            request_id: crate::types::id::RequestId::nil(),
            flow: crate::types::flow::Flow {
                meta: crate::types::meta::ObjectMeta::builder(
                    crate::types::meta::ObjectKind::Flow,
                    ulid::Ulid::nil(),
                )
                .name(
                    crate::types::meta::ObjectName::plain("default")
                        .expect("static placeholder name is valid"),
                )
                .at(crate::log::Timestamp::from_millis(0))
                .build(),
                status: crate::types::flow::FlowStatus::Active,
                latest_version: 0,
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
        // later advances `latest_version` to this version — the flow row carries the initial
        // counter here, and the version applier reconciles it (a replay-safe ordering).
        ctx.storage.put_flow(flow.clone()).await?;
        Ok(())
    }
}
