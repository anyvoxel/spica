//! `FlowVersionCreated` event projection: folds a new flow version into Storage and advances the
//! owning `Flow`'s `latest_version` counter.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::flow::Flow;
use crate::types::flow::FlowStatus;
use crate::{ApplierContext, EventApplier};

#[derive(Default)]
pub(crate) struct FlowVersionCreatedApplier;
#[async_trait]
impl EventApplier for FlowVersionCreatedApplier {
    fn event(&self) -> Event {
        Event::FlowVersionCreated {
            request_id: crate::types::id::RequestId::nil(),
            flow_version: crate::types::flow_version::FlowVersion {
                meta: crate::types::meta::ObjectMeta::builder(
                    crate::types::meta::ObjectKind::FlowVersion,
                    ulid::Ulid::nil(),
                )
                .at(crate::log::Timestamp::from_millis(0))
                .build(),
                version: 0,
                definition: String::new(),
                checksum: crate::types::flow_version::FlowVersion::definition_checksum(""),
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

        // Persist the version under its canonical object name (`{flow_name}-{version}`, executions
        // bind to its reference); the name-keyed row makes `flow_version_of` / a prefix scan resolve
        // a flow's versions in ordinal order without a separate index.
        ctx.storage.put_flow_version(flow_version.clone()).await?;

        // Advance the owning `Flow`'s `latest_version` counter so the newest ordinal is an O(1)
        // point read. A brand-new flow's `FlowCreated` row is applied earlier in the same batch
        // (with its initial counter), so `get_flow_by_name` normally finds it; get-or-create is a
        // defensive fallback for a directly-applied (non-batch) stream where the birth event may not
        // have preceded — kept replay-safe by keying the fallback off the version's own name/ids.
        // A persisted version always carries its owning flow in `meta.owner`; read its name once to
        // locate — or, as a defensive fallback, reconstruct — the owning `Flow` row.
        let flow_name = flow_version
            .flow_name()
            .expect("a persisted FlowVersion always carries its owning Flow");
        // The owning flow's uid comes from the version's owner reference, which (post-`create_flow`)
        // carries the flow's real incarnation uid — every object has its own uid, so a reconstructed
        // row must use the same one, not a nil sentinel, to stay consistent with the normal path.
        let flow_uid = flow_version
            .meta
            .owner
            .as_ref()
            .map(|o| o.uid)
            .unwrap_or_else(ulid::Ulid::nil);
        let mut flow = match ctx.storage.get_flow_by_name(flow_name.clone()).await? {
            Some(existing) => existing,
            None => Flow {
                meta: crate::types::meta::ObjectMeta::builder(
                    crate::types::meta::ObjectKind::Flow,
                    flow_uid,
                )
                .name(
                    crate::types::meta::ObjectName::plain(flow_name.as_str())
                        .expect("a valid FlowName is a valid user object name"),
                )
                .at(flow_version.meta.created_at)
                .build(),
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
