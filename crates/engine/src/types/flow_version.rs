//! The persisted definition entity: one immutable published version of a flow.

use serde::{Deserialize, Serialize};

use crate::types::id::FlowName;
use crate::types::meta::{ObjectKind, ObjectMeta, ObjectName, ObjectReference};

/// One immutable, published version of a logical flow — the durable object `Storage` persists and
/// executions bind to. It is the Zeebe analogue of a deployed `Process` (a specific
/// `processDefinitionKey`): the `definition` content plus the identity an execution references,
/// never the mutable flow aggregate itself.
///
/// Adding a version to the same [`FlowName`] produces a **new** `FlowVersion` with an incremented
/// `version` and an **`ObjectName`** `{flow_name}-{version}` (see [`Self::version_name`]), so:
/// - "updating a flow" is purely additive — a new immutable snapshot, old executions unaffected;
/// - deleting + re-creating a name later yields a brand-new flow incarnation and brand-new versions,
///   so no in-flight execution can ever be aliased onto the wrong definition.
///
/// `FlowVersion` is the single record that carries a full definition: it is the payload of
/// [`Event::FlowCreated`](crate::Event), folded into storage by the applier — the only
/// place a definition ever enters the stream (mirroring Zeebe's Deployment→Process record).
/// The definition is stored as its **raw ASL string** (the JSON-encoded form of a
/// [`StateMachine`](spica_asl::StateMachine)); the parsed model is a derived, transient value
/// (validated at the create boundary, parsed on demand at execution and cached).
///
/// Its identity is a k8s-style `meta.name` (`{flow_name}-{version}`, the **storage key**) plus a
/// `meta.uid` (the version's never-reused ulid) — [`Self::reference`] bundles them into an
/// [`ObjectReference`] that any consumer can address it by.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowVersion {
    /// Shared identity + timing metadata. **`meta.name` IS the version's addressing key**:
    /// `{flow_name}-{version}` (generated `ObjectName`, minted at birth — see [`Self::version_name`]);
    /// **`meta.uid` IS the version's never-reused identity ulid**. `meta.owner` names the owning
    /// `Flow` (a persisted version always carries one). The domain `created_at` lives inside `meta`
    /// (versions are immutable — no `updated_at`).
    pub meta: ObjectMeta,
    /// This version's ordinal within its flow (1, 2, 3, … — incremented on each create).
    pub version: u32,
    /// The ASL state machine definition this version publishes, as its raw JSON string form.
    /// Stored as a string so the durable record is the exact definition the user submitted; it is
    /// parsed into a `StateMachine` where an execution needs it ([`HandlerContext::machine`]).
    pub definition: String,
}

impl FlowVersion {
    /// The addressing `ObjectName` of version `version` under flow `name`: `{name}-{version}` with a
    /// plain **decimal** ordinal (e.g. `order-1`, `order-10`). Versions of one flow do **not** order
    /// by name — decimal lexicographic order diverges from numeric (`order-10` sorts before
    /// `order-2`) — so a storage prefix scan over `{name}-` enumerates them and callers order by the
    /// `version` field, never by key order. Version 0 is the reserved "latest" sentinel in
    /// resolution, so ordinal 0 names never occur in storage.
    pub fn version_name(flow: &FlowName, version: u32) -> ObjectName {
        ObjectName::generated_with_suffix(flow.as_str(), &version.to_string())
            .expect("a FlowName + decimal version ordinal is always a valid generated name")
    }

    /// This version's canonical [`ObjectReference`] — the `(name, uid)` pair a consumer uses to
    /// address it: `kind = FlowVersion`, `name = meta.name`, `uid = meta.uid`.
    pub fn reference(&self) -> ObjectReference {
        ObjectReference::new(
            ObjectKind::FlowVersion,
            self.meta.name.clone(),
            self.meta.uid,
        )
    }

    /// The owning flow's addressing name, read from this version's `meta.owner` (the owner's
    /// `name` is the flow's user name — see [`crate::types::meta::ObjectReference`]). `None` only for
    /// a version constructed without an owner (the applier's dispatch placeholder); a **persisted**
    /// version always carries one (set in `create_flow`), so storage keys on it with `expect`.
    pub fn flow_name(&self) -> Option<FlowName> {
        self.meta.owner.as_ref()?.name.as_flow_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::Timestamp;
    use crate::types::meta::OwnerReference;

    #[test]
    fn version_name_encodes_flow_and_decimal_ordinal() {
        let flow = FlowName::new("order").unwrap();
        assert_eq!(FlowVersion::version_name(&flow, 1).as_str(), "order-1");
        assert_eq!(FlowVersion::version_name(&flow, 255).as_str(), "order-255");
        // Plain decimal is human-readable but its lexicographic order diverges from numeric order:
        // "order-9" sorts after "order-10", so any prefixed enumeration orders by the `version`
        // field, not by name (see key.rs `flow_version_prefix`).
        assert!(
            FlowVersion::version_name(&flow, 9).as_str()
                > FlowVersion::version_name(&flow, 10).as_str()
        );
    }

    #[test]
    fn reference_bundles_name_and_uid_of_the_version() {
        let flow = FlowName::new("order").unwrap();
        let flow_uid = ulid::Ulid::new();
        let uid = ulid::Ulid::new();
        let version = FlowVersion {
            meta: ObjectMeta::born_named(
                ObjectKind::FlowVersion,
                FlowVersion::version_name(&flow, 1),
                uid,
                Timestamp::from_millis(0),
            )
            .with_owner(OwnerReference::new(
                ObjectKind::Flow,
                ObjectName::plain("order").unwrap(),
                flow_uid,
            )),
            version: 1,
            definition: String::new(),
        };
        let r = version.reference();
        assert_eq!(r.kind, ObjectKind::FlowVersion);
        assert_eq!(r.name, version.meta.name);
        assert_eq!(r.uid, uid);
        // flow_name derives from the owner reference, not the version's own name.
        assert_eq!(version.flow_name(), Some(flow));
    }
}
