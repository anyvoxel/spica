//! The persisted definition entity: one immutable published version of a flow.

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::types::flow::FlowKind;
use crate::types::id::FlowName;
use crate::types::meta::{ObjectKind, ObjectKindMarker, ObjectMeta, ObjectName, ObjectRef};

/// The [`ObjectKindMarker`] tying a [`FlowVersion`]'s meta to [`ObjectKind::FlowVersion`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowVersionKind;

impl ObjectKindMarker for FlowVersionKind {
    const KIND: ObjectKind = ObjectKind::FlowVersion;
    /// A version is published by exactly one flow — it *is* that flow's snapshot, and a version is
    /// never re-parented — so the slot names that one kind rather than a union: a version owned by
    /// anything but a [`Flow`](crate::Flow) is unrepresentable.
    type OwnedBy = ObjectRef<FlowKind>;
}

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
/// `meta.uid` (the version's never-reused ulid) — [`Self::raw_object_ref`] bundles them into an
/// [`RawObjectRef`] that any consumer can address it by.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowVersion {
    /// Shared identity + timing metadata. **`meta.name` IS the version's addressing key**:
    /// `{flow_name}-{version}` (generated `ObjectName`, minted at birth — see [`Self::version_name`]);
    /// **`meta.uid` IS the version's never-reused identity ulid**. `meta.owner` names the owning
    /// `Flow` (a persisted version always carries one). The domain `created_at` lives inside `meta`
    /// (versions are immutable — no `updated_at`).
    pub meta: ObjectMeta<FlowVersionKind>,
    /// This version's ordinal within its flow (1, 2, 3, … — incremented on each create).
    pub version: u32,
    /// The ASL state machine definition this version publishes, as its raw JSON string form.
    /// Stored as a string so the durable record is the exact definition the user submitted; it is
    /// parsed into a `StateMachine` where an execution needs it (`HandlerContext::machine`).
    pub definition: String,
    /// CRC-64/ECMA checksum of `definition`'s bytes — a cheap way to detect whether two versions
    /// hold byte-identical content without comparing the (possibly large) raw strings. See
    /// [`Self::definition_checksum`].
    #[serde(default)]
    pub checksum: u64,
}

impl FlowVersion {
    /// The addressing `ObjectName` of version `version` under flow `name`: `{name}-{version}` with a
    /// plain **decimal** ordinal (e.g. `order-1`, `order-10`). Versions of one flow do **not** order
    /// by name — decimal lexicographic order diverges from numeric (`order-10` sorts before
    /// `order-2`) — so a storage prefix scan over `{name}-` enumerates them and callers order by the
    /// `version` field, never by key order. Version 0 is the reserved "latest" sentinel in
    /// resolution, so ordinal 0 names never occur in storage.
    pub fn version_name(flow: &FlowName, version: u32) -> ObjectName {
        flow.generated_from_key(u64::from(version))
    }

    /// The owning flow, read from this version's `meta.owner` — the one owner read. A version is
    /// minted together with its owner (`create_flow`) and the slot is never cleared, so the slot's own
    /// type already guarantees a flow is there.
    pub fn flow_owner(&self) -> &ObjectRef<FlowKind> {
        &self.meta.owner
    }

    /// The owning flow's addressing name — the flow's **user** name, read off the owner. It is not
    /// derivable from the version's own name, which is the *generated* `{flow_name}-{version}` (that
    /// is why the owner is where the flow's identity is read from).
    pub fn flow_name(&self) -> FlowName {
        self.flow_owner()
            .name()
            .as_plain()
            .cloned()
            .expect("a Flow's name is a user plain name, never a generated one")
    }

    /// A CRC-64/ECMA checksum of the raw definition bytes, as its native 64-bit value. A content
    /// fingerprint for consistency/identity checks: identical definitions checksum identically, and a
    /// change to any byte flips the value with overwhelming probability (2^-64 collision odds, fine
    /// for detecting drift between versions; not a security boundary).
    pub fn definition_checksum(definition: &str) -> u64 {
        let crc = crc::Crc::<u64>::new(&crc::CRC_64_ECMA_182);
        crc.checksum(definition.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::flow::Flow;
    use crate::types::meta::{ObjectName, ObjectRef};
    use spica_machinery::Timestamp;

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
            meta: ObjectMeta::builder(uid)
                .name(FlowVersion::version_name(&flow, 1))
                .at(Timestamp::from_millis(0))
                .with_owner(ObjectRef::new(
                    ObjectName::plain("order").unwrap(),
                    flow_uid,
                )),
            version: 1,
            definition: String::new(),
            checksum: FlowVersion::definition_checksum(""),
        };
        let r = version.meta.raw_object_ref();
        assert_eq!(r.kind, ObjectKind::FlowVersion);
        assert_eq!(r.name, version.meta.name);
        assert_eq!(r.uid, uid);
        // The flow's identity is read off the version's owner, never parsed out of the version's own
        // (generated) name.
        assert_eq!(version.flow_name(), flow);
        assert_eq!(version.flow_owner().uid(), flow_uid);
    }

    /// A version's owner slot admits a `Flow` and nothing else — a version *is* a flow's snapshot, and
    /// the flow it belongs to is read off this slot ([`FlowVersion::flow_owner`]), so a row whose
    /// owner carries another kind is refused rather than read into a version with a foreign parent.
    #[test]
    fn a_version_slot_admits_only_its_flow() {
        let flow = FlowName::new("order").unwrap();
        let owner = ObjectRef::new(ObjectName::plain("order").unwrap(), ulid::Ulid::new());
        let version = FlowVersion {
            meta: ObjectMeta::builder(ulid::Ulid::new())
                .name(FlowVersion::version_name(&flow, 1))
                .at(Timestamp::from_millis(0))
                .with_owner(owner.clone()),
            version: 1,
            definition: String::new(),
            checksum: FlowVersion::definition_checksum(""),
        };
        let mut json = serde_json::to_value(&version).expect("version serializes");
        assert_eq!(json["meta"]["owner"]["kind"], serde_json::json!("Flow"));
        let back: FlowVersion =
            serde_json::from_value(json.clone()).expect("the slot admits its own kind");
        assert_eq!(back.meta.owner, owner);

        json["meta"]["owner"]["kind"] = serde_json::json!("Execution");
        let err = serde_json::from_value::<FlowVersion>(json)
            .expect_err("a version is never owned by an execution");
        let msg = err.to_string();
        assert!(msg.contains("reference kind mismatch"), "{msg}");
        assert!(msg.contains("admits only Flow"), "{msg}");
    }

    #[test]
    fn a_foreign_kind_row_is_rejected_at_the_meta() {
        let flow = FlowName::new("order").unwrap();
        let version = FlowVersion {
            meta: ObjectMeta::builder(ulid::Ulid::new())
                .name(FlowVersion::version_name(&flow, 1))
                .at(Timestamp::from_millis(0))
                .with_owner(ObjectRef::new(
                    ObjectName::plain("order").unwrap(),
                    ulid::Ulid::new(),
                )),
            version: 1,
            definition: String::new(),
            checksum: FlowVersion::definition_checksum(""),
        };
        let json = serde_json::to_value(&version).expect("version serializes");
        // Every entity declares `meta` first, so the kind guard fires before any other field
        // mismatch: a row read through the wrong type names the two kinds, not a random field.
        let err =
            serde_json::from_value::<Flow>(json).expect_err("a FlowVersion row is not a Flow");
        let msg = err.to_string();
        assert!(msg.contains("kind mismatch"), "{msg}");
        assert!(msg.contains("carries kind FlowVersion"), "{msg}");
        assert!(msg.ends_with("expects Flow"), "{msg}");
    }

    #[test]
    fn definition_checksum_is_stable_and_content_sensitive() {
        // Identical content fingerprints identically (stable across calls and processes).
        assert_eq!(
            FlowVersion::definition_checksum("{\"StartAt\":\"A\"}"),
            FlowVersion::definition_checksum("{\"StartAt\":\"A\"}")
        );
        // A one-byte change flips the checksum.
        assert_ne!(
            FlowVersion::definition_checksum("{\"StartAt\":\"A\"}"),
            FlowVersion::definition_checksum("{\"StartAt\":\"B\"}")
        );
    }
}
