//! The logical flow entity: one named, addressable aggregate that owns a sequence of
//! [`FlowVersion`](crate::FlowVersion)s.

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::Timestamp;
use crate::types::id::FlowName;
use crate::types::meta::{NoOwner, ObjectKind, ObjectKindMarker, ObjectMeta, ObjectName};

/// The lifecycle status of a [`Flow`]. Deletion is a `TODO` (M1/M2): a flow is created `Active`
/// and may become `Deleted` once a delete (cascade) operation lands — currently nothing transitions
/// it, so this field is future-proofing for the audit trail rather than a live state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowStatus {
    /// The flow exists and accepts new versions / executions.
    Active,
    /// The flow is deleted. Deleting is a **cascade**: it is only allowed once ever`FlowVersion`
    /// and every in-flight `Execution` under the flow has been cleaned up (so a re-created same-name
    /// flow can never alias a surviving revision or execution onto it).
    // TODO(delete): implement the cascade-delete command + the `Deleted` transition.
    Deleted,
}

/// The [`ObjectKindMarker`] tying a [`Flow`]'s meta to [`ObjectKind::Flow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowKind;

impl ObjectKindMarker for FlowKind {
    const KIND: ObjectKind = ObjectKind::Flow;
    /// A flow is a tenant-level root: it is addressed by name alone and hangs off nothing, so its
    /// owner slot can never be filled (see [`NoOwner`]) — [`ObjectMeta::with_owner`] does not even
    /// exist for it.
    type OwnedBy = NoOwner;
}

/// One logical flow — a named aggregate that groups a monotone sequence of immutable
/// [`FlowVersion`](crate::FlowVersion)s. Addressed by its immutable name (the **primary key**,
/// carried in [`meta.name`](crate::types::meta::ObjectMeta::name) as the `ObjectName`); `meta.uid`
/// is the flow's **incarnation id** — every object carries an independent, never-reused uid, so a
/// same-name flow re-created after a delete is a distinct incarnation, never a holdover aliased
/// onto the old one. Deleting a flow is a **cascade** (see [`FlowStatus`]): all its versions and
/// executions must be cleaned up first, so re-creating the same name yields a brand-new flow with
/// brand-new versions — never an aliased holdover.
///
/// Executions never bind to the flow itself; they bind to a specific version's reference.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Flow {
    /// Shared identity + timing metadata. `meta.name` is the flow's real, user-supplied primary key
    /// (a user `ObjectName`, equivalently a [`FlowName`](crate::types::id::FlowName)); `meta.uid` is
    /// the flow's independent never-reused incarnation id (minted at creation in `create_flow`).
    /// The domain `created_at`/`updated_at` live inside `meta` (`created_at` stamped at version
    /// creation, `updated_at` advanced by the applier from the applied entry's timestamp).
    pub meta: ObjectMeta<FlowKind>,
    pub status: FlowStatus,
    /// The highest [`FlowVersion`](crate::FlowVersion) ordinal published under this flow so far;
    /// publishing a new version increments it (see the `FlowVersionCreated` applier). Kept as an
    /// O(1) counter so "latest version" resolves without a scan; the initial version is `1`.
    pub latest_version: u32,
}

impl Flow {
    /// Birth a flow at `name`, at identity `uid` and birth time `created_at`. A flow is born the
    /// only way it can exist at birth — `Active`, holding version ordinal `1`, and a tenant-level
    /// root with no owner — so the caller supplies the minted `uid` and the moment, and no
    /// combination of fields an unborn flow cannot have is expressible.
    pub fn new(uid: ulid::Ulid, name: &FlowName, created_at: Timestamp) -> Self {
        Self {
            meta: ObjectMeta::builder(uid)
                .name(ObjectName::plain(name.as_str()).expect("a FlowName is a valid object name"))
                .at(created_at)
                .with_owner(NoOwner::new()),
            status: FlowStatus::Active,
            latest_version: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A flow is born only one way — `Active`, at ordinal `1`, ownerless — so the constructor fixes
    /// that state rather than accepting it, and names the flow after its own (plain) user name.
    #[test]
    fn new_births_an_active_ownerless_flow_at_version_one() {
        let at = Timestamp::from_millis(1_000);
        let flow = Flow::new(ulid::Ulid::new(), &FlowName::new("order").unwrap(), at);
        assert_eq!(flow.meta.name.as_str(), "order");
        assert_eq!(flow.meta.created_at, at);
        assert_eq!(flow.status, FlowStatus::Active);
        assert_eq!(flow.latest_version, 1);
        // A tenant-level root's owner slot is `NoOwner` by type — see `FlowKind::OwnedBy`.
        assert_eq!(flow.meta.owner, NoOwner::new());
    }
}
