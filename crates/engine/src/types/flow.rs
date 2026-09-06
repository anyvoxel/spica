//! The logical flow entity: one named, addressable aggregate that owns a sequence of
//! [`FlowVersion`](crate::FlowVersion)s.

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::types::meta::ObjectMeta;

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
    pub meta: ObjectMeta,
    pub status: FlowStatus,
    /// The highest [`FlowVersion`](crate::FlowVersion) ordinal published under this flow so far;
    /// publishing a new version increments it (see the `FlowVersionCreated` applier). Kept as an
    /// O(1) counter so "latest version" resolves without a scan; the initial version is `1`.
    pub latest_version: u32,
}
