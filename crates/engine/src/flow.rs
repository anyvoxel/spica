//! The logical flow entity: one named, addressable aggregate that owns a sequence of
//! [`FlowVersion`](crate::FlowVersion)s.

use serde::{Deserialize, Serialize};

use crate::id::{FlowId, FlowName, FlowVersionId};
use crate::log::Timestamp;

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
/// [`FlowVersion`](crate::FlowVersion)s. Addressed by its immutable [`name`](Self::name)
/// (the **primary key**); `flow_id` is an audit-only, never-reused generation identity so audit can
/// tell two incarnations of the same name (across a delete + re-create) apart — it is deliberately
/// **not** an addressing or join key, which is why executions bind to a
/// [`FlowVersionId`] instead.
///
/// Deleting a flow is a **cascade** (see [`FlowStatus`]): all its versions and executions must be
/// cleaned up first, so re-creating the same name yields a fresh incarnation with a new `flow_id`
/// and brand-new versions — never an aliased holdover.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Flow {
    /// Audit / generation identity — system-generated, never reused per incarnation of the name.
    /// Not the addressing key (`name` is), so it never creates a name→flow lookup layer.
    pub flow_id: FlowId,
    /// The logical flow's stable, user-supplied identity — the primary key.
    pub name: FlowName,
    /// When this incarnation first appeared (the first version was created).
    pub created_at: Timestamp,
    /// The latest applied entry's timestamp that touched this flow row (e.g. a new version advancing
    /// `latest_flow_version_id`; a future delete/cascade). Projection-derived from
    /// [`ApplierContext::timestamp`](crate::ApplierContext) — never a local `now()` — so replicas
    /// agree. Unlike `FlowVersion` (immutable), a flow mutates, so it carries an explicit update time.
    pub updated_at: Timestamp,
    pub status: FlowStatus,
    /// The newest [`FlowVersion`](crate::FlowVersion) under this flow, kept as an O(1) pointer so
    /// `latest_flow_version` is a single point read rather than a scan.
    ///
    /// TODO(derive-latest): this pointer is **denormalized duplicate state** — the same "latest"
    /// fact is implicitly recorded both here and by the `(flow_id, version)` index that
    /// `flow_version_of` / `latest_flow_version` resolve. Keeping it in two places means the pointer
    /// can drift from (and must be kept consistent with) the actual max-version row on every
    /// `FlowVersionCreated`, a cross-write atomicity concern. It exists only to make "latest" an
    /// O(1) point read; if the version index can answer "max version under `(flow_id)`" cheaply (or
    /// an execution-birth read path tolerates a bounded scan), delete this field and derive latest
    /// on demand so the fact lives in exactly one place.
    pub latest_flow_version_id: FlowVersionId,
}
