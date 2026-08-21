//! The persisted definition entity: one immutable published version of a flow.

use serde::{Deserialize, Serialize};

use crate::id::{FlowId, FlowName, FlowVersionId};
use crate::log::Timestamp;

/// One immutable, published version of a logical flow — the durable object `Storage` persists and
/// executions bind to. It is the Zeebe analogue of a deployed `Process` (a specific
/// `processDefinitionKey`): the `definition` content plus the identity an execution references,
/// never the mutable flow aggregate itself.
///
/// Adding a version to the same [`FlowName`] produces a **new** `FlowVersion` with an incremented
/// `version` and a **fresh, never-reused** [`FlowVersionId`], so:
/// - "updating a flow" is purely additive — a new immutable snapshot, old executions unaffected;
/// - deleting + re-creating a name later yields a brand-new flow incarnation (new `flow_id`) and
///   brand-new versions, so no in-flight execution can ever be aliased onto the wrong definition.
///
/// `FlowVersion` is the single record that carries a full definition: it is the payload of
/// [`Event::FlowCreated`](crate::Event), folded into storage by the applier — the only
/// place a definition ever enters the stream (mirroring Zeebe's Deployment→Process record).
/// The definition is stored as its **raw ASL string** (the JSON-encoded form of a
/// [`StateMachine`](spica_asl::StateMachine)); the parsed model is a derived, transient value
/// (validated at the create boundary, parsed on demand at execution and cached).
///
/// It references its owning flow redundantly by both `name` (the addressing key) and `flow_id`
/// (the audit / generation identity) — the latter so audit can enumerate a specific incarnation's
/// versions without a join, and to keep the version unambiguously attached to the exact flow that
/// created it even after a same-name re-create.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowVersion {
    /// System-generated, unique-per-version reference id. **Executions bind to this** (never the
    /// name, never the flow's audit id).
    pub flow_version_id: FlowVersionId,
    /// The owning flow's audit generation identity (denormalized for audit: lets a `flow_id`'s
    /// versions be enumerated without joining through the name).
    pub flow_id: FlowId,
    /// The flow this version belongs to (the addressing key; redundant with `flow_id`).
    pub name: FlowName,
    /// This version's ordinal within its flow (1, 2, 3, … — incremented on each create).
    pub version: u32,
    /// The ASL state machine definition this version publishes, as its raw JSON string form.
    /// Stored as a string so the durable record is the exact definition the user submitted; it is
    /// parsed into a `StateMachine` where an execution needs it ([`HandlerContext::machine`]).
    pub definition: String,
    /// When this version was published.
    pub created_at: Timestamp,
}
