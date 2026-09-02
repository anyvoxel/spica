use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::command::TerminationReason;
use crate::types::meta::{ObjectKind, ObjectMeta, ObjectReference};

/// Lifecycle status of an [`Execution`](crate::Execution).
///
/// The state machine is: `Running` -> `Completing` -> `Completed` (success) and
/// `Running` -> `Terminating` -> `Terminated` (abnormal). `Completing`/`Terminating` are real,
/// observable phases (not same-batch glitches): while a node owns active children it stays in the
/// winding-down phase until every child terminates and the shared cascade emits the `ed` event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExecutionStatus {
    Running,
    /// Success finish initiated; waiting on owned children (e.g. the execution's timeout timer).
    Completing,
    /// Abnormal finish initiated with its final reason already decided; waiting on owned children to
    /// terminate before the terminal ed lands.
    Terminating(TerminationReason),
    Completed,
    Terminated(TerminationReason),
}

impl ExecutionStatus {
    pub fn is_running(&self) -> bool {
        matches!(self, ExecutionStatus::Running)
    }
    pub fn is_completing(&self) -> bool {
        matches!(self, ExecutionStatus::Completing)
    }
    pub fn is_terminating(&self) -> bool {
        matches!(self, ExecutionStatus::Terminating(_))
    }
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ExecutionStatus::Completed | ExecutionStatus::Terminated(_)
        )
    }
    /// The terminal termination reason, if this status settled by `Terminating`/`Terminated`.
    pub fn termination_reason(&self) -> Option<&TerminationReason> {
        match self {
            ExecutionStatus::Terminating(r) | ExecutionStatus::Terminated(r) => Some(r),
            ExecutionStatus::Running | ExecutionStatus::Completing | ExecutionStatus::Completed => {
                None
            }
        }
    }
}

/// The event-/domain-carried value of an execution.
///
/// This is the execution entity shape the lifecycle stream carries. It is intentionally limited to
/// durable execution identity/lifecycle facts; runtime conveniences such as variable scope cursors
/// live on the storage projection instead of being repeated on every execution lifecycle event.
///
/// `Execution` is the entity of a **top-level run only** — the run a client started. A `Parallel`
/// branch or `Map` item is a different, dedicated entity type ([`Thread`](crate::Thread)) with its
/// own `state_path`/owner; it is **never** an `Execution`. Consequently an `Execution` needs neither a
/// `state_path` (it always resolves against the machine's top-level `states`) nor a
/// `root_execution` (it is its own root — its `reference()` IS the flat query anchor). Removing those
/// two fields is exactly what makes the type self-describing: no consumer must inspect fields to
/// decide whether an `Execution` is a root or a branch, because it is always a root.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Execution {
    /// The object identity + shared metadata (k8s-style `ObjectMeta` reuse). **`meta.uid` IS the
    /// execution's never-reused identity ulid** (there is no separate bare `id` — [`Self::reference`]
    /// bundles `meta.name` + `meta.uid` for anyone who must address this execution). `meta.name` is a
    /// generated placeholder (`obj-<uid>`) until user naming (P2); the domain
    /// `created_at`/`updated_at` live here likewise. A top-level execution has **no owner** — the
    /// tree's root is owned by nothing.
    pub meta: ObjectMeta,
    /// The flow version this execution is bound to (its state machine definition), addressed as a
    /// full [`ObjectReference`] (`{kind: FlowVersion, name: <flow>-<version>, uid}`). This is the
    /// CCES analogue of Zeebe's `processDefinitionKey`: the execution references a **never-reused,
    /// immutable** version (never the mutable flow name), so it always resolves its machine against
    /// exactly the definition it was created on — even after the flow is updated or its name is
    /// deleted and re-created. Every entity in a tree (a `Thread` inherited from its container)
    /// shares the same version.
    pub flow_version: ObjectReference,
    pub status: ExecutionStatus,
    /// The original execution input.
    pub input: Value,
    /// The execution's decided success output. It is written when `ExecutionCompleting` lands and is
    /// the terminal output once `ExecutionCompleted` lands.
    pub output: Option<Value>,
}

impl Execution {
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }

    /// This execution's canonical [`ObjectReference`] — the `(kind, name, uid)` triple a consumer
    /// uses to address it: `kind = Execution`, `name = meta.name`, `uid = meta.uid`. Mirrors
    /// [`FlowVersion::reference`](crate::types::flow_version::FlowVersion::reference): the storage row
    /// is keyed by the reference's `uid`, and Storage reads it back by reference (see
    /// `Storage::get_execution`).
    pub fn reference(&self) -> ObjectReference {
        ObjectReference::new(ObjectKind::Execution, self.meta.name.clone(), self.meta.uid)
    }
}
