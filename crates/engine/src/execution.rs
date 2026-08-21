use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::command::TerminationReason;
use crate::id::{ExecutionId, FlowVersionId, NodeId};

/// Lifecycle status of an [`Execution`](crate::ExecutionValue).
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
}

/// The event-/domain-carried value of an execution.
///
/// This is the execution entity shape the lifecycle stream carries. It is intentionally limited to
/// durable execution identity/lifecycle facts; runtime conveniences such as variable scope cursors
/// live on the storage projection instead of being repeated on every execution lifecycle event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionValue {
    pub id: ExecutionId,
    /// The flow version this execution is bound to (its state machine definition). This is the
    /// CCES analogue of Zeebe's `processDefinitionKey`: the execution references a **never-reused,
    /// immutable** [`FlowVersionId`] (never the mutable flow name, nor the audit-only `flow_id`),
    /// so it always resolves its machine against exactly the definition it was created on — even
    /// after the flow is updated or its name is deleted and re-created. Every execution in a tree
    /// (a Parallel branch child inherits its root's id) shares the same version.
    pub flow_version_id: FlowVersionId,
    /// The owner of the execution tree this run belongs to — **always** the top-level execution
    /// (the id `Engine::submit` returns), regardless of nesting depth. A child execution (a Parallel
    /// branch) inherits its root's id. This is the flat grouping key for "all events of one top-level
    /// run" — the CCES analogue of Zeebe's `processInstanceKey` — so a query need never recurse up
    /// the `parent` chain: filter by `root_execution == R`.
    pub root_execution: ExecutionId,
    /// Present only for a child execution (M3 Parallel/Map); always `None` for the top-level run.
    pub parent: Option<NodeId>,
    /// A JSON Pointer (RFC 6901) into the single shared `StateMachine` document locating this
    /// run's branch `states` table, e.g. `/states/P1/branches/0/states/P2/branches/1/states`.
    /// `None` for the top-level run (it resolves states against the machine's top-level `states`).
    pub state_path: Option<jsonptr::PointerBuf>,
    pub status: ExecutionStatus,
    /// The original execution input.
    pub input: Value,
    /// The execution's decided success output. It is written when `ExecutionCompleting` lands and is
    /// the terminal output once `ExecutionCompleted` lands.
    pub output: Option<Value>,
}

impl ExecutionValue {
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }
}
