use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::command::TerminationReason;
use crate::id::{ActivityId, ExecutionId, NodeId};
use crate::scope::Scope;

/// Lifecycle status of an [`Execution`].
///
/// The state machine is: `Running` -> `Completing` -> `Completed` (success) and
/// `Running` -> `Terminating` -> `Terminated` (abnormal). `Completing`/`Terminating` are real,
/// observable phases (not same-batch glitches): while a node owns `active_children` it stays in the
/// winding-down phase until every child terminates and the shared cascade emits the `ed` event.
///
/// Kept distinct from [`super::ActivityStatus`] (which mirrors the same lifecycle for an
/// [`super::Activity`]) so each node type's status is its own type — a status written for one can't
/// be assigned to the other.
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

/// One execution of a state machine — the root of its tree. Materialized by applying [`Event`](crate::Event)s.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Execution {
    pub id: ExecutionId,
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
    /// The definition itself stays as a single instance on the shared machine — this path is a
    /// small, flat locator into that document, never a copy of the branch definition, so a child
    /// execution resolves its states from its own row without querying its parent or the root.
    pub state_path: Option<jsonptr::PointerBuf>,
    pub status: ExecutionStatus,
    /// The name of the state most recently entered (set by `StateActivating`).
    pub current_state: Option<String>,
    /// The activity currently in flight (set by `StateActivating`, cleared on its terminal ed).
    pub current_activity: Option<ActivityId>,
    /// The execution's variable scope (mutated by `VariablesAssigned` events).
    pub scope: Scope,
    /// The original execution input.
    pub input: Value,
    /// The execution's decided success output. It is written when `ExecutionCompleting` lands and is
    /// the terminal output once `ExecutionCompleted` lands.
    pub output: Option<Value>,
    /// Owned nodes still in flight (active activities / timers). Terminating/Completing wait them.
    pub active_children: HashSet<NodeId>,
}

impl Execution {
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }
}
