use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::id::{NodeId, TaskId};
use crate::log::Timestamp;

/// Lifecycle status of a Task (the spica name for what Zeebe calls a *job*). Like `TimerStatus`, a
/// task is a leaf side-effect node: it never initiates its own completion — it is either claimed
/// and settled by a worker, or cancelled. The two non-terminal states model the Zeebe job lifecycle:
/// a task is created **pending** (`Pending`), a worker **claims** it (`Running`, leased to that
/// worker until `lease_until`), and only the leasing worker's `CompleteTask`/`FailTask` settles it
/// — otherwise it re-queues (`Pending`) when its lease expires. Splitting the worker out of the
/// engine's process is what this lifecycle exists for: the engine stays the single writer that
/// validates each transition, the worker is just a (possibly remote) claimant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Created and waiting for a worker to claim it (Zeebe `ACTIVATABLE`/queued). An unclaimed task
    /// sits in this state indefinitely — the engine never predicts whether a worker will appear.
    Pending,
    /// Leased to a worker for `TaskValue::lease_until`; the worker is executing it (Zeebe
    /// `ACTIVATED`).
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    /// Whether this task is still waiting for a worker to claim it (Zeebe `ACTIVATABLE`).
    pub fn is_pending(&self) -> bool {
        matches!(self, TaskStatus::Pending)
    }

    /// Whether a worker currently leases this task (settlement is validated against the lease).
    pub fn is_running(&self) -> bool {
        matches!(self, TaskStatus::Running)
    }

    /// Whether the task is in flight (pending or running) — i.e. not yet settled/cancelled. Used by
    /// cascade/sweep to decide whether a node still needs draining.
    pub fn is_in_flight(&self) -> bool {
        !self.is_terminal()
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
        )
    }
}

/// The event-/domain-carried value of a Task.
///
/// A task is an in-flight external call invoked by a `Task` state (`"Type": "Task"`) — a call to
/// a connected `Resource` with projected `arguments` as input. The value carries the task's own
/// domain facts; storage may wrap it so the domain/projection boundary stays explicit, just as it
/// does for `ActivityValue` and `TimerValue`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskValue {
    pub id: TaskId,
    /// The node that invoked it (its owner — a `NodeId::Activity` in M2). Drained by the owner's
    /// cascade.
    pub parent: NodeId,
    /// The `Resource` URI the task calls (a downstream service / activity identifier).
    pub resource: String,
    /// The projected `arguments` passed to the resource as its input payload.
    pub arguments: Value,
    pub status: TaskStatus,
    /// Optional deadline (the state's `TimeoutSeconds`) after which the task is treated as failed
    /// with `States.Timeout`. `None` if the Task state has no timeout.
    pub deadline: Option<Timestamp>,
    /// The worker that currently leases this task, set when a worker claims it (`Running`). A
    /// `CompleteTask`/`FailTask` is accepted only from this worker (the Zeebe lease-ownership
    /// invariant); cleared when the lease expires or the task settles.
    #[serde(default)]
    pub worker_id: Option<String>,
    /// Wall-clock lease expiry for the claiming worker; `Some` iff `status == Running`. When it
    /// passes without a settle, the task returns to `Pending` (re-claimable). Distinct from
    /// `deadline` (the ASL `TimeoutSeconds` terminal backstop): the lease re-queues on a crashed /
    /// stalled worker, the deadline eventually fails the task.
    #[serde(default)]
    pub lease_until: Option<Timestamp>,
}
