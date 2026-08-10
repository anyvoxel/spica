use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::id::{NodeId, TaskId};
use crate::log::Timestamp;

/// Lifecycle status of a [`Task`]. Like [`super::TimerStatus`], a task is a leaf side-effect node: it
/// never initiates its own completion — it is invoked and either completes (`Completed`), fails
/// (`Failed`), or is cancelled (`Cancelled`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Active,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, TaskStatus::Active)
    }
    pub fn is_terminal(&self) -> bool {
        !matches!(self, TaskStatus::Active)
    }
}

/// An in-flight external [`Task`] invoked by a `Task` state (`"Type": "Task"`) — a call to a
/// connected `Resource` (`TaskState::resource`) with the projected `arguments` as input. Owned by
/// the invoking [`super::Activity`]; a leaf — never owns children.
///
/// A `Task` is the external-resource analogue of a [`super::Timer`]: it is a side-effect node whose
/// lifecycle is driven from outside the decision loop. The physical invocation is handled by the
/// [`TaskService`](crate::task_service::TaskService) (in-process in M2, a real worker in a
/// distributed deployment); the decision loop only records the logical facts (`TaskActivated` /
/// `TaskCompleted` / `TaskTerminated`) and resumes the owning state when the task settles.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
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
}
