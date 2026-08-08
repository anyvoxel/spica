use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::command::{TerminationReason, TimerPurpose};
use crate::error::ExecutionError;
use crate::id::{ActivityId, ExecutionId, NodeId, TimerId};
use crate::log::Timestamp;
use crate::scope::Scope;

/// Lifecycle status of an [`Execution`] or [`Activity`].
///
/// The state machine is: `Running` -> `Completing` -> `Completed` (success) and
/// `Running` -> `Terminating` -> `Terminated` (abnormal). `Completing`/`Terminating` are real,
/// observable phases (not same-batch glitches): while a node owns `active_children` it stays in the
/// winding-down phase until every child terminates and the shared cascade emits the `ed` event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Status {
    Running,
    /// Success finish initiated; waiting on owned children (e.g. the execution's timeout timer).
    Completing,
    /// Abnormal finish initiated; waiting on owned children to terminate.
    Terminating,
    Completed,
    Terminated(TerminationReason),
}

impl Status {
    pub fn is_running(&self) -> bool {
        matches!(self, Status::Running)
    }
    pub fn is_completing(&self) -> bool {
        matches!(self, Status::Completing)
    }
    pub fn is_terminating(&self) -> bool {
        matches!(self, Status::Terminating)
    }
    pub fn is_terminal(&self) -> bool {
        matches!(self, Status::Completed | Status::Terminated(_))
    }
}

/// Lifecycle status of a [`Timer`]. Kept separate from [`Status`] because a timer has (in M1) a
/// strictly simpler shape — it never initiates its own completion; it is *armed* by a state or the
/// execution and either fires (`Completed`) or is cancelled (`Cancelled`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimerStatus {
    Active,
    Completed,
    Cancelled,
}

impl TimerStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, TimerStatus::Active)
    }
    pub fn is_terminal(&self) -> bool {
        matches!(self, TimerStatus::Completed | TimerStatus::Cancelled)
    }
}

/// One execution of a state machine — the root of its tree. Materialized by applying [`Event`]s.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Execution {
    pub id: ExecutionId,
    /// Present only for a child execution (M2 Parallel/Map); always `None` in M1.
    pub parent: Option<NodeId>,
    pub status: Status,
    /// The name of the state most recently entered (set by `StateActivating`).
    pub current_state: Option<String>,
    /// The activity currently in flight (set by `StateActivating`, cleared on its terminal ed).
    pub current_activity: Option<ActivityId>,
    /// The execution's variable scope (mutated by `VariablesAssigned` events).
    pub scope: Scope,
    /// The original execution input.
    pub input: Value,
    /// The terminal output once `ExecutionCompleted` lands.
    pub output: Option<Value>,
    /// The terminal reason once `ExecutionTerminated` lands.
    pub reason: Option<TerminationReason>,
    /// The success output decided at `ExecutionCompleting`, awaiting drain before `ExecutionCompleted`.
    pub pending_output: Option<Value>,
    /// The termination reason decided at `ExecutionTerminating`, awaiting drain before
    /// `ExecutionTerminated`.
    pub pending_reason: Option<TerminationReason>,
    /// Owned nodes still in flight (active activities / timers). Terminating/Completing wait them.
    pub active_children: HashSet<NodeId>,
}

impl Execution {
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }
}

/// The execution of a single state within an [`Execution`]. Owned by its [`Execution`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Activity {
    pub id: ActivityId,
    /// The owning node — in M1 always `NodeId::Execution`; M2 lets one activity parent another
    /// (`Parallel`/`Map` children).
    pub parent: NodeId,
    pub state: String,
    pub status: Status,
    pub input: Value,
    /// The terminal output once `StateCompleted` lands.
    pub output: Option<Value>,
    /// The terminal reason once `StateTerminated` lands.
    pub reason: Option<TerminationReason>,
    /// `output` computed during the complete step, awaiting drain before `StateCompleted`.
    pub pending_output: Option<Value>,
    /// Termination reason recorded by `StateTerminating`, awaiting drain before `StateTerminated`.
    pub pending_reason: Option<TerminationReason>,
    /// Owned nodes still in flight (e.g. this Wait's resume timer). Terminating waits on them.
    pub active_children: HashSet<NodeId>,
}

/// A timer armed by an [`Execution`] (`ExecutionTimeout`) or an [`Activity`] (`WaitResume`).
/// A leaf — never owns children.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Timer {
    pub id: TimerId,
    /// The node that armed it (its owner). Drained by the owner's cascade.
    pub parent: NodeId,
    pub purpose: TimerPurpose,
    pub status: TimerStatus,
    /// Absolute wall-clock moment the timer fires. Persisting the absolute deadline (not a relative
    /// duration) keeps the timer row self-contained: a replay can derive "how long is left" from
    /// `deadline - now` without re-arming based on a stale relative count.
    pub deadline: Timestamp,
}

/// Persistent projection of the execution tree, rebuilt by applying the [`Event`] stream.
///
/// This is the **read** interface used by handlers and the cascade (they observe snapshots and never
/// mutate). The actual projection of events is performed by [`EventApplier`](crate::applier::EventApplier)
/// implementations, which mutate this store through the `put_*` / `remove_child` methods below —
/// mirroring how [`CommandHandler`](crate::CommandHandler) implementations observe Storage but
/// delegate their output to the [`Collector`](crate::Collector).
#[async_trait]
pub trait Storage: Send + Sync {
    async fn get_execution(&self, id: ExecutionId) -> Result<Option<Execution>, ExecutionError>;
    async fn get_activity(&self, id: ActivityId) -> Result<Option<Activity>, ExecutionError>;
    async fn get_timer(&self, id: TimerId) -> Result<Option<Timer>, ExecutionError>;
    async fn get_children(&self, id: NodeId) -> Result<HashSet<NodeId>, ExecutionError>;

    /// Upsert an `Execution` record (read-modify-write by an `EventApplier`).
    async fn put_execution(&mut self, exec: Execution) -> Result<(), ExecutionError>;
    /// Upsert an `Activity` record.
    async fn put_activity(&mut self, act: Activity) -> Result<(), ExecutionError>;
    /// Upsert a `Timer` record.
    async fn put_timer(&mut self, timer: Timer) -> Result<(), ExecutionError>;
    /// Remove `child` from `parent`'s `active_children` (a terminal child draining its owner).
    async fn remove_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError>;
    /// Add `child` to `parent`'s `active_children` (a child appears when its `ing` event lands).
    async fn add_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError>;
}

/// In-process, in-memory [`Storage`] used by M1's `Engine::start`.
#[derive(Default)]
pub struct InMemoryStorage {
    executions: HashMap<ExecutionId, Execution>,
    activities: HashMap<ActivityId, Activity>,
    timers: HashMap<TimerId, Timer>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    fn children(&self, id: NodeId) -> HashSet<NodeId> {
        match id {
            NodeId::Execution(e) => self
                .executions
                .get(&e)
                .map(|x| x.active_children.clone())
                .unwrap_or_default(),
            NodeId::Activity(a) => self
                .activities
                .get(&a)
                .map(|x| x.active_children.clone())
                .unwrap_or_default(),
            // A timer is a leaf; it has no children to sweep.
            NodeId::Timer(_) => HashSet::new(),
        }
    }

    fn remove_child(&mut self, parent: NodeId, child: NodeId) {
        match parent {
            NodeId::Execution(e) => {
                if let Some(exec) = self.executions.get_mut(&e) {
                    exec.active_children.remove(&child);
                }
            }
            NodeId::Activity(a) => {
                if let Some(act) = self.activities.get_mut(&a) {
                    act.active_children.remove(&child);
                }
            }
            NodeId::Timer(_) => {}
        }
    }

    fn add_child(&mut self, parent: NodeId, child: NodeId) {
        match parent {
            NodeId::Execution(e) => {
                if let Some(exec) = self.executions.get_mut(&e) {
                    exec.active_children.insert(child);
                }
            }
            NodeId::Activity(a) => {
                if let Some(act) = self.activities.get_mut(&a) {
                    act.active_children.insert(child);
                }
            }
            NodeId::Timer(_) => {}
        }
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn get_execution(&self, id: ExecutionId) -> Result<Option<Execution>, ExecutionError> {
        Ok(self.executions.get(&id).cloned())
    }

    async fn get_activity(&self, id: ActivityId) -> Result<Option<Activity>, ExecutionError> {
        Ok(self.activities.get(&id).cloned())
    }

    async fn get_timer(&self, id: TimerId) -> Result<Option<Timer>, ExecutionError> {
        Ok(self.timers.get(&id).cloned())
    }

    async fn get_children(&self, id: NodeId) -> Result<HashSet<NodeId>, ExecutionError> {
        Ok(self.children(id))
    }

    async fn put_execution(&mut self, exec: Execution) -> Result<(), ExecutionError> {
        self.executions.insert(exec.id, exec);
        Ok(())
    }

    async fn put_activity(&mut self, act: Activity) -> Result<(), ExecutionError> {
        self.activities.insert(act.id, act);
        Ok(())
    }

    async fn put_timer(&mut self, timer: Timer) -> Result<(), ExecutionError> {
        self.timers.insert(timer.id, timer);
        Ok(())
    }

    async fn remove_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError> {
        self.remove_child(parent, child);
        Ok(())
    }

    async fn add_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError> {
        self.add_child(parent, child);
        Ok(())
    }
}
