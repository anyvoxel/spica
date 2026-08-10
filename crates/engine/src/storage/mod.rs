mod activity;
mod execution;
mod memory;
mod task;
mod timer;

use std::collections::HashSet;

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::id::{ActivityId, NodeId, TaskId, TimerId};

pub use activity::{
    Activity, ActivityState, ActivityStatus, MapActivityState, ParallelActivityState,
    RetrierAttemptState, RetryState,
};
pub use execution::{Execution, ExecutionStatus};
pub use memory::InMemoryStorage;
pub use task::{Task, TaskStatus};
pub use timer::{Timer, TimerStatus};

/// Persistent projection of the execution tree, rebuilt by applying the [`Event`](crate::Event) stream.
///
/// This is the **read** interface used by handlers and the cascade (they observe snapshots and never
/// mutate). The actual projection of events is performed by [`EventApplier`](crate::applier::EventApplier)
/// implementations, which mutate this store through the `put_*` / `remove_child` methods below —
/// mirroring how [`CommandHandler`](crate::CommandHandler) implementations observe Storage but
/// delegate their output to the [`Collector`](crate::Collector).
#[async_trait]
pub trait Storage: Send + Sync {
    async fn get_execution(
        &self,
        id: crate::id::ExecutionId,
    ) -> Result<Option<Execution>, ExecutionError>;
    async fn get_activity(&self, id: ActivityId) -> Result<Option<Activity>, ExecutionError>;
    async fn get_timer(&self, id: TimerId) -> Result<Option<Timer>, ExecutionError>;
    async fn get_task(&self, id: TaskId) -> Result<Option<Task>, ExecutionError>;
    async fn get_children(&self, id: NodeId) -> Result<HashSet<NodeId>, ExecutionError>;

    /// Upsert an `Execution` record (read-modify-write by an `EventApplier`).
    async fn put_execution(&mut self, exec: Execution) -> Result<(), ExecutionError>;
    /// Upsert an `Activity` record.
    async fn put_activity(&mut self, act: Activity) -> Result<(), ExecutionError>;
    /// Upsert a `Timer` record.
    async fn put_timer(&mut self, timer: Timer) -> Result<(), ExecutionError>;
    /// Upsert a `Task` record.
    async fn put_task(&mut self, task: Task) -> Result<(), ExecutionError>;
    /// Remove `child` from `parent`'s `active_children` (a terminal child draining its owner).
    async fn remove_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError>;
    /// Add `child` to `parent`'s `active_children` (a child appears when its `ing` event lands).
    async fn add_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError>;
}
