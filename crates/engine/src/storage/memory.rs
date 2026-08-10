use std::collections::{HashMap, HashSet};

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::id::{ActivityId, ExecutionId, NodeId, TaskId, TimerId};

use super::{Activity, Execution, Storage, Task, Timer};

/// In-process, in-memory [`Storage`] used by M1's `Engine::start`.
#[derive(Default)]
pub struct InMemoryStorage {
    executions: HashMap<ExecutionId, Execution>,
    activities: HashMap<ActivityId, Activity>,
    timers: HashMap<TimerId, Timer>,
    tasks: HashMap<TaskId, Task>,
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
            // A task is a leaf; it has no children to sweep.
            NodeId::Task(_) => HashSet::new(),
        }
    }

    fn remove_child_internal(&mut self, parent: NodeId, child: NodeId) {
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
            // A task never owns children; nothing to remove.
            NodeId::Task(_) => {}
        }
    }

    fn add_child_internal(&mut self, parent: NodeId, child: NodeId) {
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
            // A task never owns children; nothing to add.
            NodeId::Task(_) => {}
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

    async fn get_task(&self, id: TaskId) -> Result<Option<Task>, ExecutionError> {
        Ok(self.tasks.get(&id).cloned())
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

    async fn put_task(&mut self, task: Task) -> Result<(), ExecutionError> {
        self.tasks.insert(task.id, task);
        Ok(())
    }

    async fn remove_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError> {
        self.remove_child_internal(parent, child);
        Ok(())
    }

    async fn add_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError> {
        self.add_child_internal(parent, child);
        Ok(())
    }
}
