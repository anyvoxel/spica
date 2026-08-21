//! In-process, in-memory [`Storage`] implementation — non-persistent, for M1 tests and synchronous
//! runs. Mirrors the persisted RocksDB KV layout at the Rust-map level.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;

use spica_engine::{
    Activity, ActivityId, Execution, ExecutionError, ExecutionId, Flow, FlowId, FlowName,
    FlowVersion, FlowVersionId, NodeId, Storage, StorageTxn, Task, TaskId, TaskStatus, Timer,
    TimerId,
};

/// The in-memory projection state, kept behind a shared interior (see [`InMemoryStorage`]). The
/// [`RocksStorage`](crate::RocksStorage) sibling stores rows in RocksDB; here the equivalent maps are
/// the store, shared via `Arc<Mutex<..>>` so a fold transaction ([`InMemoryTxn`]) can hold an
/// independent handle to the same state and commit its buffered writes under the write lock.
#[derive(Default)]
struct InMemoryDb {
    executions: HashMap<ExecutionId, Execution>,
    activities: HashMap<ActivityId, Activity>,
    timers: HashMap<TimerId, Timer>,
    tasks: HashMap<TaskId, Task>,
    /// Flow rows, addressed by the immutable name (the primary key).
    flows: HashMap<FlowName, Flow>,
    /// Persisted flow versions, keyed by the never-reused `flow_version_id` (execution binding).
    flow_versions: HashMap<FlowVersionId, FlowVersion>,
    /// Per-incarnation version index: `flow_id → (version → flow_version_id)`, sorted by ordinal so
    /// `flow_version_of` is an ordered point read.
    versions_by_flow: HashMap<FlowId, BTreeMap<u32, FlowVersionId>>,
    /// Resume watermark — last fully processed command position (see
    /// [`Storage::last_processed_position`]). `0` = nothing processed.
    last_processed_position: i64,
}

/// In-process, in-memory [`Storage`] used as the M1 synchronous/test default.
///
/// Mirrors the persisted KV layout at the Rust-map level: a flow is addressed by its immutable
/// `name` (`flow_id` is audit-only, never an addressing key — executions bind a `FlowVersionId`),
/// and each flow's versions are kept in a `BTreeMap` by ordinal so version lookup is an ordered
/// point read.
///
/// The maps live behind an `Arc<Mutex<InMemoryDb>>` so [`Storage::begin_txn`] can hand a fold an
/// **owned** [`InMemoryTxn`] (an `Arc` clone) without borrowing `&mut self` — matching the owned
/// transaction shape of [`RocksStorage`](crate::RocksStorage). A `std` mutex (rather than a tokio
/// one) is used so the txn's synchronous [`StorageTxn::commit`] can acquire the write lock without
/// async-context restrictions; this is a non-concurrent sync/test store, so the brief lock is fine.
pub struct InMemoryStorage {
    inner: Arc<Mutex<InMemoryDb>>,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(InMemoryDb::default())),
        }
    }
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    fn db(&self) -> MutexGuard<'_, InMemoryDb> {
        self.inner.lock().expect("in-memory storage lock poisoned")
    }
}

impl InMemoryDb {
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
            // A task never owns children; nothing to remove.
            NodeId::Task(_) => {}
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
            // A task never owns children; nothing to add.
            NodeId::Task(_) => {}
        }
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn get_execution(&self, id: ExecutionId) -> Result<Option<Execution>, ExecutionError> {
        Ok(self.db().executions.get(&id).cloned())
    }

    async fn get_activity(&self, id: ActivityId) -> Result<Option<Activity>, ExecutionError> {
        Ok(self.db().activities.get(&id).cloned())
    }

    async fn get_timer(&self, id: TimerId) -> Result<Option<Timer>, ExecutionError> {
        Ok(self.db().timers.get(&id).cloned())
    }

    async fn get_task(&self, id: TaskId) -> Result<Option<Task>, ExecutionError> {
        Ok(self.db().tasks.get(&id).cloned())
    }

    async fn activatable_tasks(
        &self,
        resource: &str,
        limit: usize,
    ) -> Result<Vec<Task>, ExecutionError> {
        // A linear scan of the task map — no per-resource queue index in M1 (see the trait doc).
        let db = self.db();
        Ok(db
            .tasks
            .values()
            .filter(|t| t.value.resource == resource && t.value.status == TaskStatus::Pending)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn get_children(&self, id: NodeId) -> Result<HashSet<NodeId>, ExecutionError> {
        Ok(self.db().children(id))
    }

    async fn put_execution(&mut self, exec: Execution) -> Result<(), ExecutionError> {
        self.db().executions.insert(exec.id, exec);
        Ok(())
    }

    async fn put_activity(&mut self, act: Activity) -> Result<(), ExecutionError> {
        self.db().activities.insert(act.value.id, act);
        Ok(())
    }

    async fn put_timer(&mut self, timer: Timer) -> Result<(), ExecutionError> {
        self.db().timers.insert(timer.value.id, timer);
        Ok(())
    }

    async fn put_task(&mut self, task: Task) -> Result<(), ExecutionError> {
        self.db().tasks.insert(task.value.id, task);
        Ok(())
    }

    async fn remove_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError> {
        self.db().remove_child(parent, child);
        Ok(())
    }

    async fn add_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError> {
        self.db().add_child(parent, child);
        Ok(())
    }

    async fn get_flow_by_name(&self, name: FlowName) -> Result<Option<Flow>, ExecutionError> {
        Ok(self.db().flows.get(&name).cloned())
    }

    async fn put_flow(&mut self, flow: Flow) -> Result<(), ExecutionError> {
        self.db().flows.insert(flow.name.clone(), flow);
        Ok(())
    }

    async fn get_flow_version(
        &self,
        flow_version_id: FlowVersionId,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        Ok(self.db().flow_versions.get(&flow_version_id).cloned())
    }

    async fn put_flow_version(&mut self, ver: FlowVersion) -> Result<(), ExecutionError> {
        let mut db = self.db();
        db.versions_by_flow
            .entry(ver.flow_id)
            .or_default()
            .insert(ver.version, ver.flow_version_id);
        db.flow_versions.insert(ver.flow_version_id, ver);
        Ok(())
    }

    async fn flow_version_of(
        &self,
        flow_id: FlowId,
        version: u32,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        let db = self.db();
        Ok(db
            .versions_by_flow
            .get(&flow_id)
            .and_then(|m| m.get(&version))
            .and_then(|fvid| db.flow_versions.get(fvid))
            .cloned())
    }

    async fn last_processed_position(&self) -> Result<i64, ExecutionError> {
        Ok(self.db().last_processed_position)
    }

    async fn put_last_processed_position(&mut self, position: i64) -> Result<(), ExecutionError> {
        self.db().last_processed_position = position;
        Ok(())
    }

    fn begin_txn<'a>(&'a self) -> Result<Box<dyn StorageTxn + 'a>, ExecutionError> {
        // Hand the StreamProcessor an **owned** fold transaction: a clone of the shared interior plus an
        // empty buffered batch. Subsequent fold writes buffer; `InMemoryTxn::commit` applies them
        // under the write lock, atomically with any watermark advance. See the `StorageTxn` docs.
        Ok(Box::new(InMemoryTxn {
            inner: Arc::clone(&self.inner),
            batch: InMemoryBatch::default(),
        }))
    }
}

/// Buffered writes of one in-memory fold transaction, mirroring the row kinds of [`InMemoryDb`].
/// Applied to the shared [`InMemoryDb`] all at once (and atomically with the watermark) at commit.
#[derive(Default)]
struct InMemoryBatch {
    executions: HashMap<ExecutionId, Execution>,
    activities: HashMap<ActivityId, Activity>,
    timers: HashMap<TimerId, Timer>,
    tasks: HashMap<TaskId, Task>,
    flows: HashMap<FlowName, Flow>,
    flow_versions: HashMap<FlowVersionId, FlowVersion>,
    versions_by_flow: HashMap<FlowId, BTreeMap<u32, FlowVersionId>>,
}

/// Atomic projection transaction for [`InMemoryStorage`]. Owns a clone of the shared
/// [`InMemoryDb`] handle and a pending [`InMemoryBatch`]: reads resolve **read-your-writes** — from
/// the batch first, then committed state; writes accumulate into the batch; [`StorageTxn::commit`]
/// applies the whole batch (plus any watermark advance) under the write lock, all-or-nothing.
/// Dropping without committing discards the batch (abort).
struct InMemoryTxn {
    inner: Arc<Mutex<InMemoryDb>>,
    batch: InMemoryBatch,
}

#[async_trait]
impl StorageTxn for InMemoryTxn {
    async fn get_execution(
        &mut self,
        id: ExecutionId,
    ) -> Result<Option<Execution>, ExecutionError> {
        // Read-your-writes: resolve from the fold's buffered batch first, then committed state.
        if let Some(exec) = self.batch.executions.get(&id) {
            return Ok(Some(exec.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .executions
            .get(&id)
            .cloned())
    }

    async fn get_activity(&mut self, id: ActivityId) -> Result<Option<Activity>, ExecutionError> {
        if let Some(act) = self.batch.activities.get(&id) {
            return Ok(Some(act.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .activities
            .get(&id)
            .cloned())
    }

    async fn get_timer(&mut self, id: TimerId) -> Result<Option<Timer>, ExecutionError> {
        if let Some(timer) = self.batch.timers.get(&id) {
            return Ok(Some(timer.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .timers
            .get(&id)
            .cloned())
    }

    async fn get_task(&mut self, id: TaskId) -> Result<Option<Task>, ExecutionError> {
        if let Some(task) = self.batch.tasks.get(&id) {
            return Ok(Some(task.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .tasks
            .get(&id)
            .cloned())
    }

    async fn get_children(&mut self, id: NodeId) -> Result<HashSet<NodeId>, ExecutionError> {
        Ok(match id {
            NodeId::Execution(e) => self
                .get_execution(e)
                .await?
                .map(|x| x.active_children)
                .unwrap_or_default(),
            NodeId::Activity(a) => self
                .get_activity(a)
                .await?
                .map(|x| x.active_children)
                .unwrap_or_default(),
            NodeId::Timer(_) | NodeId::Task(_) => HashSet::new(),
        })
    }

    async fn put_execution(&mut self, exec: Execution) -> Result<(), ExecutionError> {
        self.batch.executions.insert(exec.id, exec);
        Ok(())
    }

    async fn put_activity(&mut self, act: Activity) -> Result<(), ExecutionError> {
        self.batch.activities.insert(act.value.id, act);
        Ok(())
    }

    async fn put_timer(&mut self, timer: Timer) -> Result<(), ExecutionError> {
        self.batch.timers.insert(timer.value.id, timer);
        Ok(())
    }

    async fn put_task(&mut self, task: Task) -> Result<(), ExecutionError> {
        self.batch.tasks.insert(task.value.id, task);
        Ok(())
    }

    /// Remove `child` from `parent`'s `active_children`: read the (batch-first) row, mutate a clone,
    /// and buffer the result into this fold's batch (atomicity at commit).
    async fn remove_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError> {
        self.buffer_child_mutation(parent, child, false);
        Ok(())
    }

    /// Add `child` to `parent`'s `active_children`: read the (batch-first) row, mutate a clone, and
    /// buffer the result into this fold's batch (atomicity at commit).
    async fn add_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError> {
        self.buffer_child_mutation(parent, child, true);
        Ok(())
    }

    async fn get_flow_by_name(&mut self, name: FlowName) -> Result<Option<Flow>, ExecutionError> {
        if let Some(flow) = self.batch.flows.get(&name) {
            return Ok(Some(flow.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .flows
            .get(&name)
            .cloned())
    }

    async fn put_flow(&mut self, flow: Flow) -> Result<(), ExecutionError> {
        self.batch.flows.insert(flow.name.clone(), flow);
        Ok(())
    }

    async fn get_flow_version(
        &mut self,
        flow_version_id: FlowVersionId,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        if let Some(ver) = self.batch.flow_versions.get(&flow_version_id) {
            return Ok(Some(ver.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .flow_versions
            .get(&flow_version_id)
            .cloned())
    }

    async fn put_flow_version(&mut self, ver: FlowVersion) -> Result<(), ExecutionError> {
        self.batch
            .versions_by_flow
            .entry(ver.flow_id)
            .or_default()
            .insert(ver.version, ver.flow_version_id);
        self.batch.flow_versions.insert(ver.flow_version_id, ver);
        Ok(())
    }

    async fn flow_version_of(
        &mut self,
        flow_id: FlowId,
        version: u32,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        // Read-your-writes on both the index and the version row.
        let fvid: Option<FlowVersionId> =
            if let Some(by_ordinal) = self.batch.versions_by_flow.get(&flow_id) {
                by_ordinal.get(&version).cloned()
            } else {
                self.inner
                    .lock()
                    .expect("in-memory storage lock poisoned")
                    .versions_by_flow
                    .get(&flow_id)
                    .and_then(|m| m.get(&version))
                    .cloned()
            };
        match fvid {
            Some(fvid) => self.get_flow_version(fvid).await,
            None => Ok(None),
        }
    }

    fn commit(self: Box<Self>, watermark: Option<i64>) -> Result<(), ExecutionError> {
        let batch = self.batch;
        let mut db = self.inner.lock().expect("in-memory storage lock poisoned");
        // Land the whole fold all-or-nothing, then the watermark advance in the same atomic unit.
        db.executions.extend(batch.executions);
        db.activities.extend(batch.activities);
        db.timers.extend(batch.timers);
        db.tasks.extend(batch.tasks);
        db.flows.extend(batch.flows);
        for (fid, by_ordinal) in batch.versions_by_flow {
            db.versions_by_flow
                .entry(fid)
                .or_default()
                .extend(by_ordinal);
        }
        db.flow_versions.extend(batch.flow_versions);
        if let Some(w) = watermark {
            db.last_processed_position = w;
        }
        Ok(())
    }
}

impl InMemoryTxn {
    /// Read the (batch-first) parent row for `parent`, add/remove `child` on a clone, and buffer the
    /// mutated row into this fold's batch. Reading the batch first (read-your-writes) is what lets a
    /// read-modify-write compose with an earlier `put_execution`/`add_child` of the same row in the
    /// same fold without clobbering it.
    fn buffer_child_mutation(&mut self, parent: NodeId, child: NodeId, insert: bool) {
        match parent {
            NodeId::Execution(e) => {
                let base = self.batch.executions.get(&e).cloned().or_else(|| {
                    self.inner
                        .lock()
                        .expect("in-memory storage lock poisoned")
                        .executions
                        .get(&e)
                        .cloned()
                });
                if let Some(mut exec) = base {
                    if insert {
                        exec.active_children.insert(child);
                    } else {
                        exec.active_children.remove(&child);
                    }
                    self.batch.executions.insert(e, exec);
                }
            }
            NodeId::Activity(a) => {
                let base = self.batch.activities.get(&a).cloned().or_else(|| {
                    self.inner
                        .lock()
                        .expect("in-memory storage lock poisoned")
                        .activities
                        .get(&a)
                        .cloned()
                });
                if let Some(mut act) = base {
                    if insert {
                        act.active_children.insert(child);
                    } else {
                        act.active_children.remove(&child);
                    }
                    self.batch.activities.insert(a, act);
                }
            }
            // A leaf never owns children; nothing to mutate.
            NodeId::Timer(_) | NodeId::Task(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use spica_engine::{ExecutionStatus, ExecutionValue, FlowVersionId, Timestamp, Variables};

    /// A minimal running execution row for exercising the in-memory store.
    fn sample_execution(id: ExecutionId) -> Execution {
        Execution {
            value: ExecutionValue {
                id,
                flow_version_id: FlowVersionId::nil(),
                root_execution: id,
                parent: None,
                state_path: None,
                status: ExecutionStatus::Running,
                input: Value::Null,
                output: None,
            },
            variables: Variables::new(),
            current_activity: None,
            active_children: HashSet::new(),
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        }
    }

    #[tokio::test]
    async fn last_processed_position_defaults_zero_and_roundtrips() {
        let mut store = InMemoryStorage::new();
        // Fresh store: nothing processed → 0, the resume-from-position-1 default.
        assert_eq!(store.last_processed_position().await.unwrap(), 0);
        store.put_last_processed_position(7).await.unwrap();
        assert_eq!(store.last_processed_position().await.unwrap(), 7);
        // Advancing past an existing watermark overwrites it (the StreamProcessor applies `max`).
        store.put_last_processed_position(12).await.unwrap();
        assert_eq!(store.last_processed_position().await.unwrap(), 12);
    }

    #[tokio::test]
    async fn txn_commits_and_abort_discards() {
        let store = InMemoryStorage::new();
        let id = ExecutionId::new();
        // A single fold transaction buffers writes; nothing is visible until commit.
        let mut txn = store.begin_txn().unwrap();
        txn.put_execution(sample_execution(id)).await.unwrap();
        assert!(store.get_execution(id).await.unwrap().is_none());
        // Commit the fold: projection + watermark land together.
        txn.commit(Some(7)).unwrap();
        assert!(store.get_execution(id).await.unwrap().is_some());
        assert_eq!(store.last_processed_position().await.unwrap(), 7);
        // Abort: begin a transaction, buffer a write, then drop without committing — discarded.
        let other = ExecutionId::new();
        {
            let mut txn = store.begin_txn().unwrap();
            txn.put_execution(sample_execution(other)).await.unwrap();
            // drop(txn) aborts: the buffered write is never applied.
        }
        assert!(store.get_execution(other).await.unwrap().is_none());
        assert_eq!(store.last_processed_position().await.unwrap(), 7);
    }
}
