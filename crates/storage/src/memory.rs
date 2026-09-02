//! In-process, in-memory [`Storage`] implementation — non-persistent, for M1 tests and synchronous
//! runs. Mirrors the persisted RocksDB KV layout at the Rust-map level.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;

use spica_engine::{
    ActivityRecord, ExecutionError, ExecutionRecord, Flow, FlowName, FlowVersion, ObjectKind,
    ObjectName, ObjectReference, Storage, StorageTxn, TaskRecord, TaskStatus, ThreadRecord,
    TimerRecord,
};

/// The in-memory projection state, kept behind a shared interior (see [`InMemoryStorage`]). The
/// [`RocksStorage`](crate::RocksStorage) sibling stores rows in RocksDB; here the equivalent maps are
/// the store, shared via `Arc<Mutex<..>>` so a fold transaction ([`InMemoryTxn`]) can hold an
/// independent handle to the same state and commit its buffered writes under the write lock.
#[derive(Default)]
struct InMemoryDb {
    // Execution rows, keyed by the addressing `name` (the primary key, per scope); the `uid` is a
    // secondary attribute, matching the persisted `KeyBuilder::execution`.
    executions: HashMap<ObjectName, ExecutionRecord>,
    // Thread rows (fan-out sub-runs), keyed by the addressing `name`, matching `KeyBuilder::thread`.
    threads: HashMap<ObjectName, ThreadRecord>,
    activities: HashMap<ObjectName, ActivityRecord>,
    timers: HashMap<ObjectName, TimerRecord>,
    tasks: HashMap<ObjectName, TaskRecord>,
    /// Flow rows, addressed by the immutable name (the primary key).
    flows: HashMap<FlowName, Flow>,
    /// Persisted flow versions, keyed by the version's own addressing `ObjectName` (`{flow}-{version}`,
    /// see [`FlowVersion::version_name`]); a version is located by building that name, so no separate
    /// per-flow index is needed.
    flow_versions: HashMap<ObjectName, FlowVersion>,
    /// Resume watermark — last fully processed command position (see
    /// [`Storage::last_processed_position`]). `0` = nothing processed.
    last_processed_position: i64,
}

/// In-process, in-memory [`Storage`] used as the M1 synchronous/test default.
///
/// Mirrors the persisted KV layout at the Rust-map level: a flow is addressed by its immutable
/// `name` (`flow_id` is audit-only, never an addressing key — executions bind a version's
/// [`ObjectReference`]), and each version row is keyed by its own `{flow}-{version}` name.
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
    fn children(&self, id: &ObjectReference) -> HashSet<ObjectReference> {
        match id.kind {
            ObjectKind::Execution => self
                .executions
                .get(&id.name)
                .map(|x| x.active_children.clone())
                .unwrap_or_default(),
            // A thread is a self-contained sub-run: it owns (and can drain) its own children.
            ObjectKind::Thread => self
                .threads
                .get(&id.name)
                .map(|x| x.active_children.clone())
                .unwrap_or_default(),
            ObjectKind::Activity => self
                .activities
                .get(&id.name)
                .map(|x| x.active_children.clone())
                .unwrap_or_default(),
            // A timer/task (or a non-node kind) is a leaf; it has no children to sweep.
            ObjectKind::Timer | ObjectKind::Task | ObjectKind::Flow | ObjectKind::FlowVersion => {
                HashSet::new()
            }
        }
    }

    fn remove_child(&mut self, parent: &ObjectReference, child: &ObjectReference) {
        match parent.kind {
            ObjectKind::Execution => {
                if let Some(exec) = self.executions.get_mut(&parent.name) {
                    exec.active_children.remove(child);
                }
            }
            ObjectKind::Thread => {
                if let Some(thread) = self.threads.get_mut(&parent.name) {
                    thread.active_children.remove(child);
                }
            }
            ObjectKind::Activity => {
                if let Some(act) = self.activities.get_mut(&parent.name) {
                    act.active_children.remove(child);
                }
            }
            // A timer/task/flow never owns children; nothing to remove.
            ObjectKind::Timer | ObjectKind::Task | ObjectKind::Flow | ObjectKind::FlowVersion => {}
        }
    }

    fn add_child(&mut self, parent: &ObjectReference, child: ObjectReference) {
        match parent.kind {
            ObjectKind::Execution => {
                if let Some(exec) = self.executions.get_mut(&parent.name) {
                    exec.active_children.insert(child);
                }
            }
            ObjectKind::Thread => {
                if let Some(thread) = self.threads.get_mut(&parent.name) {
                    thread.active_children.insert(child);
                }
            }
            ObjectKind::Activity => {
                if let Some(act) = self.activities.get_mut(&parent.name) {
                    act.active_children.insert(child);
                }
            }
            // A timer/task/flow never owns children; nothing to add.
            ObjectKind::Timer | ObjectKind::Task | ObjectKind::Flow | ObjectKind::FlowVersion => {}
        }
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn get_execution(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ExecutionRecord>, ExecutionError> {
        Ok(self.db().executions.get(&reference.name).cloned())
    }

    async fn get_thread(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ThreadRecord>, ExecutionError> {
        Ok(self.db().threads.get(&reference.name).cloned())
    }

    async fn get_activity(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ActivityRecord>, ExecutionError> {
        Ok(self.db().activities.get(&reference.name).cloned())
    }

    async fn get_timer(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<TimerRecord>, ExecutionError> {
        Ok(self.db().timers.get(&reference.name).cloned())
    }

    async fn get_task(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<TaskRecord>, ExecutionError> {
        Ok(self.db().tasks.get(&reference.name).cloned())
    }

    async fn activatable_tasks(
        &self,
        resource: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, ExecutionError> {
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

    async fn get_children(
        &self,
        id: ObjectReference,
    ) -> Result<HashSet<ObjectReference>, ExecutionError> {
        Ok(self.db().children(&id))
    }

    async fn put_execution(&mut self, exec: ExecutionRecord) -> Result<(), ExecutionError> {
        self.db().executions.insert(exec.meta.name.clone(), exec);
        Ok(())
    }

    async fn put_thread(&mut self, thread: ThreadRecord) -> Result<(), ExecutionError> {
        self.db().threads.insert(thread.meta.name.clone(), thread);
        Ok(())
    }

    async fn put_activity(&mut self, act: ActivityRecord) -> Result<(), ExecutionError> {
        self.db().activities.insert(act.meta.name.clone(), act);
        Ok(())
    }

    async fn put_timer(&mut self, timer: TimerRecord) -> Result<(), ExecutionError> {
        self.db().timers.insert(timer.meta.name.clone(), timer);
        Ok(())
    }

    async fn put_task(&mut self, task: TaskRecord) -> Result<(), ExecutionError> {
        self.db().tasks.insert(task.meta.name.clone(), task);
        Ok(())
    }

    async fn remove_child(
        &mut self,
        parent: ObjectReference,
        child: ObjectReference,
    ) -> Result<(), ExecutionError> {
        self.db().remove_child(&parent, &child);
        Ok(())
    }

    async fn add_child(
        &mut self,
        parent: ObjectReference,
        child: ObjectReference,
    ) -> Result<(), ExecutionError> {
        self.db().add_child(&parent, child);
        Ok(())
    }

    async fn get_flow_by_name(&self, name: FlowName) -> Result<Option<Flow>, ExecutionError> {
        Ok(self.db().flows.get(&name).cloned())
    }

    async fn put_flow(&mut self, flow: Flow) -> Result<(), ExecutionError> {
        self.db().flows.insert(
            flow.meta
                .name
                .as_flow_name()
                .expect("a Flow's meta.name is always a user FlowName"),
            flow,
        );
        Ok(())
    }

    async fn get_flow_version(
        &self,
        version: &ObjectReference,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        Ok(self.db().flow_versions.get(&version.name).cloned())
    }

    async fn put_flow_version(&mut self, ver: FlowVersion) -> Result<(), ExecutionError> {
        let mut db = self.db();
        // Canonical row keyed by the version's own addressing name (`{flow}-{version}`).
        db.flow_versions.insert(ver.meta.name.clone(), ver);
        Ok(())
    }

    async fn flow_version_of(
        &self,
        name: FlowName,
        version: u32,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        // Address the version by its derived name (`{flow}-{version}`) — a single point read.
        let db = self.db();
        Ok(db
            .flow_versions
            .get(&FlowVersion::version_name(&name, version))
            .cloned())
    }

    async fn last_processed_position(&self) -> Result<i64, ExecutionError> {
        Ok(self.db().last_processed_position)
    }

    async fn put_last_processed_position(&mut self, position: i64) -> Result<(), ExecutionError> {
        self.db().last_processed_position = position;
        Ok(())
    }

    fn begin_txn(&self) -> Result<Box<dyn StorageTxn>, ExecutionError> {
        // Hand the StreamProcessor an **owned** fold transaction: a clone of the shared interior plus an
        // empty buffered batch. The transaction owns its interior handle (not a borrow of this store),
        // so it can outlive any lock on it and span multiple log entries before committing. Subsequent
        // fold writes buffer; `InMemoryTxn::commit` applies them
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
    // Execution rows, keyed by the addressing `name` (the primary key, per scope); the `uid` is a
    // secondary attribute, matching the persisted `KeyBuilder::execution`.
    executions: HashMap<ObjectName, ExecutionRecord>,
    // Thread rows (fan-out sub-runs), keyed by the addressing `name`, matching `KeyBuilder::thread`.
    threads: HashMap<ObjectName, ThreadRecord>,
    activities: HashMap<ObjectName, ActivityRecord>,
    timers: HashMap<ObjectName, TimerRecord>,
    tasks: HashMap<ObjectName, TaskRecord>,
    flows: HashMap<FlowName, Flow>,
    // Version rows, keyed by the version's own `{flow}-{version}` name (see `InMemoryDb`).
    flow_versions: HashMap<ObjectName, FlowVersion>,
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
        reference: &ObjectReference,
    ) -> Result<Option<ExecutionRecord>, ExecutionError> {
        // Read-your-writes: resolve from the fold's buffered batch first, then committed state.
        if let Some(exec) = self.batch.executions.get(&reference.name) {
            return Ok(Some(exec.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .executions
            .get(&reference.name)
            .cloned())
    }

    async fn get_thread(
        &mut self,
        reference: &ObjectReference,
    ) -> Result<Option<ThreadRecord>, ExecutionError> {
        // Read-your-writes: resolve from the fold's buffered batch first, then committed state.
        if let Some(thread) = self.batch.threads.get(&reference.name) {
            return Ok(Some(thread.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .threads
            .get(&reference.name)
            .cloned())
    }

    async fn get_activity(
        &mut self,
        reference: &ObjectReference,
    ) -> Result<Option<ActivityRecord>, ExecutionError> {
        if let Some(act) = self.batch.activities.get(&reference.name) {
            return Ok(Some(act.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .activities
            .get(&reference.name)
            .cloned())
    }

    async fn get_timer(
        &mut self,
        reference: &ObjectReference,
    ) -> Result<Option<TimerRecord>, ExecutionError> {
        if let Some(timer) = self.batch.timers.get(&reference.name) {
            return Ok(Some(timer.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .timers
            .get(&reference.name)
            .cloned())
    }

    async fn get_task(
        &mut self,
        reference: &ObjectReference,
    ) -> Result<Option<TaskRecord>, ExecutionError> {
        if let Some(task) = self.batch.tasks.get(&reference.name) {
            return Ok(Some(task.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .tasks
            .get(&reference.name)
            .cloned())
    }

    async fn get_children(
        &mut self,
        id: ObjectReference,
    ) -> Result<HashSet<ObjectReference>, ExecutionError> {
        Ok(match id.kind {
            ObjectKind::Execution => self
                .get_execution(&id)
                .await?
                .map(|x| x.active_children)
                .unwrap_or_default(),
            ObjectKind::Thread => self
                .get_thread(&id)
                .await?
                .map(|x| x.active_children)
                .unwrap_or_default(),
            ObjectKind::Activity => self
                .get_activity(&id)
                .await?
                .map(|x| x.active_children)
                .unwrap_or_default(),
            // A timer/task (or a non-node kind) is a leaf; it has no children to sweep.
            ObjectKind::Timer | ObjectKind::Task | ObjectKind::Flow | ObjectKind::FlowVersion => {
                HashSet::new()
            }
        })
    }

    async fn put_execution(&mut self, exec: ExecutionRecord) -> Result<(), ExecutionError> {
        self.batch.executions.insert(exec.meta.name.clone(), exec);
        Ok(())
    }

    async fn put_thread(&mut self, thread: ThreadRecord) -> Result<(), ExecutionError> {
        self.batch.threads.insert(thread.meta.name.clone(), thread);
        Ok(())
    }

    async fn put_activity(&mut self, act: ActivityRecord) -> Result<(), ExecutionError> {
        self.batch.activities.insert(act.meta.name.clone(), act);
        Ok(())
    }

    async fn put_timer(&mut self, timer: TimerRecord) -> Result<(), ExecutionError> {
        self.batch.timers.insert(timer.meta.name.clone(), timer);
        Ok(())
    }

    async fn put_task(&mut self, task: TaskRecord) -> Result<(), ExecutionError> {
        self.batch.tasks.insert(task.meta.name.clone(), task);
        Ok(())
    }

    /// Remove `child` from `parent`'s `active_children`: read the (batch-first) row, mutate a clone,
    /// and buffer the result into this fold's batch (atomicity at commit).
    async fn remove_child(
        &mut self,
        parent: ObjectReference,
        child: ObjectReference,
    ) -> Result<(), ExecutionError> {
        self.buffer_child_mutation(&parent, &child, false);
        Ok(())
    }

    /// Add `child` to `parent`'s `active_children`: read the (batch-first) row, mutate a clone, and
    /// buffer the result into this fold's batch (atomicity at commit).
    async fn add_child(
        &mut self,
        parent: ObjectReference,
        child: ObjectReference,
    ) -> Result<(), ExecutionError> {
        self.buffer_child_mutation(&parent, &child, true);
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
        // Keyed by the flow's own name, derived from `meta.name` (its sole name carrier).
        let name = flow
            .meta
            .name
            .as_flow_name()
            .expect("a Flow's meta.name is always a user FlowName");
        self.batch.flows.insert(name, flow);
        Ok(())
    }

    async fn get_flow_version(
        &mut self,
        version: &ObjectReference,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        if let Some(ver) = self.batch.flow_versions.get(&version.name) {
            return Ok(Some(ver.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .flow_versions
            .get(&version.name)
            .cloned())
    }

    async fn put_flow_version(&mut self, ver: FlowVersion) -> Result<(), ExecutionError> {
        // Canonical row keyed by the version's own addressing name (`{flow}-{version}`).
        self.batch.flow_versions.insert(ver.meta.name.clone(), ver);
        Ok(())
    }

    async fn flow_version_of(
        &mut self,
        name: FlowName,
        version: u32,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        // Address the version by its derived name (`{flow}-{version}`), read-your-writes: batch first.
        let vname = FlowVersion::version_name(&name, version);
        if let Some(ver) = self.batch.flow_versions.get(&vname) {
            return Ok(Some(ver.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .flow_versions
            .get(&vname)
            .cloned())
    }

    fn commit(self: Box<Self>, watermark: Option<i64>) -> Result<(), ExecutionError> {
        let batch = self.batch;
        let mut db = self.inner.lock().expect("in-memory storage lock poisoned");
        // Land the whole fold all-or-nothing, then the watermark advance in the same atomic unit.
        db.executions.extend(batch.executions);
        db.threads.extend(batch.threads);
        db.activities.extend(batch.activities);
        db.timers.extend(batch.timers);
        db.tasks.extend(batch.tasks);
        db.flows.extend(batch.flows);
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
    fn buffer_child_mutation(
        &mut self,
        parent: &ObjectReference,
        child: &ObjectReference,
        insert: bool,
    ) {
        match parent.kind {
            ObjectKind::Execution => {
                let base = self
                    .batch
                    .executions
                    .get(&parent.name)
                    .cloned()
                    .or_else(|| {
                        self.inner
                            .lock()
                            .expect("in-memory storage lock poisoned")
                            .executions
                            .get(&parent.name)
                            .cloned()
                    });
                if let Some(mut exec) = base {
                    if insert {
                        exec.active_children.insert(child.clone());
                    } else {
                        exec.active_children.remove(child);
                    }
                    self.batch.executions.insert(parent.name.clone(), exec);
                }
            }
            ObjectKind::Thread => {
                let base = self.batch.threads.get(&parent.name).cloned().or_else(|| {
                    self.inner
                        .lock()
                        .expect("in-memory storage lock poisoned")
                        .threads
                        .get(&parent.name)
                        .cloned()
                });
                if let Some(mut thread) = base {
                    if insert {
                        thread.active_children.insert(child.clone());
                    } else {
                        thread.active_children.remove(child);
                    }
                    self.batch.threads.insert(parent.name.clone(), thread);
                }
            }
            ObjectKind::Activity => {
                let base = self
                    .batch
                    .activities
                    .get(&parent.name)
                    .cloned()
                    .or_else(|| {
                        self.inner
                            .lock()
                            .expect("in-memory storage lock poisoned")
                            .activities
                            .get(&parent.name)
                            .cloned()
                    });
                if let Some(mut act) = base {
                    if insert {
                        act.active_children.insert(child.clone());
                    } else {
                        act.active_children.remove(child);
                    }
                    self.batch.activities.insert(parent.name.clone(), act);
                }
            }
            // A leaf (or non-node kind) never owns children; nothing to mutate.
            ObjectKind::Timer | ObjectKind::Task | ObjectKind::Flow | ObjectKind::FlowVersion => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use spica_engine::{
        Execution, ExecutionStatus, ObjectKind, ObjectName, ObjectReference, Timestamp, Variables,
    };

    /// A distinct execution reference (`obj-<uid>`), matching `Execution::reference()`.
    fn test_exec_ref() -> ObjectReference {
        let uid = ulid::Ulid::new();
        ObjectReference::new(
            ObjectKind::Execution,
            ObjectName::generated_with_suffix("child", &uid.to_string()).unwrap(),
            uid,
        )
    }

    /// A minimal running execution row for exercising the in-memory store.
    fn sample_execution(id: ObjectReference) -> ExecutionRecord {
        ExecutionRecord {
            value: Execution {
                flow_version: ObjectReference::new(
                    ObjectKind::FlowVersion,
                    ObjectName::generated_with_suffix("flow", "00000001").unwrap(),
                    ulid::Ulid::nil(),
                ),
                status: ExecutionStatus::Running,
                input: Value::Null,
                output: None,
                meta: spica_engine::ObjectMeta::placeholder_with_times(
                    spica_engine::ObjectKind::Execution,
                    id.uid,
                    Timestamp::from_millis(0),
                    Timestamp::from_millis(0),
                ),
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
        let id = test_exec_ref();
        // A single fold transaction buffers writes; nothing is visible until commit.
        let mut txn = store.begin_txn().unwrap();
        txn.put_execution(sample_execution(id.clone()))
            .await
            .unwrap();
        assert!(store.get_execution(&id).await.unwrap().is_none());
        // Commit the fold: projection + watermark land together.
        txn.commit(Some(7)).unwrap();
        assert!(store.get_execution(&id).await.unwrap().is_some());
        assert_eq!(store.last_processed_position().await.unwrap(), 7);
        // Abort: begin a transaction, buffer a write, then drop without committing — discarded.
        let other = test_exec_ref();
        {
            let mut txn = store.begin_txn().unwrap();
            txn.put_execution(sample_execution(other.clone()))
                .await
                .unwrap();
            // drop(txn) aborts: the buffered write is never applied.
        }
        assert!(store.get_execution(&other).await.unwrap().is_none());
        assert_eq!(store.last_processed_position().await.unwrap(), 7);
    }
}
