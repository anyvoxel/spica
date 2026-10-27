//! In-process, in-memory [`Storage`] implementation — non-persistent, for M1 tests and synchronous
//! runs. Mirrors the persisted RocksDB KV layout at the Rust-map level.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;

use spica_engine_types::{
    Activity, ActivityKind, Execution, ExecutionKind, Flow, FlowName, FlowVersion, FlowVersionKind,
    ObjectKind, ObjectName, ObjectRef, RawObjectRef, Storage, StorageError, StorageTxn, Task,
    TaskKind, Thread, ThreadKind, Timestamp, Variables,
};

/// The in-memory projection state, kept behind a shared interior (see [`InMemoryStorage`]). The
/// [`RocksStorage`](crate::RocksStorage) sibling stores rows in RocksDB; here the equivalent maps are
/// the store, shared via `Arc<Mutex<..>>` so a fold transaction ([`InMemoryTxn`]) can hold an
/// independent handle to the same state and commit its buffered writes under the write lock.
#[derive(Default)]
struct InMemoryDb {
    // Execution rows, keyed by the addressing `name` (the primary key, per scope); the `uid` is a
    // secondary attribute, matching the persisted `KeyBuilder::execution`.
    executions: HashMap<ObjectName, Execution>,
    // Thread rows (fan-out sub-runs), keyed by the addressing `name`, matching `KeyBuilder::thread`.
    threads: HashMap<ObjectName, Thread>,
    /// Thread variable scopes (`Storage::get_variables`), keyed by the owning thread's addressing
    /// name — the map-level twin of the `_index/variables/…` key. A separate map rather than a field
    /// on the thread row, so an `Assign` never rewrites the row and a whole-row `put_thread` can
    /// never wipe the scope.
    variables: HashMap<ObjectName, Variables>,
    activities: HashMap<ObjectName, Activity>,
    tasks: HashMap<ObjectName, Task>,
    /// Flow rows, addressed by the immutable name (the primary key).
    flows: HashMap<FlowName, Flow>,
    /// Persisted flow versions, keyed by the version's own addressing `ObjectName` (`{flow}-{version}`,
    /// see [`FlowVersion::version_name`]); a version is located by building that name, so no separate
    /// per-flow index is needed.
    flow_versions: HashMap<ObjectName, FlowVersion>,
    /// Resume watermark — last fully processed command position (see
    /// [`Storage::last_processed_position`]). `0` = nothing processed.
    last_processed_position: i64,
    /// Partition-local generated-name counter (see [`Storage::next_generated_seq`]), the next suffix
    /// a generated object name may take. `0` = none minted yet.
    next_generated_seq: i64,
    /// Ownership edges (`Storage::get_children`), keyed by the `(parent, child)` pair — the map-level
    /// twin of the `_index/children/…` key range. Pair-keyed rather than a per-parent child set so an
    /// add/remove is one insert/remove with no read-modify-write of the parent.
    children: HashSet<(RawObjectRef, RawObjectRef)>,
}

/// In-process, in-memory [`Storage`] used as the M1 synchronous/test default.
///
/// Mirrors the persisted KV layout at the Rust-map level: a flow is addressed by its immutable
/// `name` (`flow_id` is audit-only, never an addressing key — executions bind a version's
/// [`RawObjectRef`]), and each version row is keyed by its own `{flow}-{version}` name.
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
    /// The children `parent` currently owns, read off the edge set.
    fn children(&self, parent: &RawObjectRef) -> HashSet<RawObjectRef> {
        self.children
            .iter()
            .filter(|(p, _)| p == parent)
            .map(|(_, c)| c.clone())
            .collect()
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn get_execution(
        &self,
        reference: &ObjectRef<ExecutionKind>,
    ) -> Result<Option<Execution>, StorageError> {
        Ok(self.db().executions.get(reference.name()).cloned())
    }

    async fn get_thread(
        &self,
        reference: &ObjectRef<ThreadKind>,
    ) -> Result<Option<Thread>, StorageError> {
        Ok(self.db().threads.get(reference.name()).cloned())
    }

    async fn get_variables(
        &self,
        reference: &ObjectRef<ThreadKind>,
    ) -> Result<Variables, StorageError> {
        Ok(self
            .db()
            .variables
            .get(reference.name())
            .cloned()
            .unwrap_or_default())
    }

    async fn get_activity(
        &self,
        reference: &ObjectRef<ActivityKind>,
    ) -> Result<Option<Activity>, StorageError> {
        Ok(self.db().activities.get(reference.name()).cloned())
    }

    async fn get_task(
        &self,
        reference: &ObjectRef<TaskKind>,
    ) -> Result<Option<Task>, StorageError> {
        Ok(self.db().tasks.get(reference.name()).cloned())
    }

    async fn activatable_tasks(
        &self,
        resource: &str,
        now: Timestamp,
        limit: usize,
    ) -> Result<Vec<Task>, StorageError> {
        // A linear scan of the task map — no per-resource queue index in M1 (see the trait doc).
        let db = self.db();
        Ok(db
            .tasks
            .values()
            .filter(|t| t.resource == resource && t.is_claimable_at(now))
            .take(limit)
            .cloned()
            .collect())
    }

    async fn get_children(&self, id: RawObjectRef) -> Result<HashSet<RawObjectRef>, StorageError> {
        Ok(self.db().children(&id))
    }

    async fn list_kind(
        &self,
        kind: ObjectKind,
        start_after: Option<&ObjectName>,
        limit: usize,
    ) -> Result<Vec<(ObjectName, Vec<u8>)>, StorageError> {
        // Mirror the persisted RocksDB range scan at the map level: gather the kind's rows, order
        // by the addressing name (stable key order, matching Rocks), then apply the strict-after
        // pagination offset and page cutoff. Values are serialized to their JSON bytes exactly as
        // `RocksStorage` stores them, so the two backends feed the Query facade identically.
        let db = self.db();
        let mut rows: Vec<(ObjectName, Vec<u8>)> = match kind {
            ObjectKind::Execution => encode_rows(&db.executions),
            ObjectKind::Thread => encode_rows(&db.threads),
            ObjectKind::Activity => encode_rows(&db.activities),
            ObjectKind::Task => encode_rows(&db.tasks),
            // Flows are keyed by their immutable `FlowName`, a plain name — convert to an
            // `ObjectName` so the returned key matches the other kinds' addressing shape.
            ObjectKind::Flow => db
                .flows
                .iter()
                .map(|(name, flow)| {
                    (
                        ObjectName::plain(name.as_str())
                            .expect("a FlowName is a valid plain ObjectName"),
                        serde_json::to_vec(flow).expect("Flow serializes"),
                    )
                })
                .collect(),
            ObjectKind::FlowVersion => encode_rows(&db.flow_versions),
        };
        rows.sort_by_key(|a| a.0.as_str());
        let after = start_after.map(|a| a.as_str());
        Ok(rows
            .into_iter()
            .filter(|(name, _)| match &after {
                Some(a) => name.as_str().as_bytes() > a.as_bytes(),
                None => true,
            })
            .take(limit)
            .collect())
    }

    async fn put_execution(&mut self, exec: Execution) -> Result<(), StorageError> {
        self.db().executions.insert(exec.meta.name.clone(), exec);
        Ok(())
    }

    async fn put_thread(&mut self, thread: Thread) -> Result<(), StorageError> {
        self.db().threads.insert(thread.meta.name.clone(), thread);
        Ok(())
    }

    async fn put_variables(
        &mut self,
        reference: &ObjectRef<ThreadKind>,
        variables: Variables,
    ) -> Result<(), StorageError> {
        self.db()
            .variables
            .insert(reference.name().clone(), variables);
        Ok(())
    }

    async fn put_activity(&mut self, act: Activity) -> Result<(), StorageError> {
        self.db().activities.insert(act.meta.name.clone(), act);
        Ok(())
    }

    async fn put_task(&mut self, task: Task) -> Result<(), StorageError> {
        self.db().tasks.insert(task.meta.name.clone(), task);
        Ok(())
    }

    async fn remove_child(
        &mut self,
        parent: RawObjectRef,
        child: RawObjectRef,
    ) -> Result<(), StorageError> {
        self.db().children.remove(&(parent, child));
        Ok(())
    }

    async fn add_child(
        &mut self,
        parent: RawObjectRef,
        child: RawObjectRef,
    ) -> Result<(), StorageError> {
        self.db().children.insert((parent, child));
        Ok(())
    }

    async fn get_flow_by_name(&self, name: FlowName) -> Result<Option<Flow>, StorageError> {
        Ok(self.db().flows.get(&name).cloned())
    }

    async fn put_flow(&mut self, flow: Flow) -> Result<(), StorageError> {
        self.db().flows.insert(
            flow.meta
                .name
                .as_plain()
                .expect("a Flow's meta.name is always a user FlowName")
                .clone(),
            flow,
        );
        Ok(())
    }

    async fn get_flow_version(
        &self,
        version: &ObjectRef<FlowVersionKind>,
    ) -> Result<Option<FlowVersion>, StorageError> {
        Ok(self.db().flow_versions.get(version.name()).cloned())
    }

    async fn put_flow_version(&mut self, ver: FlowVersion) -> Result<(), StorageError> {
        let mut db = self.db();
        // Canonical row keyed by the version's own addressing name (`{flow}-{version}`).
        db.flow_versions.insert(ver.meta.name.clone(), ver);
        Ok(())
    }

    async fn flow_version_of(
        &self,
        name: FlowName,
        version: u32,
    ) -> Result<Option<FlowVersion>, StorageError> {
        // Address the version by its derived name (`{flow}-{version}`) — a single point read.
        let db = self.db();
        Ok(db
            .flow_versions
            .get(&FlowVersion::version_name(&name, version))
            .cloned())
    }

    async fn last_processed_position(&self) -> Result<i64, StorageError> {
        Ok(self.db().last_processed_position)
    }

    async fn put_last_processed_position(&mut self, position: i64) -> Result<(), StorageError> {
        self.db().last_processed_position = position;
        Ok(())
    }
    async fn next_generated_seq(&self) -> Result<i64, StorageError> {
        Ok(self.db().next_generated_seq)
    }

    fn begin_txn(&self) -> Result<Box<dyn StorageTxn>, StorageError> {
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
    executions: HashMap<ObjectName, Execution>,
    // Thread rows (fan-out sub-runs), keyed by the addressing `name`, matching `KeyBuilder::thread`.
    threads: HashMap<ObjectName, Thread>,
    /// Thread variable scopes (`Storage::get_variables`), keyed by the owning thread's addressing
    /// name — the map-level twin of the `_index/variables/…` key. A separate map rather than a field
    /// on the thread row, so an `Assign` never rewrites the row and a whole-row `put_thread` can
    /// never wipe the scope.
    variables: HashMap<ObjectName, Variables>,
    activities: HashMap<ObjectName, Activity>,
    tasks: HashMap<ObjectName, Task>,
    flows: HashMap<FlowName, Flow>,
    // Version rows, keyed by the version's own `{flow}-{version}` name (see `InMemoryDb`).
    flow_versions: HashMap<ObjectName, FlowVersion>,
    // Pending bump of the partition-local generated-name counter (`None` = no bump in this fold).
    next_generated_seq: Option<i64>,
    /// Pending ownership-edge decisions, keyed by the `(parent, child)` pair with the *final*
    /// decision for that edge as the value — so add-then-remove (or the reverse) within one fold
    /// resolves to whatever the fold decided last, without a tombstone concept. Landed at commit.
    children: HashMap<(RawObjectRef, RawObjectRef), bool>,
}

impl InMemoryBatch {
    /// The edge set for `parent` as this fold sees it: the committed edges overlaid with this fold's
    /// pending decisions. Mirrors what the `RocksTxn` iterator gets for free from its pending batch.
    fn children(&self, db: &InMemoryDb, parent: &RawObjectRef) -> HashSet<RawObjectRef> {
        let mut out = db.children(parent);
        for ((p, c), present) in &self.children {
            if p != parent {
                continue;
            }
            if *present {
                out.insert(c.clone());
            } else {
                out.remove(c);
            }
        }
        out
    }
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
        reference: &ObjectRef<ExecutionKind>,
    ) -> Result<Option<Execution>, StorageError> {
        // Read-your-writes: resolve from the fold's buffered batch first, then committed state.
        if let Some(exec) = self.batch.executions.get(reference.name()) {
            return Ok(Some(exec.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .executions
            .get(reference.name())
            .cloned())
    }

    async fn get_thread(
        &mut self,
        reference: &ObjectRef<ThreadKind>,
    ) -> Result<Option<Thread>, StorageError> {
        // Read-your-writes: resolve from the fold's buffered batch first, then committed state.
        if let Some(thread) = self.batch.threads.get(reference.name()) {
            return Ok(Some(thread.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .threads
            .get(reference.name())
            .cloned())
    }

    async fn get_variables(
        &mut self,
        reference: &ObjectRef<ThreadKind>,
    ) -> Result<Variables, StorageError> {
        // Read-your-writes: an assignment folded earlier in this batch is seen by a later read.
        if let Some(variables) = self.batch.variables.get(reference.name()) {
            return Ok(variables.clone());
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .variables
            .get(reference.name())
            .cloned()
            .unwrap_or_default())
    }

    async fn get_activity(
        &mut self,
        reference: &ObjectRef<ActivityKind>,
    ) -> Result<Option<Activity>, StorageError> {
        if let Some(act) = self.batch.activities.get(reference.name()) {
            return Ok(Some(act.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .activities
            .get(reference.name())
            .cloned())
    }

    async fn get_task(
        &mut self,
        reference: &ObjectRef<TaskKind>,
    ) -> Result<Option<Task>, StorageError> {
        if let Some(task) = self.batch.tasks.get(reference.name()) {
            return Ok(Some(task.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .tasks
            .get(reference.name())
            .cloned())
    }

    async fn activatable_tasks(
        &mut self,
        resource: &str,
        now: Timestamp,
        limit: usize,
    ) -> Result<Vec<Task>, StorageError> {
        // read-your-writes: the committed rows, with the pending batch winning by name — so a task
        // folded earlier in this batch is seen (and its replacement status supersedes the committed one).
        let mut tasks: HashMap<ObjectName, Task> = self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .tasks
            .clone();
        for (k, v) in &self.batch.tasks {
            tasks.insert(k.clone(), v.clone());
        }
        Ok(tasks
            .values()
            .filter(|t| t.resource == resource && t.is_claimable_at(now))
            .take(limit)
            .cloned()
            .collect())
    }

    async fn get_children(
        &mut self,
        id: RawObjectRef,
    ) -> Result<HashSet<RawObjectRef>, StorageError> {
        let db = self.inner.lock().expect("in-memory storage lock poisoned");
        Ok(self.batch.children(&db, &id))
    }

    async fn put_execution(&mut self, exec: Execution) -> Result<(), StorageError> {
        self.batch.executions.insert(exec.meta.name.clone(), exec);
        Ok(())
    }

    async fn put_thread(&mut self, thread: Thread) -> Result<(), StorageError> {
        self.batch.threads.insert(thread.meta.name.clone(), thread);
        Ok(())
    }

    async fn put_variables(
        &mut self,
        reference: &ObjectRef<ThreadKind>,
        variables: Variables,
    ) -> Result<(), StorageError> {
        self.batch
            .variables
            .insert(reference.name().clone(), variables);
        Ok(())
    }

    async fn put_activity(&mut self, act: Activity) -> Result<(), StorageError> {
        self.batch.activities.insert(act.meta.name.clone(), act);
        Ok(())
    }

    async fn put_task(&mut self, task: Task) -> Result<(), StorageError> {
        self.batch.tasks.insert(task.meta.name.clone(), task);
        Ok(())
    }

    /// Record the removal of the `parent` → `child` edge into this fold's batch: the pair's final
    /// decision in this fold is now "absent".
    async fn remove_child(
        &mut self,
        parent: RawObjectRef,
        child: RawObjectRef,
    ) -> Result<(), StorageError> {
        self.batch.children.insert((parent, child), false);
        Ok(())
    }

    /// Record the `parent` → `child` edge into this fold's batch (last decision for the pair wins).
    async fn add_child(
        &mut self,
        parent: RawObjectRef,
        child: RawObjectRef,
    ) -> Result<(), StorageError> {
        self.batch.children.insert((parent, child), true);
        Ok(())
    }

    async fn get_flow_by_name(&mut self, name: FlowName) -> Result<Option<Flow>, StorageError> {
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

    async fn put_flow(&mut self, flow: Flow) -> Result<(), StorageError> {
        // Keyed by the flow's own name, derived from `meta.name` (its sole name carrier).
        let name = flow
            .meta
            .name
            .as_plain()
            .expect("a Flow's meta.name is always a user FlowName")
            .clone();
        self.batch.flows.insert(name, flow);
        Ok(())
    }

    async fn get_flow_version(
        &mut self,
        version: &ObjectRef<FlowVersionKind>,
    ) -> Result<Option<FlowVersion>, StorageError> {
        if let Some(ver) = self.batch.flow_versions.get(version.name()) {
            return Ok(Some(ver.clone()));
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .flow_versions
            .get(version.name())
            .cloned())
    }

    async fn put_flow_version(&mut self, ver: FlowVersion) -> Result<(), StorageError> {
        // Canonical row keyed by the version's own addressing name (`{flow}-{version}`).
        self.batch.flow_versions.insert(ver.meta.name.clone(), ver);
        Ok(())
    }

    async fn flow_version_of(
        &mut self,
        name: FlowName,
        version: u32,
    ) -> Result<Option<FlowVersion>, StorageError> {
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

    async fn next_generated_seq(&mut self) -> Result<i64, StorageError> {
        // Overlay-first: a fold must see its own earlier bump within the same batch.
        if let Some(seq) = self.batch.next_generated_seq {
            return Ok(seq);
        }
        Ok(self
            .inner
            .lock()
            .expect("in-memory storage lock poisoned")
            .next_generated_seq)
    }

    async fn put_next_generated_seq(&mut self, seq: i64) -> Result<(), StorageError> {
        self.batch.next_generated_seq = Some(seq);
        Ok(())
    }

    fn commit(self: Box<Self>, watermark: Option<i64>) -> Result<(), StorageError> {
        let batch = self.batch;
        let mut db = self.inner.lock().expect("in-memory storage lock poisoned");
        // Land the whole fold all-or-nothing, then the watermark advance in the same atomic unit.
        db.executions.extend(batch.executions);
        db.threads.extend(batch.threads);
        db.variables.extend(batch.variables);
        db.activities.extend(batch.activities);
        db.tasks.extend(batch.tasks);
        db.flows.extend(batch.flows);
        db.flow_versions.extend(batch.flow_versions);
        // Land this fold's edge decisions: each pair's final decision is either an insert or a
        // removal, applied straight to the edge set (last-write-wins was already resolved in the map).
        for (edge, present) in batch.children {
            if present {
                db.children.insert(edge);
            } else {
                db.children.remove(&edge);
            }
        }
        if let Some(seq) = batch.next_generated_seq {
            db.next_generated_seq = db.next_generated_seq.max(seq);
        }
        if let Some(w) = watermark {
            db.last_processed_position = w;
        }
        Ok(())
    }
}

/// Serialize a name-keyed row map into `(name, JSON bytes)` pairs — the in-memory twin of the
/// RocksDB `list_kind` range scan's raw-value feed. Rows always serialize (they derive `Serialize`),
/// so the `expect` documents that invariant rather than guarding a recoverable failure.
fn encode_rows<K, V>(map: &HashMap<K, V>) -> Vec<(ObjectName, Vec<u8>)>
where
    K: Clone + Into<ObjectName>,
    V: serde::Serialize,
{
    map.iter()
        .map(|(k, v)| {
            (
                k.clone().into(),
                serde_json::to_vec(v).expect("storage rows are serializable"),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use spica_engine_types::{
        Execution, ExecutionStatus, HasRawObjectRef, ObjectRef, PlainName, RetryState, StatePath,
        Task, TaskStatus, ThreadOwner, ThreadStatus, Timestamp,
    };

    /// A distinct execution reference (`obj-<uid>`), matching `meta.raw_object_ref()`.
    fn test_exec_ref() -> ObjectRef<ExecutionKind> {
        let uid = ulid::Ulid::new();
        ObjectRef::new(
            PlainName::new("child")
                .unwrap()
                .generated_from_key(uid.0 as u64),
            uid,
        )
    }

    /// A distinct thread reference (`obj-<uid>`), matching `meta.raw_object_ref()`.
    fn thread_ref() -> ObjectRef<ThreadKind> {
        let uid = ulid::Ulid::new();
        ObjectRef::new(
            PlainName::new("child")
                .unwrap()
                .generated_from_key(uid.0 as u64),
            uid,
        )
    }

    /// A minimal running thread row. Only its own identity/lifecycle facts matter here; the variable
    /// scope sits in the sibling cell under test.
    fn sample_thread(id: ObjectRef<ThreadKind>) -> Thread {
        Thread {
            meta: spica_engine_types::ObjectMeta::builder(id.uid())
                .name(id.name().clone())
                .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                .with_owner(ThreadOwner::Execution(test_exec_ref())),
            execution: test_exec_ref(),
            state_path: StatePath::root(),
            start_at: "S".into(),
            index: 0,
            status: ThreadStatus::Running,
            input: json!({}),
            output: None,
        }
    }

    /// A distinct activity reference (`obj-<uid>`), matching `meta.raw_object_ref()` — used as an
    /// arbitrary child in the edge tests.
    fn activity_ref() -> ObjectRef<ActivityKind> {
        let uid = ulid::Ulid::new();
        ObjectRef::new(
            PlainName::new("child")
                .unwrap()
                .generated_from_key(uid.0 as u64),
            uid,
        )
    }

    /// A minimal running execution row for exercising the in-memory store.
    fn sample_execution(id: ObjectRef<ExecutionKind>) -> Execution {
        Execution {
            deadline: None,
            flow_version: ObjectRef::new(
                PlainName::new("flow").unwrap().generated_from_key(1),
                ulid::Ulid::nil(),
            ),
            status: ExecutionStatus::Running,
            input: Value::Null,
            output: None,
            meta: spica_engine_types::ObjectMeta::builder(id.uid())
                .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                .with_owner(spica_engine_types::NoOwner::new()),
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

    /// A task row of `resource`, for exercising the claimable-at-`now` discovery: the caller sets
    /// exactly the facts the predicate reads (status, lease expiry, retry gate).
    fn sample_task(
        id: ulid::Ulid,
        resource: &str,
        status: TaskStatus,
        worker_id: Option<&str>,
        lease_expires_at: Option<Timestamp>,
        next_available_at: Option<Timestamp>,
    ) -> Task {
        Task {
            execution: ObjectRef::nil(),
            resource: resource.to_string(),
            arguments: Value::Null,
            status,
            deadline: None,
            worker_id: worker_id.map(str::to_string),
            lease_expires_at,
            retry_plan: vec![],
            retry_state: RetryState {
                attempts: 0,
                retrier_attempts: vec![],
                next_available_at,
            },
            meta: spica_engine_types::ObjectMeta::builder(id)
                .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                .with_owner(ObjectRef::new(
                    PlainName::new("invoke").unwrap().generated_from_key(1),
                    ulid::Ulid::nil(),
                )),
        }
    }

    #[tokio::test]
    async fn activatable_tasks_is_exactly_the_claimable_set_of_the_resource() {
        // The discovery half of a poll: only rows of `resource` that `now` makes claimable come back —
        // a lapsed lease does, a live one does not — and `limit` counts *eligible* rows, so a
        // non-claimable row can never occupy a slot the poll could have filled.
        let mut store = InMemoryStorage::new();
        let now = Timestamp::from_millis(1_000_000);
        let expired = ulid::Ulid::new();
        let live = ulid::Ulid::new();
        let gated = ulid::Ulid::new();
        let foreign = ulid::Ulid::new();
        store
            .put_task(sample_task(
                expired,
                "r",
                TaskStatus::Running,
                Some("w1"),
                Some(Timestamp::from_millis(999_999)),
                None,
            ))
            .await
            .unwrap();
        store
            .put_task(sample_task(
                live,
                "r",
                TaskStatus::Running,
                Some("w1"),
                Some(Timestamp::from_millis(1_000_001)),
                None,
            ))
            .await
            .unwrap();
        store
            .put_task(sample_task(
                gated,
                "r",
                TaskStatus::Pending,
                None,
                None,
                Some(Timestamp::from_millis(1_000_001)),
            ))
            .await
            .unwrap();
        store
            .put_task(sample_task(
                foreign,
                "other",
                TaskStatus::Pending,
                None,
                None,
                None,
            ))
            .await
            .unwrap();

        let found = store.activatable_tasks("r", now, 10).await.unwrap();
        assert_eq!(
            found.iter().map(|t| t.meta.uid).collect::<Vec<_>>(),
            vec![expired],
            "only the lapsed lease is claimable at `now`"
        );
        // `limit` is applied to the eligible set, not the scanned set: the non-claimable rows ahead of
        // it in the scan may not consume the one slot a poll asked for.
        let one = store
            .activatable_tasks("r", Timestamp::from_millis(1_000_001), 1)
            .await
            .unwrap();
        assert_eq!(one.len(), 1);
        // A zero cap is an empty page, not an unbounded scan: every backend reads `limit` the same
        // way, so a poll asking for nothing gets nothing.
        assert_eq!(
            store
                .activatable_tasks("r", Timestamp::from_millis(1_000_001), 0)
                .await
                .unwrap()
                .len(),
            0
        );

        // One second later the live lease has lapsed and the gated retry has come due.
        let later = store
            .activatable_tasks("r", Timestamp::from_millis(1_000_001), 10)
            .await
            .unwrap();
        let mut uids: Vec<_> = later.iter().map(|t| t.meta.uid).collect();
        uids.sort();
        let mut expected = vec![expired, live, gated];
        expected.sort();
        assert_eq!(uids, expected);
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

    /// The in-memory twin of the RocksDB edge tests: edges live off the parent row, so they compose
    /// with a row write instead of clobbering it, and a fold sees its own pending add/remove.
    #[tokio::test]
    async fn edges_are_independent_of_the_parent_row_and_replay_through_a_fold() {
        let mut store = InMemoryStorage::new();
        let id = test_exec_ref();
        let parent = id.clone().into_raw_object_ref();
        let child = activity_ref().into_raw_object_ref();
        let other = activity_ref().into_raw_object_ref();
        store
            .put_execution(sample_execution(id.clone()))
            .await
            .unwrap();

        assert!(store.get_children(parent.clone()).await.unwrap().is_empty());
        store
            .add_child(parent.clone(), child.clone())
            .await
            .unwrap();
        // Writing the parent row from a bare domain value (no re-attached child set) keeps the edge.
        let mut row = sample_execution(id.clone());
        row.meta.with_update_at(Timestamp::from_millis(42));
        store.put_execution(row).await.unwrap();
        assert_eq!(
            store.get_children(parent.clone()).await.unwrap(),
            HashSet::from([child.clone()])
        );
        assert_eq!(
            store
                .get_execution(&id)
                .await
                .unwrap()
                .unwrap()
                .meta
                .updated_at,
            Timestamp::from_millis(42)
        );

        // Inside one fold: add then remove the same edge resolves to the last decision, and an edge
        // added earlier in the fold is visible to a read of that same fold (read-your-writes).
        let mut txn = store.begin_txn().unwrap();
        txn.add_child(parent.clone(), other.clone()).await.unwrap();
        assert_eq!(
            txn.get_children(parent.clone()).await.unwrap(),
            HashSet::from([child.clone(), other.clone()])
        );
        txn.remove_child(parent.clone(), other.clone())
            .await
            .unwrap();
        assert_eq!(
            txn.get_children(parent.clone()).await.unwrap(),
            HashSet::from([child.clone()])
        );
        // Nothing landed before commit; after it, the removal holds and the untouched edge survives.
        assert_eq!(
            store.get_children(parent.clone()).await.unwrap(),
            HashSet::from([child.clone()])
        );
        txn.commit(None).unwrap();
        assert_eq!(
            store.get_children(parent).await.unwrap(),
            HashSet::from([child])
        );
    }

    /// The in-memory twin of the RocksDB scope tests: the variable scope lives off the thread row, so
    /// a whole-row `put_thread` cannot clobber it, an `Assign` never rewrites the row, and a fold sees
    /// its own pending write.
    #[tokio::test]
    async fn variables_are_independent_of_the_thread_row_and_replay_through_a_fold() {
        let mut store = InMemoryStorage::new();
        let id = thread_ref();
        store.put_thread(sample_thread(id.clone())).await.unwrap();
        // No scope yet: a thread reads as an empty map rather than "missing".
        assert!(store.get_variables(&id).await.unwrap().is_empty());

        store
            .put_variables(&id, Variables::from([("g".to_string(), json!("hi"))]))
            .await
            .unwrap();
        // Rewriting the whole thread row (a lifecycle update carrying no scope) keeps the scope.
        let mut row = sample_thread(id.clone());
        row.meta.with_update_at(Timestamp::from_millis(42));
        store.put_thread(row).await.unwrap();
        assert_eq!(
            store.get_variables(&id).await.unwrap().get("g"),
            Some(&json!("hi"))
        );

        // Inside one fold a write is visible to a read of that same fold (read-your-writes), and it
        // lands on commit.
        let mut txn = store.begin_txn().unwrap();
        txn.put_variables(&id, Variables::from([("x".to_string(), json!(1))]))
            .await
            .unwrap();
        assert_eq!(
            txn.get_variables(&id).await.unwrap().get("x"),
            Some(&json!(1))
        );
        // The fold's snapshot replaced the committed one wholesale.
        txn.commit(None).unwrap();
        let scope = store.get_variables(&id).await.unwrap();
        assert_eq!(scope.get("x"), Some(&json!(1)));
        assert_eq!(scope.get("g"), None);
    }
}
