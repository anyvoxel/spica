//! A persistent [`Storage`](spica_engine::Storage) projection backed by [RocksDB].
//!
//! This is the durable sibling of the in-memory [`InMemoryStorage`](crate::InMemoryStorage), sharing
//! the same [`Storage`](spica_engine::Storage) contract so the `StreamProcessor` can read/mutate either
//! interchangeably. Rows are keyed by entity type + id and stored as their JSON encoding (every row
//! type already derives [`serde::Serialize`]).
//!
//! # Durability model
//!
//! Writes go through RocksDB's **default WAL** but are **not fsync'd per call**. That is deliberate:
//! [`Storage`](spica_engine::Storage) is a *rebuildable projection* — the single source of truth is
//! the (fsync-on-append) log, `LogStream` — so a torn storage row is never fatal. Recovery re-derives
//! the store by replaying events against it (the CCES invariant: `Storage` = `fold(events)`). Forcing
//! a synchronous write here would just harden a derivative cache at the cost of every event apply.
//!
//! Within that non-fsync model, a single Event fold is still committed **atomically**: the StreamProcessor
//! wraps one fold's writes (plus its watermark advance) in a
//! [`begin_txn`](Storage::begin_txn) → [`StorageTxn::commit`] pair that lands as one RocksDB
//! transaction — all-or-nothing, so a half-applied fold is impossible.
//!
//! A fold is backed by a native RocksDB [`Transaction`] (the store is opened as an
//! [`OptimisticTransactionDB`]). `Transaction::get` resolves **read-your-writes** natively — it
//! checks the transaction's own pending writes before the committed store — which is what lets the
//! read-modify-write appliers (`add_child`/`put_execution` on the same row within one fold, e.g.
//! `StateActivating`) compose correctly on the same base row. This replaces the hand-rolled overlay
//! we previously kept, taking the same semantics from RocksDB itself.
//!
//! # Key layout
//!
//! Keys are produced by the shared [`KeyBuilder`](crate::KeyBuilder) (see its module docs for the
//! exact encoding): every row lives at `/<tenant>/<namespace>/<kind>/<identifier>` and every derived
//! index under the reserved `_index` namespace, all inside one RocksDB column family. Because the
//! only thing that identifies a row is its key, adds/removes of different entity kinds never collide
//! and a scope's rows are a clean prefix range.
//!
//! [RocksDB]: https://rocksdb.org

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use rocksdb::{Direction, IteratorMode, OptimisticTransactionDB, Transaction};

use spica_engine::{
    ActivityRecord, ExecutionError, ExecutionRecord, Flow, FlowName, FlowVersion, InfraError,
    ObjectKind, ObjectName, ObjectReference, Storage, StorageTxn, TaskRecord, TaskStatus,
    ThreadRecord, TimerRecord,
};

use crate::{KeyBuilder, Kind, Scope};

/// Read a single committed row: deserialize `T` from the value at `key`, or `None` if absent.
// Storage-boundary helper returning the engine façade (see `open` for the `result_large_err` rationale).
#[allow(clippy::result_large_err)]
fn get_row<T: serde::de::DeserializeOwned>(
    db: &OptimisticTransactionDB,
    key: Vec<u8>,
) -> Result<Option<T>, ExecutionError> {
    match db
        .get(key)
        .map_err(|e| ExecutionError::Infra(InfraError::Log(format!("rocksdb read: {e}"))))?
    {
        Some(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| ExecutionError::Infra(InfraError::Log(format!("deserialize row: {e}")))),
        None => Ok(None),
    }
}

/// Read one row inside an open fold from a RockSDB [`Transaction`]. `Transaction::get` resolves
/// **read-your-writes** natively: it checks the transaction's own pending writes first, then the
/// committed store. This is what lets read-modify-write appliers compose on a single base row within
/// one fold (e.g. `add_child` then `put_execution` of the same ExecutionRecord row, as `StateActivating`
/// does) without clobbering each other's buffered mutations.
// Storage-boundary helper returning the engine façade (see `open` for the `result_large_err` rationale).
#[allow(clippy::result_large_err)]
fn txn_get<T: serde::de::DeserializeOwned>(
    txn: &Transaction<'_, OptimisticTransactionDB>,
    key: Vec<u8>,
) -> Result<Option<T>, ExecutionError> {
    match txn
        .get(key)
        .map_err(|e| ExecutionError::Infra(InfraError::Log(format!("txn read: {e}"))))?
    {
        Some(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| ExecutionError::Infra(InfraError::Log(format!("deserialize row: {e}")))),
        None => Ok(None),
    }
}

/// Buffer one row into an open fold's [`Transaction`]: serialize `value` and record the write at
/// `key`. Nothing is visible to the committed store until [`StorageTxn::commit`].
// Storage-boundary helper returning the engine façade (see `open` for the `result_large_err` rationale).
#[allow(clippy::result_large_err)]
fn txn_put<T: serde::Serialize>(
    txn: &Transaction<'_, OptimisticTransactionDB>,
    key: Vec<u8>,
    value: &T,
) -> Result<(), ExecutionError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|e| ExecutionError::Infra(InfraError::Log(format!("serialize row: {e}"))))?;
    txn.put(key, bytes)
        .map_err(|e| ExecutionError::Infra(InfraError::Log(format!("txn write: {e}"))))?;
    Ok(())
}

/// Write one row straight to the store: serialize `value` and upsert it at `key` through the default
/// (WAL, no-fsync) write path as a single-row write. Used by the raw [`Storage`] write methods
/// (outside a fold); the transactional path buffers rows into a [`Transaction`] instead.
// Storage-boundary helper returning the engine façade (see `open` for the `result_large_err` rationale).
#[allow(clippy::result_large_err)]
fn put_row<T: serde::Serialize>(
    db: &OptimisticTransactionDB,
    key: Vec<u8>,
    value: &T,
) -> Result<(), ExecutionError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|e| ExecutionError::Infra(InfraError::Log(format!("serialize row: {e}"))))?;
    db.put(key, bytes)
        .map_err(|e| ExecutionError::Infra(InfraError::Log(format!("rocksdb write: {e}"))))?;
    Ok(())
}

/// A [`Storage`](spica_engine::Storage) whose rows are durably stored in RocksDB. Holds no internal
/// lock: the `Storage` trait already grants the sole writer exclusive `&mut self` (the `StreamProcessor`'s
/// run loop), so the `&self` RocksDB handle is never contended by two mutators of this store.
pub struct RocksStorage {
    /// Shared handle to the store, opened as an [`OptimisticTransactionDB`] so a fold transaction
    /// ([`RocksTxn`]) can begin a native RocksDB [`Transaction`] — which gives read-your-writes for
    /// free, replacing the hand-rolled overlay we previously kept. It is wrapped in `Arc` because a
    /// `Transaction` borrows the DB (rocksdb's DB is not `Clone`); the `Arc` keeps the store
    /// `Sync`/shareable and lets the checkpoint path later reach the same DB. Reads during a fold
    /// resolve through the open transaction (its own pending writes first); a txn's writes are not
    /// visible outside it until commit.
    db: Arc<OptimisticTransactionDB>,
    /// Canonical key encoding. Phase A uses the single default [`Scope`](crate::Scope); Phase B
    /// threads a per-call scope by swapping the builder this store was constructed with.
    keys: KeyBuilder,
}

impl RocksStorage {
    /// Open (creating if needed) the RocksDB store rooted at `path`. Existing committed rows are
    /// recovered automatically from RocksDB's WAL.
    ///
    /// NOTE: `RocksStorage` and `RocksLogStream` each open their own RocksDB handle/path today. A
    /// future refactor could fuse them into one DB with two column families (see the module docs) so
    /// the log and its projection share crash-recovery and `fsync` localities.
    //
    // `ExecutionError` (128B) trips `result_large_err`: the façade intentionally embeds the
    // per-concern `RuntimeError`/`InfraError`/`Reject` (the point of the error split), and these
    // storage-boundary helpers legitimately surface it — boxing would forfeit the match ergonomics.
    #[allow(clippy::result_large_err)]
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ExecutionError> {
        let db =
            Arc::new(OptimisticTransactionDB::open_default(path).map_err(|e| {
                ExecutionError::Infra(InfraError::Log(format!("rocksdb open: {e}")))
            })?);
        let keys = KeyBuilder::new(Scope::default_scope());
        Ok(Self { db, keys })
    }
}

#[async_trait]
impl Storage for RocksStorage {
    async fn get_execution(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ExecutionRecord>, ExecutionError> {
        get_row(&self.db, self.keys.execution(reference))
    }

    async fn get_thread(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ThreadRecord>, ExecutionError> {
        get_row(&self.db, self.keys.thread(reference))
    }

    async fn get_activity(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ActivityRecord>, ExecutionError> {
        get_row(&self.db, self.keys.activity(reference))
    }

    async fn get_timer(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<TimerRecord>, ExecutionError> {
        get_row(&self.db, self.keys.timer(reference))
    }

    async fn get_task(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<TaskRecord>, ExecutionError> {
        get_row(&self.db, self.keys.task(reference))
    }

    async fn activatable_tasks(
        &self,
        resource: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, ExecutionError> {
        // A forward prefix scan over the `task` kind (no per-resource queue index in M1 — see the
        // trait doc). Row values are their JSON encoding, decoded and filtered by resource+status.
        let prefix = self.keys.task_prefix();
        let mut out: Vec<TaskRecord> = Vec::new();
        for item in self.db.prefix_iterator(prefix) {
            let (_, value) = item.map_err(|e| {
                ExecutionError::Infra(InfraError::Log(format!("rocksdb task scan: {e}")))
            })?;
            let task: TaskRecord = serde_json::from_slice(&value).map_err(|e| {
                ExecutionError::Infra(InfraError::Log(format!("rocksdb task decode: {e}")))
            })?;
            if task.value.resource == resource && task.value.status == TaskStatus::Pending {
                out.push(task);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    async fn list_kind(
        &self,
        kind: ObjectKind,
        start_after: Option<&ObjectName>,
        limit: usize,
    ) -> Result<Vec<(ObjectName, Vec<u8>)>, ExecutionError> {
        // A forward prefix scan over one kind's range (k8s-style LIST). Keys are byte-ordered text,
        // so the pure-ASCII name suffix compares lexicographically — a `>`-from-`start_after` filter
        // and the page cutoff are both plain string/bounds checks; the raw value bytes are returned
        // untouched for the facade to decode into the kind's typed row.
        let prefix = self.keys.kind_prefix(Kind::from_object(kind));
        let after = start_after.as_ref().map(|n| n.as_str());
        let mut out: Vec<(ObjectName, Vec<u8>)> = Vec::new();
        for item in self.db.prefix_iterator(prefix.clone()) {
            let (key, value) = item.map_err(|e| {
                ExecutionError::Infra(InfraError::Log(format!("rocksdb {kind:?} scan: {e}")))
            })?;
            let name = &key[prefix.len()..];
            let name = std::str::from_utf8(name).map_err(|e| {
                ExecutionError::Infra(InfraError::Log(format!("rocksdb {kind:?} name: {e}")))
            })?;
            if after
                .as_ref()
                .is_some_and(|a| name.as_bytes() <= a.as_bytes())
            {
                continue;
            }
            let name = ObjectName::from_parsed(name).map_err(|e| {
                ExecutionError::Infra(InfraError::Log(format!("rocksdb {kind:?} name: {e}")))
            })?;
            out.push((name, value.to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// `active_children` is stored on the parent row, so this reads it back like
    /// [`InMemoryStorage`](crate::InMemoryStorage) does (a leaf — TimerRecord/TaskRecord — owns nothing).
    async fn get_children(
        &self,
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
        put_row(&self.db, self.keys.execution(&exec.reference()), &exec)
    }

    async fn put_thread(&mut self, thread: ThreadRecord) -> Result<(), ExecutionError> {
        put_row(&self.db, self.keys.thread(&thread.reference()), &thread)
    }

    async fn put_activity(&mut self, act: ActivityRecord) -> Result<(), ExecutionError> {
        put_row(&self.db, self.keys.activity(&act.reference()), &act)
    }

    async fn put_timer(&mut self, timer: TimerRecord) -> Result<(), ExecutionError> {
        put_row(&self.db, self.keys.timer(&timer.reference()), &timer)
    }

    async fn put_task(&mut self, task: TaskRecord) -> Result<(), ExecutionError> {
        put_row(&self.db, self.keys.task(&task.reference()), &task)
    }

    /// Read-modify-write `parent`'s `active_children` minus `child`, exactly like
    /// [`InMemoryStorage`](crate::InMemoryStorage), persisted back through the DB.
    async fn remove_child(
        &mut self,
        parent: ObjectReference,
        child: ObjectReference,
    ) -> Result<(), ExecutionError> {
        match parent.kind {
            ObjectKind::Execution => {
                if let Some(mut exec) = self.get_execution(&parent).await? {
                    exec.active_children.remove(&child);
                    self.put_execution(exec).await?;
                }
            }
            ObjectKind::Thread => {
                if let Some(mut thread) = self.get_thread(&parent).await? {
                    thread.active_children.remove(&child);
                    self.put_thread(thread).await?;
                }
            }
            ObjectKind::Activity => {
                if let Some(mut act) = self.get_activity(&parent).await? {
                    act.active_children.remove(&child);
                    self.put_activity(act).await?;
                }
            }
            // A leaf (or non-node kind) never owns children; nothing to remove.
            ObjectKind::Timer | ObjectKind::Task | ObjectKind::Flow | ObjectKind::FlowVersion => {}
        }
        Ok(())
    }

    /// Read-modify-write `parent`'s `active_children` plus `child`, persisted back through the DB.
    async fn add_child(
        &mut self,
        parent: ObjectReference,
        child: ObjectReference,
    ) -> Result<(), ExecutionError> {
        match parent.kind {
            ObjectKind::Execution => {
                if let Some(mut exec) = self.get_execution(&parent).await? {
                    exec.active_children.insert(child);
                    self.put_execution(exec).await?;
                }
            }
            ObjectKind::Thread => {
                if let Some(mut thread) = self.get_thread(&parent).await? {
                    thread.active_children.insert(child);
                    self.put_thread(thread).await?;
                }
            }
            ObjectKind::Activity => {
                if let Some(mut act) = self.get_activity(&parent).await? {
                    act.active_children.insert(child);
                    self.put_activity(act).await?;
                }
            }
            // A leaf (or non-node kind) never owns children; nothing to add.
            ObjectKind::Timer | ObjectKind::Task | ObjectKind::Flow | ObjectKind::FlowVersion => {}
        }
        Ok(())
    }

    async fn get_flow_by_name(&self, name: FlowName) -> Result<Option<Flow>, ExecutionError> {
        get_row(&self.db, self.keys.flow(&name))
    }

    async fn put_flow(&mut self, flow: Flow) -> Result<(), ExecutionError> {
        // Canonical row, addressed by the immutable name (the primary key) — derived from
        // `meta.name`, the flow's sole name carrier.
        let name = flow
            .meta
            .name
            .as_plain()
            .expect("a Flow's meta.name is always a user FlowName")
            .clone();
        put_row(&self.db, self.keys.flow(&name), &flow)
    }

    async fn get_flow_version(
        &self,
        version: &ObjectReference,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        get_row(&self.db, self.keys.flow_version(&version.name))
    }

    async fn put_flow_version(&mut self, ver: FlowVersion) -> Result<(), ExecutionError> {
        // Canonical version row, keyed by the version's own addressing name
        // (`{flow_name}-{version}`, which doubles as its storage address — no separate index).
        put_row(&self.db, self.keys.flow_version(&ver.meta.name), &ver)?;
        Ok(())
    }

    async fn flow_version_of(
        &self,
        name: FlowName,
        version: u32,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        // Address the version by its derived name (`{flow}-{version}`) — a single point read.
        get_row(
            &self.db,
            self.keys
                .flow_version(&FlowVersion::version_name(&name, version)),
        )
    }

    async fn last_processed_position(&self) -> Result<i64, ExecutionError> {
        // A global scalar: absent key ⇒ nothing processed yet, so 0 (the "resume from position 1"
        // default) — no stored row means a fresh store, indistinguishable from the initial state.
        Ok(get_row(&self.db, self.keys.last_processed_position())?.unwrap_or(0))
    }

    async fn put_last_processed_position(&mut self, position: i64) -> Result<(), ExecutionError> {
        // A standalone watermark write, used outside a fold; inside a fold the StreamProcessor passes the
        // advance to `StorageTxn::commit` so it lands in the same atomic batch as the projection.
        put_row(&self.db, self.keys.last_processed_position(), &position)
    }

    async fn next_generated_seq(&self) -> Result<i64, ExecutionError> {
        Ok(get_row(&self.db, self.keys.next_generated_seq())?.unwrap_or(0))
    }

    fn begin_txn(&self) -> Result<Box<dyn StorageTxn>, ExecutionError> {
        // Hand the StreamProcessor an **owned** fold transaction backed by a native RocksDB
        // `Transaction` begun on this store's DB (which reads its own pending writes — read-your-
        // writes — for free). The transaction holds an `Arc` clone of the DB, so it can outlive
        // `&self` and any lock on the store, and be held across multiple log entries (a follower
        // spans a whole batch) without serializing concurrent readers. See the `StorageTxn` docs.
        let db = Arc::clone(&self.db);
        // Safety: `Transaction<'db, _>`'s `'db` is a phantom lifetime (the struct only leans on the
        // C++ DB object, held by the `_db` Arc below). Borrowing it as `'static` here is sound
        // because the returned `RocksTxn` keeps a DB clone alive for its entire lifetime.
        let db_ref: &'static OptimisticTransactionDB = unsafe { &*Arc::as_ptr(&db) };
        Ok(Box::new(RocksTxn {
            txn: db_ref.transaction(),
            _db: db,
            keys: self.keys.clone(),
        }))
    }
}

/// Atomic projection transaction for [`RocksStorage`], backed by a native RocksDB [`Transaction`].
///
/// Owns its DB reachability (an `Arc` clone), so the transaction is not lifetime-bound to the store
/// and can be held across multiple log entries. Reads go through `Transaction::get`, which resolves
/// **read-your-writes** natively (the transaction keeps an index over its own pending writes, then
/// falls back to the committed store); writes buffer into the transaction;
/// [`StorageTxn::commit`] advances the watermark inside the transaction and commits it all-or-nothing
/// through the default (WAL, no-fsync) path. Dropping without committing aborts (the transaction is
/// never written). Because RocksDB's `Transaction` is `Send` but **not** `Sync`, the trait's methods
/// are all `&mut self`.
struct RocksTxn {
    /// The RocksDB transaction, borrowing the DB as `'static` (see `begin_txn`'s safety note).
    txn: Transaction<'static, OptimisticTransactionDB>,
    keys: KeyBuilder,
    /// A DB clone keeping the C++ DB object alive for the transaction's whole life. Declared *after*
    /// `txn` so it drops after the transaction frees its C++ handle (fields drop in declaration
    /// order) — the DB can never be destroyed beneath the transaction.
    _db: Arc<OptimisticTransactionDB>,
}

#[async_trait]
impl StorageTxn for RocksTxn {
    async fn get_execution(
        &mut self,
        reference: &ObjectReference,
    ) -> Result<Option<ExecutionRecord>, ExecutionError> {
        txn_get(&self.txn, self.keys.execution(reference))
    }

    async fn get_thread(
        &mut self,
        reference: &ObjectReference,
    ) -> Result<Option<ThreadRecord>, ExecutionError> {
        txn_get(&self.txn, self.keys.thread(reference))
    }

    async fn get_activity(
        &mut self,
        reference: &ObjectReference,
    ) -> Result<Option<ActivityRecord>, ExecutionError> {
        txn_get(&self.txn, self.keys.activity(reference))
    }

    async fn get_timer(
        &mut self,
        reference: &ObjectReference,
    ) -> Result<Option<TimerRecord>, ExecutionError> {
        txn_get(&self.txn, self.keys.timer(reference))
    }

    async fn get_task(
        &mut self,
        reference: &ObjectReference,
    ) -> Result<Option<TaskRecord>, ExecutionError> {
        txn_get(&self.txn, self.keys.task(reference))
    }

    async fn activatable_tasks(
        &mut self,
        resource: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, ExecutionError> {
        // A forward prefix scan through the transaction iterator, which applies the pending
        // WriteBatch over the committed store (read-your-writes): a task folded earlier in this
        // batch is seen, with its buffered status winning. No per-resource queue index in M1.
        let prefix = self.keys.task_prefix();
        let iter = self
            .txn
            .iterator(IteratorMode::From(&prefix, Direction::Forward));
        let mut out: Vec<TaskRecord> = Vec::new();
        for item in iter {
            let (key, value) = item.map_err(|e| {
                ExecutionError::Infra(InfraError::Log(format!("rocksdb txn task scan: {e}")))
            })?;
            if !key.starts_with(&prefix) {
                break;
            }
            let task: TaskRecord = serde_json::from_slice(&value).map_err(|e| {
                ExecutionError::Infra(InfraError::Log(format!("rocksdb txn task decode: {e}")))
            })?;
            if task.value.resource == resource && task.value.status == TaskStatus::Pending {
                out.push(task);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
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
        txn_put(&self.txn, self.keys.execution(&exec.reference()), &exec)
    }

    async fn put_thread(&mut self, thread: ThreadRecord) -> Result<(), ExecutionError> {
        txn_put(&self.txn, self.keys.thread(&thread.reference()), &thread)
    }

    async fn put_activity(&mut self, act: ActivityRecord) -> Result<(), ExecutionError> {
        txn_put(&self.txn, self.keys.activity(&act.reference()), &act)
    }

    async fn put_timer(&mut self, timer: TimerRecord) -> Result<(), ExecutionError> {
        txn_put(&self.txn, self.keys.timer(&timer.reference()), &timer)
    }

    async fn put_task(&mut self, task: TaskRecord) -> Result<(), ExecutionError> {
        txn_put(&self.txn, self.keys.task(&task.reference()), &task)
    }

    /// Read-modify-write `parent`'s `active_children` minus `child`: read the (read-your-writes)
    /// row, remove `child`, and buffer the result back into the transaction (atomicity at commit).
    async fn remove_child(
        &mut self,
        parent: ObjectReference,
        child: ObjectReference,
    ) -> Result<(), ExecutionError> {
        match parent.kind {
            ObjectKind::Execution => {
                if let Some(mut exec) = self.get_execution(&parent).await? {
                    exec.active_children.remove(&child);
                    self.put_execution(exec).await?;
                }
            }
            ObjectKind::Thread => {
                if let Some(mut thread) = self.get_thread(&parent).await? {
                    thread.active_children.remove(&child);
                    self.put_thread(thread).await?;
                }
            }
            ObjectKind::Activity => {
                if let Some(mut act) = self.get_activity(&parent).await? {
                    act.active_children.remove(&child);
                    self.put_activity(act).await?;
                }
            }
            // A leaf (or non-node kind) never owns children; nothing to remove.
            ObjectKind::Timer | ObjectKind::Task | ObjectKind::Flow | ObjectKind::FlowVersion => {}
        }
        Ok(())
    }

    /// Read-modify-write `parent`'s `active_children` plus `child`: read the (read-your-writes) row,
    /// insert `child`, and buffer the result back into the transaction (atomicity at commit).
    async fn add_child(
        &mut self,
        parent: ObjectReference,
        child: ObjectReference,
    ) -> Result<(), ExecutionError> {
        match parent.kind {
            ObjectKind::Execution => {
                if let Some(mut exec) = self.get_execution(&parent).await? {
                    exec.active_children.insert(child);
                    self.put_execution(exec).await?;
                }
            }
            ObjectKind::Thread => {
                if let Some(mut thread) = self.get_thread(&parent).await? {
                    thread.active_children.insert(child);
                    self.put_thread(thread).await?;
                }
            }
            ObjectKind::Activity => {
                if let Some(mut act) = self.get_activity(&parent).await? {
                    act.active_children.insert(child);
                    self.put_activity(act).await?;
                }
            }
            // A leaf (or non-node kind) never owns children; nothing to add.
            ObjectKind::Timer | ObjectKind::Task | ObjectKind::Flow | ObjectKind::FlowVersion => {}
        }
        Ok(())
    }

    async fn get_flow_by_name(&mut self, name: FlowName) -> Result<Option<Flow>, ExecutionError> {
        txn_get(&self.txn, self.keys.flow(&name))
    }

    async fn put_flow(&mut self, flow: Flow) -> Result<(), ExecutionError> {
        // Keyed by the flow's own name, derived from `meta.name` (its sole name carrier).
        let name = flow
            .meta
            .name
            .as_plain()
            .expect("a Flow's meta.name is always a user FlowName")
            .clone();
        txn_put(&self.txn, self.keys.flow(&name), &flow)
    }

    async fn get_flow_version(
        &mut self,
        version: &ObjectReference,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        txn_get(&self.txn, self.keys.flow_version(&version.name))
    }

    async fn put_flow_version(&mut self, ver: FlowVersion) -> Result<(), ExecutionError> {
        // Canonical version row, keyed by the version's own addressing name
        // (`{flow_name}-{version}`, which doubles as its storage address — no separate index).
        txn_put(&self.txn, self.keys.flow_version(&ver.meta.name), &ver)?;
        Ok(())
    }

    async fn flow_version_of(
        &mut self,
        name: FlowName,
        version: u32,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        // Address the version by its derived name (`{flow}-{version}`) — a single point read.
        txn_get(
            &self.txn,
            self.keys
                .flow_version(&FlowVersion::version_name(&name, version)),
        )
    }

    async fn next_generated_seq(&mut self) -> Result<i64, ExecutionError> {
        // The native RocksDB Transaction reads its own pending writes (read-your-writes for free),
        // so a fold sees its own earlier bump within the same batch.
        Ok(txn_get(&self.txn, self.keys.next_generated_seq())?.unwrap_or(0))
    }

    async fn put_next_generated_seq(&mut self, seq: i64) -> Result<(), ExecutionError> {
        txn_put(&self.txn, self.keys.next_generated_seq(), &seq)?;
        Ok(())
    }

    fn commit(self: Box<Self>, watermark: Option<i64>) -> Result<(), ExecutionError> {
        // Move the transaction out of the box (and drop the DB clone with it).
        let RocksTxn { txn, keys, .. } = *self;
        // Fold the watermark advance into the same transaction as the projection, so it can never run
        // ahead of the projection (the invariant that makes restart-from-W+1 safe).
        if let Some(w) = watermark {
            let bytes = serde_json::to_vec(&w).map_err(|e| {
                ExecutionError::Infra(InfraError::Log(format!("serialize watermark: {e}")))
            })?;
            txn.put(keys.last_processed_position(), bytes)
                .map_err(|e| {
                    ExecutionError::Infra(InfraError::Log(format!("txn write watermark: {e}")))
                })?;
        }
        // Atomic all-or-nothing commit of the whole fold (projection + watermark) through the default
        // (WAL, no-fsync) write path — a half-applied fold is impossible. Dropping the box instead of
        // committing aborts: the transaction is never written.
        txn.commit().map_err(|e| {
            ExecutionError::Infra(InfraError::Log(format!("rocksdb txn commit: {e}")))
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use spica_engine::{
        Execution, ExecutionStatus, ObjectKind, ObjectReference, PlainName, Timer, TimerId,
        TimerPurpose, TimerStatus, Timestamp, Variables,
    };

    /// A distinct execution reference (`obj-<uid>`), matching `Execution::reference()`.
    fn test_exec_ref() -> ObjectReference {
        let uid = ulid::Ulid::new();
        ObjectReference::new(
            ObjectKind::Execution,
            PlainName::new("child")
                .unwrap()
                .generated_from_key(uid.0 as u64),
            uid,
        )
    }

    /// A distinct timer reference (`child-<uid>`), matching a `Timer::reference()`.
    fn timer_ref() -> ObjectReference {
        let uid = ulid::Ulid::new();
        ObjectReference::new(
            ObjectKind::Timer,
            PlainName::new("child")
                .unwrap()
                .generated_from_key(uid.0 as u64),
            uid,
        )
    }

    /// A unique per-test store path under the system temp dir, removed after the test.
    fn temp_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("spica-rocks-storage-{tag}-{}", ulid::Ulid::new()))
    }

    /// A minimal running execution row for exercising the store.
    fn sample_execution(id: ObjectReference) -> ExecutionRecord {
        ExecutionRecord {
            value: Execution {
                flow_version: ObjectReference::new(
                    ObjectKind::FlowVersion,
                    PlainName::new("flow").unwrap().generated_from_key(1),
                    ulid::Ulid::nil(),
                ),
                status: ExecutionStatus::Running,
                input: Value::Null,
                output: None,
                meta: spica_engine::ObjectMeta::builder(
                    spica_engine::ObjectKind::Execution,
                    id.uid,
                )
                .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                .build(),
            },
            variables: Variables::new(),
            current_activity: None,
            active_children: HashSet::new(),
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        }
    }

    #[tokio::test]
    async fn put_get_and_reopen_persist_rows() {
        let path = temp_path("rows");
        let id = test_exec_ref();
        {
            let mut store = RocksStorage::open(&path).unwrap();
            assert!(store.get_execution(&id).await.unwrap().is_none());
            store
                .put_execution(sample_execution(id.clone()))
                .await
                .unwrap();
            let got = store.get_execution(&id).await.unwrap().unwrap();
            assert_eq!(got.reference(), id);
        }
        // Reopened store must still see the row (durability via RocksDB WAL across a drop/reopen).
        {
            let store = RocksStorage::open(&path).unwrap();
            assert_eq!(
                store.get_execution(&id).await.unwrap().unwrap().reference(),
                id
            );
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn add_and_remove_child_tracks_active_children() {
        let path = temp_path("children");
        let id = test_exec_ref();
        let child = timer_ref();
        {
            let mut store = RocksStorage::open(&path).unwrap();
            store
                .put_execution(sample_execution(id.clone()))
                .await
                .unwrap();

            let parent = id.clone();
            // no children initially
            assert!(store.get_children(parent.clone()).await.unwrap().is_empty());
            // add, then reflect in the persisted row
            store
                .add_child(parent.clone(), child.clone())
                .await
                .unwrap();
            assert_eq!(
                store.get_children(parent.clone()).await.unwrap(),
                HashSet::from([child.clone()])
            );
            // a leaf parent (TimerRecord) owns nothing and never grows
            assert!(store.get_children(timer_ref()).await.unwrap().is_empty());
            // remove, then reflect
            store
                .remove_child(parent.clone(), child.clone())
                .await
                .unwrap();
            assert!(store.get_children(parent).await.unwrap().is_empty());
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn different_entity_kinds_do_not_collide() {
        let path = temp_path("kinds");
        let id = test_exec_ref();
        {
            let mut store = RocksStorage::open(&path).unwrap();
            store
                .put_execution(sample_execution(id.clone()))
                .await
                .unwrap();
            // A timer under the same numeric-ish space is a distinct key namespace.
            let timer_uid = TimerId::new();
            let uid: ulid::Ulid = timer_uid.into();
            let timer_ref = ObjectReference::new(
                ObjectKind::Timer,
                PlainName::new("child")
                    .unwrap()
                    .generated_from_key(uid.0 as u64),
                uid,
            );
            let t = TimerRecord::from_value(Timer {
                execution: id.clone(),
                purpose: TimerPurpose::WaitResume,
                status: TimerStatus::Active,
                deadline: Timestamp::from_millis(0),
                meta: spica_engine::ObjectMeta::builder(
                    spica_engine::ObjectKind::Timer,
                    timer_uid.into(),
                )
                .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                .build()
                .with_owner(id.clone()),
            });
            store.put_timer(t.clone()).await.unwrap();
            assert_eq!(
                store
                    .get_timer(&timer_ref)
                    .await
                    .unwrap()
                    .unwrap()
                    .reference(),
                timer_ref
            );
            // the execution row is untouched by writing a timer
            assert_eq!(
                store.get_execution(&id).await.unwrap().unwrap().reference(),
                id
            );
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn last_processed_position_defaults_zero_and_roundtrips() {
        let path = temp_path("watermark");
        {
            let mut store = RocksStorage::open(&path).unwrap();
            // Fresh store: nothing processed → 0, the resume-from-position-1 default.
            assert_eq!(store.last_processed_position().await.unwrap(), 0);
            store.put_last_processed_position(7).await.unwrap();
            assert_eq!(store.last_processed_position().await.unwrap(), 7);
        }
        // The watermark survives a reopen (durability via the shared WAL write path).
        {
            let store = RocksStorage::open(&path).unwrap();
            assert_eq!(store.last_processed_position().await.unwrap(), 7);
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn txn_commits_atomically_and_abort_discards() {
        let path = temp_path("txn");
        let id = test_exec_ref();
        {
            let store = RocksStorage::open(&path).unwrap();
            // A single fold transaction buffers its writes; nothing is visible until commit.
            let mut txn = store.begin_txn().unwrap();
            txn.put_execution(sample_execution(id.clone()))
                .await
                .unwrap();
            // In-flight writes resolve from the txn overlay (read-your-writes)...
            assert!(txn.get_execution(&id).await.unwrap().is_some());
            // ...but are not visible to the committed store (external readers) until commit.
            assert!(store.get_execution(&id).await.unwrap().is_none());
            assert_eq!(store.last_processed_position().await.unwrap(), 0);
            // Commit the fold: the projection AND the watermark land together, all-or-nothing.
            txn.commit(Some(7)).unwrap();
            assert!(store.get_execution(&id).await.unwrap().is_some());
            assert_eq!(store.last_processed_position().await.unwrap(), 7);
        }
        // The committed fold is durable across a reopen (via the shared WAL path).
        {
            let store = RocksStorage::open(&path).unwrap();
            assert!(store.get_execution(&id).await.unwrap().is_some());
            assert_eq!(store.last_processed_position().await.unwrap(), 7);
        }
        // Abort: begin a transaction, buffer a write, then drop it without committing — the
        // discarded overlay must never reach the DB.
        {
            let store = RocksStorage::open(&path).unwrap();
            let other = test_exec_ref();
            let mut txn = store.begin_txn().unwrap();
            txn.put_execution(sample_execution(other.clone()))
                .await
                .unwrap();
            // drop(txn) aborts the fold: the buffered write is discarded, never committed.
            drop(txn);
            assert!(store.get_execution(&other).await.unwrap().is_none());
        }
        {
            let store = RocksStorage::open(&path).unwrap();
            assert_eq!(store.last_processed_position().await.unwrap(), 7);
        }
        let _ = std::fs::remove_dir_all(&path);
    }
}
