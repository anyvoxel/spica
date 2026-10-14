//! A persistent [`Storage`](spica_engine_types::Storage) projection backed by [RocksDB].
//!
//! This is the durable sibling of the in-memory [`InMemoryStorage`](crate::InMemoryStorage), sharing
//! the same [`Storage`](spica_engine_types::Storage) contract so the `StreamProcessor` can read/mutate either
//! interchangeably. Rows are keyed by entity type + id and stored as their JSON encoding (every row
//! type already derives [`serde::Serialize`]).
//!
//! # Durability model
//!
//! Writes go through RocksDB's **default WAL** but are **not fsync'd per call**. That is deliberate:
//! [`Storage`](spica_engine_types::Storage) is a *rebuildable projection* — the single source of truth is
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
//! checks the transaction's own pending writes before the committed store — which is what lets a
//! fold read a row and then write it back (an applier reconstructing a record from its loaded base)
//! compose correctly within the same fold. This replaces the hand-rolled overlay we previously kept,
//! taking the same semantics from RocksDB itself.
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

use spica_engine_types::{
    Activity, ActivityKind, Execution, ExecutionKind, Flow, FlowName, FlowVersion, FlowVersionKind,
    ObjectKind, ObjectName, ObjectRef, RawObjectRef, Storage, StorageError, StorageTxn, Task,
    TaskKind, Thread, ThreadKind, Timer, TimerKind, Timestamp, Variables,
};

use crate::{KeyBuilder, Kind, Scope};

/// A [`Storage`](spica_engine_types::Storage) whose rows are durably stored in RocksDB. Holds no internal
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
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let db = Arc::new(
            OptimisticTransactionDB::open_default(path)
                .map_err(|e| StorageError::Backend(format!("rocksdb open: {e}")))?,
        );
        let keys = KeyBuilder::new(Scope::default_scope());
        Ok(Self { db, keys })
    }

    /// Read a single committed row outside any fold: deserialize `T` from the value at `key`, or `None`
    /// if absent.
    fn get_row<T: serde::de::DeserializeOwned>(
        &self,
        key: Vec<u8>,
    ) -> Result<Option<T>, StorageError> {
        match self
            .db
            .get(key)
            .map_err(|e| StorageError::Backend(format!("rocksdb read: {e}")))?
        {
            Some(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| StorageError::Codec(format!("deserialize row: {e}"))),
            None => Ok(None),
        }
    }

    /// Write one row straight to the store: serialize `value` and upsert it at `key` through the default
    /// (WAL, no-fsync) write path as a single-row write. Used by the raw [`Storage`] write methods
    /// (outside a fold); the transactional path buffers rows into a [`Transaction`] instead.
    fn put_row<T: serde::Serialize>(&self, key: Vec<u8>, value: &T) -> Result<(), StorageError> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| StorageError::Codec(format!("serialize row: {e}")))?;
        self.db
            .put(key, bytes)
            .map_err(|e| StorageError::Backend(format!("rocksdb write: {e}")))?;
        Ok(())
    }

    /// Delete one row at `key` through the same default (WAL, no-fsync) path. An absent key is not an
    /// error — a delete is idempotent, which is what makes removing an ownership edge twice harmless.
    fn delete_row(&self, key: Vec<u8>) -> Result<(), StorageError> {
        self.db
            .delete(key)
            .map_err(|e| StorageError::Backend(format!("rocksdb delete: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl Storage for RocksStorage {
    async fn get_execution(
        &self,
        reference: &ObjectRef<ExecutionKind>,
    ) -> Result<Option<Execution>, StorageError> {
        self.get_row(self.keys.execution(reference))
    }

    async fn get_thread(
        &self,
        reference: &ObjectRef<ThreadKind>,
    ) -> Result<Option<Thread>, StorageError> {
        self.get_row(self.keys.thread(reference))
    }

    async fn get_variables(
        &self,
        reference: &ObjectRef<ThreadKind>,
    ) -> Result<Variables, StorageError> {
        // A thread with no `Assign` yet has no row here — an empty scope, not a missing one.
        Ok(self
            .get_row(self.keys.variables(reference.name()))?
            .unwrap_or_default())
    }

    async fn get_activity(
        &self,
        reference: &ObjectRef<ActivityKind>,
    ) -> Result<Option<Activity>, StorageError> {
        self.get_row(self.keys.activity(reference))
    }

    async fn get_timer(
        &self,
        reference: &ObjectRef<TimerKind>,
    ) -> Result<Option<Timer>, StorageError> {
        self.get_row(self.keys.timer(reference))
    }

    async fn get_task(
        &self,
        reference: &ObjectRef<TaskKind>,
    ) -> Result<Option<Task>, StorageError> {
        self.get_row(self.keys.task(reference))
    }

    async fn activatable_tasks(
        &self,
        resource: &str,
        now: Timestamp,
        limit: usize,
    ) -> Result<Vec<Task>, StorageError> {
        // A forward prefix scan over the `task` kind (no per-resource queue index in M1 — see the
        // trait doc). Row values are their JSON encoding, decoded and filtered by resource+status.
        let prefix = self.keys.task_prefix();
        let mut out: Vec<Task> = Vec::new();
        for item in self.db.prefix_iterator(prefix) {
            let (_, value) =
                item.map_err(|e| StorageError::Backend(format!("rocksdb task scan: {e}")))?;
            let task: Task = serde_json::from_slice(&value)
                .map_err(|e| StorageError::Codec(format!("rocksdb task decode: {e}")))?;
            if task.resource == resource && task.is_claimable_at(now) {
                // The cap counts rows already collected, so it is tested *before* the row joins
                // them: `limit == 0` yields an empty page on every backend.
                if out.len() >= limit {
                    break;
                }
                out.push(task);
            }
        }
        Ok(out)
    }

    async fn list_kind(
        &self,
        kind: ObjectKind,
        start_after: Option<&ObjectName>,
        limit: usize,
    ) -> Result<Vec<(ObjectName, Vec<u8>)>, StorageError> {
        // A forward prefix scan over one kind's range (k8s-style LIST). Keys are byte-ordered text,
        // so the pure-ASCII name suffix compares lexicographically — a `>`-from-`start_after` filter
        // and the page cutoff are both plain string/bounds checks; the raw value bytes are returned
        // untouched for the facade to decode into the kind's typed row.
        let prefix = self.keys.kind_prefix(Kind::from_object(kind));
        let after = start_after.as_ref().map(|n| n.as_str());
        let mut out: Vec<(ObjectName, Vec<u8>)> = Vec::new();
        for item in self.db.prefix_iterator(prefix.clone()) {
            let (key, value) =
                item.map_err(|e| StorageError::Backend(format!("rocksdb {kind:?} scan: {e}")))?;
            let name = &key[prefix.len()..];
            let name = std::str::from_utf8(name)
                .map_err(|e| StorageError::InvalidKey(format!("rocksdb {kind:?} name: {e}")))?;
            if after
                .as_ref()
                .is_some_and(|a| name.as_bytes() <= a.as_bytes())
            {
                continue;
            }
            let name = ObjectName::from_parsed(name)
                .map_err(|e| StorageError::InvalidKey(format!("rocksdb {kind:?} name: {e}")))?;
            out.push((name, value.to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// Read `id`'s ownership edges: a prefix scan over the `_index/children/<id>/…` range, decoding
    /// each edge's stored child reference. One contiguous range per parent, whatever kind it is.
    async fn get_children(&self, id: RawObjectRef) -> Result<HashSet<RawObjectRef>, StorageError> {
        let prefix = self.keys.child_edge_prefix(&id);
        let mut out = HashSet::new();
        for item in self.db.prefix_iterator(prefix.clone()) {
            let (key, value) =
                item.map_err(|e| StorageError::Backend(format!("rocksdb edge scan: {e}")))?;
            if !key.starts_with(&prefix) {
                break;
            }
            out.insert(
                serde_json::from_slice(&value)
                    .map_err(|e| StorageError::Codec(format!("rocksdb edge decode: {e}")))?,
            );
        }
        Ok(out)
    }

    async fn put_execution(&mut self, exec: Execution) -> Result<(), StorageError> {
        self.put_row(self.keys.execution(&exec.meta.object_ref()), &exec)
    }

    async fn put_thread(&mut self, thread: Thread) -> Result<(), StorageError> {
        self.put_row(self.keys.thread(&thread.meta.object_ref()), &thread)
    }

    /// Overwrite the scope row — a single put, with no read of either cell: the event carries the
    /// full post-assign snapshot.
    async fn put_variables(
        &mut self,
        reference: &ObjectRef<ThreadKind>,
        variables: Variables,
    ) -> Result<(), StorageError> {
        self.put_row(self.keys.variables(reference.name()), &variables)
    }

    async fn put_activity(&mut self, act: Activity) -> Result<(), StorageError> {
        self.put_row(self.keys.activity(&act.meta.object_ref()), &act)
    }

    async fn put_timer(&mut self, timer: Timer) -> Result<(), StorageError> {
        self.put_row(self.keys.timer(&timer.meta.object_ref()), &timer)
    }

    async fn put_task(&mut self, task: Task) -> Result<(), StorageError> {
        self.put_row(self.keys.task(&task.meta.object_ref()), &task)
    }

    /// Delete the ownership edge `parent` → `child`: one single-row delete, with no read-modify-write
    /// of the parent (see [`KeyBuilder::child_edge`](crate::KeyBuilder::child_edge)).
    async fn remove_child(
        &mut self,
        parent: RawObjectRef,
        child: RawObjectRef,
    ) -> Result<(), StorageError> {
        self.delete_row(self.keys.child_edge(&parent, &child))
    }

    /// Write the ownership edge `parent` → `child` as its own row. Because the edge is not a column on
    /// the parent, this cannot clobber a concurrent write of the parent's own row (nor vice versa).
    async fn add_child(
        &mut self,
        parent: RawObjectRef,
        child: RawObjectRef,
    ) -> Result<(), StorageError> {
        self.put_row(self.keys.child_edge(&parent, &child), &child)
    }

    async fn get_flow_by_name(&self, name: FlowName) -> Result<Option<Flow>, StorageError> {
        self.get_row(self.keys.flow(&name))
    }

    async fn put_flow(&mut self, flow: Flow) -> Result<(), StorageError> {
        // Canonical row, addressed by the immutable name (the primary key) — derived from
        // `meta.name`, the flow's sole name carrier.
        let name = flow
            .meta
            .name
            .as_plain()
            .expect("a Flow's meta.name is always a user FlowName")
            .clone();
        self.put_row(self.keys.flow(&name), &flow)
    }

    async fn get_flow_version(
        &self,
        version: &ObjectRef<FlowVersionKind>,
    ) -> Result<Option<FlowVersion>, StorageError> {
        self.get_row(self.keys.flow_version(version.name()))
    }

    async fn put_flow_version(&mut self, ver: FlowVersion) -> Result<(), StorageError> {
        // Canonical version row, keyed by the version's own addressing name
        // (`{flow_name}-{version}`, which doubles as its storage address — no separate index).
        self.put_row(self.keys.flow_version(&ver.meta.name), &ver)?;
        Ok(())
    }

    async fn flow_version_of(
        &self,
        name: FlowName,
        version: u32,
    ) -> Result<Option<FlowVersion>, StorageError> {
        // Address the version by its derived name (`{flow}-{version}`) — a single point read.
        self.get_row(
            self.keys
                .flow_version(&FlowVersion::version_name(&name, version)),
        )
    }

    async fn last_processed_position(&self) -> Result<i64, StorageError> {
        // A global scalar: absent key ⇒ nothing processed yet, so 0 (the "resume from position 1"
        // default) — no stored row means a fresh store, indistinguishable from the initial state.
        Ok(self
            .get_row(self.keys.last_processed_position())?
            .unwrap_or(0))
    }

    async fn put_last_processed_position(&mut self, position: i64) -> Result<(), StorageError> {
        // A standalone watermark write, used outside a fold; inside a fold the StreamProcessor passes the
        // advance to `StorageTxn::commit` so it lands in the same atomic batch as the projection.
        self.put_row(self.keys.last_processed_position(), &position)
    }

    async fn next_generated_seq(&self) -> Result<i64, StorageError> {
        Ok(self.get_row(self.keys.next_generated_seq())?.unwrap_or(0))
    }

    fn begin_txn(&self) -> Result<Box<dyn StorageTxn>, StorageError> {
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

impl RocksTxn {
    /// Read one row through this fold's [`Transaction`]. Read-your-writes (see the type docs) is what
    /// lets a fold that reads a row and writes it back compose on its own earlier write.
    fn get_row<T: serde::de::DeserializeOwned>(
        &self,
        key: Vec<u8>,
    ) -> Result<Option<T>, StorageError> {
        match self
            .txn
            .get(key)
            .map_err(|e| StorageError::Backend(format!("txn read: {e}")))?
        {
            Some(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| StorageError::Codec(format!("deserialize row: {e}"))),
            None => Ok(None),
        }
    }

    /// Buffer one row into this fold's [`Transaction`]: serialize `value` and record the write at
    /// `key`. Nothing is visible to the committed store until [`StorageTxn::commit`].
    fn put_row<T: serde::Serialize>(&self, key: Vec<u8>, value: &T) -> Result<(), StorageError> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| StorageError::Codec(format!("serialize row: {e}")))?;
        self.txn
            .put(key, bytes)
            .map_err(|e| StorageError::Backend(format!("txn write: {e}")))?;
        Ok(())
    }

    /// Buffer a row delete into this fold's [`Transaction`] (the ownership-edge removal path);
    /// buffered deletes compose with earlier buffered writes of the same key, last one winning.
    fn delete_row(&self, key: Vec<u8>) -> Result<(), StorageError> {
        self.txn
            .delete(key)
            .map_err(|e| StorageError::Backend(format!("txn delete: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl StorageTxn for RocksTxn {
    async fn get_execution(
        &mut self,
        reference: &ObjectRef<ExecutionKind>,
    ) -> Result<Option<Execution>, StorageError> {
        self.get_row(self.keys.execution(reference))
    }

    async fn get_thread(
        &mut self,
        reference: &ObjectRef<ThreadKind>,
    ) -> Result<Option<Thread>, StorageError> {
        self.get_row(self.keys.thread(reference))
    }

    /// Read the scope inside this fold (read-your-writes: a put buffered earlier in the same fold is
    /// visible, since the RocksDB `Transaction` reads through its own batch).
    async fn get_variables(
        &mut self,
        reference: &ObjectRef<ThreadKind>,
    ) -> Result<Variables, StorageError> {
        Ok(self
            .get_row(self.keys.variables(reference.name()))?
            .unwrap_or_default())
    }

    async fn get_activity(
        &mut self,
        reference: &ObjectRef<ActivityKind>,
    ) -> Result<Option<Activity>, StorageError> {
        self.get_row(self.keys.activity(reference))
    }

    async fn get_timer(
        &mut self,
        reference: &ObjectRef<TimerKind>,
    ) -> Result<Option<Timer>, StorageError> {
        self.get_row(self.keys.timer(reference))
    }

    async fn get_task(
        &mut self,
        reference: &ObjectRef<TaskKind>,
    ) -> Result<Option<Task>, StorageError> {
        self.get_row(self.keys.task(reference))
    }

    async fn activatable_tasks(
        &mut self,
        resource: &str,
        now: Timestamp,
        limit: usize,
    ) -> Result<Vec<Task>, StorageError> {
        // A forward prefix scan through the transaction iterator, which applies the pending
        // WriteBatch over the committed store (read-your-writes): a task folded earlier in this
        // batch is seen, with its buffered status winning. No per-resource queue index in M1.
        let prefix = self.keys.task_prefix();
        let iter = self
            .txn
            .iterator(IteratorMode::From(&prefix, Direction::Forward));
        let mut out: Vec<Task> = Vec::new();
        for item in iter {
            let (key, value) =
                item.map_err(|e| StorageError::Backend(format!("rocksdb txn task scan: {e}")))?;
            if !key.starts_with(&prefix) {
                break;
            }
            let task: Task = serde_json::from_slice(&value)
                .map_err(|e| StorageError::Codec(format!("rocksdb txn task decode: {e}")))?;
            if task.resource == resource && task.is_claimable_at(now) {
                // Same cap-before-collect ordering as the committed-store scan above.
                if out.len() >= limit {
                    break;
                }
                out.push(task);
            }
        }
        Ok(out)
    }

    /// Read `id`'s ownership edges through this fold's [`Transaction`] iterator, which applies the
    /// pending batch over the committed store (read-your-writes): an edge added earlier in this fold
    /// is seen, and one removed earlier is gone.
    async fn get_children(
        &mut self,
        id: RawObjectRef,
    ) -> Result<HashSet<RawObjectRef>, StorageError> {
        let prefix = self.keys.child_edge_prefix(&id);
        let iter = self
            .txn
            .iterator(IteratorMode::From(&prefix, Direction::Forward));
        let mut out = HashSet::new();
        for item in iter {
            let (key, value) =
                item.map_err(|e| StorageError::Backend(format!("rocksdb txn edge scan: {e}")))?;
            if !key.starts_with(&prefix) {
                break;
            }
            out.insert(
                serde_json::from_slice(&value)
                    .map_err(|e| StorageError::Codec(format!("rocksdb txn edge decode: {e}")))?,
            );
        }
        Ok(out)
    }

    async fn put_execution(&mut self, exec: Execution) -> Result<(), StorageError> {
        self.put_row(self.keys.execution(&exec.meta.object_ref()), &exec)
    }

    async fn put_thread(&mut self, thread: Thread) -> Result<(), StorageError> {
        self.put_row(self.keys.thread(&thread.meta.object_ref()), &thread)
    }

    /// Overwrite the scope row — a single put, with no read of either cell: the event carries the
    /// full post-assign snapshot.
    async fn put_variables(
        &mut self,
        reference: &ObjectRef<ThreadKind>,
        variables: Variables,
    ) -> Result<(), StorageError> {
        self.put_row(self.keys.variables(reference.name()), &variables)
    }

    async fn put_activity(&mut self, act: Activity) -> Result<(), StorageError> {
        self.put_row(self.keys.activity(&act.meta.object_ref()), &act)
    }

    async fn put_timer(&mut self, timer: Timer) -> Result<(), StorageError> {
        self.put_row(self.keys.timer(&timer.meta.object_ref()), &timer)
    }

    async fn put_task(&mut self, task: Task) -> Result<(), StorageError> {
        self.put_row(self.keys.task(&task.meta.object_ref()), &task)
    }

    /// Buffer the deletion of the ownership edge `parent` → `child` into this fold's transaction.
    async fn remove_child(
        &mut self,
        parent: RawObjectRef,
        child: RawObjectRef,
    ) -> Result<(), StorageError> {
        self.delete_row(self.keys.child_edge(&parent, &child))
    }

    /// Buffer the ownership edge `parent` → `child` into this fold's transaction as its own row.
    async fn add_child(
        &mut self,
        parent: RawObjectRef,
        child: RawObjectRef,
    ) -> Result<(), StorageError> {
        self.put_row(self.keys.child_edge(&parent, &child), &child)
    }

    async fn get_flow_by_name(&mut self, name: FlowName) -> Result<Option<Flow>, StorageError> {
        self.get_row(self.keys.flow(&name))
    }

    async fn put_flow(&mut self, flow: Flow) -> Result<(), StorageError> {
        // Keyed by the flow's own name, derived from `meta.name` (its sole name carrier).
        let name = flow
            .meta
            .name
            .as_plain()
            .expect("a Flow's meta.name is always a user FlowName")
            .clone();
        self.put_row(self.keys.flow(&name), &flow)
    }

    async fn get_flow_version(
        &mut self,
        version: &ObjectRef<FlowVersionKind>,
    ) -> Result<Option<FlowVersion>, StorageError> {
        self.get_row(self.keys.flow_version(version.name()))
    }

    async fn put_flow_version(&mut self, ver: FlowVersion) -> Result<(), StorageError> {
        // Canonical version row, keyed by the version's own addressing name
        // (`{flow_name}-{version}`, which doubles as its storage address — no separate index).
        self.put_row(self.keys.flow_version(&ver.meta.name), &ver)?;
        Ok(())
    }

    async fn flow_version_of(
        &mut self,
        name: FlowName,
        version: u32,
    ) -> Result<Option<FlowVersion>, StorageError> {
        // Address the version by its derived name (`{flow}-{version}`) — a single point read.
        self.get_row(
            self.keys
                .flow_version(&FlowVersion::version_name(&name, version)),
        )
    }

    async fn next_generated_seq(&mut self) -> Result<i64, StorageError> {
        // The native RocksDB Transaction reads its own pending writes (read-your-writes for free),
        // so a fold sees its own earlier bump within the same batch.
        Ok(self.get_row(self.keys.next_generated_seq())?.unwrap_or(0))
    }

    async fn put_next_generated_seq(&mut self, seq: i64) -> Result<(), StorageError> {
        self.put_row(self.keys.next_generated_seq(), &seq)?;
        Ok(())
    }

    fn commit(self: Box<Self>, watermark: Option<i64>) -> Result<(), StorageError> {
        // Fold the watermark advance into the same transaction as the projection, so it can never run
        // ahead of the projection (the invariant that makes restart-from-W+1 safe).
        if let Some(w) = watermark {
            self.put_row(self.keys.last_processed_position(), &w)?;
        }
        // Move the transaction out of the box (and drop the DB clone with it).
        let RocksTxn { txn, .. } = *self;
        // Atomic all-or-nothing commit of the whole fold (projection + watermark) through the default
        // (WAL, no-fsync) write path — a half-applied fold is impossible. Dropping the box instead of
        // committing aborts: the transaction is never written.
        txn.commit()
            .map_err(|e| StorageError::Backend(format!("rocksdb txn commit: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use spica_engine_types::{
        Execution, ExecutionKind, ExecutionStatus, HasRawObjectRef, ObjectRef, PlainName,
        RetryState, StatePath, Task, TaskStatus, ThreadOwner, ThreadStatus, Timer, TimerOwner,
        TimerStatus, Timestamp,
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

    /// A distinct thread reference (`child-<uid>`), matching a `meta.raw_object_ref()`.
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

    /// A distinct timer reference (`child-<uid>`), matching a `meta.raw_object_ref()`.
    fn timer_ref() -> ObjectRef<TimerKind> {
        let uid = ulid::Ulid::new();
        ObjectRef::new(
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
            assert_eq!(got.meta.object_ref(), id);
        }
        // Reopened store must still see the row (durability via RocksDB WAL across a drop/reopen).
        {
            let store = RocksStorage::open(&path).unwrap();
            assert_eq!(
                store
                    .get_execution(&id)
                    .await
                    .unwrap()
                    .unwrap()
                    .meta
                    .object_ref(),
                id
            );
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn add_and_remove_child_tracks_ownership_edges() {
        let path = temp_path("children");
        let id = test_exec_ref();
        let child = timer_ref();
        {
            let mut store = RocksStorage::open(&path).unwrap();
            store
                .put_execution(sample_execution(id.clone()))
                .await
                .unwrap();

            // `add_child`/`remove_child` speak the flat address the parent's own set holds, so the
            // typed parent/child cross the erasure seam here.
            let parent = id.clone();
            // no children initially
            assert!(
                store
                    .get_children(parent.clone().into_raw_object_ref())
                    .await
                    .unwrap()
                    .is_empty()
            );
            // add, then reflect in the edge table
            store
                .add_child(
                    parent.clone().into_raw_object_ref(),
                    child.clone().into_raw_object_ref(),
                )
                .await
                .unwrap();
            assert_eq!(
                store
                    .get_children(parent.clone().into_raw_object_ref())
                    .await
                    .unwrap(),
                HashSet::from([child.clone().into_raw_object_ref()])
            );
            // remove, then reflect
            store
                .remove_child(
                    parent.clone().into_raw_object_ref(),
                    child.clone().into_raw_object_ref(),
                )
                .await
                .unwrap();
            assert!(
                store
                    .get_children(parent.clone().into_raw_object_ref())
                    .await
                    .unwrap()
                    .is_empty()
            );
            // Removing an absent edge is idempotent, not an error (its row simply is not there).
            store
                .remove_child(
                    parent.into_raw_object_ref(),
                    child.clone().into_raw_object_ref(),
                )
                .await
                .unwrap();
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    /// The edge table is keyed by `(parent_kind, parent_name)`, so two parents that merely *share a
    /// name* across kinds hold different child sets — the property `child_edge`'s kind-in-key note
    /// pins. A leaf kind is also a legal parent now: nothing about the edge space is node-specific,
    /// which is what lets a Task own its timeout timer.
    #[tokio::test]
    async fn edge_sets_are_per_parent_kind_and_admit_any_parent() {
        let path = temp_path("edges-kinds");
        let name = PlainName::new("order").unwrap().generated_from_key(7);
        let exec = ObjectRef::<ExecutionKind>::new(name.clone(), ulid::Ulid::new());
        let thread = ObjectRef::<ThreadKind>::new(name, exec.uid());
        let timer = timer_ref();
        {
            let mut store = RocksStorage::open(&path).unwrap();
            store
                .put_execution(sample_execution(exec.clone()))
                .await
                .unwrap();

            let child = timer.clone().into_raw_object_ref();
            store
                .add_child(exec.clone().into_raw_object_ref(), child.clone())
                .await
                .unwrap();
            // The same-named thread does not inherit the execution's child...
            assert!(
                store
                    .get_children(thread.clone().into_raw_object_ref())
                    .await
                    .unwrap()
                    .is_empty()
            );
            // ...and giving the thread its own edge leaves the execution's set untouched.
            store
                .add_child(thread.clone().into_raw_object_ref(), child.clone())
                .await
                .unwrap();
            assert_eq!(
                store
                    .get_children(thread.into_raw_object_ref())
                    .await
                    .unwrap(),
                HashSet::from([child.clone()])
            );
            assert_eq!(
                store
                    .get_children(exec.into_raw_object_ref())
                    .await
                    .unwrap(),
                HashSet::from([child.clone()])
            );
            // A leaf kind is a legal parent too — the edge space is not node-specific, which is what
            // lets a Task own its timeout timer.
            store
                .add_child(timer.clone().into_raw_object_ref(), child.clone())
                .await
                .unwrap();
            assert_eq!(
                store
                    .get_children(timer.into_raw_object_ref())
                    .await
                    .unwrap(),
                HashSet::from([child])
            );
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    /// The edge and the parent row are independent cells: writing the parent row (a status change)
    /// leaves its children intact, and adding a child leaves the row intact — neither write has to
    /// re-attach the other, so a status transition can never drop a child it did not mean to touch.
    #[tokio::test]
    async fn a_parent_write_and_its_edges_do_not_clobber_each_other() {
        let path = temp_path("edges-clobber");
        let id = test_exec_ref();
        let child = timer_ref();
        {
            let mut store = RocksStorage::open(&path).unwrap();
            let mut row = sample_execution(id.clone());
            store.put_execution(row.clone()).await.unwrap();
            store
                .add_child(
                    id.clone().into_raw_object_ref(),
                    child.clone().into_raw_object_ref(),
                )
                .await
                .unwrap();

            // A row write straight from a domain value (no re-attached child set) keeps the edge.
            row.meta.with_update_at(Timestamp::from_millis(42));
            store.put_execution(row).await.unwrap();
            assert_eq!(
                store
                    .get_children(id.clone().into_raw_object_ref())
                    .await
                    .unwrap(),
                HashSet::from([child.clone().into_raw_object_ref()])
            );

            // And the reverse: a child edge write leaves the row's own facts alone.
            store
                .add_child(
                    id.clone().into_raw_object_ref(),
                    timer_ref().into_raw_object_ref(),
                )
                .await
                .unwrap();
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
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    /// The persisted twin of the in-memory scope test: the variable scope is its own cell, so a
    /// whole-row `put_thread` cannot clobber it, a fold's snapshot overwrites it wholesale, and it
    /// survives a drop/reopen.
    #[tokio::test]
    async fn variables_are_independent_of_the_thread_row_and_persist() {
        let path = temp_path("variables");
        let id = thread_ref();
        {
            let mut store = RocksStorage::open(&path).unwrap();
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

            // Inside one fold a write is visible to a read of that same fold (read-your-writes).
            let mut txn = store.begin_txn().unwrap();
            txn.put_variables(&id, Variables::from([("x".to_string(), json!(1))]))
                .await
                .unwrap();
            assert_eq!(
                txn.get_variables(&id).await.unwrap().get("x"),
                Some(&json!(1))
            );
            txn.commit(None).unwrap();
        }
        // Reopen: the scope outlives the store instance (durability via the RocksDB WAL), and the
        // fold's snapshot replaced the committed one wholesale.
        let store = RocksStorage::open(&path).unwrap();
        let scope = store.get_variables(&id).await.unwrap();
        assert_eq!(scope.get("x"), Some(&json!(1)));
        assert_eq!(scope.get("g"), None);
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
            let uid: ulid::Ulid = ulid::Ulid::new();
            let timer_ref = ObjectRef::<TimerKind>::new(
                PlainName::new("child")
                    .unwrap()
                    .generated_from_key(uid.0 as u64),
                uid,
            );
            let t = Timer {
                execution: id.clone(),
                status: TimerStatus::Active,
                deadline: Timestamp::from_millis(0),
                meta: spica_engine_types::ObjectMeta::builder(uid)
                    .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                    .with_owner(TimerOwner::Execution(id.clone())),
            };
            store.put_timer(t.clone()).await.unwrap();
            assert_eq!(
                store
                    .get_timer(&timer_ref)
                    .await
                    .unwrap()
                    .unwrap()
                    .meta
                    .object_ref(),
                timer_ref
            );
            // the execution row is untouched by writing a timer
            assert_eq!(
                store
                    .get_execution(&id)
                    .await
                    .unwrap()
                    .unwrap()
                    .meta
                    .object_ref(),
                id
            );
        }
        let _ = std::fs::remove_dir_all(&path);
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
        // The discovery half of a poll, over a real prefix scan: only rows of `resource` that `now`
        // makes claimable come back — a lapsed lease does, a live one does not — and `limit` counts
        // *eligible* rows, so a non-claimable row can never occupy a slot the poll could have filled.
        let path = temp_path("activatable");
        let now = Timestamp::from_millis(1_000_000);
        let expired = ulid::Ulid::new();
        let live = ulid::Ulid::new();
        let gated = ulid::Ulid::new();
        let foreign = ulid::Ulid::new();
        {
            let mut store = RocksStorage::open(&path).unwrap();
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
            // `limit` is applied to the eligible set, not the scanned set.
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

    /// Pins the absent-key contract of both read seats: RocksDB reports a miss as a null value, **not**
    /// as a `NotFound` status, so `db.get`/`txn.get` answer `Ok(None)` and the backend-error mapping on
    /// each path stays reserved for genuine faults. If a miss ever became an `Err`, every
    /// read-modify-write applier and `ActivityContainer::open`'s optional answer would break — hence the
    /// `Ok(None)` shape is asserted with `matches!` rather than an `is_none()` on an unwrapped value, so
    /// an `Err` cannot masquerade as a miss.
    #[tokio::test]
    async fn an_absent_key_reads_as_none_not_as_a_backend_fault() {
        let path = temp_path("absent");
        let store = RocksStorage::open(&path).unwrap();
        let absent = test_exec_ref();

        // Committed-store seat: a key the store never saw.
        let read = store.get_execution(&absent).await;
        assert!(
            matches!(read, Ok(None)),
            "an absent row is Ok(None), not a backend fault: {read:?}"
        );

        // Fold seat: the fold holds a pending write of its own, and the queried key is not among them —
        // a miss inside a populated fold is still a miss, resolved from the committed store (which also
        // has nothing), and still not an error.
        let present = test_exec_ref();
        let mut txn = store.begin_txn().unwrap();
        txn.put_execution(sample_execution(present.clone()))
            .await
            .unwrap();
        let hit = txn.get_execution(&present).await;
        assert!(
            matches!(hit, Ok(Some(_))),
            "the fold's own pending write is readable (read-your-writes): {hit:?}"
        );
        let read = txn.get_execution(&absent).await;
        assert!(
            matches!(read, Ok(None)),
            "an absent row inside a fold is Ok(None), not a backend fault: {read:?}"
        );

        drop(txn);
        let _ = std::fs::remove_dir_all(&path);
    }
}
