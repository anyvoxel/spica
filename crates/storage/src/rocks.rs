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
use rocksdb::{OptimisticTransactionDB, Transaction};

use spica_engine::{
    Activity, ActivityId, Execution, ExecutionError, ExecutionId, Flow, FlowId, FlowName,
    FlowVersion, FlowVersionId, NodeId, Storage, StorageTxn, Task, TaskId, TaskStatus, Timer,
    TimerId,
};

use crate::{KeyBuilder, Scope};

/// Read a single committed row: deserialize `T` from the value at `key`, or `None` if absent.
fn get_row<T: serde::de::DeserializeOwned>(
    db: &OptimisticTransactionDB,
    key: Vec<u8>,
) -> Result<Option<T>, ExecutionError> {
    match db
        .get(key)
        .map_err(|e| ExecutionError::Log(format!("rocksdb read: {e}")))?
    {
        Some(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| ExecutionError::Log(format!("deserialize row: {e}"))),
        None => Ok(None),
    }
}

/// Read one row inside an open fold from a RockSDB [`Transaction`]. `Transaction::get` resolves
/// **read-your-writes** natively: it checks the transaction's own pending writes first, then the
/// committed store. This is what lets read-modify-write appliers compose on a single base row within
/// one fold (e.g. `add_child` then `put_execution` of the same Execution row, as `StateActivating`
/// does) without clobbering each other's buffered mutations.
fn txn_get<T: serde::de::DeserializeOwned>(
    txn: &Transaction<'_, OptimisticTransactionDB>,
    key: Vec<u8>,
) -> Result<Option<T>, ExecutionError> {
    match txn
        .get(key)
        .map_err(|e| ExecutionError::Log(format!("txn read: {e}")))?
    {
        Some(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| ExecutionError::Log(format!("deserialize row: {e}"))),
        None => Ok(None),
    }
}

/// Buffer one row into an open fold's [`Transaction`]: serialize `value` and record the write at
/// `key`. Nothing is visible to the committed store until [`StorageTxn::commit`].
fn txn_put<T: serde::Serialize>(
    txn: &Transaction<'_, OptimisticTransactionDB>,
    key: Vec<u8>,
    value: &T,
) -> Result<(), ExecutionError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|e| ExecutionError::Log(format!("serialize row: {e}")))?;
    txn.put(key, bytes)
        .map_err(|e| ExecutionError::Log(format!("txn write: {e}")))?;
    Ok(())
}

/// Write one row straight to the store: serialize `value` and upsert it at `key` through the default
/// (WAL, no-fsync) write path as a single-row write. Used by the raw [`Storage`] write methods
/// (outside a fold); the transactional path buffers rows into a [`Transaction`] instead.
fn put_row<T: serde::Serialize>(
    db: &OptimisticTransactionDB,
    key: Vec<u8>,
    value: &T,
) -> Result<(), ExecutionError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|e| ExecutionError::Log(format!("serialize row: {e}")))?;
    db.put(key, bytes)
        .map_err(|e| ExecutionError::Log(format!("rocksdb write: {e}")))?;
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
    /// Open (creating if needed) the store rooted at `path`. Existing committed rows are recovered
    /// automatically from RocksDB's WAL.
    ///
    /// NOTE: `RocksStorage` and `RocksLogStream` each open their own RocksDB handle/path today. A
    /// future refactor could fuse them into one DB with two column families (see the module docs) so
    /// the log and its projection share crash-recovery and `fsync` localities.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ExecutionError> {
        let db = Arc::new(
            OptimisticTransactionDB::open_default(path)
                .map_err(|e| ExecutionError::Log(format!("rocksdb open: {e}")))?,
        );
        let keys = KeyBuilder::new(Scope::default_scope());
        Ok(Self { db, keys })
    }
}

#[async_trait]
impl Storage for RocksStorage {
    async fn get_execution(&self, id: ExecutionId) -> Result<Option<Execution>, ExecutionError> {
        get_row(&self.db, self.keys.execution(id))
    }

    async fn get_activity(&self, id: ActivityId) -> Result<Option<Activity>, ExecutionError> {
        get_row(&self.db, self.keys.activity(id))
    }

    async fn get_timer(&self, id: TimerId) -> Result<Option<Timer>, ExecutionError> {
        get_row(&self.db, self.keys.timer(id))
    }

    async fn get_task(&self, id: TaskId) -> Result<Option<Task>, ExecutionError> {
        get_row(&self.db, self.keys.task(id))
    }

    async fn activatable_tasks(
        &self,
        resource: &str,
        limit: usize,
    ) -> Result<Vec<Task>, ExecutionError> {
        // A forward prefix scan over the `task` kind (no per-resource queue index in M1 — see the
        // trait doc). Row values are their JSON encoding, decoded and filtered by resource+status.
        let prefix = self.keys.task_prefix();
        let mut out: Vec<Task> = Vec::new();
        for item in self.db.prefix_iterator(prefix) {
            let (_, value) =
                item.map_err(|e| ExecutionError::Log(format!("rocksdb task scan: {e}")))?;
            let task: Task = serde_json::from_slice(&value)
                .map_err(|e| ExecutionError::Log(format!("rocksdb task decode: {e}")))?;
            if task.value.resource == resource && task.value.status == TaskStatus::Pending {
                out.push(task);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// `active_children` is stored on the parent row, so this reads it back like
    /// [`InMemoryStorage`](crate::InMemoryStorage) does (a leaf — Timer/Task — owns nothing).
    async fn get_children(&self, id: NodeId) -> Result<HashSet<NodeId>, ExecutionError> {
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
        put_row(&self.db, self.keys.execution(exec.id), &exec)
    }

    async fn put_activity(&mut self, act: Activity) -> Result<(), ExecutionError> {
        put_row(&self.db, self.keys.activity(act.value.id), &act)
    }

    async fn put_timer(&mut self, timer: Timer) -> Result<(), ExecutionError> {
        put_row(&self.db, self.keys.timer(timer.value.id), &timer)
    }

    async fn put_task(&mut self, task: Task) -> Result<(), ExecutionError> {
        put_row(&self.db, self.keys.task(task.value.id), &task)
    }

    /// Read-modify-write `parent`'s `active_children` minus `child`, exactly like
    /// [`InMemoryStorage`](crate::InMemoryStorage), persisted back through the DB.
    async fn remove_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError> {
        match parent {
            NodeId::Execution(e) => {
                if let Some(mut exec) = self.get_execution(e).await? {
                    exec.active_children.remove(&child);
                    self.put_execution(exec).await?;
                }
            }
            NodeId::Activity(a) => {
                if let Some(mut act) = self.get_activity(a).await? {
                    act.active_children.remove(&child);
                    self.put_activity(act).await?;
                }
            }
            // A leaf never owns children; nothing to remove.
            NodeId::Timer(_) | NodeId::Task(_) => {}
        }
        Ok(())
    }

    /// Read-modify-write `parent`'s `active_children` plus `child`, persisted back through the DB.
    async fn add_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError> {
        match parent {
            NodeId::Execution(e) => {
                if let Some(mut exec) = self.get_execution(e).await? {
                    exec.active_children.insert(child);
                    self.put_execution(exec).await?;
                }
            }
            NodeId::Activity(a) => {
                if let Some(mut act) = self.get_activity(a).await? {
                    act.active_children.insert(child);
                    self.put_activity(act).await?;
                }
            }
            // A leaf never owns children; nothing to add.
            NodeId::Timer(_) | NodeId::Task(_) => {}
        }
        Ok(())
    }

    async fn get_flow_by_name(&self, name: FlowName) -> Result<Option<Flow>, ExecutionError> {
        get_row(&self.db, self.keys.flow(&name))
    }

    async fn put_flow(&mut self, flow: Flow) -> Result<(), ExecutionError> {
        // Canonical row, addressed by the immutable name (the primary key).
        put_row(&self.db, self.keys.flow(&flow.name), &flow)
    }

    async fn get_flow_version(
        &self,
        flow_version_id: FlowVersionId,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        get_row(&self.db, self.keys.flow_version(flow_version_id))
    }

    async fn put_flow_version(&mut self, ver: FlowVersion) -> Result<(), ExecutionError> {
        // Canonical version row, keyed by the never-reused `flow_version_id` (execution binding).
        put_row(&self.db, self.keys.flow_version(ver.flow_version_id), &ver)?;
        // Per-incarnation version index `(flow_id, version) → flow_version_id`, so `flow_version_of`
        // resolves by point read instead of scanning every row of the flow.
        put_row(
            &self.db,
            self.keys.flow_version_index(ver.flow_id, ver.version),
            &ver.flow_version_id,
        )?;
        Ok(())
    }

    async fn flow_version_of(
        &self,
        flow_id: FlowId,
        version: u32,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        let fvid: Option<FlowVersionId> =
            get_row(&self.db, self.keys.flow_version_index(flow_id, version))?;
        match fvid {
            Some(fvid) => self.get_flow_version(fvid).await,
            None => Ok(None),
        }
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

    fn begin_txn<'a>(&'a self) -> Result<Box<dyn StorageTxn + 'a>, ExecutionError> {
        // Hand the StreamProcessor an **owned** fold transaction backed by a native RocksDB `Transaction`
        // begun on this store's DB (which reads its own pending writes — read-your-writes — for
        // free). The transaction borrows the store's DB handle, so the box is lifetime-bound to this
        // `&self` borrow; the StreamProcessor holds `&mut Box<dyn Storage>` for the whole fold, which the
        // single-writer model keeps exclusive. See the `StorageTxn` docs for why the commit receiver
        // (`self: Box<Self>`) keeps commit StreamProcessor-only.
        let db: &OptimisticTransactionDB = self.db.as_ref();
        Ok(Box::new(RocksTxn {
            txn: db.transaction(),
            keys: self.keys.clone(),
        }))
    }
}

/// Atomic projection transaction for [`RocksStorage`], backed by a native RocksDB [`Transaction`].
///
/// Borrows the store's DB handle and wraps a `Transaction` begun on it. Reads go through
/// `Transaction::get`, which resolves **read-your-writes** natively (the transaction keeps an index
/// over its own pending writes, then falls back to the committed store); writes buffer into the
/// transaction; [`StorageTxn::commit`] advances the watermark inside the transaction and commits it
/// all-or-nothing through the default (WAL, no-fsync) path. Dropping without committing aborts (the
/// transaction is never written). Because RocksDB's `Transaction` is `Send` but **not** `Sync`, the
/// trait's methods are all `&mut self`.
struct RocksTxn<'a> {
    txn: Transaction<'a, OptimisticTransactionDB>,
    keys: KeyBuilder,
}

#[async_trait]
impl StorageTxn for RocksTxn<'_> {
    async fn get_execution(
        &mut self,
        id: ExecutionId,
    ) -> Result<Option<Execution>, ExecutionError> {
        txn_get(&self.txn, self.keys.execution(id))
    }

    async fn get_activity(&mut self, id: ActivityId) -> Result<Option<Activity>, ExecutionError> {
        txn_get(&self.txn, self.keys.activity(id))
    }

    async fn get_timer(&mut self, id: TimerId) -> Result<Option<Timer>, ExecutionError> {
        txn_get(&self.txn, self.keys.timer(id))
    }

    async fn get_task(&mut self, id: TaskId) -> Result<Option<Task>, ExecutionError> {
        txn_get(&self.txn, self.keys.task(id))
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
        txn_put(&self.txn, self.keys.execution(exec.id), &exec)
    }

    async fn put_activity(&mut self, act: Activity) -> Result<(), ExecutionError> {
        txn_put(&self.txn, self.keys.activity(act.value.id), &act)
    }

    async fn put_timer(&mut self, timer: Timer) -> Result<(), ExecutionError> {
        txn_put(&self.txn, self.keys.timer(timer.value.id), &timer)
    }

    async fn put_task(&mut self, task: Task) -> Result<(), ExecutionError> {
        txn_put(&self.txn, self.keys.task(task.value.id), &task)
    }

    /// Read-modify-write `parent`'s `active_children` minus `child`: read the (read-your-writes)
    /// row, remove `child`, and buffer the result back into the transaction (atomicity at commit).
    async fn remove_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError> {
        match parent {
            NodeId::Execution(e) => {
                if let Some(mut exec) = self.get_execution(e).await? {
                    exec.active_children.remove(&child);
                    self.put_execution(exec).await?;
                }
            }
            NodeId::Activity(a) => {
                if let Some(mut act) = self.get_activity(a).await? {
                    act.active_children.remove(&child);
                    self.put_activity(act).await?;
                }
            }
            // A leaf never owns children; nothing to remove.
            NodeId::Timer(_) | NodeId::Task(_) => {}
        }
        Ok(())
    }

    /// Read-modify-write `parent`'s `active_children` plus `child`: read the (read-your-writes) row,
    /// insert `child`, and buffer the result back into the transaction (atomicity at commit).
    async fn add_child(&mut self, parent: NodeId, child: NodeId) -> Result<(), ExecutionError> {
        match parent {
            NodeId::Execution(e) => {
                if let Some(mut exec) = self.get_execution(e).await? {
                    exec.active_children.insert(child);
                    self.put_execution(exec).await?;
                }
            }
            NodeId::Activity(a) => {
                if let Some(mut act) = self.get_activity(a).await? {
                    act.active_children.insert(child);
                    self.put_activity(act).await?;
                }
            }
            // A leaf never owns children; nothing to add.
            NodeId::Timer(_) | NodeId::Task(_) => {}
        }
        Ok(())
    }

    async fn get_flow_by_name(&mut self, name: FlowName) -> Result<Option<Flow>, ExecutionError> {
        txn_get(&self.txn, self.keys.flow(&name))
    }

    async fn put_flow(&mut self, flow: Flow) -> Result<(), ExecutionError> {
        txn_put(&self.txn, self.keys.flow(&flow.name), &flow)
    }

    async fn get_flow_version(
        &mut self,
        flow_version_id: FlowVersionId,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        txn_get(&self.txn, self.keys.flow_version(flow_version_id))
    }

    async fn put_flow_version(&mut self, ver: FlowVersion) -> Result<(), ExecutionError> {
        // Canonical version row, keyed by the never-reused `flow_version_id` (execution binding).
        txn_put(&self.txn, self.keys.flow_version(ver.flow_version_id), &ver)?;
        // Per-incarnation version index `(flow_id, version) → flow_version_id`, so `flow_version_of`
        // resolves by point read instead of scanning every row of the flow.
        txn_put(
            &self.txn,
            self.keys.flow_version_index(ver.flow_id, ver.version),
            &ver.flow_version_id,
        )?;
        Ok(())
    }

    async fn flow_version_of(
        &mut self,
        flow_id: FlowId,
        version: u32,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        let fvid: Option<FlowVersionId> =
            txn_get(&self.txn, self.keys.flow_version_index(flow_id, version))?;
        match fvid {
            Some(fvid) => self.get_flow_version(fvid).await,
            None => Ok(None),
        }
    }

    fn commit(self: Box<Self>, watermark: Option<i64>) -> Result<(), ExecutionError> {
        // Move the transaction out of the box (and drop the borrow of the store's DB).
        let RocksTxn { txn, keys } = *self;
        // Fold the watermark advance into the same transaction as the projection, so it can never run
        // ahead of the projection (the invariant that makes restart-from-W+1 safe).
        if let Some(w) = watermark {
            let bytes = serde_json::to_vec(&w)
                .map_err(|e| ExecutionError::Log(format!("serialize watermark: {e}")))?;
            txn.put(keys.last_processed_position(), bytes)
                .map_err(|e| ExecutionError::Log(format!("txn write watermark: {e}")))?;
        }
        // Atomic all-or-nothing commit of the whole fold (projection + watermark) through the default
        // (WAL, no-fsync) write path — a half-applied fold is impossible. Dropping the box instead of
        // committing aborts: the transaction is never written.
        txn.commit()
            .map_err(|e| ExecutionError::Log(format!("rocksdb txn commit: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use spica_engine::{
        ExecutionId, ExecutionStatus, ExecutionValue, FlowVersionId, NodeId, TimerId, TimerPurpose,
        TimerStatus, TimerValue, Timestamp, Variables,
    };

    /// A unique per-test store path under the system temp dir, removed after the test.
    fn temp_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("spica-rocks-storage-{tag}-{}", ulid::Ulid::new()))
    }

    /// A minimal running execution row for exercising the store.
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
    async fn put_get_and_reopen_persist_rows() {
        let path = temp_path("rows");
        let id = ExecutionId::new();
        {
            let mut store = RocksStorage::open(&path).unwrap();
            assert!(store.get_execution(id).await.unwrap().is_none());
            store.put_execution(sample_execution(id)).await.unwrap();
            let got = store.get_execution(id).await.unwrap().unwrap();
            assert_eq!(got.id, id);
        }
        // Reopened store must still see the row (durability via RocksDB WAL across a drop/reopen).
        {
            let store = RocksStorage::open(&path).unwrap();
            assert_eq!(store.get_execution(id).await.unwrap().unwrap().id, id);
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn add_and_remove_child_tracks_active_children() {
        let path = temp_path("children");
        let id = ExecutionId::new();
        let child = NodeId::Timer(TimerId::new());
        {
            let mut store = RocksStorage::open(&path).unwrap();
            store.put_execution(sample_execution(id)).await.unwrap();

            let parent = NodeId::Execution(id);
            // no children initially
            assert!(store.get_children(parent).await.unwrap().is_empty());
            // add, then reflect in the persisted row
            store.add_child(parent, child).await.unwrap();
            assert_eq!(
                store.get_children(parent).await.unwrap(),
                HashSet::from([child])
            );
            // a leaf parent (Timer) owns nothing and never grows
            assert!(
                store
                    .get_children(NodeId::Timer(TimerId::new()))
                    .await
                    .unwrap()
                    .is_empty()
            );
            // remove, then reflect
            store.remove_child(parent, child).await.unwrap();
            assert!(store.get_children(parent).await.unwrap().is_empty());
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn different_entity_kinds_do_not_collide() {
        let path = temp_path("kinds");
        let id = ExecutionId::new();
        {
            let mut store = RocksStorage::open(&path).unwrap();
            store.put_execution(sample_execution(id)).await.unwrap();
            // A timer under the same numeric-ish space is a distinct key namespace.
            let t = Timer::from_value(TimerValue {
                id: TimerId::new(),
                parent: NodeId::Execution(id),
                purpose: TimerPurpose::WaitResume,
                status: TimerStatus::Active,
                deadline: Timestamp::from_millis(0),
            });
            store.put_timer(t.clone()).await.unwrap();
            assert_eq!(store.get_timer(t.id).await.unwrap().unwrap().id, t.id);
            // the execution row is untouched by writing a timer
            assert_eq!(store.get_execution(id).await.unwrap().unwrap().id, id);
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
        let id = ExecutionId::new();
        {
            let store = RocksStorage::open(&path).unwrap();
            // A single fold transaction buffers its writes; nothing is visible until commit.
            let mut txn = store.begin_txn().unwrap();
            txn.put_execution(sample_execution(id)).await.unwrap();
            // In-flight writes resolve from the txn overlay (read-your-writes)...
            assert!(txn.get_execution(id).await.unwrap().is_some());
            // ...but are not visible to the committed store (external readers) until commit.
            assert!(store.get_execution(id).await.unwrap().is_none());
            assert_eq!(store.last_processed_position().await.unwrap(), 0);
            // Commit the fold: the projection AND the watermark land together, all-or-nothing.
            txn.commit(Some(7)).unwrap();
            assert!(store.get_execution(id).await.unwrap().is_some());
            assert_eq!(store.last_processed_position().await.unwrap(), 7);
        }
        // The committed fold is durable across a reopen (via the shared WAL path).
        {
            let store = RocksStorage::open(&path).unwrap();
            assert!(store.get_execution(id).await.unwrap().is_some());
            assert_eq!(store.last_processed_position().await.unwrap(), 7);
        }
        // Abort: begin a transaction, buffer a write, then drop it without committing — the
        // discarded overlay must never reach the DB.
        {
            let store = RocksStorage::open(&path).unwrap();
            let other = ExecutionId::new();
            let mut txn = store.begin_txn().unwrap();
            txn.put_execution(sample_execution(other)).await.unwrap();
            // drop(txn) aborts the fold: the buffered write is discarded, never committed.
            drop(txn);
            assert!(store.get_execution(other).await.unwrap().is_none());
        }
        {
            let store = RocksStorage::open(&path).unwrap();
            assert_eq!(store.last_processed_position().await.unwrap(), 7);
        }
        let _ = std::fs::remove_dir_all(&path);
    }
}
