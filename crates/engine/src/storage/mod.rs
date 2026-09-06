//! The CCES storage **contract**: the [`Storage`] trait plus the projection row types it names.
//!
//! This module owns the *interface* (and the four projection row wrappers [`ExecutionRecord`],
//! [`ActivityRecord`], [`TaskRecord`], [`TimerRecord`]) because `spica-engine` is the crate that consumes storage.
//! The concrete implementations — durable [`RocksStorage`], in-memory [`InMemoryStorage`], and the
//! byte [`KeyBuilder`] encoding — live in the separate `spica-storage` crate, which **depends on**
//! this one to implement the trait. Keeping the crate dependency `spica-storage → spica-engine` (and
//! never the reverse) is what keeps the graph acyclic; a binary supplies the concrete storage via
//! [`EngineBuilder::with_backends`](crate::EngineBuilder::with_backends).

mod activity;
mod execution;
mod scope;
mod task;
mod thread;
mod timer;

use std::collections::HashSet;

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::flow::Flow;
use crate::types::flow_version::FlowVersion;
use crate::types::id::FlowName;
use crate::types::meta::{ObjectKind, ObjectName, ObjectReference};

pub use activity::ActivityRecord;
pub use execution::ExecutionRecord;
// Re-export the two scope-resolution entry points consumers use. `load_scope` itself (reference-based)
// stays module-private — `load_scope_ref` routes every reference through it — until a caller needs
// to resolve a scope by a bare reference.
pub use scope::{ScopeRecord, load_scope_ref, resolve_scope_flow_version};
pub use task::TaskRecord;
pub use thread::ThreadRecord;
pub use timer::TimerRecord;

/// Persistent projection of the execution tree, rebuilt by applying the [`Event`](crate::Event) stream.
///
/// This is the **read** interface used by handlers and the cascade (they observe snapshots and never
/// mutate). The actual projection of events is performed by [`EventApplier`](crate::applier::EventApplier)
/// implementations, which mutate this store through the `put_*` / `remove_child` methods below —
/// mirroring how [`CommandHandler`](crate::CommandHandler) implementations observe Storage but
/// delegate their output to the [`Collector`](crate::Collector).
/// `#[auto_impl(Box)]` mechanically generates `impl<T: Storage + ?Sized> Storage for Box<T>` — so a
/// `Box<dyn Storage>` is itself a *Sized* implementor and the generic [`StreamProcessor::run`] (whose
/// `S: Storage` bound stays `Sized`) accepts config-selected trait objects directly.
#[async_trait]
#[auto_impl::auto_impl(Box)]
pub trait Storage: Send + Sync {
    async fn get_execution(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ExecutionRecord>, ExecutionError>;
    async fn get_thread(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ThreadRecord>, ExecutionError>;
    async fn get_activity(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ActivityRecord>, ExecutionError>;
    async fn get_timer(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<TimerRecord>, ExecutionError>;
    async fn get_task(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<TaskRecord>, ExecutionError>;
    async fn get_children(
        &self,
        id: ObjectReference,
    ) -> Result<HashSet<ObjectReference>, ExecutionError>;

    /// Scan up to `limit` available (`TaskStatus::Pending`) tasks of `resource` — the discovery
    /// query behind a worker pull ([`TaskApi::poll_tasks`](crate::task_api::TaskApi::poll_tasks)). A
    /// straight scan over the task rows (there is no per-resource ready-queue index in M1); a
    /// production store would maintain a ready-queue per job-type to serve this in O(queued) instead
    /// of O(tasks). Restart-safe for the same reason every read is: it observes the durable
    /// projection, so a task created before a restart and still `Pending` is found and claimed.
    async fn activatable_tasks(
        &self,
        resource: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, ExecutionError>;

    /// Enumerate `kind`'s rows in storage-key order (k8s-style LIST over one kind), returning each
    /// row's canonical addressing [`ObjectName`] + its raw serialized (serde JSON) value bytes —
    /// the uniform, kind-agnostic feed the Query read facade deserializes into typed rows. Starts
    /// **after** `start_after` (exclusive; `None` = from the first row) and returns at most `limit`
    /// rows, so a caller pages by passing the previous page's last name back as `start_after`.
    ///
    /// The default refuses: only the real backends (`RocksStorage`/`InMemoryStorage`) implement the
    /// range scan; the read facade is the only consumer, never the handler path.
    async fn list_kind(
        &self,
        kind: ObjectKind,
        _start_after: Option<&ObjectName>,
        _limit: usize,
    ) -> Result<Vec<(ObjectName, Vec<u8>)>, ExecutionError> {
        Err(ExecutionError::Runtime(
            crate::types::error::RuntimeError::InvalidDefinition(format!(
                "list_kind({kind:?}) not implemented by this storage backend"
            )),
        ))
    }

    /// Upsert an `ExecutionRecord` record (read-modify-write by an `EventApplier`).
    async fn put_execution(&mut self, exec: ExecutionRecord) -> Result<(), ExecutionError>;
    /// Upsert a `ThreadRecord` record (a scoped sub-run's projection row).
    async fn put_thread(&mut self, thread: ThreadRecord) -> Result<(), ExecutionError>;
    /// Upsert an `ActivityRecord` record.
    async fn put_activity(&mut self, act: ActivityRecord) -> Result<(), ExecutionError>;
    /// Upsert a `TimerRecord` record.
    async fn put_timer(&mut self, timer: TimerRecord) -> Result<(), ExecutionError>;
    /// Upsert a `TaskRecord` record.
    async fn put_task(&mut self, task: TaskRecord) -> Result<(), ExecutionError>;
    /// Remove `child` from `parent`'s `active_children` (a terminal child draining its owner).
    async fn remove_child(
        &mut self,
        parent: ObjectReference,
        child: ObjectReference,
    ) -> Result<(), ExecutionError>;
    /// Add `child` to `parent`'s `active_children` (a child appears when its `ing` event lands).
    async fn add_child(
        &mut self,
        parent: ObjectReference,
        child: ObjectReference,
    ) -> Result<(), ExecutionError>;

    /// Fetch a flow by its addressing key (`name`, the immutable primary key).
    async fn get_flow_by_name(&self, name: FlowName) -> Result<Option<Flow>, ExecutionError>;
    /// Upsert a flow row (written by the `FlowCreated` applier; advances `latest_version`).
    async fn put_flow(&mut self, flow: Flow) -> Result<(), ExecutionError>;

    /// Fetch a persisted flow version by its [`ObjectReference`]. This is how handlers resolve the
    /// machine an execution is bound to, lazily loading it into the StreamProcessor's definition
    /// cache. Returns `None` if the version no longer exists (deleted/GC'd).
    async fn get_flow_version(
        &self,
        version: &ObjectReference,
    ) -> Result<Option<FlowVersion>, ExecutionError>;
    /// Persist a flow version (written by the `FlowCreated` applier). The canonical row is keyed by
    /// the version's own `ObjectName` (`{flow_name}-{version}`), so it is addressable by that name.
    async fn put_flow_version(&mut self, version: FlowVersion) -> Result<(), ExecutionError>;
    /// The specific `FlowVersion` at ordinal `version` under `name` (client references a
    /// definition by `(name, version)`; the flow's name is its sole identity, so lookups key on it).
    async fn flow_version_of(
        &self,
        name: FlowName,
        version: u32,
    ) -> Result<Option<FlowVersion>, ExecutionError>;

    /// Begin an **atomic projection transaction**, returning an owned, write-scoped [`StorageTxn`]
    /// handle. The subsequent fold operations accumulate into a single pending batch — instead of
    /// hitting the store one-by-one — and become visible all-or-nothing only when the StreamProcessor
    /// commits the returned handle via [`StorageTxn::commit`]. A fold (one Event's projection plus
    /// its watermark advance) commits as one unit, so a torn (half-applied) projection is
    /// impossible, matching Zeebe's "state + `lastProcessedPosition` advance in the same RocksDB
    /// transaction" (§B.1 of `docs/durable-execution-recovery-design.md`).
    ///
    /// The returned value is the StreamProcessor's own fold transaction. It is **owned** — not
    /// lifetime-bound to this store — so the StreamProcessor may hold it across multiple log entries
    /// (a follower opens one at its batch's first Event and commits at the Noop) without keeping a
    /// borrow of the store. The StreamProcessor keeps sole ownership of the `Box<dyn StorageTxn>`;
    /// it hands `EventApplier`s only a `&mut dyn StorageTxn`, which structurally *cannot* begin a
    /// nested transaction nor commit, because commit consumes the `Box` ([`StorageTxn::commit`]).
    /// Aborting = dropping the handle without committing, which discards the pending writes (safe: a
    /// discarded fold is never written). The transaction keeps its DB reachable itself (each backend
    /// owns a handle clone), so the store's `MutexGuard` may be released as soon as the transaction
    /// is begun — holding a fold transaction no longer serializes concurrent readers.
    ///
    /// Takes `&self` and is synchronous: it only begins a transaction (no I/O), and every fold write
    /// goes through the returned `StorageTxn`, not the store.
    fn begin_txn(&self) -> Result<Box<dyn StorageTxn>, ExecutionError>;

    /// The last **fully processed** command position — its produced events have also been folded
    /// into this projection. `0` means nothing processed yet (fresh store). This is the durable
    /// resume watermark: a restarted `StreamProcessor` reads the log from `W + 1` instead of re-deriving
    /// it from position 1 (see `docs/durable-execution-recovery-design.md`, single-node model).
    /// Recorded at **apply time**: it advances only after an event's fold is durable here, which
    /// implies the producing command's effects are as well.
    async fn last_processed_position(&self) -> Result<i64, ExecutionError>;
    /// Advance and persist the resume watermark (see [`Storage::last_processed_position`]). Written
    /// with the rest of an event fold so the watermark is never ahead of the projection.
    async fn put_last_processed_position(&mut self, position: i64) -> Result<(), ExecutionError>;
    /// This partition's generated-name counter (see `KeyBuilder::next_generated_seq`): the next
    /// suffix a generated object name may take, unique within the partition without cross-partition
    /// coordination. Derived — rebuilt by re-folding the create events on replay, so it is never
    /// ahead of the projection.
    async fn next_generated_seq(&self) -> Result<i64, ExecutionError>;
}

/// A write-scoped, **owned** projection transaction, handed to an [`EventApplier`]
/// (crate::applier) during exactly one Event fold.
///
/// It is created by [`Storage::begin_txn`] and returned as a `Box<dyn StorageTxn>`. It exposes
/// **only** the projection fold operations — reads fall through to the committed store; writes
/// accumulate into the transaction's pending batch — and deliberately **omits** the transaction
/// controls and the resume watermark (`last_processed_position`/`put_last_processed_position`).
///
/// The key structural guarantee is the *receiver* of the commit method, not its absence:
/// [`StorageTxn::commit`] takes `self: Box<Self>`, so only the caller **owning** the `Box` — the
/// StreamProcessor — can commit. An `EventApplier` is handed a mere `&mut dyn StorageTxn`, which cannot
/// move the `Box`; it can therefore neither begin/commit/abort the outer transaction nor touch the
/// watermark. The atomicity of a fold is thus *type-enforced* (a method the applier cannot invoke),
/// not a convention appliers are trusted to respect.
///
/// Implementations live in the `spica-storage` crate (durable [`RocksStorage`], in-memory
/// [`InMemoryStorage`]); the trait sits here alongside [`Storage`] so the engine names the contract
/// and the storage crate depends on `spica-engine` to implement it (keeping the crate graph acyclic).
/// **Every** method takes `&mut self` — including the reads — because the RocksDB `Transaction`
/// backing `RocksTxn` is `Send` but **not** `Sync` (an async `&self` method would need the txn `Sync`
/// for its future to be `Send`). The applier already holds `&mut dyn StorageTxn`, so the `&mut`
/// reads cost nothing at call sites.
#[async_trait]
pub trait StorageTxn: Send {
    async fn get_execution(
        &mut self,
        reference: &ObjectReference,
    ) -> Result<Option<ExecutionRecord>, ExecutionError>;
    async fn get_thread(
        &mut self,
        reference: &ObjectReference,
    ) -> Result<Option<ThreadRecord>, ExecutionError>;
    async fn get_activity(
        &mut self,
        reference: &ObjectReference,
    ) -> Result<Option<ActivityRecord>, ExecutionError>;
    async fn get_timer(
        &mut self,
        reference: &ObjectReference,
    ) -> Result<Option<TimerRecord>, ExecutionError>;
    async fn get_task(
        &mut self,
        reference: &ObjectReference,
    ) -> Result<Option<TaskRecord>, ExecutionError>;
    async fn get_children(
        &mut self,
        id: ObjectReference,
    ) -> Result<HashSet<ObjectReference>, ExecutionError>;

    /// Scan up to `limit` available (`TaskStatus::Pending`) tasks of `resource`, read-your-writes:
    /// the pending batch is consulted first, then committed rows — the overlay equivalent of
    /// [`Storage::activatable_tasks`], letting a handler pulling work see its own just-emitted
    /// task rows (the no-op scheduler's overlay never arms physical side effects).
    async fn activatable_tasks(
        &mut self,
        resource: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, ExecutionError>;

    async fn get_flow_by_name(&mut self, name: FlowName) -> Result<Option<Flow>, ExecutionError>;
    async fn get_flow_version(
        &mut self,
        version: &ObjectReference,
    ) -> Result<Option<FlowVersion>, ExecutionError>;
    async fn flow_version_of(
        &mut self,
        name: FlowName,
        version: u32,
    ) -> Result<Option<FlowVersion>, ExecutionError>;

    /// Upsert an `ExecutionRecord` record into this transaction's pending batch.
    async fn put_execution(&mut self, exec: ExecutionRecord) -> Result<(), ExecutionError>;
    /// Upsert a `ThreadRecord` record into this transaction's pending batch.
    async fn put_thread(&mut self, thread: ThreadRecord) -> Result<(), ExecutionError>;
    /// Upsert an `ActivityRecord` record into this transaction's pending batch.
    async fn put_activity(&mut self, act: ActivityRecord) -> Result<(), ExecutionError>;
    /// Upsert a `TimerRecord` record into this transaction's pending batch.
    async fn put_timer(&mut self, timer: TimerRecord) -> Result<(), ExecutionError>;
    /// Upsert a `TaskRecord` record into this transaction's pending batch.
    async fn put_task(&mut self, task: TaskRecord) -> Result<(), ExecutionError>;
    /// Remove `child` from `parent`'s `active_children` (a terminal child draining its owner).
    async fn remove_child(
        &mut self,
        parent: ObjectReference,
        child: ObjectReference,
    ) -> Result<(), ExecutionError>;
    /// Add `child` to `parent`'s `active_children` (a child appears when its `ing` event lands).
    async fn add_child(
        &mut self,
        parent: ObjectReference,
        child: ObjectReference,
    ) -> Result<(), ExecutionError>;
    /// Upsert a flow row into this transaction's pending batch.
    async fn put_flow(&mut self, flow: Flow) -> Result<(), ExecutionError>;
    /// Persist a flow version into this transaction's pending batch (also the version index row).
    async fn put_flow_version(&mut self, version: FlowVersion) -> Result<(), ExecutionError>;

    /// Read this partition's generated-name counter (see [`Storage::next_generated_seq`]),
    /// overlay-first so a fold sees its own earlier bump.
    async fn next_generated_seq(&mut self) -> Result<i64, ExecutionError>;
    /// Advance this partition's generated-name counter into this fold's pending batch; landed
    /// atomically with the rest of the fold on [`StorageTxn::commit`].
    async fn put_next_generated_seq(&mut self, seq: i64) -> Result<(), ExecutionError>;

    /// Atomically commit this fold: land **all** buffered writes and, when `watermark` is `Some`,
    /// the resume watermark advance, all-or-nothing. Making the watermark part of the same atomic
    /// batch as the projection is what guarantees it can never run ahead of the projection.
    ///
    /// `self: Box<Self>` reserves commit to the *owner* of the `Box` (the StreamProcessor): dropping the
    /// handle instead of committing aborts the fold (the uncommitted batch is discarded, never
    /// written). Synchronous because the underlying writes are synchronous (a RocksDB `WriteBatch`
    /// write / an in-memory merge) and bounded in size; it must be called only by the StreamProcessor.
    ///
    /// The projection remains a rebuildable cache, so this is a non-fsync path: writes go through
    /// RocksDB's default WAL without a per-call fsync (see `crates/storage` module docs).
    fn commit(self: Box<Self>, watermark: Option<i64>) -> Result<(), ExecutionError>;
}

/// The **read-only** face of a working [`StorageTxn`]: the row/flow reads plus the work-pull scan,
/// all `&self`. A [`CommandHandler`](crate::CommandHandler) reads state through this — it can never
/// write, commit, or move the watermark — while the same underlying txn is mutated by the
/// applier/collector. `Storage` is also one (see the blanket below), so helpers that only need to
/// read stay generic over this narrower contract instead of the full `Storage`.
#[async_trait]
pub trait ReadonlyStorageTxn: Send + Sync {
    async fn get_execution(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ExecutionRecord>, ExecutionError>;
    async fn get_thread(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ThreadRecord>, ExecutionError>;
    async fn get_activity(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ActivityRecord>, ExecutionError>;
    async fn get_timer(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<TimerRecord>, ExecutionError>;
    async fn get_task(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<TaskRecord>, ExecutionError>;
    async fn get_children(
        &self,
        id: ObjectReference,
    ) -> Result<HashSet<ObjectReference>, ExecutionError>;
    async fn activatable_tasks(
        &self,
        resource: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, ExecutionError>;
    async fn get_flow_by_name(&self, name: FlowName) -> Result<Option<Flow>, ExecutionError>;
    async fn get_flow_version(
        &self,
        version: &ObjectReference,
    ) -> Result<Option<FlowVersion>, ExecutionError>;
    async fn flow_version_of(
        &self,
        name: FlowName,
        version: u32,
    ) -> Result<Option<FlowVersion>, ExecutionError>;
    async fn next_generated_seq(&self) -> Result<i64, ExecutionError>;
}

/// Every [`Storage`] is already a `ReadonlyStorageTxn`: its read methods are exactly this face, so
/// the blanket forwards them — letting a helper accept `&dyn ReadonlyStorageTxn` and be called with
/// either committed storage or the working overlay, with no trait upcasting.
#[async_trait]
impl<T: ?Sized + Storage> ReadonlyStorageTxn for T {
    async fn get_execution(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ExecutionRecord>, ExecutionError> {
        <T as Storage>::get_execution(self, reference).await
    }
    async fn get_thread(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ThreadRecord>, ExecutionError> {
        <T as Storage>::get_thread(self, reference).await
    }
    async fn get_activity(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ActivityRecord>, ExecutionError> {
        <T as Storage>::get_activity(self, reference).await
    }
    async fn get_timer(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<TimerRecord>, ExecutionError> {
        <T as Storage>::get_timer(self, reference).await
    }
    async fn get_task(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<TaskRecord>, ExecutionError> {
        <T as Storage>::get_task(self, reference).await
    }
    async fn get_children(
        &self,
        id: ObjectReference,
    ) -> Result<HashSet<ObjectReference>, ExecutionError> {
        <T as Storage>::get_children(self, id).await
    }
    async fn activatable_tasks(
        &self,
        resource: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, ExecutionError> {
        <T as Storage>::activatable_tasks(self, resource, limit).await
    }
    async fn get_flow_by_name(&self, name: FlowName) -> Result<Option<Flow>, ExecutionError> {
        <T as Storage>::get_flow_by_name(self, name).await
    }
    async fn get_flow_version(
        &self,
        version: &ObjectReference,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        <T as Storage>::get_flow_version(self, version).await
    }
    async fn flow_version_of(
        &self,
        name: FlowName,
        version: u32,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        <T as Storage>::flow_version_of(self, name, version).await
    }
    async fn next_generated_seq(&self) -> Result<i64, ExecutionError> {
        <T as Storage>::next_generated_seq(self).await
    }
}
