//! The leader's ephemeral **working state**: a private projection overlay that lets the inline
//! parent-reaction cascade read its own writes (Zeebe's interleaved-apply model).
//!
//! During one `process_command`, the leader opens a single [`StorageTxn`] over committed storage (the
//! driver opens a fresh txn via `Storage::begin_txn`) and folds each emitted `Event` into it
//! immediately, at `emit_event` time (via the collector's overlay). Handlers read through this type's
//! [`ReadonlyStorageTxn`] face, which resolves overlay-then-committed — so an inline `child_settled`
//! sees the just-emitted terminal's effect (its parent's `active_children` drained) and can converge
//! the whole settled ancestor chain in the same batch.
//!
//! The working txn is now the **single authoritative fold** of a produced batch: the driver commits
//! it (with the batch-end watermark) once the batch is durably appended, and drops it — rolling back —
//! on an append failure. No post-append `apply_batch` re-fold exists on the live path. The fold is a
//! pure projection: no applier performs external side effects, so nothing here accumulates or drains —
//! timer arming is re-derived by the consumer from the durable event via the Hook.
//!
//! The handler view is a single shared [`Mutex`] around the txn: reads and the collector's eager-apply
//! both lock it (the leader is single-threaded over the log, so it is uncontended). A plain `Mutex`
//! rather than an `RwLock` because `StorageTxn` reads take `&mut self`, so every access — read or
//! write — needs the exclusive guard; an `RwLock` would add overhead for no read-parallelism.

use std::sync::Arc;

use async_trait::async_trait;

use crate::applier::{ApplierContext, EventDispatcher};
use crate::log::Timestamp;
use crate::storage::{
    ActivityRecord, ExecutionRecord, ReadonlyStorageTxn, StorageTxn, TaskRecord, ThreadRecord,
    TimerRecord,
};
use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::flow::Flow;
use crate::types::flow_version::FlowVersion;
use crate::types::id::FlowName;
use crate::types::meta::ObjectReference;

/// The ephemeral overlay backing one handled command. Shared between the handler's read face and the
/// collector's eager-apply through a mutex; the leader is single-threaded over the log, so the lock
/// is uncontended. The txn is `Option` so the driver can `mem::take` it out and commit (it owns the
/// [`StorageTxn::commit`] authority); dropping the `WorkingState` without committing rolls the txn
/// back.
pub(crate) struct WorkingState {
    txn: Arc<tokio::sync::Mutex<Option<Box<dyn StorageTxn>>>>,
}

impl WorkingState {
    pub(crate) fn new(txn: Box<dyn StorageTxn>) -> Self {
        Self {
            txn: Arc::new(tokio::sync::Mutex::new(Some(txn))),
        }
    }

    /// A working overlay with no transaction behind it — fold and commit are both no-ops. Used by
    /// roles that never produce a foldable batch (the follower's `process_command` returns an empty
    /// `CommandProcessed` whose `work` the driver never commits).
    #[allow(dead_code)] // reachable only through the not-yet-wired Follower role (see follower.rs).
    pub(crate) fn empty() -> Self {
        Self {
            txn: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Fold one emitted `Event` into the overlay. A no-op once the txn has been taken/committed.
    pub(crate) async fn apply_projection(
        &self,
        dispatcher: &EventDispatcher,
        event: &Event,
        timestamp: Timestamp,
    ) -> Result<(), ExecutionError> {
        let mut txn = self.txn.lock().await;
        let Some(storage) = txn.as_mut() else {
            return Ok(()); // already committed/taken — a defensive no-op.
        };
        let mut ctx = ApplierContext {
            storage: &mut **storage,
            timestamp,
        };
        dispatcher.apply(&mut ctx, event).await
    }

    /// Take the txn out and commit it with `watermark` (the batch-end position). Consumes the overlay
    /// — the caller must hold the last reference. A no-op if the txn was already taken.
    pub(crate) async fn commit(self, watermark: Option<i64>) -> Result<(), ExecutionError> {
        let mut txn = self.txn.lock().await;
        if let Some(t) = txn.take() {
            t.commit(watermark)?;
        }
        Ok(())
    }

    /// Mint the next generated-name suffix: read this partition's counter and **advance it** in the
    /// same working txn, returning the pre-increment value (read-your-writes within the batch). A
    /// single leader thread drives the log, so the read-modify-write is atomic; the advance lands
    /// with the batch on commit, so a name minted here is never re-issued even though its create
    /// event may fold in a later batch. Separately, the create appliers raise the counter to
    /// `suffix + 1` on replay, reconstructing it from events (this mint does not re-run).
    pub(crate) async fn mint_generated_seq(&self) -> u64 {
        let mut txn = self.txn.lock().await;
        let storage = txn.as_mut().expect("working txn alive during dispatch");
        let n = storage.next_generated_seq().await.unwrap_or(0);
        let _ = storage.put_next_generated_seq(n + 1).await;
        n as u64
    }
}

/// The handler's read-only view over the working overlay: each read locks the shared txn and
/// resolves overlay-then-committed (read-your-writes), so a handler sees both committed state and its
/// own just-emitted writes. Write/commit/watermark are absent from this face — handlers can only read.
#[async_trait]
impl ReadonlyStorageTxn for WorkingState {
    async fn get_execution(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ExecutionRecord>, ExecutionError> {
        let mut txn = self.txn.lock().await;
        let storage = txn.as_mut().expect("working txn alive during dispatch");
        storage.get_execution(reference).await
    }
    async fn get_thread(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ThreadRecord>, ExecutionError> {
        let mut txn = self.txn.lock().await;
        let storage = txn.as_mut().expect("working txn alive during dispatch");
        storage.get_thread(reference).await
    }
    async fn get_activity(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<ActivityRecord>, ExecutionError> {
        let mut txn = self.txn.lock().await;
        let storage = txn.as_mut().expect("working txn alive during dispatch");
        storage.get_activity(reference).await
    }
    async fn get_timer(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<TimerRecord>, ExecutionError> {
        let mut txn = self.txn.lock().await;
        let storage = txn.as_mut().expect("working txn alive during dispatch");
        storage.get_timer(reference).await
    }
    async fn get_task(
        &self,
        reference: &ObjectReference,
    ) -> Result<Option<TaskRecord>, ExecutionError> {
        let mut txn = self.txn.lock().await;
        let storage = txn.as_mut().expect("working txn alive during dispatch");
        storage.get_task(reference).await
    }
    async fn get_children(
        &self,
        id: ObjectReference,
    ) -> Result<std::collections::HashSet<ObjectReference>, ExecutionError> {
        let mut txn = self.txn.lock().await;
        let storage = txn.as_mut().expect("working txn alive during dispatch");
        storage.get_children(id).await
    }
    async fn activatable_tasks(
        &self,
        resource: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, ExecutionError> {
        let mut txn = self.txn.lock().await;
        let storage = txn.as_mut().expect("working txn alive during dispatch");
        storage.activatable_tasks(resource, limit).await
    }
    async fn get_flow_by_name(&self, name: FlowName) -> Result<Option<Flow>, ExecutionError> {
        let mut txn = self.txn.lock().await;
        let storage = txn.as_mut().expect("working txn alive during dispatch");
        storage.get_flow_by_name(name).await
    }
    async fn get_flow_version(
        &self,
        version: &ObjectReference,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        let mut txn = self.txn.lock().await;
        let storage = txn.as_mut().expect("working txn alive during dispatch");
        storage.get_flow_version(version).await
    }
    async fn flow_version_of(
        &self,
        name: FlowName,
        version: u32,
    ) -> Result<Option<FlowVersion>, ExecutionError> {
        let mut txn = self.txn.lock().await;
        let storage = txn.as_mut().expect("working txn alive during dispatch");
        storage.flow_version_of(name, version).await
    }
    async fn next_generated_seq(&self) -> Result<i64, ExecutionError> {
        let mut txn = self.txn.lock().await;
        let storage = txn.as_mut().expect("working txn alive during dispatch");
        storage.next_generated_seq().await
    }
}
