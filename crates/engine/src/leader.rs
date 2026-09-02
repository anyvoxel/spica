//! The **leader** role of [`ProcessingStateMachine`](crate::processing::ProcessingStateMachine): full
//! processing — dispatch a [`Command`] into Events, fold each [`Event`] into the projection, advance
//! the resume watermark, and deliver acknowledgements.
//!
//! This is a faithful port of the processing that used to live inline in
//! [`StreamProcessor::run`](crate::StreamProcessor::run): the per-entry arms are now methods on this
//! type, reached through the role-aware [`ProcessingStateMachine`](crate::processing::ProcessingStateMachine)
//! trait so the single driver can later run a **follower** (replicate-only) role instead — the seam a
//! distributed deployment needs for failover. See [`StreamProcessor`](crate::StreamProcessor) for the
//! driver side. Transactions are **driver-owned**: the leader folds the Events of a produced batch
//! (or a recovery residue) into a `StorageTxn` the driver opens and commits, then lets the driver's
//! `after_commit` drain the correlated acks — the leader never calls `begin_txn`/`commit` itself.
//!
//! TODO(multi-node): a `Follower` implementing the same trait, which appends replicated entries
//! without dispatching Commands or folding the projection, and advances the resume watermark from the
//! leader's commit index rather than at fold time. Not part of the single-node milestone.

use std::collections::HashMap;
use std::mem::discriminant;
use std::sync::Arc;

use async_trait::async_trait;
use spica_asl::StateMachine;
use tracing::debug;

use crate::applier::{ApplierContext, EventDispatcher};
use crate::engine::AckRouter;
use crate::eval_env::EvalEnv;
use crate::handler::{AckSideEffect, Collector, CommandHandler, HandlerContext};
use crate::log::{Entry, EntryPayload, Timestamp};
use crate::processing::{CommandProcessed, ProcessingHandles, ProcessingStateMachine, Role};
use crate::storage::{Storage, StorageTxn};
use crate::types::command::Command;
use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::id::EntryId;
use crate::types::meta::ObjectReference;

/// The leader processing state machine: owns the command dispatch table, the per-version machine
/// cache, the eval environment, the event applier, the resume watermark, and the deferred
/// acknowledgement queue. All of it is leader-private — a follower holds none of it.
pub(crate) struct Leader {
    /// Dispatch table mapping each [`Command`] variant (by [`std::mem::Discriminant`]) to its handler.
    /// Built once in [`StreamProcessor::new`](crate::StreamProcessor::new) and moved in here.
    handlers: HashMap<std::mem::Discriminant<Command>, Box<dyn CommandHandler + Send + Sync>>,
    /// Lazily-populated per-version machine cache (see [`handler::HandlerContext::machine`]).
    definitions: HashMap<ObjectReference, Arc<StateMachine>>,
    /// Shared eval environment; wrapped once (unlike the old `StreamProcessor`, which wrapped it per
    /// `run`), so both `process_command` and the single-shot `dispatch` lock it the same way.
    env: Arc<tokio::sync::Mutex<EvalEnv>>,
    /// Table-driven event applier (fold an [`Event`] into [`Storage`]).
    dispatcher: EventDispatcher,
    /// Resume watermark — the highest fully-applied Command position, advanced at commit time.
    watermark: i64,
    /// Deferred acknowledgement side effects, drained once their matching [`Event`] is applied.
    pending_acks: Vec<AckSideEffect>,
    /// The Events the driver just committed in the fold it owns — set by `apply_batch` / `apply_event`
    /// and consumed by [`ProcessingStateMachine::after_commit`] to drain the deferred acks whose
    /// matching Event is now durable (the Zeebe post-commit model).
    last_applied: Vec<Event>,
}

impl Leader {
    /// Build the leader around an already-populated command-handler table. The machinery cache, eval
    /// environment, and dispatcher start empty/fresh; the resume watermark is installed later, when
    /// the driver reads it from [`Storage`] at boot (`set_resume_position`).
    pub(crate) fn new(
        handlers: HashMap<std::mem::Discriminant<Command>, Box<dyn CommandHandler + Send + Sync>>,
    ) -> Self {
        Self {
            handlers,
            definitions: HashMap::new(),
            env: Arc::new(tokio::sync::Mutex::new(EvalEnv::new())),
            dispatcher: EventDispatcher::new(),
            watermark: 0,
            pending_acks: Vec::new(),
            last_applied: Vec::new(),
        }
    }

    /// Routes a [`Command`] to its handler, returning the [`Entry`]s it emitted (already enveloped with
    /// `cause_id`/`timestamp` and placeholder `entry_id`/`stream_id`), ready to append atomically (the
    /// append stamps the real positions and the log's own stream id).
    ///
    /// Single-shot, synchronous dispatch usable outside `run` (tests). Timer *scheduling* is not part
    /// of dispatch: applying a `TimerActivated` event is what arms a timer, and that only happens in
    /// `apply_event`/`apply_batch` where the injected [`Scheduler`] lives — so a single-shot caller
    /// driven purely by `dispatch` cannot fire timers (it must drive `run`).
    pub async fn dispatch<S: Storage>(
        &mut self,
        command: &Command,
        storage: &S,
        cause_id: EntryId,
    ) -> Result<Vec<Entry>, ExecutionError> {
        let mut out = Collector::new(cause_id);
        let mut env_g = self.env.lock().await;
        let mut ctx = HandlerContext {
            env: &mut env_g,
            storage,
            definitions: &mut self.definitions,
        };
        let handler = self
            .handlers
            .get(&discriminant(command))
            .expect("a handler is registered for every Command variant");
        handler.handle(command, &mut ctx, &mut out).await;
        Ok(out.into_entries())
    }

    /// Deliver any deferred [`AckSideEffect`] whose Event is among `applied`. The leader defers
    /// event-correlated acks (see [`Collector::ack_request`]) until the Event they correlate to is
    /// actually folded; in the eager-apply model that happens once per produced batch, in
    /// [`ProcessingStateMachine::apply_batch`]. Correlation is by event
    /// *variant* plus the echoed `request_id` — never full value equality, because Event payloads do
    /// not round-trip through the LogStream byte-for-byte (float precision / defaulted serde fields
    /// drift on serialize/deserialize).
    async fn deliver_matching_acks(
        &mut self,
        applied: &[Event],
        ack: &Arc<tokio::sync::Mutex<AckRouter>>,
    ) {
        let mut ack_g = ack.lock().await;
        let mut i = 0;
        while i < self.pending_acks.len() {
            // Find the first among `applied` that resolves this pending ack (same variant + echoed
            // request id). The id's uniqueness pins the operation, so concurrent requests never alias.
            let matched = match &self.pending_acks[i] {
                AckSideEffect::CompleteRequest { request_id, event } => {
                    let want = *request_id;
                    let expected = event.as_ref();
                    applied.iter().find_map(|applied_ev| {
                        let applied_ev: &Event = applied_ev;
                        if discriminant(applied_ev) != discriminant(expected) {
                            return None;
                        }
                        let hits = matches!(
                            applied_ev,
                            Event::FlowCreated { request_id: r, .. }
                                | Event::FlowVersionCreated { request_id: r, .. }
                                | Event::ExecutionCreated { request_id: r, .. }
                                | Event::TaskCompleted { request_id: r, .. }
                                if *r == want
                        );
                        hits.then(|| applied_ev.clone())
                    })
                }
                // `CompleteRequestTasks` is never deferred here — it is delivered in the command arm
                // (via the driver, right after the append/apply) — so nothing in `pending_acks` is
                // ever this variant.
                AckSideEffect::CompleteRequestTasks { .. } => None,
            };
            let Some(ev) = matched else {
                i += 1;
                continue;
            };
            let side = self.pending_acks.swap_remove(i);
            // Only `CompleteRequest` is ever matched above, so the payload is the applied Event that
            // produced it. `complete` ignores a dropped receiver (the await was cancelled), so no
            // error handling is needed here.
            if let AckSideEffect::CompleteRequest { request_id, .. } = side {
                ack_g.complete(request_id, ev);
            }
        }
    }
}

#[async_trait]
impl ProcessingStateMachine for Leader {
    fn role(&self) -> Role {
        Role::Leader
    }

    fn set_resume_position(&mut self, position: i64) {
        self.watermark = position;
    }

    async fn process_command(
        &mut self,
        entry_id: EntryId,
        command: &Command,
        handles: &ProcessingHandles,
    ) -> Result<CommandProcessed, ExecutionError> {
        let (entries, acks) = {
            // Acquire Storage only for the duration of this command's dispatch (per-entry, not for
            // the whole run), so the Engine's other threads can read projection state — e.g.
            // `Engine::start_for` resolving the latest revision — while the driver sits between
            // entries.
            let storage_guard = handles.storage.lock().await;
            let mut env_g = self.env.lock().await;
            let mut out = Collector::new(entry_id);
            let mut ctx = HandlerContext {
                env: &mut env_g,
                storage: &*storage_guard,
                definitions: &mut self.definitions,
            };
            let handler = self
                .handlers
                .get(&discriminant(command))
                .expect("a handler is registered for every Command variant");
            handler.handle(command, &mut ctx, &mut out).await;
            out.into_parts()
        };
        // Split the declared acknowledgements: task grants are delivered by the driver right after
        // the append (they must be answered even when the pull produced no events); the
        // event-correlated ones wait until their Event is applied in `after_commit` (post-commit).
        let mut grants = Vec::new();
        for side in acks {
            match side {
                AckSideEffect::CompleteRequestTasks {
                    request_id, tasks, ..
                } => grants.push((request_id, tasks)),
                other => self.pending_acks.push(other),
            }
        }
        Ok(CommandProcessed { entries, grants })
    }

    async fn apply_event(
        &mut self,
        txn: &mut dyn StorageTxn,
        entry_id: EntryId,
        timestamp: Timestamp,
        cause_id: Option<EntryId>,
        event: &Event,
        handles: &ProcessingHandles,
    ) -> Result<Option<i64>, ExecutionError> {
        // Recovery fold, on read-back. In the eager-apply model this Event's batch was already folded —
        // and the resume watermark advanced past it — in `apply_batch` at the moment its producing
        // Command was appended, before the read-back loop reaches it here. The driver gates on
        // `is_already_applied` and only calls this for the residual **crash window** (a batch durably
        // appended but not yet folded before the crash — see
        // `docs/durable-execution-recovery-design.md`), so the fold below is the idempotent replay path.
        // The driver supplies the already-open `txn`; we fold into it and return the watermark to commit.
        {
            // Apply the event's projection and feed any side effects to the scheduler / task service.
            // `cause_id` for a scheduled timer / invoked task is this event's own entry_id, so the
            // later resumption command causally hangs off this event.
            let mut ctx = ApplierContext {
                storage: &mut *txn,
                scheduler: handles.scheduler.as_ref(),
                cause_id: entry_id,
                // The entry's own frozen timestamp is the deterministic source for projection
                // `created_at`/`updated_at`; see `ApplierContext::timestamp`.
                timestamp,
            };
            self.dispatcher.apply(&mut ctx, event).await?;
        } // the write handle drops here; the transaction stays open until the driver commits.

        // Advance the resume watermark once this event's fold is durable, keeping it
        // never-ahead-of-projection. The `Some` watermark is written by the driver's `commit` inside
        // the same atomic batch as the fold.
        let advanced = if let Some(cause) = cause_id.filter(|c| c.get() > self.watermark) {
            self.watermark = cause.get();
            debug!(
                watermark = self.watermark,
                "resume watermark advanced (replay)"
            );
            Some(self.watermark)
        } else {
            None
        };

        // Record the applied event so `after_commit` (driver-invoked post-commit) can drain any
        // deferred acknowledgement correlated to it — the recovery counterpart to `apply_batch`'s
        // drain; there are no in-flight acks from a prior run, so this is normally empty.
        self.last_applied = vec![event.clone()];
        Ok(advanced)
    }

    fn is_already_applied(&self, entry_id: EntryId) -> bool {
        // Everything at or below the watermark was folded eagerly at production (`apply_batch`), so a
        // read-back Event at that position must be skipped rather than folded twice — only crash-residue
        // Events above it fall through to the recovery `apply_event`.
        entry_id.get() <= self.watermark
    }

    async fn apply_batch(
        &mut self,
        txn: &mut dyn StorageTxn,
        batch: &[Entry],
        handles: &ProcessingHandles,
    ) -> Result<Option<i64>, ExecutionError> {
        // Eager apply: fold the just-appended batch's Events now, in the one atomic projection
        // transaction the driver opened for it, and advance the resume watermark to the batch's
        // terminating `Noop` position. This is what keeps the leader from re-processing its own
        // Events on read-back (`is_already_applied` / `apply_event` skip anything at or below the
        // watermark) — a Command's effects are folded exactly once, at production time. Folding the
        // whole batch in one transaction makes its Events all-or-nothing, so apply atomicity matches
        // the append's atomicity.
        let mut applied: Vec<Event> = Vec::new();
        for entry in batch {
            if let EntryPayload::Event(event) = &entry.payload {
                // The entry's own frozen timestamp is the deterministic source for the projection's
                // `created_at`/`updated_at` (replicas replaying the same entries agree); its
                // `entry_id` is the causal cause for anything this event arms (timers / resumption).
                let mut ctx = ApplierContext {
                    storage: &mut *txn,
                    scheduler: handles.scheduler.as_ref(),
                    cause_id: entry.entry_id,
                    timestamp: entry.timestamp,
                };
                self.dispatcher.apply(&mut ctx, event).await?;
                applied.push(event.clone());
            }
        }
        // Advance the watermark to the batch's last entry — the `Noop` the driver appends as the
        // batch terminator. `batch` is non-empty here (the driver only calls this for a non-empty
        // produced batch), so `last` is the Noop's position and everything at or below it belongs to
        // this now-fully-applied batch. The `Some` watermark is written by the driver's `commit` in
        // the same atomic batch as the fold, so it can never run ahead of the projection.
        let batch_end = batch
            .last()
            .map(|e| e.entry_id.get())
            .unwrap_or(self.watermark);
        if batch_end > self.watermark {
            self.watermark = batch_end;
        }
        debug!(
            watermark = self.watermark,
            "batch applied; watermark advanced"
        );
        // Remember the just-applied Events so the driver's `after_commit` can drain the deferred acks
        // once this fold is durable (the Zeebe post-commit model).
        self.last_applied = applied;
        Ok(Some(self.watermark))
    }

    async fn commit_at_noop(
        &mut self,
        _txn: &mut dyn StorageTxn,
        _entry_id: EntryId,
        _handles: &ProcessingHandles,
    ) -> Result<Option<i64>, ExecutionError> {
        // A Noop read back by the leader rounds off a batch it already applied eagerly at production
        // time (`apply_batch`), so there is nothing here to flush. The Noop is consumed for the log's
        // structured batching, and by a follower closing its own atomic apply — not by the leader.
        Ok(None)
    }

    async fn after_commit(&mut self, handles: &ProcessingHandles) {
        // Drain the deferred acknowledgements whose Event the driver just committed. In the
        // driver-owned-txn model the fold (which populated `last_applied`) and the leader-visible
        // commit are separate, so this is the driver's hook to fire the Zeebe post-commit side
        // effects exactly once the commit is durable.
        let applied = core::mem::take(&mut self.last_applied);
        self.deliver_matching_acks(&applied, &handles.ack).await;
    }
}
