//! The **leader** role of the engine's [`StateMachine`](crate::processing::StateMachine): full
//! processing — dispatch a [`Command`] into Events, fold each [`Event`] into the projection, advance
//! the resume watermark, and deliver acknowledgements.
//!
//! This is a faithful port of the processing that used to live inline in
//! [`StreamProcessor::run`](crate::StreamProcessor::run): the per-entry arms are now methods on this
//! type, reached through the [`StateMachine::Leader`](crate::processing::StateMachine::Leader) variant
//! so the single driver can later run a **follower** (replicate-only) role instead — the seam a
//! distributed deployment needs for failover. See [`StreamProcessor`](crate::StreamProcessor) for the
//! driver side. Transactions are **driver-owned**: the leader folds the Events of a produced batch
//! (or a recovery residue) into a `StorageTxn` the driver opens and commits, then lets the driver's
//! `after_commit` drain the correlated acks — the leader never calls `begin_txn`/`commit` itself.
//!
//! TODO(multi-node): a `Follower` sibling under `StateMachine`, which appends replicated entries
//! without dispatching Commands or folding the projection, and advances the resume watermark from the
//! leader's commit index rather than at fold time. Not part of the single-node milestone.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use spica_asl::StateMachine;
use spica_machinery::{Clock, IdGenerator};
use tracing::{debug, warn};

use crate::applier::{ApplierContext, dispatch_event};
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext, OverlaySink, ProcessingError};
use crate::handlers::state_handler::StateHandlerRegistry;
use crate::handlers::{build_state_handlers, dispatch_command};
use crate::log::{Entry, EntryPayload, Timestamp};
use crate::processing::{CommandProcessed, ProcessingHandles};
use crate::storage::{Storage, StorageTxn};
use crate::types::command::Command;
use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::id::{EntryId, RequestId};
use crate::types::meta::ObjectReference;
use crate::types::reject::RejectionType;
use crate::working::WorkingState;

/// How many times one command's dispatch may be attempted before an unexpected failure is given up
/// on and the command refused (see [`Leader::process_command`]). A fixed, small budget: each attempt
/// re-runs the whole dispatch, and a failure that survives three attempts is a persistent condition
/// rather than a transient one.
pub(crate) const MAX_COMMAND_ATTEMPTS: u32 = 3;

/// The first retry's backoff, doubling per attempt. Time enters the engine as an input the caller
/// controls (see [`Clock`]) — but this delay is a real wait between attempts, not a decision read
/// off the clock, so it is a fixed schedule rather than a `Timestamp` comparison.
const COMMAND_RETRY_BACKOFF_MS: u64 = 50;

/// The backoff before attempt `attempt + 1` — doubling, so a persistent fault is probed at widening
/// intervals instead of hammering the backend that is failing.
fn command_retry_backoff(attempt: u32) -> Duration {
    Duration::from_millis(COMMAND_RETRY_BACKOFF_MS << (attempt - 1))
}

/// The leader processing state machine: owns the per-version machine cache, the eval environment, the
/// event applier, the resume watermark, and the deferred acknowledgement queue. All of it is
/// leader-private — a follower holds none of it.
pub(crate) struct Leader {
    /// Lazily-populated per-version machine cache (see [`handler::HandlerContext::machine`]).
    definitions: HashMap<ObjectReference, Arc<StateMachine>>,
    /// Shared eval environment; wrapped once (unlike the old `StreamProcessor`, which wrapped it per
    /// `run`), so both `process_command` and the single-shot `dispatch` lock it the same way.
    env: Arc<tokio::sync::Mutex<EvalEnv>>,
    /// Shared `State` → [`StateHandlerFactory`] dispatch table, threaded into every handler context so
    /// the inline child-settled cascade can replenish a `Running` container (see [`HandlerContext`]).
    state_handlers: StateHandlerRegistry,
    /// Resume watermark — the highest fully-applied Command position, advanced at commit time.
    watermark: i64,
    /// The Events the driver just committed in the fold it owns — set by `process_command` / `apply_event`
    /// and consumed by `after_commit` to report each as a `Hook` fact once it is durable (the Zeebe
    /// post-commit model).
    last_applied: Vec<Event>,
    /// The engine's injected [`Clock`], handed to every dispatch it drives (both the collector's
    /// envelope stamps and the handler context's decisions read it) — see [`Clock`].
    clock: Arc<dyn Clock>,
    /// The engine's injected [`IdGenerator`], handed to every dispatch alongside the clock: a
    /// dispatch names the objects it creates through it, so identity is as controllable as time.
    ids: Arc<dyn IdGenerator>,
}

impl Leader {
    /// Build the leader. The machinery cache, eval environment, and state-handler registry start
    /// empty/fresh; the resume watermark is installed later, when the driver reads it from [`Storage`]
    /// at boot (`set_resume_position`).
    pub(crate) fn new(clock: Arc<dyn Clock>, ids: Arc<dyn IdGenerator>) -> Self {
        Self {
            definitions: HashMap::new(),
            env: Arc::new(tokio::sync::Mutex::new(EvalEnv::new())),
            state_handlers: build_state_handlers(),
            watermark: 0,
            last_applied: Vec::new(),
            clock,
            ids,
        }
    }

    /// The clock this role dispatches with — the driver's own stamping (the batch's `Noop` commit
    /// marker) reads the same source as the handlers it drives, so one batch's records agree on time.
    pub(crate) fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }

    /// Routes a [`Command`] to its handler, returning the [`Entry`]s it emitted (already enveloped with
    /// `cause_id`/`timestamp` and placeholder `entry_id`/`stream_id`), ready to append atomically (the
    /// append stamps the real positions and the log's own stream id).
    ///
    /// Single-shot, synchronous dispatch usable outside `run` (tests). Timer *scheduling* is not part
    /// of dispatch: a `TimerActivated` event's durable arm is observed post-commit by the injected
    /// `Hook` (a consumer re-derives the physical schedule), so a single-shot caller driven purely by
    /// `dispatch` fetches no environment and fires no timers.
    pub async fn dispatch<S: Storage>(
        &mut self,
        command: &Command,
        storage: &S,
        cause_id: EntryId,
    ) -> Result<Vec<Entry>, ExecutionError> {
        // Open an ephemeral working overlay so an inline cascade reads its own writes; the emitted
        // events are folded into it eagerly at `append_event` time, and the overlay is dropped (aborted)
        // at command end. There is no durable fold on this path; `run` commits a real batch instead.
        let work = WorkingState::new(storage.begin_txn()?);
        let overlay = OverlaySink::new(&work);
        let mut out = Collector::new(
            cause_id,
            Some(overlay),
            Arc::clone(&self.clock),
            Arc::clone(&self.ids),
        );
        let mut env_g = self.env.lock().await;
        let mut ctx = HandlerContext {
            env: &mut env_g,
            storage: &work,
            clock: Arc::clone(&self.clock),
            ids: Arc::clone(&self.ids),
            definitions: &mut self.definitions,
            state_handlers: &self.state_handlers,
        };
        // A single-shot dispatch is synchronous by contract — the caller reads the entries straight
        // back — so nothing is retried here: an engine fault is surfaced to the caller (who drives
        // the retry by dispatching again), and a refusal is already recorded in-band.
        match dispatch_command(command, &mut ctx, &mut out).await {
            Ok(()) => Ok(out.into_entries()),
            Err(ProcessingError::Unexpected(e)) => Err(e),
            Err(ProcessingError::Rejected(ty, reason)) => {
                out.reject(
                    command.request_id().unwrap_or_else(RequestId::nil),
                    ty,
                    reason,
                );
                Ok(out.into_entries())
            }
        }
    }
}

impl Leader {
    /// Install the resume position read from [`Storage`] at boot — the last position whose effects
    /// are durable, so the driver resumes the log tail from `position + 1`. The leader advances it at
    /// fold time (a follower advances it from each Noop instead).
    pub(crate) fn set_resume_position(&mut self, position: i64) {
        self.watermark = position;
    }

    /// Advance a command to its outcome: dispatch it, and — where that produced none — apply this
    /// leader's **policy** to the classification it returned ([`ProcessingError`] itself says only
    /// *whose* failure it was, never what to do about it).
    ///
    /// Policy: an **unexpected** failure came out of the engine's own machinery, which makes it worth
    /// another attempt, so the same command is re-dispatched up to [`MAX_COMMAND_ATTEMPTS`] times with
    /// a doubling backoff; one that survives the budget is given up on and the command refused with
    /// [`RejectionType::ProcessingError`]. A **refusal** ([`ProcessingError::Rejected`]) is decided by
    /// the command's own state, so re-dispatching could only repeat it — it is refused immediately.
    ///
    /// Either way a refusal is recorded as the command's **single** response entry, so the engine's
    /// *every command has a subsequent entry* invariant holds even for a dispatch that produced
    /// nothing: the awaiting caller is woken by [`Collector::reject`]'s record rather than left
    /// hanging on a silent return. The failed attempt's own batch is dropped unappended (its working
    /// txn was never committed), so the refusal is recorded on a fresh batch.
    ///
    /// Kept off the caller's stack: the driver's `?` sees only the unexpected failures of the
    /// *engine* (a store that cannot even open the refusal's transaction), which are not the command's.
    pub(crate) async fn process_command(
        &mut self,
        entry_id: EntryId,
        command: &Command,
        handles: &ProcessingHandles,
    ) -> Result<CommandProcessed, ExecutionError> {
        let mut attempt = 1u32;
        loop {
            match self.dispatch_once(entry_id, command, handles).await {
                Ok(produced) => return Ok(produced),
                Err(err) if err.is_unexpected() && attempt < MAX_COMMAND_ATTEMPTS => {
                    let backoff = command_retry_backoff(attempt);
                    warn!(
                        entry_id = entry_id.get(),
                        attempt,
                        max_attempts = MAX_COMMAND_ATTEMPTS,
                        backoff_ms = backoff.as_millis(),
                        error = %err.rejection_reason(),
                        "command dispatch failed on the engine's side; retrying"
                    );
                    // Between attempts, never inside one: the storage lock and the working txn are
                    // per-attempt (see `dispatch_once`), so a backoff stalls no reader.
                    tokio::time::sleep(backoff).await;
                    attempt += 1;
                }
                Err(err) => return self.reject_command(entry_id, command, handles, &err).await,
            }
        }
    }

    /// Dispatch `command` once, over a **fresh** working overlay and under the Storage lock held only
    /// for this attempt — see [`Self::process_command`] for the retry that wraps it.
    ///
    /// The lock is taken per-entry (not for the whole run) so the Engine's other threads can read
    /// projection state — e.g. `Engine::start_for` resolving the latest revision — while the driver
    /// sits between entries, and the working txn is returned to the driver *outside* it:
    /// `WorkingState` owns the txn (wrapped in its own mutex), so committing after the durable append
    /// does not need the Storage lock.
    async fn dispatch_once(
        &mut self,
        entry_id: EntryId,
        command: &Command,
        handles: &ProcessingHandles,
    ) -> Result<CommandProcessed, ProcessingError> {
        let storage_guard = handles.storage.lock().await;
        let mut env_g = self.env.lock().await;
        let work = WorkingState::new((**storage_guard).begin_txn()?);
        let overlay = OverlaySink::new(&work);
        let mut out = Collector::new(
            entry_id,
            Some(overlay),
            Arc::clone(&self.clock),
            Arc::clone(&self.ids),
        );
        let mut ctx = HandlerContext {
            env: &mut env_g,
            storage: &work,
            clock: Arc::clone(&self.clock),
            ids: Arc::clone(&self.ids),
            definitions: &mut self.definitions,
            state_handlers: &self.state_handlers,
        };
        // On the fault path the batch is dropped uncommitted: `out` (and with it every entry the
        // attempt emitted before the fault) and `work` both go out of scope, so nothing durable — and
        // nothing in `last_applied` — survives an attempt that produced no outcome.
        dispatch_command(command, &mut ctx, &mut out).await?;
        let entries = out.into_parts();
        // Record the produced Events for the driver's `after_commit` once this batch is durable
        // (the Zeebe post-commit report). Set here rather than in `apply_batch` (which no longer
        // exists on the live path): the fold happened eagerly at `append_event` time.
        self.last_applied = entries
            .iter()
            .filter_map(|e| match &e.payload {
                EntryPayload::Event(ev) => Some(ev.clone()),
                _ => None,
            })
            .collect();
        Ok(CommandProcessed { entries, work })
    }

    /// Record a dispatch's failure as the command's single response: one [`Reject`] entry, on a fresh
    /// working txn (no projection is folded for a refusal — see [`Collector::reject`]).
    ///
    /// The refusal is keyed by the command's own [`Command::request_id`] where it has one, so a
    /// client-originated command's awaiter is woken by exactly the id it awaits; an internal command
    /// (nothing waiting) records the refusal with a nil id, as the in-band refusals do. A handler that
    /// refuses **in-band** still carries whatever id it received — this path is for the decisions the
    /// engine itself makes around a command.
    async fn reject_command(
        &self,
        entry_id: EntryId,
        command: &Command,
        handles: &ProcessingHandles,
        err: &ProcessingError,
    ) -> Result<CommandProcessed, ExecutionError> {
        let (rejection_type, reason) = match err {
            ProcessingError::Rejected(ty, reason) => (*ty, reason.clone()),
            // A failure that outlived the retry budget: reported as a processing failure, with the
            // budget in the reason so the durable record explains why this command has no outcome.
            ProcessingError::Unexpected(_) => (
                RejectionType::ProcessingError,
                format!(
                    "dispatch failed on all {MAX_COMMAND_ATTEMPTS} attempts: {}",
                    err.rejection_reason()
                ),
            ),
        };
        let storage_guard = handles.storage.lock().await;
        let work = WorkingState::new((**storage_guard).begin_txn()?);
        drop(storage_guard);
        let mut out = Collector::new(
            entry_id,
            None,
            Arc::clone(&self.clock),
            Arc::clone(&self.ids),
        );
        out.reject(
            command.request_id().unwrap_or_else(RequestId::nil),
            rejection_type,
            reason,
        );
        Ok(CommandProcessed {
            entries: out.into_parts(),
            work,
        })
    }

    pub(crate) async fn apply_event(
        &mut self,
        txn: &mut dyn StorageTxn,
        timestamp: Timestamp,
        cause_id: Option<EntryId>,
        event: &Event,
        _handles: &ProcessingHandles,
    ) -> Result<Option<i64>, ExecutionError> {
        // Recovery fold, on read-back. In the eager-apply model this Event's batch was already folded —
        // and the resume watermark advanced past it — by the work txn at the moment its producing
        // Command was appended, before the read-back loop reaches it here. The driver gates on
        // `is_already_applied` and only calls this for the residual **crash window** (a batch durably
        // appended but not yet folded before the crash — see
        // `docs/durable-execution-recovery-design.md`), so the fold below is the idempotent replay path.
        // The driver supplies the already-open `txn`; we fold into it and return the watermark to commit.
        {
            // Fold the event's projection. The recovery fold must not re-arm/cancel physical timers —
            // a replayed fold derives no scheduler side effect, only the projection rows.
            // TODO(timer-recovery): restore pending timers from Storage during recovery so the fold's
            // durable `TimerActivated`/`TimerCancelled` can re-drive the physical schedule via the Hook.
            let mut ctx = ApplierContext {
                storage: &mut *txn,
                // The entry's own frozen timestamp is the deterministic source for projection
                // `created_at`/`updated_at`; see `ApplierContext::timestamp`.
                timestamp,
            };
            dispatch_event(&mut ctx, event).await?;
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
        // deferred acknowledgement correlated to it — the recovery counterpart to the production
        // drain; there are no in-flight acks from a prior run, so this is normally empty.
        self.last_applied = vec![event.clone()];
        Ok(advanced)
    }

    pub(crate) fn is_already_applied(&self, entry_id: EntryId) -> bool {
        // Everything at or below the watermark was folded eagerly at production (the work txn), so a
        // read-back Event at that position must be skipped rather than folded twice — only crash-residue
        // Events above it fall through to the recovery `apply_event`.
        entry_id.get() <= self.watermark
    }

    pub(crate) async fn after_commit(&mut self, handles: &ProcessingHandles) {
        // Report the Events the driver just committed as `Hook` facts. In the driver-owned-txn model
        // the fold (which populated `last_applied`) and the leader-visible commit are separate, so
        // this is the driver's point to fire each durable event exactly once the commit lands — the
        // post-commit boundary an observer (e.g. a concrete AckHook) treats as ground truth.
        let applied = core::mem::take(&mut self.last_applied);
        for ev in applied {
            handles.hook.on_event_applied(&ev).await;
        }
    }
}
