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

use spica_asl::StateMachine;
use tracing::debug;

use crate::applier::{ApplierContext, dispatch_event};
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext, OverlaySink};
use crate::handlers::state_handler::StateHandlerRegistry;
use crate::handlers::{build_state_handlers, dispatch_command};
use crate::log::{Entry, EntryPayload, Timestamp};
use crate::processing::{CommandProcessed, ProcessingHandles};
use crate::storage::{Storage, StorageTxn};
use crate::types::command::Command;
use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::id::EntryId;
use crate::types::meta::ObjectReference;
use crate::working::WorkingState;

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
}

impl Leader {
    /// Build the leader. The machinery cache, eval environment, and state-handler registry start
    /// empty/fresh; the resume watermark is installed later, when the driver reads it from [`Storage`]
    /// at boot (`set_resume_position`).
    pub(crate) fn new() -> Self {
        Self {
            definitions: HashMap::new(),
            env: Arc::new(tokio::sync::Mutex::new(EvalEnv::new())),
            state_handlers: build_state_handlers(),
            watermark: 0,
            last_applied: Vec::new(),
        }
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
        let mut out = Collector::new(cause_id, Some(overlay));
        let mut env_g = self.env.lock().await;
        let mut ctx = HandlerContext {
            env: &mut env_g,
            storage: &work,
            definitions: &mut self.definitions,
            state_handlers: &self.state_handlers,
        };
        dispatch_command(command, &mut ctx, &mut out).await;
        Ok(out.into_entries())
    }
}

impl Leader {
    /// Install the resume position read from [`Storage`] at boot — the last position whose effects
    /// are durable, so the driver resumes the log tail from `position + 1`. The leader advances it at
    /// fold time (a follower advances it from each Noop instead).
    pub(crate) fn set_resume_position(&mut self, position: i64) {
        self.watermark = position;
    }

    pub(crate) async fn process_command(
        &mut self,
        entry_id: EntryId,
        command: &Command,
        handles: &ProcessingHandles,
    ) -> Result<CommandProcessed, ExecutionError> {
        let (entries, work) = {
            // Acquire Storage only for the duration of this command's dispatch (per-entry, not for
            // the whole run), so the Engine's other threads can read projection state — e.g.
            // `Engine::start_for` resolving the latest revision — while the driver sits between
            // entries.
            //
            // The working txn is opened inside the Storage lock and returned to the driver *outside*
            // it: `WorkingState` owns the txn (wrapped in its own mutex), so committing after the
            // durable append does not need the Storage lock.
            let storage_guard = handles.storage.lock().await;
            let mut env_g = self.env.lock().await;
            let work = WorkingState::new((**storage_guard).begin_txn()?);
            let overlay = OverlaySink::new(&work);
            let mut out = Collector::new(entry_id, Some(overlay));
            let mut ctx = HandlerContext {
                env: &mut env_g,
                storage: &work,
                definitions: &mut self.definitions,
                state_handlers: &self.state_handlers,
            };
            dispatch_command(command, &mut ctx, &mut out).await;
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
            (entries, work)
        };
        Ok(CommandProcessed { entries, work })
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
