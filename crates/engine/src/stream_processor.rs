use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

use crate::TaskStatus;
use crate::handler::CommandHandler;
use crate::handlers::{
    ActivateStateHandler, ActivateTaskHandler, CancelTaskHandler, CancelTimerHandler,
    ClaimTasksHandler, CompleteExecutionHandler, CompleteStateHandler, CompleteTaskHandler,
    CompleteThreadHandler, ContinueCompleteHandler, ContinueTerminateHandler,
    CreateExecutionHandler, CreateFlowHandler, FailTaskHandler, ReleaseTaskLeaseHandler,
    SpawnThreadHandler, TerminateExecutionHandler, TerminateStateHandler, TerminateThreadHandler,
    TriggerTimerHandler,
};
use crate::hook::Hook;
use crate::leader::Leader;
use crate::log::{Entry, EntryPayload, LogStream, Timestamp};
use crate::processing::{ProcessingHandles, ProcessingStateMachine, Role};
use crate::storage::{Storage, StorageTxn};
use crate::types::command::Command;
use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::id::{EntryId, StreamId};
use crate::types::reject::Reject;

/// Registers one or more [`CommandHandler`]s into a `Discriminant<Command>` dispatch map.
///
/// Each handler knows which [`Command`] variant it serves via [`CommandHandler::command`], which
/// returns that variant as a `Default` placeholder (used only to read its discriminant — real
/// commands are built by the state handlers). The handler type is therefore the single source of
/// truth for its own key; there is no hand-written placeholder to keep in sync. `$handler` is
/// captured as a `path` so it can serve both as a type (`<… as CommandHandler>`) and as a
/// `Default`-constructible value (`<$handler>::default()`).
///
/// Recursive: `command_handler_entry!` handles the first handler and recurses into the
/// `command_handler_entry!`-rest form; the tail emits nothing. The map key type is fixed by the
/// map's declared type, so the macro doesn't need to name `Command`.
macro_rules! command_handler_entry {
    // A single entry: derive the key from the handler's own `command()`, then recurse on the rest.
    ($map:expr, $handler:path $(, $rest:path)*) => {{
        let sample = <$handler as CommandHandler>::command(&<$handler>::default());
        $map.insert(std::mem::discriminant(&sample), Box::new(<$handler>::default()));
        command_handler_entry!($map $(, $rest)*);
    }};
    ($map:expr) => {};
}

/// The **driver** of a single execution log: tails a [`LogStream`], routes each [`Entry`] by its
/// [`EntryPayload`], appends produced entries back to the log, and emits the execution trace — but
/// branches on the installed [`ProcessingStateMachine`] role (Zeebe's `ProcessingStateMachine` /
/// `LeaderStateMachine` split) into a **leader** loop (write path: dispatch Commands, eager-apply
/// them, deliver acks, skip its own read-back) or a **follower** loop (replicate-only: fold each
/// batch into one held transaction, commit it atomically at its Noop).
///
/// The driver owns **every** transaction: it opens a [`StorageTxn`](crate::storage::StorageTxn) for a
/// produced batch (leader) or an entire replicated batch (follower — opened at the first Event, held
/// across the siblings, committed at the Noop), hands it to the role to fold into, and commits the
/// returned watermark — only the driver, as the `Box` owner, may call
/// [`StorageTxn::commit`](crate::storage::StorageTxn::commit). A `Follower` plugs into the same box,
/// which is what a distributed deployment needs for failover. All the shared plumbing — reading /
/// resuming the tail, appending, routing by variant, tracing — stays here, role-independent.
///
/// Named *stream* processor (after the stream/ledger-processing role in CCES) to disambiguate it from
/// future fan-out/worker types and from per-state [`CommandHandler`]s.
pub struct StreamProcessor {
    /// The installed processing role (a [`Leader`] in M1; `TODO(multi-node)` a `Follower`). The
    /// driver branches on [`ProcessingStateMachine::role`] to pick its loop (`run_leader` /
    /// `run_follower`).
    state_machine: Box<dyn ProcessingStateMachine>,
}

impl StreamProcessor {
    /// Build a fresh StreamProcessor with the empty per-variant handler table, installed as a
    /// [`Leader`] — the single-node default role.
    pub fn new() -> Self {
        let mut handlers: HashMap<
            std::mem::Discriminant<Command>,
            Box<dyn crate::handler::CommandHandler + Send + Sync>,
        > = HashMap::new();
        command_handler_entry!(
            handlers,
            CreateFlowHandler,
            CreateExecutionHandler,
            CompleteExecutionHandler,
            CompleteThreadHandler,
            ContinueCompleteHandler,
            ContinueTerminateHandler,
            TerminateExecutionHandler,
            ActivateStateHandler,
            CompleteStateHandler,
            TerminateStateHandler,
            TriggerTimerHandler,
            CancelTimerHandler,
            ActivateTaskHandler,
            ClaimTasksHandler,
            CompleteTaskHandler,
            FailTaskHandler,
            ReleaseTaskLeaseHandler,
            CancelTaskHandler,
            SpawnThreadHandler,
            TerminateThreadHandler
        );
        StreamProcessor {
            state_machine: Box::new(Leader::new(handlers)),
        }
    }

    /// Runs the StreamProcessor as a **service**: tails the stream indefinitely, forwarding Commands
    /// and Events to the installed processing role. Does NOT exit when a single execution completes —
    /// multiple executions share the stream and are all processed. Returns `Ok(())` when the log is
    /// closed (shut down) or when the `cancel` token is triggered (controlled shutdown). For M1's
    /// synchronous `Engine::start`, a caller-tail loop drives the StreamProcessor and cancels it once
    /// the target execution reaches a terminal state.
    ///
    /// Real-time scheduling: armed timers are *facts* (`TimerActivated`), and the actual wall-clock
    /// delay is a side effect **derived downstream** from that durable fact via the injected
    /// [`Hook`](crate::Hook) observer — this loop does not know a scheduler. After a batch's commit,
    /// `after_commit` reports each applied event (including `TimerActivated`, which carries the
    /// timer's absolute `deadline`); a consumer re-derives the arm/cancel on its scheduler from those
    /// facts. An expired timer re-enters this loop as an ordinary inbound `TriggerTimer` command
    /// appended by the consumer's sink — so the engine never pulls fired timers and the log keeps a
    /// single writer.
    ///
    /// **Resume watermark (single-node model):** `Storage` is a disposable projection rebuilt by
    /// replaying Events (idempotent fold) — Commands are **never re-run** here. To make "which
    /// Commands already ran" decidable on restart, this loop reads a single scalar
    /// `last_processed_position` = the last Command whose causal batch has been folded into Storage,
    /// installs it on the role, and resumes the tail from `position + 1`. Recovery is a **bounded
    /// startup pass** — [`recover_leader`](Self::recover_leader) first folds any crash-residual batches
    /// durably appended above the watermark, so after it the watermark sits on the last durable Noop and
    /// the normal leader loop never folds Events (see the design doc below). The role advances the
    /// watermark at apply time (see [`ProcessingStateMachine::set_resume_position`] / the leader's fold);
    /// see `docs/durable-execution-recovery-design.md`.
    ///
    /// TODO(recovery+design, multi-node): this single scalar is leader-private today — a follower has
    /// no dispatch history, so making "which Commands ran" decidable from the log alone during
    /// failover is still open design work. The `Box<dyn ProcessingStateMachine>` seam is the entry
    /// point for that work; not in this single-node milestone.
    ///
    /// The `cancel` token is this loop's **controlled-shutdown** signal: when it is triggered the loop
    /// drains the current iteration and returns `Ok(())`, rather than being hard-killed from outside.
    #[instrument(skip_all)]
    pub(crate) async fn run<L: LogStream<EntryPayload>>(
        &mut self,
        logstream: &L,
        storage: Arc<Mutex<Box<dyn Storage>>>,
        hook: Arc<dyn Hook>,
        cancel: CancellationToken,
    ) -> Result<(), ExecutionError> {
        let handles = ProcessingHandles { storage, hook };
        tracing::debug!(
            role = ?self.state_machine.role(),
            "processing role installed"
        );

        // Load the durable resume watermark — the last position fully applied to Storage (0 = fresh
        // store, nothing processed). Resuming from `W + 1` (== 1 on a fresh store) reads the log from
        // exactly where work is not yet applied, instead of re-deriving it from the head. Acquire
        // Storage briefly here (a single point read) and release it before the loops' per-fold
        // acquisitions.
        let watermark = handles
            .storage
            .lock()
            .await
            .last_processed_position()
            .await?;
        debug!(watermark, "resuming log stream");
        self.state_machine.set_resume_position(watermark);

        // Two per-role loops (`run_leader` / `run_follower`): the driver (not the role) opens and
        // commits every transaction, because it alone owns the `Box<dyn StorageTxn>` and thus the
        // only permitted `commit` (see `Storage::begin_txn`).
        match self.state_machine.role() {
            Role::Leader => {
                // Single-node recovery: fold any crash-residual batches (durably appended but not
                // yet applied before the crash) above the watermark, **before** tailing. This is the
                // only place the leader folds Events outside production; after it, every Event
                // `run_leader` reads back is at/below the advanced watermark, so the loop never folds
                // Events (its Event arm is a pure skip). Resuming from `w + 1` (the last durable
                // Noop) picks up exactly where recovery left off.
                let w = self.recover_leader(logstream, &handles, watermark).await?;
                self.state_machine.set_resume_position(w);
                let mut stream = logstream.stream_read(EntryId::new(w + 1));
                self.run_leader(&handles, &mut stream, logstream, cancel)
                    .await
            }
            Role::Follower => {
                // A follower never produces, so it has no residues to recover — it just starts
                // tailing from the watermark and folds every replicated batch on read-back.
                let mut stream = logstream.stream_read(EntryId::new(watermark + 1));
                self.run_follower(&handles, &mut stream, logstream, cancel)
                    .await
            }
        }
    }

    /// Recover a single-node leader after a crash: fold any **crash-residual** batches — durably
    /// appended before the crash but whose eager-apply never committed — above `watermark`, one
    /// driver-owned transaction per batch, committed atomically at the batch's Noop.
    ///
    /// Runs **before** `run_leader` tails the live log, and does what the old read-back Event fold
    /// used to do, but as a bounded startup pass. After it, the watermark sits on the last durable
    /// Noop and every Entry `run_leader` reads back is at/below it, so the loop never folds Events.
    ///
    /// Bounding is position-based: `LogStream::read` returns `None` past the durable tail, which is
    /// the hand-off point. Because appends are atomic (a batch and its terminating Noop land
    /// together), the residue is whole batches only — every Command is followed by its batch (b), so
    /// there is never a lone Command or a tail-mid-batch to wait on here: `read` at the tail simply
    /// returns `None`.
    ///
    /// Commands are **skipped**, never re-dispatched: a residual Command's batch is already durable
    /// and folded here, so dispatching it again would duplicate the append. Follower leaves acks
    /// undelivered (c): the producing client may have left this node, so no `after_commit` — and on a
    /// fresh start the leader's deferred-ack queue is empty anyway.
    async fn recover_leader<L: LogStream<EntryPayload>>(
        &mut self,
        logstream: &L,
        handles: &ProcessingHandles,
        watermark: i64,
    ) -> Result<i64, ExecutionError> {
        let mut pos = watermark + 1;
        // The driver-owned transaction for the current residual batch, opened at its first Event and
        // held until its Noop (owned — `begin_txn` no longer borrows the store, so the guard drops
        // immediately); `None` between batches.
        let mut inflight: Option<Box<dyn StorageTxn>> = None;
        loop {
            let entry = logstream.read(EntryId::new(pos)).await?;
            let Some(entry) = entry else {
                break; // durable tail reached — recovery done
            };
            match entry.payload {
                EntryPayload::Command(_) => {
                    // A residual Command: its batch is durable and folded below by this pass, so skip
                    // it — re-dispatching would duplicate the append.
                    debug!(entry_id = %entry.entry_id, "recover: skip residual command");
                }
                EntryPayload::Event(event) => {
                    log_event(&event);
                    // First Event of a residual batch: open one owned transaction and hold it across
                    // the batch.
                    if inflight.is_none() {
                        let storage_guard = handles.storage.lock().await;
                        inflight = Some(storage_guard.begin_txn()?);
                        drop(storage_guard); // txn is owned; no borrow left on the store
                    }
                    // Fold into the held txn. The leader impl returns `Some(advanced)` (commit-now
                    // semantics for its old read-back use); we ignore it and commit the whole batch
                    // at its Noop instead, keeping the watermark on a batch boundary.
                    self.state_machine
                        .apply_event(
                            inflight.as_deref_mut().expect("opened at first event"),
                            entry.entry_id,
                            entry.timestamp,
                            entry.cause_id,
                            &event,
                            handles,
                        )
                        .await?;
                }
                EntryPayload::Noop => {
                    // Close the batch: commit the now-whole transaction at the Noop's watermark.
                    let Some(txn) = inflight.take() else {
                        // Defensive: a lone Noop with no preceding Event can't occur given the atomic
                        // batch invariant; if it somehow does, nothing to commit, just advance.
                        debug!(entry_id = %entry.entry_id, "recover: noop without in-flight batch");
                        pos += 1;
                        continue;
                    };
                    txn.commit(Some(entry.entry_id.get()))?;
                    debug!(entry_id = %entry.entry_id, "recover: committed residual batch at noop");
                }
                EntryPayload::Reject(reject) => {
                    // Audit the durable refusal; a restart leaves no awaiting caller and the client
                    // may be gone, so just trace it like a follower.
                    log_reject(&reject);
                }
            }
            pos += 1;
        }
        // Re-read the watermark from Storage — the last residue Noop's position we committed; that's
        // where `run_leader` resumes tailing.
        let w = handles
            .storage
            .lock()
            .await
            .last_processed_position()
            .await?;
        debug!(watermark = w, "leader recovered");
        Ok(w)
    }

    /// The **leader** driver loop: write path + own read-back. Tails the log and for each entry:
    ///
    /// - **Command** — dispatch it; if it produced a non-empty batch, append `batch ++ [Noop]`
    ///   atomically, then commit the work transaction the dispatch already eager-folded into (with
    ///   the batch-end watermark), replay its deferred timer side effects, and (post-commit) drain the
    ///   correlated acks. Grants and rejections are answered right after the append.
    /// - **Event** — always skipped: `recover_leader` folded any crash residue at startup, and the
    ///   work transaction eagerly applied this batch, so the Event is at/below the watermark.
    ///   The leader never folds Events in its live loop (`is_already_applied` is only a debug guard).
    /// - **Noop / Reject** — skipped: the batch was applied at production, and a rejection's ack was
    ///   already delivered in the Command arm that produced it.
    ///
    /// Recovery is a separate bounded pass (`recover_leader`) that runs before this loop — see `run`.
    ///
    /// The transaction is opened *after* dispatch (never before): dispatch takes the Storage lock to
    /// read projection state, so holding it across dispatch would self-deadlock.
    #[instrument(skip_all)]
    async fn run_leader<L: LogStream<EntryPayload>>(
        &mut self,
        handles: &ProcessingHandles,
        stream: &mut Pin<Box<dyn Stream<Item = Entry> + Send + 'static>>,
        logstream: &L,
        cancel: CancellationToken,
    ) -> Result<(), ExecutionError> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    // Controlled shutdown requested: stop tailing and return (the caller owns any
                    // further teardown). Nothing mid-batch is dropped here — `select!` yields only
                    // between iterations.
                    debug!("leader loop cancelled");
                    return Ok(());
                }
                entry = stream.next() => {
                    let Some(entry) = entry else {
                        return Ok(());
                    };
                    match entry {
                        Entry {
                            entry_id,
                            payload: EntryPayload::Command(command),
                            ..
                        } => {
                            debug!(entry_id = %entry_id, command = ?command, "dispatching command");
                            let produced = self
                                .state_machine
                                .process_command(entry_id, &command, handles)
                                .await?;
                            debug!(entries = produced.entries.len(), "command produced entries");

                            // Append the produced batch atomically, terminated by a `Noop` commit
                            // marker (one atomic append = one causal batch). Empty batches (a pure
                            // query / grant-only command that produced nothing) append nothing: there
                            // is nothing to fold, so no terminator is needed and re-running them is a
                            // harmless no-op.
                            if !produced.entries.is_empty() {
                                let mut to_append: Vec<Entry> = produced.entries.clone();
                                to_append.push(noop(StreamId::nil(), entry_id));
                                // Append the batch atomically; on failure the `?` propagates and
                                // `produced.work` is dropped, rolling the working txn back — nothing
                                // durable was written, so there is no dirty data.
                                let last = logstream.append(to_append).await?;
                                // Commit the eager-folded working txn now that its append is durable,
                                // advancing the resume watermark to the batch-end position (the Noop).
                                // This is the single authoritative fold of the batch — no post-append
                                // `apply_batch` re-fold exists on the live path.
                                produced.work.commit(Some(last.get())).await?;
                                self.state_machine.set_resume_position(last.get());
                                // Post-commit: drain the deferred acks whose Event is now durable.
                                self.state_machine.after_commit(handles).await;
                            }

                            // Report durable rejections to the injected Hook. Rejections are decided at
                            // dispatch and answered even when the pull produced **no events** — with no
                            // event to carry them, they must be reported directly or the awaiting caller
                            // would hang. (Events of a produced batch were already reported by
                            // `after_commit` above, post-commit; a task grant rides its durable
                            // `TasksClaimed` and needs no separate report.)
                            for rej in produced.entries.iter().filter_map(|e| match &e.payload {
                                EntryPayload::Reject(r) => Some(r),
                                _ => None,
                            }) {
                                log_reject(rej);
                                handles
                                    .hook
                                    .on_command_rejected(rej.request_id, rej)
                                    .await;
                            }
                        }
                        Entry {
                            entry_id,
                            payload: EntryPayload::Event(event),
                            ..
                        } => {
                            log_event(&event);
                            // After `recover_leader` (startup) and the Command arm's eager-apply
                            // (production), every Event read back here is at or below the watermark —
                            // the leader never folds Events in its live loop. Assert the invariant:
                            // `is_already_applied` only guards against a logic regression; folding is
                            // explicitly out of scope for the read-back arm.
                            debug_assert!(
                                self.state_machine.is_already_applied(entry_id),
                                "leader read back an unapplied Event after recovery; not folding"
                            );
                            continue;
                        }
                        Entry {
                            entry_id,
                            payload: EntryPayload::Noop,
                            ..
                        } => {
                            // Skip: the batch this Noop terminates was already eager-applied (and its
                            // watermark committed) in the Command arm that produced it.
                            debug!(entry_id = %entry_id, "leader skip batch commit mark (noop)");
                        }
                        Entry {
                            entry_id,
                            payload: EntryPayload::Reject(_),
                            ..
                        } => {
                            // Skip: the rejection's ack (and the `log_reject` audit trace) was already
                            // delivered in the Command arm that produced this record.
                            debug!(entry_id = %entry_id, "leader skip rejection (ack sent at production)");
                        }
                    }
                }
            }
        }
    }

    /// The **follower** driver loop: replicate-only. Tails the log and for each entry:
    ///
    /// - **Command** — skipped: a follower never dispatches (it is read-only on the log).
    /// - **Event** — folded into the **one held driver-owned transaction** opened at this batch's
    ///   first Event (`apply_event`); never committed mid-batch.
    /// - **Noop** — closes the in-flight batch: `commit_at_noop` rounds it off, then the still-open
    ///   transaction is committed atomically (the Noop's position as the watermark) and released.
    /// - **Reject** — audited by the `log_reject` trace; a follower has no awaiting caller.
    ///
    /// The transaction is held across the batch's sibling Events, not opened per entry — `begin_txn`
    /// no longer borrows the store, so the txn outlives the `MutexGuard` and survives between entries.
    /// Nothing commits until a Noop closes a batch, so a crash mid-batch leaves *none* of the partial
    /// siblings applied (idempotent re-fold + one commit on restart).
    #[instrument(skip_all)]
    async fn run_follower<L: LogStream<EntryPayload>>(
        &mut self,
        handles: &ProcessingHandles,
        stream: &mut Pin<Box<dyn Stream<Item = Entry> + Send + 'static>>,
        _logstream: &L,
        cancel: CancellationToken,
    ) -> Result<(), ExecutionError> {
        // The driver-owned transaction for the **in-flight batch**, opened at its first Event and held
        // across the remaining siblings until the Noop. `Box<dyn StorageTxn>` is `Send`, so the held
        // txn can live across `.await` points in this loop.
        let mut inflight: Option<Box<dyn StorageTxn>> = None;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    // Controlled shutdown: stop tailing and return; nothing mid-batch is committed (the
                    // uncommitted txn is dropped un-committed and re-folded on restart).
                    debug!("follower loop cancelled");
                    return Ok(());
                }
                entry = stream.next() => {
                    let Some(entry) = entry else {
                        return Ok(());
                    };
                    match entry {
                        Entry {
                            entry_id,
                            payload: EntryPayload::Command(_),
                            ..
                        } => {
                            // A follower neither dispatches nor appends: every Command read back is
                            // the leader's decision to replay, reflected only through the Events that
                            // follow in the same batch (which we fold).
                            debug!(entry_id = %entry_id, "follower skip command");
                        }
                        Entry {
                            entry_id,
                            timestamp,
                            cause_id,
                            payload: EntryPayload::Event(event),
                            ..
                        } => {
                            log_event(&event);
                            // First Event of a batch: open the one held transaction and immediately
                            // drop the guard — the txn is owned (see `Storage::begin_txn`), so it no
                            // longer borrows the store and survives until the Noop.
                            if inflight.is_none() {
                                let storage_guard = handles.storage.lock().await;
                                inflight = Some(storage_guard.begin_txn()?);
                                drop(storage_guard);
                            }
                            // Fold this sibling into the held transaction; the follower returns `None`
                            // so nothing is committed until the batch's Noop rounds it off.
                            self.state_machine
                                .apply_event(
                                    inflight.as_deref_mut().expect("opened at first event"),
                                    entry_id,
                                    timestamp,
                                    cause_id,
                                    &event,
                                    handles,
                                )
                                .await?;
                        }
                        Entry {
                            entry_id,
                            payload: EntryPayload::Noop,
                            ..
                        } => {
                            // The batch is whole. Round it off and commit the still-open, now-whole
                            // transaction atomically, then release it. A Noop with no in-flight
                            // siblings (nothing to fold) is defensive-skipped.
                            let Some(mut txn) = inflight.take() else {
                                debug!(entry_id = %entry_id, "follower noop without in-flight batch; skip");
                                continue;
                            };
                            let w = self
                                .state_machine
                                .commit_at_noop(&mut *txn, entry_id, handles)
                                .await?;
                            txn.commit(w)?;
                            debug!(entry_id = %entry_id, "follower committed batch at noop");
                        }
                        Entry {
                            entry_id,
                            payload: EntryPayload::Reject(reject),
                            ..
                        } => {
                            // Audit the refusal (durable on the log); a follower has no awaiter to
                            // wake and folds no projection for a rejection.
                            log_reject(&reject);
                            debug!(entry_id = %entry_id, "follower recorded rejection");
                        }
                    }
                }
            }
        }
    }

    /// Routes a [`Command`] to its handler, producing the [`Entry`]s it emitted (single-shot,
    /// dispatch-only — no append, no timer scheduling). This is the **leader** role's capability (a
    /// follower produces no entries), so it is reached by downcasting the installed role; M1's only
    /// role is [`Leader`], so the cast always succeeds.
    ///
    /// Usable outside `run` (tests): it drives the handler exactly as the service loop would, but
    /// returns the entries instead of appending them.
    pub async fn dispatch<S: Storage>(
        &mut self,
        command: &Command,
        storage: &S,
        cause_id: EntryId,
    ) -> Result<Vec<Entry>, ExecutionError> {
        // `Any` downcast through the role seam: `dispatch` is leader-only, so reach the concrete
        // `Leader` behind the boxed trait object.
        let any: &mut dyn std::any::Any = &mut *self.state_machine;
        let leader = any
            .downcast_mut::<Leader>()
            .expect("StreamProcessor::dispatch requires the leader role (M1's only role)");
        leader.dispatch(command, storage, cause_id).await
    }
}

impl Default for StreamProcessor {
    fn default() -> Self {
        Self::new()
    }
}

/// Build a [`Noop`](EntryPayload::Noop) batch-commit marker. `cause_id` is the producing Command's
/// position, giving the batch a stable identity (see [`EntryPayload::Noop`]). `stream_id`/`entry_id`
/// are placeholders the log stamps on append, unless the caller materializes them first.
fn noop(stream_id: StreamId, cause_id: EntryId) -> Entry {
    Entry {
        stream_id,
        entry_id: EntryId::nil(),
        cause_id: Some(cause_id),
        timestamp: Timestamp::now(),
        payload: EntryPayload::Noop,
    }
}

/// Emits a human-friendly tracing line for each [`Reject`] as the run loop reads it — a refused
/// command, concurrently with its durable record on the log. Logged at `warn`: a rejection is
/// always noteworthy (a client command that could not be honored), but it is a normal control-flow
/// outcome rather than an engine fault.
pub(crate) fn log_reject(reject: &Reject) {
    warn!(
        request_id = %reject.request_id,
        rejection_type = %reject.rejection_type,
        rejection_reason = %reject.rejection_reason,
        "command rejected"
    );
}

/// Emits a human-friendly tracing line for each [`Event`] as it is applied to Storage — the
/// detailed execution trace. Milestones (execution started / terminal) are `info`; per-phase
/// (activating/activated/completing/completed) and timers are `debug`; failures are `warn`.
pub(crate) fn log_event(event: &Event) {
    match event {
        Event::FlowCreated { flow, .. } => {
            info!(
                name = %flow.meta.name,
                "flow created (brand-new name entered the system)"
            );
        }
        Event::FlowVersionCreated { flow_version, .. } => {
            info!(
                flow_version = %flow_version.reference(),
                name = ?flow_version.flow_name(),
                version = flow_version.version,
                "flow version created"
            );
        }
        Event::ExecutionCreated { execution, .. } => {
            info!(
                execution = %execution.reference(),
                input = %execution.input,
                "execution created"
            );
        }
        Event::ExecutionCompleting { execution } => {
            debug!(
                execution = %execution.reference(),
                output = %execution.output.as_ref().unwrap_or(&serde_json::Value::Null),
                "execution completing"
            );
        }
        Event::ExecutionCompleted { execution } => {
            info!(
                execution = %execution.reference(),
                output = %execution.output.as_ref().unwrap_or(&serde_json::Value::Null),
                "execution completed"
            );
        }
        Event::ExecutionTerminating { execution } => {
            warn!(
                execution = %execution.reference(),
                reason = %format!("{:?}", execution.status),
                "execution terminating"
            );
        }
        Event::ExecutionTerminated { execution } => {
            warn!(
                execution = %execution.reference(),
                reason = %format!("{:?}", execution.status),
                "execution terminated"
            );
        }
        Event::ThreadCreated { thread, .. } => {
            info!(
                thread = %thread.reference(),
                root = %thread.execution,
                "thread created (fan-out child)"
            );
        }
        Event::ThreadCompleting { thread } => {
            debug!(thread = %thread.reference(), output = %thread.output.as_ref().unwrap_or(&serde_json::Value::Null), "thread completing");
        }
        Event::ThreadCompleted { thread } => {
            info!(thread = %thread.reference(), output = %thread.output.as_ref().unwrap_or(&serde_json::Value::Null), "thread completed");
        }
        Event::ThreadTerminating { thread } => {
            warn!(thread = %thread.reference(), reason = %format!("{:?}", thread.status), "thread terminating");
        }
        Event::ThreadTerminated { thread } => {
            warn!(thread = %thread.reference(), reason = %format!("{:?}", thread.status), "thread terminated");
        }
        Event::StateActivating { activity } => {
            info!(
                activity = %activity.reference(),
                state = %crate::handlers::state_name_from_path(activity.state_path.as_ptr()),
                // `input` is pinned to Null on the entering event (the processed view lands only on
                // `StateActivated`), so log the raw input — the meaningful value at this moment.
                raw_input = %activity.raw_input,
                "entered state"
            );
        }
        Event::StateActivated { activity } => {
            debug!(activity = %activity.reference(), "state activated");
        }
        Event::StateCompleting { activity } => {
            debug!(activity = %activity.reference(), "state completing");
        }
        Event::StateCompleted { activity } => {
            debug!(
                activity = %activity.reference(),
                output = %activity.output.as_ref().unwrap_or(&serde_json::Value::Null),
                "state completed"
            );
        }
        Event::StateTerminating { activity } => {
            warn!(
                activity = %activity.reference(),
                reason = %format!("{:?}", activity.status),
                "state terminating"
            );
        }
        Event::StateTerminated { activity } => {
            warn!(
                activity = %activity.reference(),
                reason = %format!("{:?}", activity.status),
                "state terminated"
            );
        }
        Event::TimerActivated { timer } => {
            debug!(
                timer = %timer.reference(),
                parent = ?timer.meta.owner,
                purpose = %format!("{:?}", timer.purpose),
                deadline_ms = timer.deadline.as_millis(),
                "timer activated"
            );
        }
        Event::TimerTriggered { timer } => {
            debug!(
                timer = %timer.reference(),
                parent = ?timer.meta.owner,
                purpose = %format!("{:?}", timer.purpose),
                deadline_ms = timer.deadline.as_millis(),
                "timer completed"
            );
        }
        Event::TimerCancelled { timer } => {
            debug!(
                timer = %timer.reference(),
                parent = ?timer.meta.owner,
                purpose = %format!("{:?}", timer.purpose),
                deadline_ms = timer.deadline.as_millis(),
                "timer cancelled"
            );
        }
        Event::TaskActivated { task } => {
            debug!(
                task = %task.reference(),
                parent = ?task.meta.owner,
                resource = %task.resource,
                "task activated"
            );
        }
        Event::TasksClaimed {
            request_id: _,
            tasks,
        } => {
            debug!(
                count = tasks.len(),
                worker = ?tasks.first().and_then(|t| t.worker_id.as_deref()),
                lease_until = ?tasks.first().and_then(|t| t.lease_until),
                "tasks claimed to worker"
            );
        }
        Event::TaskLeaseExpired { task } => {
            debug!(task = %task.reference(), "task lease expired; re-queued");
        }
        Event::TaskCompleted {
            request_id: _,
            task,
            output,
        } => {
            // A settled task (status Completed); `output` feeds the owning activity's raw_output.
            debug!(task = %task.reference(), output = %output, "task completed");
        }
        Event::TaskFailed { task, error } => {
            // The task entity's `status` distinguishes a scheduled retry (`Pending` — the task
            // re-queues, claimable after `next_available_at`) from a terminal failure (`Failed`).
            if task.status == TaskStatus::Pending {
                warn!(
                    task = %task.reference(),
                    error = %format!("{error:?}"),
                    attempts = task.retry_state.attempts,
                    next_available_at = ?task.retry_state.next_available_at,
                    "task failed; retry scheduled"
                );
            } else {
                warn!(task = %task.reference(), error = %format!("{error:?}"), "task failed (terminal)");
            }
        }
        Event::TaskCancelled { task } => {
            debug!(task = %task.reference(), "task cancelled");
        }
        Event::VariablesAssigned { variables, .. } => {
            let keys: Vec<&String> = variables.keys().collect();
            debug!(keys = ?keys, "variables assigned");
        }
        Event::StateTransitioned { activity, next, .. } => {
            info!(activity = %activity, next = %next, "state routed to next");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex as StdMutex};

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use crate::engine::NoopHook;
    use crate::log::InMemoryLogStream;
    use crate::storage::{
        ActivityRecord, ExecutionRecord, StorageTxn, TaskRecord, ThreadRecord, TimerRecord,
    };
    use crate::types::command::Command;
    use crate::types::flow::Flow;
    use crate::types::flow_version::FlowVersion;
    use crate::types::id::{FlowName, RequestId, TimerId};
    use crate::types::meta::ObjectReference;

    use super::*;

    /// The durable state a [`FakeStore`] commit materializes: the resume watermark plus a count of
    /// projection writes folded into the batch. A write only lands when its transaction **commits**,
    /// so this is the observability point for "recovery folds a residue exactly once, at its Noop".
    #[derive(Default)]
    struct FakeState {
        committed_watermark: Option<i64>,
        committed_writes: usize,
    }

    /// A transaction-shaped write buffer whose writes are invisible until `commit` folds them into
    /// [`FakeState`] all together.
    struct FakeTxn {
        state: Arc<StdMutex<FakeState>>,
        pending_writes: usize,
    }

    #[async_trait]
    impl StorageTxn for FakeTxn {
        // The fold touches only the flow rows the test Event's applier needs; the rest is unused.
        async fn get_flow_by_name(&mut self, _n: FlowName) -> Result<Option<Flow>, ExecutionError> {
            Ok(None)
        }
        async fn put_flow(&mut self, _f: Flow) -> Result<(), ExecutionError> {
            self.pending_writes += 1;
            Ok(())
        }
        async fn put_flow_version(&mut self, _v: FlowVersion) -> Result<(), ExecutionError> {
            self.pending_writes += 1;
            Ok(())
        }
        fn commit(self: Box<Self>, watermark: Option<i64>) -> Result<(), ExecutionError> {
            let mut s = self.state.lock().unwrap();
            s.committed_watermark = watermark;
            s.committed_writes += self.pending_writes;
            Ok(())
        }

        async fn get_execution(
            &mut self,
            _reference: &ObjectReference,
        ) -> Result<Option<ExecutionRecord>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn get_thread(
            &mut self,
            _reference: &ObjectReference,
        ) -> Result<Option<ThreadRecord>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn get_activity(
            &mut self,
            _reference: &ObjectReference,
        ) -> Result<Option<ActivityRecord>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn get_timer(
            &mut self,
            _reference: &ObjectReference,
        ) -> Result<Option<TimerRecord>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn get_task(
            &mut self,
            _reference: &ObjectReference,
        ) -> Result<Option<TaskRecord>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn get_children(
            &mut self,
            _id: ObjectReference,
        ) -> Result<HashSet<ObjectReference>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn get_flow_version(
            &mut self,
            _id: &ObjectReference,
        ) -> Result<Option<FlowVersion>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn flow_version_of(
            &mut self,
            _name: FlowName,
            _v: u32,
        ) -> Result<Option<FlowVersion>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn activatable_tasks(
            &mut self,
            _r: &str,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn put_execution(&mut self, _e: ExecutionRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn put_thread(&mut self, _t: ThreadRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn put_activity(&mut self, _a: ActivityRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn put_timer(&mut self, _t: TimerRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn put_task(&mut self, _t: TaskRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn remove_child(
            &mut self,
            _p: ObjectReference,
            _c: ObjectReference,
        ) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn add_child(
            &mut self,
            _p: ObjectReference,
            _c: ObjectReference,
        ) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn next_generated_seq(&mut self) -> Result<i64, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn put_next_generated_seq(&mut self, _seq: i64) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
    }

    /// A minimal in-crate [`Storage`] whose fold commits are observable, matching the follower tests'
    /// fake (`spica-storage` implements a distinct copy of this crate's trait, so a real store can't
    /// satisfy the trait bound in an in-crate unit test).
    struct FakeStore(Arc<StdMutex<FakeState>>);

    #[async_trait]
    impl Storage for FakeStore {
        fn begin_txn(&self) -> Result<Box<dyn StorageTxn>, ExecutionError> {
            Ok(Box::new(FakeTxn {
                state: self.0.clone(),
                pending_writes: 0,
            }))
        }
        async fn last_processed_position(&self) -> Result<i64, ExecutionError> {
            Ok(self.0.lock().unwrap().committed_watermark.unwrap_or(0))
        }
        async fn put_last_processed_position(&mut self, p: i64) -> Result<(), ExecutionError> {
            self.0.lock().unwrap().committed_watermark = Some(p);
            Ok(())
        }
        async fn next_generated_seq(&self) -> Result<i64, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }

        async fn get_execution(
            &self,
            _reference: &ObjectReference,
        ) -> Result<Option<ExecutionRecord>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn get_thread(
            &self,
            _reference: &ObjectReference,
        ) -> Result<Option<ThreadRecord>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn get_activity(
            &self,
            _reference: &ObjectReference,
        ) -> Result<Option<ActivityRecord>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn get_timer(
            &self,
            _reference: &ObjectReference,
        ) -> Result<Option<TimerRecord>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn get_task(
            &self,
            _reference: &ObjectReference,
        ) -> Result<Option<TaskRecord>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn get_children(
            &self,
            _id: ObjectReference,
        ) -> Result<HashSet<ObjectReference>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn activatable_tasks(
            &self,
            _r: &str,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn put_execution(&mut self, _e: ExecutionRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn put_thread(&mut self, _t: ThreadRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn put_activity(&mut self, _a: ActivityRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn put_timer(&mut self, _t: TimerRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn put_task(&mut self, _t: TaskRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn remove_child(
            &mut self,
            _p: ObjectReference,
            _c: ObjectReference,
        ) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn add_child(
            &mut self,
            _p: ObjectReference,
            _c: ObjectReference,
        ) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn get_flow_by_name(&self, _n: FlowName) -> Result<Option<Flow>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn put_flow(&mut self, _f: Flow) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn get_flow_version(
            &self,
            _id: &ObjectReference,
        ) -> Result<Option<FlowVersion>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn put_flow_version(&mut self, _v: FlowVersion) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
        async fn flow_version_of(
            &self,
            _name: FlowName,
            _v: u32,
        ) -> Result<Option<FlowVersion>, ExecutionError> {
            unimplemented!("not exercised by the recovery test")
        }
    }

    fn handles(storage: Arc<Mutex<Box<dyn Storage>>>) -> ProcessingHandles {
        ProcessingHandles {
            storage,
            hook: Arc::new(NoopHook),
        }
    }

    /// A hook that records the events it observed, so a test can assert the engine reports the
    /// durable events it produces.
    #[derive(Default)]
    struct RecordingHook {
        applied: StdMutex<Vec<Event>>,
    }
    #[async_trait]
    impl Hook for RecordingHook {
        async fn on_event_applied(&self, event: &Event) {
            self.applied.lock().unwrap().push(event.clone());
        }
    }

    /// The leader reports each event it makes durable as an `on_event_applied` Hook fact
    /// (post-commit), which is what an injected `Hook` observer correlates to a waiting request.
    #[tokio::test]
    async fn leader_reports_applied_events_to_the_hook() {
        let mut sp = StreamProcessor::new();
        let state = Arc::new(StdMutex::new(FakeState::default()));
        let storage: Arc<Mutex<Box<dyn Storage>>> =
            Arc::new(Mutex::new(Box::new(FakeStore(state.clone()))));
        let hook = Arc::new(RecordingHook::default());
        let handles = ProcessingHandles {
            storage,
            hook: Arc::clone(&hook) as Arc<dyn Hook>,
        };
        let request_id = RequestId::new();
        let cmd = Command::CreateFlow {
            request_id,
            name: FlowName::new("flow").expect("literal name is valid"),
            definition: r#"{ "StartAt": "A", "States": { "A": { "Type": "Succeed" } } }"#
                .to_string(),
        };
        let produced = sp
            .state_machine
            .process_command(EntryId::new(1), &cmd, &handles)
            .await
            .unwrap();
        assert!(
            !produced.entries.is_empty(),
            "CreateFlow produced an emitted batch"
        );
        // The driver commits the working txn, then `after_commit` reports the batch's events.
        produced.work.commit(Some(1)).await.unwrap();
        sp.state_machine.after_commit(&handles).await;
        let applied = hook.applied.lock().unwrap();
        assert!(
            applied.iter().any(|ev| matches!(
                ev,
                Event::FlowVersionCreated {
                    request_id: r,
                    ..
                } if *r == request_id
            )),
            "expected the echoed FlowVersionCreated to be reported, got {applied:?}"
        );
    }

    /// A `FlowVersionCreated` event that folds cleanly into the fake store (its applier touches only
    /// `get_flow_by_name` / `put_flow_version` / `put_flow`), used as the crash-residue Event.
    fn flow_version_event() -> (Timestamp, Event) {
        (
            Timestamp::now(),
            Event::FlowVersionCreated {
                request_id: RequestId::new(),
                flow_version: FlowVersion {
                    meta: crate::types::meta::ObjectMeta::builder(
                        crate::types::meta::ObjectKind::FlowVersion,
                        ulid::Ulid::new(),
                    )
                    .name(FlowVersion::version_name(
                        &FlowName::new("flow").expect("literal name is valid"),
                        1,
                    ))
                    .at(Timestamp::now())
                    .build()
                    .with_owner(crate::types::meta::OwnerReference::new(
                        crate::types::meta::ObjectKind::Flow,
                        crate::types::meta::ObjectName::plain("flow")
                            .expect("literal name is valid"),
                        ulid::Ulid::nil(),
                    )),
                    version: 1,
                    definition: String::new(),
                    checksum: FlowVersion::definition_checksum(""),
                },
            },
        )
    }

    /// Recovery folds a durably-appended but never-applied batch (the crash residue) into the
    /// projection **once**, advances the watermark to the batch's Noop, and does not re-append
    /// anything. This is the bounded startup pass that lets `run_leader`'s Event arm stay a pure skip.
    #[tokio::test]
    async fn recover_leader_folds_crash_residue_once() {
        // A crash residue: a batch durably appended (Command + its Event + the terminating Noop)
        // whose eager-apply never committed before the crash — the O the storage still says 0.
        let log = InMemoryLogStream::<EntryPayload>::default();
        let (ts, ev) = flow_version_event();
        let residual_uid: ulid::Ulid = TimerId::new().into();
        let residual_timer = crate::types::meta::ObjectReference::new(
            crate::types::meta::ObjectKind::Timer,
            crate::types::meta::PlainName::new("child")
                .expect("static literal is a valid segment")
                .generated_from_key(residual_uid.0 as u64),
            residual_uid,
        );
        log.append(vec![
            // The residual Command: recovery must skip it — re-dispatching would re-append a duplicate
            // batch. `CancelTimer` needs only a TimerId, so it's the cheapest Command to fabricate.
            Entry {
                stream_id: StreamId::nil(),
                entry_id: EntryId::nil(),
                cause_id: None,
                timestamp: ts,
                payload: EntryPayload::Command(Command::CancelTimer {
                    timer: residual_timer,
                }),
            },
            Entry {
                stream_id: StreamId::nil(),
                entry_id: EntryId::nil(),
                cause_id: Some(EntryId::new(1)),
                timestamp: ts,
                payload: EntryPayload::Event(ev),
            },
            Entry {
                stream_id: StreamId::nil(),
                entry_id: EntryId::nil(),
                cause_id: Some(EntryId::new(1)),
                timestamp: ts,
                payload: EntryPayload::Noop,
            },
        ])
        .await
        .unwrap();
        assert_eq!(
            log.entries().len(),
            3,
            "residue batch lands at positions 1..=3"
        );

        let state = Arc::new(StdMutex::new(FakeState::default()));
        let storage: Arc<Mutex<Box<dyn Storage>>> =
            Arc::new(Mutex::new(Box::new(FakeStore(state.clone()))));
        let h = handles(storage);

        let mut sp = StreamProcessor::new();
        // Watermark 0 = the store has processed nothing; the residue at 1..=3 is all above it.
        let w = sp.recover_leader(&log, &h, 0).await.unwrap();

        // Folded exactly once (one projection write committed), watermark on the batch's Noop.
        assert_eq!(w, 3, "recovery resumes from the last durable Noop");
        {
            // Scoped so the std guard drops before the next await (`recover_leader` idempotence below).
            let st = state.lock().unwrap();
            assert_eq!(st.committed_watermark, Some(3));
            // The one residue Event folds its two rows (`put_flow` index + `put_flow_version`), all in
            // the single commit at the Noop — proof the whole batch landed atomically, exactly once.
            assert_eq!(st.committed_writes, 2, "residue Event folded exactly once");
        }
        // The log's length must be unchanged: recovery folds residues in place, never re-appends.
        assert_eq!(
            log.entries().len(),
            3,
            "no duplicate append during recovery"
        );

        // The residue is now at/below the watermark, so a subsequent `recover_leader` with the
        // advanced watermark sees nothing above it to fold — a second pass is idempotent.
        let w2 = sp.recover_leader(&log, &h, w).await.unwrap();
        assert_eq!(w2, 3, "idempotent across restarts");
        assert_eq!(state.lock().unwrap().committed_writes, 2, "no double fold");
    }
}
