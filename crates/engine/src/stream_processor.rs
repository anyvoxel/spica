use std::collections::HashMap;
use std::mem::discriminant;
use std::sync::Arc;

use spica_asl::StateMachine;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

use crate::applier::{ApplierContext, EventDispatcher};
use crate::command::Command;
use crate::engine::AckRouter;
use crate::error::ExecutionError;
use crate::eval_env::EvalEnv;
use crate::event::Event;
use crate::handler::{AckSideEffect, Collector, CommandHandler, HandlerContext};
use crate::handlers::{
    ActivateStateHandler, ActivateTaskHandler, ActivateTimerHandler, AssignTaskHandler,
    CancelTaskHandler, CancelTimerHandler, CompleteExecutionHandler, CompleteStateHandler,
    CompleteTaskHandler, CreateExecutionHandler, CreateFlowHandler, FailTaskHandler,
    ProcessChildCompletedHandler, PullTasksHandler, ReleaseTaskLeaseHandler, SpawnBranchHandler,
    TerminateExecutionHandler, TerminateStateHandler, TriggerTimerHandler,
};
use crate::id::{EntryId, FlowVersionId};
use crate::log::{Entry, EntryPayload, LogStream};
use crate::reject::Reject;
use crate::scheduler::Scheduler;
use crate::storage::Storage;

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

/// Drives a single execution: reads [`Entry`]s from a [`LogStream`], dispatches [`Command`]s to
/// the appropriate handler (producing more entries, appended atomically with causal links), and
/// applies [`Event`]s to [`Storage`] via the [`EventDispatcher`] — which also feeds timer
/// scheduling to the injected [`Scheduler`] contract.
///
/// The StreamProcessor holds a dispatch table mapping each [`Command`] variant (by
/// [`Discriminant`](std::mem::Discriminant)) to its [`CommandHandler`], so `dispatch` is a
/// table lookup — no `match`.
///
/// Named *stream* processor (after the stream/ledger-processing role in CCES) to disambiguate it
/// from future fan-out/worker types and from per-state [`CommandHandler`]s: this type is precisely
/// the single loop that tails a [`LogStream`] and drives one execution.
pub struct StreamProcessor {
    /// Lazily-populated per-version machine cache. Definitions are **not** owned by the StreamProcessor
    /// up front: handlers resolve the machine an execution is bound to via
    /// [`HandlerContext::machine`], which loads it from Storage (keyed by the never-reused
    /// [`FlowVersionId`]) into this cache on first use. This is what lets a recovered Engine
    /// re-resolve a definition from storage without the caller re-supplying it.
    definitions: HashMap<FlowVersionId, Arc<StateMachine>>,
    env: EvalEnv,
    handlers: HashMap<std::mem::Discriminant<Command>, Box<dyn CommandHandler + Send + Sync>>,
}

impl StreamProcessor {
    /// Build a fresh StreamProcessor with the empty per-variant handler table. External `Task` calls are
    /// dispatched by the injected [`TaskService`] (see [`StreamProcessor::run`]), not by this StreamProcessor,
    /// so no handler map is held here.
    pub fn new() -> Self {
        let mut handlers: HashMap<
            std::mem::Discriminant<Command>,
            Box<dyn CommandHandler + Send + Sync>,
        > = HashMap::new();
        command_handler_entry!(
            handlers,
            CreateFlowHandler,
            CreateExecutionHandler,
            CompleteExecutionHandler,
            TerminateExecutionHandler,
            ActivateStateHandler,
            CompleteStateHandler,
            TerminateStateHandler,
            ActivateTimerHandler,
            TriggerTimerHandler,
            CancelTimerHandler,
            ProcessChildCompletedHandler,
            ActivateTaskHandler,
            AssignTaskHandler,
            PullTasksHandler,
            CompleteTaskHandler,
            FailTaskHandler,
            ReleaseTaskLeaseHandler,
            CancelTaskHandler,
            SpawnBranchHandler
        );
        StreamProcessor {
            definitions: HashMap::new(),
            env: EvalEnv::new(),
            handlers,
        }
    }

    /// Runs the StreamProcessor as a **service**: tails the stream indefinitely, dispatching Commands and
    /// applying Events. Does NOT exit when a single execution completes — multiple executions share
    /// the stream and are all processed. Returns `Ok(())` when the log is closed (shut down) or when
    /// the `cancel` token is triggered (controlled shutdown). For M1's synchronous `Engine::start`, a
    /// caller-tail loop drives the StreamProcessor and cancels it once the target execution reaches a
    /// terminal state.
    ///
    /// Real-time scheduling: armed timers are *facts* (`TimerActivated`), and the actual
    /// wall-clock delay is a side effect driven by applying that event — the run loop feeds each
    /// event through an [`EventDispatcher`], and the `TimerActivated` applier hands the arm to the
    /// injected [`Scheduler`]. The scheduler owns a single `DelayQueue` loop and, on expiry, calls
    /// the engine's injected [`TimerSink`] (see `crate::scheduler`), which appends the `TriggerTimer`
    /// *through the engine's controlled write entry* — so this loop never pulls fired timers itself
    /// and the log keeps a single writer (the log assigns each entry's position, so no caller-run
    /// id counter is needed). The loop is also equally reachable by an external
    /// `TerminateExecution` while a timer is pending — the earlier deadlock where an inline `sleep`
    /// blocked the whole stream can't recur.
    ///
    /// **Resume watermark (single-node model):** `Storage` is a disposable projection rebuilt by
    /// replaying Events (idempotent fold, see [`EventDispatcher`]) — Commands are **never re-run**
    /// here. To make "which Commands already ran" decidable on restart, this loop persists a single
    /// scalar `last_processed_position` = the last Command whose causal batch (its produced Events)
    /// has been folded into Storage. It advances at **apply time**: on folding an Event with
    /// `cause_id = Some(c)` we raise the watermark to `max(W, c)` and persist it in the same group of
    /// writes as the fold (so W is never ahead of the projection), then resume from `W + 1` on boot —
    /// skipping already-applied Commands instead of re-deriving the log from position 1. This is the
    /// "no special recovery phase" single-node design; see `docs/durable-execution-recovery-design.md`.
    ///
    /// TODO(recovery+design, multi-node): this single scalar is leader-private — a follower has no
    /// dispatch history, so making "which Commands ran" decidable from the log alone during failover
    /// is still open design work. Not in this single-node milestone.
    ///
    /// The `cancel` token is this loop's **controlled-shutdown** signal: when it is triggered the
    /// loop drains the current iteration and returns `Ok(())`, rather than being hard-killed from
    /// outside. This lets the caller cleanly stop a stream listener (e.g. an `Engine` being dropped
    /// / shut down, or a test ending) without leaking the spawned task — contrast with
    /// `JoinHandle::abort`, which tears the future down mid-iteration with no cleanup.
    #[instrument(skip_all)]
    pub(crate) async fn run<L: LogStream<EntryPayload>>(
        &mut self,
        logstream: &L,
        storage: Arc<Mutex<Box<dyn Storage>>>,
        scheduler: Arc<dyn Scheduler>,
        ack: Arc<Mutex<AckRouter>>,
        cancel: CancellationToken,
    ) -> Result<(), ExecutionError> {
        let env = Arc::new(Mutex::new(std::mem::take(&mut self.env)));
        // TODO(shutdown+join): `run` returns on the `cancel` token, but the scheduler is a
        // caller-injected `Arc<dyn …>` that the engine does *not* spawn — its loop's shutdown is owned
        // by the concrete implementation, and this loop already relies on drop: when the engine drops
        // its clone, the internal queue closes, the loop drains and breaks, and `next_fired` reports
        // `None`. Nothing here awaits that loop. The task worker is likewise caller-owned and observes
        // its own cancel token (see `crate::task_service`); this loop no longer references it — worker
        // settlements arrive as ordinary inbound commands appended to the log.
        let dispatcher = EventDispatcher::new();

        // Load the durable resume watermark — the last Command position fully applied to Storage
        // (0 = fresh store, nothing processed). Resuming from `W + 1` (== 1 on a fresh store) reads
        // the log from exactly where work is not yet applied, instead of re-deriving it from the
        // head; see the run() doc for the apply-time advancement contract. Acquire Storage briefly
        // here (a single point read) and release it before the loop's per-entry acquisitions.
        let mut watermark = storage.lock().await.last_processed_position().await?;
        debug!(watermark, "resuming log stream");
        let mut stream = logstream.stream_read(EntryId::new(watermark + 1));
        // Deferred acknowledgement side effects declared by handlers (see `AckSideEffect`). A
        // command's dispatch appends to this; each is executed once its *matching Event* is applied
        // in the event arm below — the Zeebe post-commit side-effect model, where a response is
        // delivered only after the effects that produced it are durable in Storage.
        let mut pending_acks: Vec<AckSideEffect> = Vec::new();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    // Controlled shutdown requested: stop tailing and return (the caller owns any
                    // further teardown). Nothing mid-batch is dropped here — `select!` yields only
                    // between iterations.
                    debug!("processor loop cancelled");
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
                            let (entries, acks) = {
                                // Acquire Storage only for the duration of this command's dispatch
                                // (per-entry, not for the whole run), so the Engine's other threads can
                                // read projection state — e.g. `Engine::start_for` resolving the latest
                                // revision — while the StreamProcessor sits between entries.
                                let storage_guard = storage.lock().await;
                                let mut env_g = env.lock().await;
                                let mut out = Collector::new(entry_id);
                                let mut ctx = HandlerContext {
                                    env: &mut env_g,
                                    storage: &*storage_guard,
                                    definitions: &mut self.definitions,
                                };
                                let handler = self.handlers
                                    .get(&discriminant(&command))
                                    .expect("a handler is registered for every Command variant");
                                handler.handle(&command, &mut ctx, &mut out).await;
                                out.into_parts()
                            };
                            // Defer the handler-declared acknowledgement side effects until their
                            // corresponding Event is applied in the event arm below.
                            debug!(entries = entries.len(), "command produced entries");
                            logstream.append(entries).await?;
                            // Deliver task-grant acks now, and defer only the event-correlated ones. A
                            // `PullTasks` grant is decided by the handler at discovery and must be
                            // answered even when the pull produced **no events** (e.g. an empty ready
                            // queue) — with no event to correlate, the event arm below would never
                            // fire it and `activate` would hang. Delivering right after append is the
                            // Zeebe post-commit spirit: the append is durable, and the produced events
                            // are the very next stream entries applied immediately after.
                            for side in acks {
                                match side {
                                    AckSideEffect::CompleteRequestTasks {
                                        request_id, tasks, ..
                                    } => ack.lock().await.complete_tasks(request_id, tasks),
                                    other => pending_acks.push(other),
                                }
                            }
                        }
                        Entry {
                            entry_id,
                            timestamp,
                            cause_id,
                            payload: EntryPayload::Event(event),
                            ..
                        } => {
                            log_event(&event);
                            // Apply the event's projection and feed any side effects to the scheduler
                            // / task service. `cause_id` for a scheduled timer / invoked task is this
                            // event's own entry_id, so the later resumption command causally hangs
                            // off this event. Like the command arm, Storage is acquired per entry.
                            let storage_guard = storage.lock().await;
                            // Open one atomic projection transaction for this fold: the event's
                            // writes and the watermark advance below commit together as a single
                            // all-or-nothing native RocksDB transaction (see `Storage::begin_txn`).
                            // The returned `Box` carries the fold's own `StorageTxn`, so after opening
                            // it the raw store is not touched again this fold. The applier sees only
                            // the write-scoped `StorageTxn` handle — it cannot commit (which consumes
                            // the `Box`) nor move the watermark — so the fold boundary is
                            // type-checked, not a convention appliers are trusted to respect. On an
                            // apply error `run` returns (fatal) before `txn.commit`; the dropped `Box`
                            // discards the transaction, never written.
                            let mut txn = storage_guard.begin_txn()?;
                            {
                                let mut ctx = ApplierContext {
                                    storage: &mut *txn,
                                    scheduler: scheduler.as_ref(),
                                    cause_id: entry_id,
                                    // The entry's own frozen timestamp is the deterministic source
                                    // for projection `created_at`/`updated_at`; see
                                    // `ApplierContext::timestamp`.
                                    timestamp,
                                };
                                dispatcher.apply(&mut ctx, &event).await?;
                            } // the write handle drops here; the transaction stays open until commit.

                            // Advance the resume watermark once this event's fold is durable: its
                            // producing Command's effects (incl. this event) are now in Storage. The
                            // entry's `cause_id` is that Command's position; one Command may fold
                            // several Events (same cause), so take the max — keeping the watermark at
                            // the highest fully-applied Command. The `Some` watermark is written by
                            // `commit` **inside the same atomic batch as the fold**, which is what
                            // makes restart-from-W+1 safe: it can never run ahead of the projection.
                            // A `None` cause (never a real position) leaves the watermark untouched.
                            let advanced = if let Some(cause) = cause_id.filter(|c| c.get() > watermark) {
                                watermark = cause.get();
                                debug!(watermark, "resume watermark advanced");
                                Some(watermark)
                            } else {
                                None
                            };

                            // Atomic commit: the fold and (any) watermark advance become visible
                            // all-or-nothing. Consuming the `Box` here is what reserves commit to
                            // the StreamProcessor.
                            txn.commit(advanced)?;

                            // Execute any deferred acknowledgement side effects whose Event has now
                            // been applied. We deliberately do NOT correlate by full Event value
                            // equality: `Event` payloads (executions/flows/activities) do not
                            // round-trip through the LogStream byte-for-byte — float-precision /
                            // defaulted serde fields drift on serialize/deserialize — so a handler's
                            // in-memory ack event can be `!=` its own stored-then-read counterpart
                            // even though it is the same logical operation. Instead we match on the
                            // event's variant plus a stable identity that survives serialization —
                            // the echoed `request_id` — and deliver the *applied* event (the
                            // authoritative durable copy). This is the Zeebe model: respond *after*
                            // the producing Event is durable — never race the projection. Drain
                            // `pending_acks` in place; `complete` ignores a dropped receiver (the
                            // awaiting task was cancelled), so no error handling is needed here.
                            let mut ack_g = ack.lock().await;
                            let mut i = 0;
                            while i < pending_acks.len() {
                                let matched = match (&pending_acks[i], &event) {
                                    (
                                        AckSideEffect::CompleteRequest {
                                            request_id,
                                            event: expected,
                                        },
                                        applied,
                                    ) => {
                                        // A request ack fires on an event of the same variant that
                                        // echoes its request id (e.g. `FlowVersionCreated` for a
                                        // `CreateFlow`, `ExecutionCreated` for a `start`); the id's
                                        // uniqueness pins the operation, so concurrent requests
                                        // never alias.
                                        discriminant(applied) == discriminant(expected.as_ref())
                                            && matches!(
                                                applied,
                                                Event::FlowCreated { request_id: r, .. }
                                                    | Event::FlowVersionCreated {
                                                        request_id: r,
                                                        ..
                                                    }
                                                    | Event::ExecutionCreated {
                                                        request_id: r,
                                                        ..
                                                    }
                                                    if *r == *request_id
                                            )
                                    }
                                    // `CompleteRequestTasks` is never deferred here — it is delivered
                                    // to its awaiter in the command arm right after append (see that
                                    // block) — so nothing in `pending_acks` is ever this variant.
                                    (AckSideEffect::CompleteRequestTasks { .. }, _) => false,
                                };
                                if !matched {
                                    i += 1;
                                    continue;
                                }
                                let ack = pending_acks.swap_remove(i);
                                match ack {
                                    AckSideEffect::CompleteRequest { request_id, .. } => {
                                        ack_g.complete(request_id, event.clone());
                                    }
                                    AckSideEffect::CompleteRequestTasks { .. } => unreachable!(
                                        "task-grant acks are delivered in the command arm, never deferred here"
                                    ),
                                }
                            }
                        }
                        Entry {
                            entry_id,
                            payload: EntryPayload::Reject(reject),
                            ..
                        } => {
                            // A command was refrained from applying: awake its awaiting caller with
                            // the rejection. Unlike the `Event` arm above, there is **no** deferred
                            // ack to correlate — the `Reject` record carries its own `request_id`
                            // (the echoing correlate of the refused command), so we complete that
                            // request directly as we read the record. Nothing is folded into
                            // Storage: a refusal has no projection. `reject` ignores a dropped
                            // receiver (the awaiting task was cancelled), so no error handling is
                            // needed here; the record remains on the log for the durable audit
                            // trail even when nothing awaits it.
                            log_reject(&reject);
                            ack.lock().await.reject(reject.request_id, reject);
                            debug!(entry_id = %entry_id, "rejected command");
                        }
                    }
                }
            }
        }
    }

    /// Routes a [`Command`] to its handler, building the [`HandlerContext`] from Storage and a
    /// [`Collector`] for output. Returns the [`Entry`]s the handler emitted (already enveloped with
    /// `cause_id`/`timestamp` and placeholder `entry_id`/`stream_id`), ready to append atomically
    /// (the append stamps the real positions and the log's own stream id).
    ///
    /// Single-shot, synchronous dispatch usable outside `run` (tests, [`Engine::submit`]'s
    /// consumers). Timer *scheduling* is not part of dispatch: applying a `TimerActivated` event is
    /// what arms a timer, and that only happens in `run` where the injected [`Scheduler`] lives — so a
    /// single-shot caller driven purely by `dispatch` cannot fire timers (it must drive `run`).
    pub async fn dispatch<S: Storage>(
        &mut self,
        command: &Command,
        storage: &S,
        cause_id: EntryId,
    ) -> Result<Vec<Entry>, ExecutionError> {
        let mut out = Collector::new(cause_id);
        let mut ctx = HandlerContext {
            env: &mut self.env,
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
}

impl Default for StreamProcessor {
    fn default() -> Self {
        Self::new()
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
                flow_id = %flow.flow_id,
                name = %flow.name,
                "flow created (brand-new name entered the system)"
            );
        }
        Event::FlowVersionCreated { flow_version, .. } => {
            info!(
                flow_id = %flow_version.flow_id,
                flow_version_id = %flow_version.flow_version_id,
                name = %flow_version.name,
                version = flow_version.version,
                "flow version created"
            );
        }
        Event::ExecutionCreated { execution, .. } => {
            info!(
                execution = %execution.id,
                root = %execution.root_execution,
                input = %execution.input,
                "execution created"
            );
        }
        Event::ExecutionCompleting { execution } => {
            debug!(
                execution = %execution.id,
                output = %execution.output.as_ref().unwrap_or(&serde_json::Value::Null),
                "execution completing"
            );
        }
        Event::ExecutionCompleted { execution } => {
            info!(
                execution = %execution.id,
                output = %execution.output.as_ref().unwrap_or(&serde_json::Value::Null),
                "execution completed"
            );
        }
        Event::ExecutionTerminating { execution } => {
            warn!(
                execution = %execution.id,
                reason = %format!("{:?}", execution.status),
                "execution terminating"
            );
        }
        Event::ExecutionTerminated { execution } => {
            warn!(
                execution = %execution.id,
                reason = %format!("{:?}", execution.status),
                "execution terminated"
            );
        }
        Event::StateActivating { activity } => {
            info!(
                activity = %activity.id,
                state = %crate::handlers::state_name_from_path(activity.state_path.as_ptr()),
                input = %activity.input,
                "entered state"
            );
        }
        Event::StateActivated { activity } => {
            debug!(activity = %activity.id, "state activated");
        }
        Event::ParallelBranchSpawned {
            activity,
            index,
            execution,
        } => {
            debug!(activity = %activity, index, child = %execution, "parallel branch spawned");
        }
        Event::StateCompleting { activity } => {
            debug!(activity = %activity.id, "state completing");
        }
        Event::StateCompleted { activity } => {
            debug!(
                activity = %activity.id,
                output = %activity.output.as_ref().unwrap_or(&serde_json::Value::Null),
                "state completed"
            );
        }
        Event::StateTerminating { activity } => {
            warn!(
                activity = %activity.id,
                reason = %format!("{:?}", activity.status),
                "state terminating"
            );
        }
        Event::StateTerminated { activity } => {
            warn!(
                activity = %activity.id,
                reason = %format!("{:?}", activity.status),
                "state terminated"
            );
        }
        Event::TimerActivated { timer } => {
            debug!(
                timer = %timer.id,
                parent = ?timer.parent,
                purpose = %format!("{:?}", timer.purpose),
                deadline_ms = timer.deadline.as_millis(),
                "timer activated"
            );
        }
        Event::TimerTriggered { timer } => {
            debug!(
                timer = %timer.id,
                parent = ?timer.parent,
                purpose = %format!("{:?}", timer.purpose),
                deadline_ms = timer.deadline.as_millis(),
                "timer completed"
            );
        }
        Event::TimerCancelled { timer } => {
            debug!(
                timer = %timer.id,
                parent = ?timer.parent,
                purpose = %format!("{:?}", timer.purpose),
                deadline_ms = timer.deadline.as_millis(),
                "timer cancelled"
            );
        }
        Event::TaskActivated { task } => {
            debug!(
                task = %task.id,
                parent = ?task.parent,
                resource = %task.resource,
                "task activated"
            );
        }
        Event::TaskLeased { task } => {
            debug!(
                task = %task.id,
                worker = ?task.worker_id,
                lease_until = ?task.lease_until,
                "task leased to worker"
            );
        }
        Event::TaskLeaseExpired { task } => {
            debug!(task = %task.id, "task lease expired; re-queued");
        }
        Event::TaskCompleted { task, output } => {
            debug!(task = %task.id, output = %output, "task completed");
        }
        Event::TaskFailed { task, error } => {
            warn!(task = %task.id, error = %format!("{error:?}"), "task failed");
        }
        Event::RetryScheduled {
            activity,
            retrier_index,
            retrier_attempt,
            retry_count,
            scheduled_at,
        } => {
            debug!(
                activity = %activity,
                retrier_index = %retrier_index,
                retrier_attempt = %retrier_attempt,
                retry_count = %retry_count,
                scheduled_at = %scheduled_at,
                "task retry scheduled"
            );
        }
        Event::TaskCancelled { task } => {
            debug!(task = %task.id, "task cancelled");
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
