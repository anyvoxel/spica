use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Weak};

use serde_json::Value;
use tokio::sync::{Mutex, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::log::{Entry, EntryPayload, LogStream, Timestamp};
use crate::scheduler::{Scheduler, TimerSink};
use crate::storage::Storage;
use crate::stream_processor::StreamProcessor;
use crate::task_api::{ActivatedTask, TaskApi};
use crate::types::command::Command;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::id::{EntryId, FlowName, RequestId, StreamId};
use crate::types::meta::{ObjectName, ObjectReference};
use crate::types::reject::Reject;
use crate::types::result::ExecutionResult;

/// Executes ASL state machines via the CCES architecture (Causal Command Event Sourcing).
///
/// The running engine is [`Engine`]; its unstarted predecessor is [`EngineBuilder`].
///
/// ## Lifecycle is encoded in the types
///
/// `Engine` only exists **after** a StreamProcessor is booted: [`EngineBuilder`] owns the (caller-supplied)
/// persistence backends, and its [`start`](EngineBuilder::start) **consumes it**, spawning the
/// Engine's single long-lived StreamProcessor and returning the running [`Engine`]. Because `start` takes
/// the builder **by value** and returns a fresh `Engine`, and `stop` consumes the `Engine`, the
/// compiler enforces a linear lifecycle — an unstarted state has **no** `create_flow` /
/// `start_for_revision` / `stop` methods (calling them before `start` is a compile error, not a
/// runtime check), an `Engine` cannot be started twice (its builder is already consumed), and a
/// stopped `Engine` is gone (reusing it would replay already-dispatched Commands — see
/// [`Engine::stop`]).
///
/// The backends are chosen once at construction via [`EngineBuilder::with_backends`], which takes
/// the log and storage as type-erased trait objects that the *caller* has already built (see the
/// builder's docs for why the engine no longer fabricates concrete backends itself).
///
/// The running [`Engine`] drives **one long-lived StreamProcessor** for its whole lifetime. That single
/// StreamProcessor tails the log and folds every Command/Event into Storage in strict log order. Every
/// "do something" Engine method ([`create_flow`](Self::create_flow), [`start_for_revision`]) is thin
/// and follows the same shape: **register a one-shot acknowledgement channel under an opaque
/// per-request id, append its Command, then await its own channel**. The StreamProcessor completes each
/// awaiter's channel when it applies that command's outcome — per-request futures (Zeebe's
/// `requestId` model, see the `ack` field), so any number of operations can be in flight
/// concurrently.
///
/// Definitions are **created** (persisted by id) then **executed by that id**: [`Engine::create_flow`]
/// appends a `CreateFlow` command that folds a new immutable [`FlowVersion`](crate::FlowVersion)
/// into Storage; [`Engine::start_for_revision`] spawns executions that bind to a created version.
///
/// `Engine::terminate` remains a **free function** that writes to a caller-supplied
/// [`LogStream`] — the low-level CCES seam.
///
/// ## How an acknowledgement is awaited
///
/// Each blocking Engine method mints an opaque request id ([`RequestId`]) and registers a one-shot
/// channel under it in the shared `ack` registry, appends its Command (carrying that `request_id`),
/// then awaits its **own** channel. The StreamProcessor is the single writer: when it applies the outcome
/// Event it routes each ack by `request_id` — a `FlowVersionCreated` echoes its own id, an execution's
/// terminal event is attributed via its `CreateExecution`'s id (see [`AckRouter`]) — waking exactly
/// that awaiter. Registration precedes the append, so the ack always lands after registration and is
/// never missed — no position bookkeeping. Because the registry key is the opaque request id and not
/// the target entity id, many simultaneous operations on the **same** flow or execution coexist
/// without aliasing, each on its own future.
/// The **unstarted** engine: owns the config-selected persistence backends but has no StreamProcessor yet.
///
/// Construct one with [`with_backends`](EngineBuilder::with_backends), which takes the caller's
/// already-built log + storage trait objects, then call
/// [`start`](EngineBuilder::start) — which consumes it and returns the running [`Engine`]. There are
/// deliberately **no** operation/`stop` methods here: an engine that isn't running has no StreamProcessor
/// to fold commands, and the type system says so — the fields mirror the running [`Engine`]'s but
/// without the mandatory `processor`; `start` is what closes that gap (and enforces the linear
/// lifecycle described in the module docs).
pub struct EngineBuilder {
    /// See the running [`Engine::log`] — owned here until [`start`](Self::start) moves it into the
    /// `Engine` it returns.
    log: Arc<Box<dyn LogStream<EntryPayload>>>,
    /// See the running [`Engine::storage`] — owned here until [`start`](Self::start) moves it.
    storage: Arc<Mutex<Box<dyn Storage>>>,
    /// The injected timer [`Scheduler`] contract. `None` until [`with_scheduler`](Self::with_scheduler)
    /// is called, and required by [`start`](Self::start): like the backends, the engine never
    /// fabricates a concrete scheduler, so the caller supplies it (see `with_scheduler`). The
    /// `Arc<dyn Scheduler>` is moved into the StreamProcessor on `start`.
    scheduler: Option<Arc<dyn Scheduler>>,
    /// See the running [`Engine::ack`] — owned here until [`start`](Self::start) moves it.
    ack: Arc<Mutex<AckRouter>>,
}

/// Executes ASL state machines via the CCES architecture (Causal Command Event Sourcing).
///
/// A running `Engine` — obtained **only** from [`EngineBuilder::start`], which consumes the
/// unstarted builder (see the module docs for why this makes misuse a compile error).
///
/// `Engine` is a thin **single-owner wrapper** over `Arc<EngineInner>`: the real state lives behind
/// the `Arc` so the timer [`Scheduler`] can *weakly* point back at the running engine (see
/// `EngineTimerSink`) without forming a strong reference cycle. Because `Engine` is deliberately
/// **not** `Clone`, the wrapper remains the one strong reference — so `stop` can `Arc::try_unwrap`
/// it by construction. Every operation method lives on [`EngineInner`] and is reached through this
/// wrapper via `Deref` (so `engine.create_flow(..)` still reads naturally).
pub struct Engine {
    /// The running engine's state, shared (weakly) with the timer sink. `Engine` holds the only
    /// strong reference — see the type doc.
    inner: Arc<EngineInner>,
}

/// Provide read-only [`EngineInner`] methods through the [`Engine`] wrapper (auto-deref), so callers
/// write `engine.create_flow(..)` rather than `engine.inner.create_flow(..)`. The wrapper is
/// single-owner and not `Clone`, so this does not weaken the linear lifecycle `stop` relies on.
impl Deref for Engine {
    type Target = EngineInner;

    fn deref(&self) -> &EngineInner {
        &self.inner
    }
}

/// The actual run state of a running engine, owned via [`Engine`]'s `Arc<EngineInner>` (and exposed
/// through its `Deref`).
///
/// Split out of [`Engine`] so the timer sink can hold a `Weak<EngineInner>` that upgrades only while
/// the engine (the strong reference) is alive — handing the sink a bare logStream instead would let
/// it bypass the engine, and handing it a strong `Arc` would cycle with the `Arc<dyn Scheduler>` the
/// engine drives. All operation methods live here and are reached through [`Engine`]'s `Deref`, so
/// callers keep writing `engine.create_flow(..)`; the type is `pub` only so that `Deref` target does
/// not leak a private type.
pub struct EngineInner {
    /// The configured LogStream. Its `stream_read` returns `'static` per-consumer streams so the
    /// StreamProcessor task consumes it independently. Stored as `Box<dyn …>` (not `dyn …` directly) so
    /// the box itself is a *Sized* [`LogStream`] implementor (via `#[auto_impl(Box)]` on the trait)
    /// that `StreamProcessor::run`'s generic `L: LogStream` accepts.
    log: Arc<Box<dyn LogStream<EntryPayload>>>,
    /// The configured Storage — a rebuildable projection of the log, mutated by the StreamProcessor.
    /// Wrapped in a `Mutex` (`Storage::put_*` take `&mut self`). The StreamProcessor acquires it **per
    /// entry** — holding `&mut` only for the duration of each command dispatch / event apply and
    /// releasing it between entries — so the engine's own methods can read projection state (e.g.
    /// `resolve_version_id` resolving the latest revision) while the StreamProcessor runs. `Box<dyn …>`
    /// so `StreamProcessor::run` sees a Sized type.
    storage: Arc<Mutex<Box<dyn Storage>>>,
    /// Handle to the engine's single long-lived StreamProcessor run loop, booted by
    /// [`EngineBuilder::start`] and shut down by [`Engine::stop`]. Present **unconditionally** — the
    /// type guarantees this engine is running (it is only produced by `EngineBuilder::start`, which
    /// consumes the builder), so every operation method can rely on the StreamProcessor without a
    /// `None` check.
    processor: StreamProcessorTask,
    /// The engine's response registry: every operation that must wait for an acknowledgement — flow
    /// creation landing, an execution reaching a terminal state — registers a one-shot channel
    /// keyed by a unique [`Ack`] (its own `ExecutionId`), then awaits its receiver. The
    /// internal StreamProcessor completes the matching entry when it applies that outcome (see
    /// [`AckRouter`]). This is Zeebe's `requestId → future` model: entries are keyed by an opaque
    /// per-request id (never the target entity id), so any number of operations — including several
    /// on the same flow or execution — run in flight concurrently without aliasing. Shared with the
    /// StreamProcessor via `Arc`; created in [`EngineBuilder::with_backends`].
    ack: Arc<Mutex<AckRouter>>,
}

/// Handle to the engine's internal [`StreamProcessor`] run loop, so `Engine::stop` can request its
/// controlled shutdown (cancel token) and await the task to drain its current iteration.
struct StreamProcessorTask {
    cancel: CancellationToken,
    handle: tokio::task::JoinHandle<Result<(), ExecutionError>>,
}

/// The [`TimerSink`] the running engine injects into its [`Scheduler`]: the single controlled write
/// entry an expired timer flows through.
///
/// The scheduler **calls the engine** on expiry rather than writing to the log itself — see
/// [`EngineInner::trigger_timer`]. It holds only a `Weak<EngineInner>`: not a bare log (which would
/// bypass the engine's validation), and not a strong `Arc` (which would cycle with the
/// `Arc<dyn Scheduler>` the engine drives). If the engine is already gone (its single strong
/// reference released), `upgrade` yields `None` and the fire is dropped — correct, since there is no
/// engine left to resume.
struct EngineTimerSink {
    /// A weak handle to the running engine; upgraded per-fire to route the write through it.
    engine: Weak<EngineInner>,
}

#[async_trait::async_trait]
impl TimerSink for EngineTimerSink {
    async fn trigger(&self, timer: &ObjectReference, cause_id: EntryId) {
        // Route the expiry write through the engine's controlled entry — never to the log raw. If
        // the engine is gone (last strong reference dropped), there is nothing left to resume; drop.
        if let Some(engine) = self.engine.upgrade() {
            engine.trigger_timer(timer, cause_id).await;
        }
    }
}

/// The Engine's response-registry state, shared with the internal StreamProcessor under one `Mutex`
/// (`Arc<Mutex<AckRouter>>`).
///
/// `pending` is the registry of in-flight "append a Command, then await its acknowledgement"
/// operations, keyed by an opaque per-request id ([`RequestId`]). It is deliberately **not** keyed by
/// a target entity id (`ExecutionId`): a single flow or execution can be the target of many
/// concurrent requests, and keying by the entity would alias them into one slot — a second `insert`
/// overwrites the first caller's sender, so one await hangs forever and the other gets a misrouted
/// ack. Acks are correlated per-*request* (Zeebe's `requestId`), never per-entity.
///
/// `pending` is the registry of in-flight "append a Command, then await its acknowledgement"
/// operations, keyed by an opaque per-request id ([`RequestId`]). It is deliberately **not** keyed by
/// a target entity id (`ExecutionId`): a single flow or execution can be the target of many
/// concurrent requests, and keying by the entity would alias them into one slot — a second `insert`
/// overwrites the first caller's sender, so one await hangs forever and the other gets a misrouted
/// ack. Acks are correlated per-*request* (Zeebe's `requestId`), never per-entity.
///
/// Every awaited command is correlated on an event that **echoes the request id itself** (`FlowCreated`
/// / `FlowVersionCreated` for a `CreateFlow`, `ExecutionCreated` for a `start_for_revision`). The
/// `start` flow acknowledges at execution birth (the `ExecutionCreated` it awaits), not at the
/// execution's terminal event — terminal state is observed by the caller via `wait_for_execution`
/// polling Storage, so no `execution → request` routing bookkeeping is needed here.
pub(crate) struct AckRouter {
    pending: HashMap<RequestId, oneshot::Sender<AckOutcome>>,
}

impl AckRouter {
    pub(crate) fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    /// Route `event` to the awaiting operation keyed by `request_id`, completing its channel with the
    /// *applied* event and removing the entry. A dropped receiver (the awaiting task was cancelled)
    /// makes the `send` fail, which we ignore — there is nothing to deliver to.
    pub(crate) fn complete(&mut self, request_id: RequestId, event: Event) {
        if let Some(tx) = self.pending.remove(&request_id) {
            let _ = tx.send(AckOutcome::Applied(Box::new(event)));
        }
    }

    /// Route a rejection to the awaiting operation keyed by `request_id`, completing its channel with
    /// the `Reject` (the awaited command was refrained from applying) and removing the entry. As with
    /// [`Self::complete`], a dropped receiver makes the `send` fail and is ignored.
    pub(crate) fn reject(&mut self, request_id: RequestId, reject: Reject) {
        if let Some(tx) = self.pending.remove(&request_id) {
            let _ = tx.send(AckOutcome::Rejected(reject));
        }
    }

    /// Route the **granted task set** of a `ClaimTasks` to the awaiting `poll_tasks` keyed by
    /// `request_id`, completing it with [`AckOutcome::Granted`] and removing the entry. Unlike
    /// [`Self::complete`] there is no single applied `Event` to deliver — the hard-set was decided by
    /// the `ClaimTasksHandler` at discovery — so the router hands back the list directly. A dropped
    /// receiver (the `poll_tasks` was cancelled) makes the `send` fail, which we ignore.
    pub(crate) fn complete_tasks(&mut self, request_id: RequestId, tasks: Vec<ActivatedTask>) {
        if let Some(tx) = self.pending.remove(&request_id) {
            let _ = tx.send(AckOutcome::Granted(tasks));
        }
    }
}

/// The payload an acknowledgement delivers to an awaiting operation: either the **applied**
/// [`Event`] that resolves the awaited command (boxed — an `Event` is large, and this is a
/// one-per-awaited-operation send, not a hot hot-path value), a [`Reject`] if the command was
/// refrained from applying (see [`AckRouter::reject`]), or — for a `ClaimTasks` poll — the
/// **granted task set** (see [`AckRouter::complete_tasks`]) decided by the handler at discovery.
/// Widening the channel from bare `Event` to this is what lets a rejected command wake its awaiter
/// with the reason — instead of a call that hangs forever or silently returns nothing.
pub(crate) enum AckOutcome {
    Applied(Box<Event>),
    Rejected(Reject),
    /// A `ClaimTasks` poll granted `tasks` to its awaiting `poll_tasks` (see
    /// [`AckRouter::complete_tasks`]).
    Granted(Vec<ActivatedTask>),
}

/// Failure modes of awaiting an acknowledgement (the other side of [`AckOutcome`]): either the
/// channel was dropped with no delivery (`Dropped` — the engine was never started, or its StreamProcessor
/// never applied the awaited outcome), or the awaited command was [`Rejected`](AckFailure::Rejected).
enum AckFailure {
    Dropped,
    Rejected(Reject),
}

impl EngineBuilder {
    /// Construct an unstarted engine on **caller-supplied** backends. This is the initialization
    /// point where a process hands over its chosen [`LogStream`] and [`Storage`] implementations —
    /// InMemory vs Rocks, or a bespoke distributed seam — type-erased to trait objects so `Engine`
    /// holds them without generics. The choice persists for the engine's lifetime (the backends are
    /// owned by the `Engine` [`start`](Self::start) returns).
    ///
    /// Construction itself cannot fail (no IO happens here): opening a durable backend (RocksDB log /
    /// storage) is the *caller's* job *before* this call — typically a binary (`spica-server`) that
    /// builds its `RocksLogStream` / `RocksStorage` and boxes them. The engine deliberately never
    /// fabricates concrete backends itself, so it has no dependency on the implementation crates
    /// (keeping `storage → engine`, not the reverse, acyclic — see `crate::storage`).
    pub fn with_backends(log: Box<dyn LogStream<EntryPayload>>, storage: Box<dyn Storage>) -> Self {
        Self {
            log: Arc::new(log),
            storage: Arc::new(Mutex::new(storage)),
            scheduler: None,
            ack: Arc::new(Mutex::new(AckRouter::new())),
        }
    }

    /// Inject the timer [`Scheduler`] the StreamProcessor drives. Required before [`start`](Self::start):
    /// the engine holds it as `Option` here and `start` refuses to boot without it (see that
    /// method's `expect`).
    ///
    /// Like the backends, this is the **assembly point**: the caller builds a concrete scheduler
    /// (e.g. `spica_scheduler::InMemoryScheduler::spawn()`) and hands it over type-erased, so the
    /// engine never fabricates a runtime and has no dependency on the implementation crate (keeping
    /// `scheduler → engine`, not the reverse, acyclic — see `crate::scheduler`). The concrete
    /// scheduler's loop lives as long as this `Arc`; it is moved into the StreamProcessor on `start`.
    pub fn with_scheduler(mut self, scheduler: Arc<dyn Scheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    /// Boot the Engine: spawns its **single long-lived StreamProcessor** on these log+storage backends and
    /// returns the running [`Engine`].
    ///
    /// This is the **only** way to obtain an [`Engine`], and it consumes the builder (`self`), so a
    /// given set of backends can be booted at most once and the returned `Engine` is *guaranteed*
    /// running — its `processor` is never `None`. The concrete scheduler injected via
    /// [`with_scheduler`](Self::with_scheduler) is fixed for the Engine's lifetime (one StreamProcessor
    /// serves every execution).
    ///
    /// Non-blocking: `start` spawns the StreamProcessor task and returns; the StreamProcessor processes appends
    /// as they land. Engine methods follow the same shape — *register* an acknowledgement
    /// channel, *append* a Command, then await their own channel (see the `ack` field) — so they
    /// never coordinate a StreamProcessor session themselves.
    ///
    /// NOTE(recovery+design): one long-lived StreamProcessor is exactly the model the recovery-watermark
    /// TODO in `processor.rs` anticipates — one worker tailing the whole log and applying each entry
    /// once. Persisting that watermark is still TODO; because this method *consumes* the builder the
    /// type cannot express "stop then restart on the same Engine," which is the safe reading of the
    /// old convention (boot once, run many commands, stop once, drop).
    pub async fn start(self) -> Result<Engine, ExecutionError> {
        // The scheduler is the third injected backend, and — like the log/store — the engine never
        // fabricates it. A programmer who forgets `with_scheduler` is denied here at boot (a
        // developer-error, not a runtime condition), the same guarded-registration style used
        // elsewhere in this crate.
        let scheduler = self
            .scheduler
            .expect("EngineBuilder::start requires with_scheduler to have been called");
        let mut processor = StreamProcessor::new();
        let log = Arc::clone(&self.log);
        let storage = Arc::clone(&self.storage);
        let ack = Arc::clone(&self.ack);
        // Controlled-shutdown token for the StreamProcessor loop: on `stop` we `cancel()` it and await
        // the task, letting the loop drain its current iteration and return cleanly — rather than
        // `JoinHandle::abort`, which hard-kills the spawned future mid-iteration with no teardown.
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        // Clone the scheduler handle for the loop: the original is kept here to inject the timer sink
        // afterwards, since the sink weakly references the running engine's `Arc<EngineInner>`, which
        // only exists once the loop has been spawned and handed its `StreamProcessorTask` handle.
        let scheduler_for_loop = Arc::clone(&scheduler);
        let handle = tokio::spawn(async move {
            // `&*log` is `&Box<dyn LogStream<EntryPayload>>` — a Sized implementor of `LogStream` via
            // `#[auto_impl(Box)]`, which `StreamProcessor::run`'s `L: LogStream` accepts. The StreamProcessor is
            // handed the storage `Arc` and acquires the `Mutex` per entry (releasing it between
            // entries), so it never holds the lock for the whole run; it also owns the shared
            // response registry (`ack`), completing each awaiter's channel when it applies the
            // matching event.
            processor
                .run(&*log, storage, scheduler_for_loop, ack, task_cancel)
                .await
        });
        // Assemble the engine's run state behind the `Arc` the weak timer sink will reference.
        let inner = Arc::new(EngineInner {
            log: self.log,
            storage: self.storage,
            ack: self.ack,
            processor: StreamProcessorTask { cancel, handle },
        });
        // Inject the engine's controlled write entry into the scheduler: the sink routes an expired
        // timer *into the engine* (`trigger_timer`) via a `Weak`, so it neither bypasses the engine's
        // validation (a bare log) nor cycles with the scheduler (a strong `Arc`). Attached here, before
        // `start` returns; arming only occurs when the loop applies a `TimerActivated`, well after the
        // caller drives anything, so the sink is always attached before it can fire.
        scheduler.attach_sink(Arc::new(EngineTimerSink {
            engine: Arc::downgrade(&inner),
        }));
        let engine = Engine { inner };
        info!("engine started: internal processor running");
        Ok(engine)
    }
}

impl Engine {
    /// Stop the running Engine: requests its internal StreamProcessor's controlled shutdown and awaits it,
    /// so the loop drains its current iteration before returning. Consumes `self` — after `stop`
    /// there is no `Engine`, which is the type-level reflection of the recovery NOTE above: a stopped
    /// StreamProcessor is gone, and booting a new one over the same state would replay already-dispatched
    /// Commands (duplicate events), so the type refuses to let a stopped Engine be reused. Convention
    /// today: boot once, run many commands, stop once.
    pub async fn stop(self) {
        // `inner` must be the engine's sole strong owner here: any worker a caller spawned against
        // [`Self::task_api`] holds a strong `Arc<dyn TaskApi>` → `Arc<EngineInner>` back to it, so the
        // caller MUST stop (and release) that worker *before* stopping the engine, or this unwrap
        // fails. That is the worker-lifecycle contract spelled out in [`Self::task_api`].
        let Engine { inner } = self;
        // Signal the run loop to stop tailing.
        inner.processor.cancel.cancel();
        // Drain the loop's current iteration and await it, so shutdown is a closed loop (nothing
        // drains after we return). (`unreachable!` rather than `expect` so this needs no `Debug` on
        // `EngineInner`.)
        let inner = match Arc::try_unwrap(inner) {
            Ok(inner) => inner,
            Err(_) => unreachable!("engine is the sole remaining owner of its inner state"),
        };
        let task = inner.processor;
        let _ = task.handle.await;
        info!("engine stopped: internal processor shut down");
    }

    /// Hand a worker the engine-hosted inbound [`TaskApi`] (this running engine's `EngineInner`
    /// implements it), so the worker can claim/settle tasks externally — the engine no longer spawns
    /// or owns any worker. The returned `Arc<dyn TaskApi>` is a strong reference to the engine's inner
    /// state, so **stop any worker you spawn with it before [`Engine::stop`]** — otherwise the engine
    /// cannot be unwrapped and cleanly shut down.
    pub fn task_api(&self) -> Arc<dyn TaskApi> {
        self.inner.clone()
    }
}

impl EngineInner {
    /// The engine's controlled write entry for an expired timer, called by the injected
    /// [`TimerSink`](crate::TimerSink) on expiry.
    ///
    /// This is where an expired timer's `TriggerTimer` is written — the scheduler calls the *engine*,
    /// never the log raw, so future validation (e.g. "is this timer still active") lands here
    /// centrally. Today the guard is the funnel itself: the authoritative duplicate/race guard is the
    /// idempotent `TriggerTimerHandler` (a timer since cancelled is a no-op there), mirroring how
    /// Zeebe leaves the same protection in the event-driven fold. Envelopes with placeholders; the
    /// log stamps the real position and its own stream id, and `cause_id` causally hangs this off the
    /// `TimerActivated` that armed it.
    async fn trigger_timer(&self, timer: &ObjectReference, cause_id: EntryId) {
        let entry = Entry {
            stream_id: StreamId::nil(), // the log stamps its own id at append.
            entry_id: EntryId::nil(),   // placeholder — the log assigns the real position.
            cause_id: Some(cause_id),
            timestamp: Timestamp::now(),
            payload: EntryPayload::Command(Command::TriggerTimer {
                timer: timer.clone(),
            }),
        };
        // The append only fails if the engine's own log handle is already gone (shut down); log the
        // dropped fire rather than panic, which would kill the scheduler loop and every other timer.
        if let Err(e) = self.log.append(vec![entry]).await {
            tracing::error!(%timer, error = %e, "failed to append a fired TriggerTimer");
        }
    }

    /// Register a one-shot receiver under an opaque [`RequestId`] in the shared response registry and
    /// hand it back, so the caller can append its Command and then [`await_ack`](Self::await_ack) the
    /// outcome.
    ///
    /// Registration must precede the append: the StreamProcessor only learns of a Command — and therefore
    /// only arrives at its acknowledgement — *after* the append lands, so registering first guarantees
    /// the ack is delivered to us and never missed. The key is an opaque, never-reused request id
    /// (never the target entity id), so concurrent requests on the same flow/execution never alias in
    /// the registry.
    async fn register_ack(&self, key: RequestId) -> oneshot::Receiver<AckOutcome> {
        let (tx, rx) = oneshot::channel();
        self.ack.lock().await.pending.insert(key, tx);
        rx
    }

    /// Await the acknowledgement the internal StreamProcessor delivers on `rx`. This is the generic
    /// "append a Command, then await its ack" primitive each blocking Engine method is built on —
    /// but every awaiter has its **own** channel, so any number can be in flight concurrently
    /// (Zeebe's `requestId → future` model). Returns `Ok(Event)` when the awaited outcome was
    /// applied, or [`AckFailure::Rejected`] when the command was refrained from applying (the
    /// StreamProcessor routes a `Reject` to the awaiter), or [`AckFailure::Dropped`] when the channel is
    /// dropped without an ack — the engine was not started, or its StreamProcessor never applied the
    /// awaited outcome.
    async fn await_ack(rx: oneshot::Receiver<AckOutcome>) -> Result<Event, AckFailure> {
        match rx.await {
            Ok(AckOutcome::Applied(event)) => Ok(*event),
            Ok(AckOutcome::Rejected(reject)) => Err(AckFailure::Rejected(reject)),
            // `await_ack` is only reached by event-acking commands; a task grant is awaited through
            // `await_ack_tasks` instead, so `Granted` is a programming error here.
            Ok(AckOutcome::Granted(_)) => {
                unreachable!("a task grant is awaited via await_ack_tasks, not await_ack")
            }
            Err(_) => Err(AckFailure::Dropped),
        }
    }

    /// Await the **granted task set** of a [`Command::ClaimTasks`] poll on `rx` (the channel
    /// registered by [`Self::register_ack`]). This is the same "append a Command, then await its ack"
    /// primitive as [`Self::await_ack`], specialized for the `AckOutcome::Granted` a `ClaimTasks`
    /// delivers (there is no single applied `Event` to hand back — the list was decided by the
    /// `ClaimTasksHandler` at discovery). Returns `Ok(tasks)` on a successful grant, or the same
    /// [`AckFailure`]s as [`Self::await_ack`] (rejected / dropped).
    async fn await_ack_tasks(
        rx: oneshot::Receiver<AckOutcome>,
    ) -> Result<Vec<ActivatedTask>, AckFailure> {
        match rx.await {
            Ok(AckOutcome::Granted(tasks)) => Ok(tasks),
            Ok(AckOutcome::Rejected(reject)) => Err(AckFailure::Rejected(reject)),
            Ok(AckOutcome::Applied(_)) => {
                unreachable!("a ClaimTasks ack delivers Granted, never an applied Event")
            }
            Err(_) => Err(AckFailure::Dropped),
        }
    }

    /// Map an [`AckFailure`] from [`Self::await_ack`] into the [`ExecutionError`] an Engine API
    /// surfaces to its caller: a dropped channel is an engine/StreamProcessor liveness problem (reported
    /// as before), while a rejected command becomes [`ExecutionError::Rejected`] carrying the
    /// machine-readable reason.
    fn ack_failure_error(failure: AckFailure) -> ExecutionError {
        match failure {
            AckFailure::Dropped => ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "no acknowledgement received (engine not started, or its StreamProcessor never applied the \
                 awaited outcome)"
                    .to_string(),
            )),
            AckFailure::Rejected(reject) => ExecutionError::Rejected(reject),
        }
    }

    /// Create a new version of a flow under `name` from its raw ASL definition string
    /// (the JSON-encoded form of a [`StateMachine`]), persisting it to Storage, and return
    /// the **never-reused [`ObjectReference`]** executions of this definition bind to. `CreateFlow`
    /// creates a **new** flow: passing a `name` that **already exists** is rejected with
    /// [`ExecutionError::InvalidDefinition`] (a future "add a version to an existing flow" operation
    /// is a separate concern); the version's `ObjectName` (`{name}-{version}`) and `uid` are minted by the handler.
    ///
    /// The definition is **validated at this boundary** — a string that doesn't parse as a
    /// `StateMachine` is rejected with [`ExecutionError::InvalidDefinition`] before anything is
    /// written — and stored as the raw string (the parsed model is derived on demand at execution).
    ///
    /// The Engine's single long-lived StreamProcessor (booted by [`EngineBuilder::start`]) folds the `CreateFlow`
    /// command; this method mints an opaque [`RequestId`], **registers** a one-shot acknowledgement
    /// channel under it, **appends** the `CreateFlow` command, and **awaits the matching
    /// acknowledgement** on its own channel ([`await_ack`](Self::await_ack)) — the signal the
    /// definition is durably on the log. It does **not** spawn its own StreamProcessor session.
    ///
    /// Ordering guarantee: because `create_flow` returns only once its `FlowVersionCreated` has been
    /// appended, and the single StreamProcessor applies entries in strict log order, a later
    /// [`start_for_revision`](Self::start_for_revision) (which appends `CreateExecution` after that
    /// event) always dispatches after the definition is folded — the create-then-execute ordering is
    /// resolved by the single StreamProcessor without any per-call session.
    pub async fn create_flow(
        &self,
        name: FlowName,
        definition: &str,
    ) -> Result<ObjectReference, ExecutionError> {
        // Validate the definition at the boundary, **before anything is written** (fail fast): a raw
        // definition that doesn't parse as a `StateMachine` is rejected up front, so a malformed flow
        // can never enter the log or Storage. The parsed model is discarded here — the durable record
        // is the raw string; execution parses it on demand via `HandlerContext::machine`.
        if serde_json::from_str::<spica_asl::StateMachine>(definition).is_err() {
            return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "malformed flow definition: does not parse as a StateMachine".to_string(),
            )));
        }

        // Boundary pre-check: `CreateFlow` creates only new names, so refuse an already-existing one
        // **before** registering/awaiting anything. This read acquires the Storage lock per-read and
        // drops it immediately (the StreamProcessor holds it only per-entry), so it never blocks an apply;
        // it is a fast, user-facing guard against the common duplicate-create mistake. The handler
        // re-checks atomically at dispatch time (see `create_flow.rs`), the authoritative serialized
        // point, so this early check is an optimization + clear error, not the enforcement mechanism.
        // NOTE: this is not atomic with the later append — two concurrent `CreateFlow(same_name)`
        // could both pass this read; the handler's in-order check bounds the damage (the loser emits
        // nothing). Fully atomic conflict detection is a TODO (rejected-event ack).
        {
            let storage = self.storage.lock().await;
            if storage.get_flow_by_name(name.clone()).await?.is_some() {
                return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                    format!("flow {name} already exists"),
                )));
            }
        }

        // An opaque request id keys this operation's ack. The identities (the version's
        // `ObjectReference`, which this method returns) are minted by the handler and echoed back on the
        // `FlowVersionCreated` event, so we learn them only once the ack lands — registration still
        // happens **before** the append (see `register_ack`): the long-lived StreamProcessor can only emit
        // `FlowVersionCreated` after it reads the appended `CreateFlow`, so an early register
        // guarantees we never miss our ack.
        let request_id = RequestId::new();
        let rx = self.register_ack(request_id).await;
        self.log
            .append(vec![Entry {
                // `stream_id` is a placeholder — the log stamps its own id at append (a log is one
                // stream), so the engine never chooses a stream.
                stream_id: StreamId::nil(),
                entry_id: EntryId::nil(), // placeholder — the log assigns the position.
                cause_id: None,
                timestamp: Timestamp::now(),
                payload: EntryPayload::Command(Command::CreateFlow {
                    request_id,
                    name: name.clone(),
                    definition: definition.to_owned(),
                }),
            }])
            .await?;

        // Wait for the acknowledgement that this definition is durably applied — the matching
        // `FlowVersionCreated` event (the one the StreamProcessor routes a CreateFlow ack on) delivered to
        // our own channel — and read back the handler-minted version reference (the identity
        // executions bind to). The await is the ordering guarantee that the definition is folded into
        // Storage before `create_flow` returns (so a later `start_for_revision` always dispatches
        // after it) — see the method-level note. A slack path: if the handler refuses the command
        // (e.g. a forged duplicate `CreateFlow` sneaks past the boundary pre-check), it emits a
        // `Reject` instead of `FlowVersionCreated`, and we surface that as
        // [`ExecutionError::Rejected`] rather than hanging or mis-reading a non-event.
        let event = match Self::await_ack(rx).await {
            Ok(event) => event,
            Err(failure) => return Err(Self::ack_failure_error(failure)),
        };
        let Event::FlowVersionCreated { flow_version, .. } = event else {
            unreachable!(
                "ack registry routes CreateFlow's ack only to a FlowVersionCreated event; got {event:?}"
            );
        };
        tracing::info!(name = %name, flow_version = %flow_version.reference(), version = flow_version.version, "flow created, acked");
        Ok(flow_version.reference())
    }

    /// Aborts a running execution by appending `TerminateExecution{Cancelled}` to the log. The
    /// StreamProcessor drives the unwind: `ExecutionTerminating`, then child cleanup, then
    /// `ExecutionTerminated{Cancelled}`. The termination handler resolves the target by `name` (with
    /// an optional `uid` incarnation guard) from Storage (not by stream), so the appended entry needs
    /// no stream chosen here — the log stamps its own id at append.
    pub async fn terminate(
        name: ObjectName,
        uid: Option<ulid::Ulid>,
        logstream: &(impl LogStream<EntryPayload> + ?Sized),
    ) -> Result<(), ExecutionError> {
        logstream
            .append(vec![Entry {
                // Stream placeholder — the log stamps its own id at append.
                stream_id: StreamId::nil(),
                entry_id: crate::types::id::EntryId::nil(), // placeholder — the log assigns the position.
                cause_id: None,
                timestamp: Timestamp::now(),
                payload: EntryPayload::Command(Command::TerminateExecution {
                    name,
                    uid,
                    reason: crate::types::command::TerminationReason::Cancelled,
                }),
            }])
            .await?;
        Ok(())
    }

    /// Resolve the latest created version's [`ObjectReference`] under `name` from the **persisted**
    /// Storage projection. Used by [`resolve_version_id`](Self::resolve_version_id) (version `0` =
    /// latest); a past-state lookup, so it does not go through the future-awaiting
    /// [`await_ack`](Self::await_ack).
    ///
    /// This read is safe while the long-lived StreamProcessor runs: it acquires the Storage lock **per**
    /// entry, releasing it between entries (see the `storage` field doc), so this read never issues a
    /// blocking write-hold on the StreamProcessor's applies.
    async fn latest_version(&self, name: &FlowName) -> Result<ObjectReference, ExecutionError> {
        let storage = self.storage.lock().await;
        // The owning `Flow` keeps an O(1) counter of its newest ordinal; resolve that ordinal to the
        // concrete version row (keyed by `{flow_name}-{version}`), then to its reference.
        let flow = storage
            .get_flow_by_name(name.clone())
            .await?
            .ok_or_else(|| {
                ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                    "flow {name} has no created version"
                )))
            })?;
        storage
            .flow_version_of(name.clone(), flow.latest_version)
            .await?
            .map(|ver| ver.reference())
            .ok_or_else(|| {
                ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                    "flow {name} has no version {}",
                    flow.latest_version
                )))
            })
    }

    /// Starts an execution against a **specific created version** (`flow_version` — an
    /// [`ObjectReference`] naming the version's `object_name`+`uid`), with no Task handlers beyond
    /// those the Engine was booted with. The definition must already be created.
    ///
    /// The Engine's single long-lived StreamProcessor drives the execution; this method mints an opaque
    /// [`RequestId`], **registers** a one-shot acknowledgement channel under it, **appends** the
    /// `CreateExecution` command (carrying that `request_id`), then awaits the birth
    /// [`Event::ExecutionCreated`] on its own channel, returning the new [`ExecutionId`]. It does
    /// **not** wait for the execution to settle — terminal state (success output or failure reason)
    /// is observed by the caller via [`Engine::wait_for_execution`], which polls Storage. Returning
    /// the id at birth (rather than blocking on the terminal event) is what lets a caller issue many
    /// executions and await each at its own pace, and what lets `wait_for_execution` run on the
    /// durable projection alone, even across an Engine restart.
    pub async fn start_for_revision(
        &self,
        name: ObjectName,
        flow_version: ObjectReference,
        input: Value,
    ) -> Result<ObjectReference, ExecutionError> {
        // A running Engine is guaranteed by the type; no "engine not started" guard is needed.

        // Boundary pre-check: `CreateExecution` creates only new names — the name is now the
        // execution's storage primary key (per-scope unique) — so refuse an already-live one
        // **before** registering/awaiting anything. This read acquires the Storage lock per-read and
        // drops it immediately (the StreamProcessor holds it only per-entry), so it never blocks an
        // apply; it is a fast user-facing guard against the common duplicate-start mistake. The
        // handler re-checks atomically at dispatch time (see `create_execution.rs`), the
        // authoritative serialized point — this early check is an optimization + clear error, not the
        // enforcement mechanism. NOTE: not atomic with the later append — two concurrent same-name
        // starts could both pass this read; the handler's in-order check bounds the damage (the loser
        // emits a `Reject` → `ExecutionError::Rejected`).
        {
            let storage = self.storage.lock().await;
            // A name-only probe (nil uid) suffices: storage keys executions by name, so the uid is
            // irrelevant to the read.
            let probe = crate::types::meta::ObjectReference::new(
                crate::types::meta::ObjectKind::Execution,
                name.clone(),
                ulid::Ulid::nil(),
            );
            if storage.get_execution(&probe).await?.is_some() {
                return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                    format!("execution {name} already exists"),
                )));
            }
        }

        // A fresh opaque `request_id` (never the target `execution_id`) keys this operation's ack,
        // registered *before* the `CreateExecution` append (see `register_ack`): the StreamProcessor only
        // emits the echoed `ExecutionCreated` after it applies the appended command, so an early
        // register guarantees we never miss it. The append is inlined here so registration can
        // precede it. The
        // birth event echoes this request id (see `AckRouter`), so — unlike the previous terminal-ack
        // flow — no `execution → request` routing bookkeeping is needed. The machine resolves from
        // Storage (by `flow_version` reference) at dispatch time.
        let request_id = RequestId::new();
        let rx = self.register_ack(request_id).await;
        // The execution's durable `uid` is **not** minted here — the `CreateExecution` handler mints it
        // at dispatch and echoes the finished `Execution` back on `ExecutionCreated`. That stays
        // replay-deterministic because the produced event lands in the same atomic batch as the
        // command (a committed command is conclusive; an aborted one re-dispatches fresh). The caller
        // only ever observes the reference via the awaited birth event below, so no uid is needed up
        // front.
        self.log
            .append(vec![Entry {
                // Stream placeholder — the log stamps its own id at append.
                stream_id: StreamId::nil(),
                entry_id: EntryId::nil(), // placeholder — the log assigns the position.
                cause_id: None,
                timestamp: Timestamp::now(),
                payload: EntryPayload::Command(Command::CreateExecution {
                    request_id,
                    name,
                    flow_version,
                    input,
                }),
            }])
            .await?;

        // Await the execution's *birth* ack on our own channel — the StreamProcessor completes it when it
        // applies `ExecutionCreated`. The call returns the persisted id at once; settling (success
        // or failure) is a separate `wait_for_execution` poll.
        let event = match Self::await_ack(rx).await {
            Ok(event) => event,
            Err(failure) => return Err(Self::ack_failure_error(failure)),
        };
        match event {
            Event::ExecutionCreated { execution, .. } => Ok(execution.reference()),
            _ => unreachable!("await_ack only delivers this request's ExecutionCreated"),
        }
    }

    /// Abort a running execution by issuing a `TerminateExecution{Cancelled}` on the Engine's own
    /// log — the `&self` (running-engine) analogue of the raw-seam free function [`terminate`](Self::terminate),
    /// so the Server's `Arc<Engine>` can cancel by `name` (with an optional `uid` incarnation guard)
    /// without holding a caller-supplied log or knowing the execution's stream up front.
    ///
    /// Non-blocking: appends the command and returns once it is durable; settlement is observed by
    /// polling [`wait_for_execution`](Self::wait_for_execution) / [`execution_status`](Self::execution_status),
    /// which surface it as `Terminated(Cancelled)`. The termination handler resolves the target by
    /// `name` from Storage (not by stream), and the log stamps its own stream id at append — so the
    /// cancellation needs no stream chosen here.
    pub async fn cancel_execution(
        &self,
        name: ObjectName,
        uid: Option<ulid::Ulid>,
    ) -> Result<(), ExecutionError> {
        // The handler ignores which stream the command lands on, resolving the execution by name (and
        // its optional incarnation guard); the log stamps its own stream id at append.
        Self::terminate(name, uid, &*self.log).await
    }

    /// Wait for the execution started by [`start_for_revision`](Self::start_for_revision) to reach a
    /// terminal state, returning its success output or, if it failed, the failure [`ExecutionError`].
    ///
    /// Reads only the persisted Storage projection, so it never awaits a live ack: it works whether
    /// or not the original caller is still alive, and even across an Engine restart. Polls at a
    /// fixed interval (no overall timeout) until the execution lands in a terminal status:
    ///
    /// - `Completed` → the decided [`ExecutionResult`](crate::types::result::ExecutionResult).
    /// - `Terminated(reason)` → `Err(reason.to_execution_error())`.
    /// - `Running`/`Completing`/`Terminating` → keep polling.
    ///
    /// If the execution id is unknown to Storage (never created, or its projection was GC'd) this
    /// returns [`ExecutionError`] immediately rather than polling forever.
    pub async fn wait_for_execution(
        &self,
        execution: &ObjectReference,
    ) -> Result<ExecutionResult, ExecutionError> {
        // Poll the durable projection. The interval keeps contention on the shared Storage lock low
        // (the StreamProcessor releases it between entries — see the `storage` field doc — so this read
        // interleaves cleanly) while still surfacing settlement promptly; a real service would tune
        // it to its tail-latency budget. No overall timeout: an execution that never settles (e.g. a
        // hung external Task) is surfaced by the caller via a separate deadline, not painted on here.
        const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);
        loop {
            let storage = self.storage.lock().await;
            let exec = storage.get_execution(execution).await?.ok_or_else(|| {
                ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                    "execution {execution} has no projection to await (never created or GC'd)"
                )))
            })?;
            drop(storage);
            match &exec.status {
                crate::ExecutionStatus::Completed => {
                    return Ok(ExecutionResult {
                        output: exec.output.clone().unwrap_or(serde_json::Value::Null),
                    });
                }
                crate::ExecutionStatus::Terminated(reason) => {
                    return Err(reason.to_execution_error());
                }
                // Still in flight — poll again after a short pause.
                _ => tokio::time::sleep(POLL_INTERVAL).await,
            }
        }
    }

    /// Resolve the persisted [`ObjectReference`] for `(name, version)` **without blocking on settlement**
    /// or awaiting any live ack: `version == 0` selects the latest created version (the "latest"
    /// convention), any other `version` resolves through the flow's `(name, version)` index.
    ///
    /// This is the non-awaiting *resolution* step a caller performs before
    /// [`start_for_revision`](Self::start_for_revision), exposed so the Server's `StartExecution` can
    /// bind a revision by name+version and still return the execution id at birth — settling is left
    /// to the client's `GetExecution` poll rather than blocked here.
    pub async fn resolve_version_id(
        &self,
        name: FlowName,
        version: u32,
    ) -> Result<ObjectReference, ExecutionError> {
        // Version 0 is the server's "latest" convention.
        if version == 0 {
            return self.latest_version(&name).await;
        }
        let storage = self.storage.lock().await;
        // The version is keyed by the flow's own name + ordinal (its sole identity), so a (missing)
        // flow and a (missing) version both surface as a version lookup miss.
        let ver = storage
            .flow_version_of(name.clone(), version)
            .await?
            .ok_or_else(|| {
                ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                    "flow {name} has no version {version}"
                )))
            })?;
        Ok(ver.reference())
    }

    /// Read a **non-blocking** snapshot of an execution's current state from the persisted
    /// projection.
    ///
    /// This is the read half of [`wait_for_execution`](Self::wait_for_execution): instead of polling
    /// internally until terminal, it returns immediately with wherever the execution is right now
    /// (`Running`/`Completing`/`Completed`/`Terminating`/`Terminated`), letting the caller poll at
    /// its own cadence. Returns `Ok(None)` when no projection exists for `execution_id` (never
    /// created, or GC'd) — which `wait_for_execution` would turn into an error — so the caller can
    /// distinguish "unknown" from "in flight".
    pub async fn execution_status(
        &self,
        execution: &ObjectReference,
    ) -> Result<Option<crate::types::result::ExecutionStatusSnapshot>, ExecutionError> {
        let storage = self.storage.lock().await;
        let Some(exec) = storage.get_execution(execution).await? else {
            return Ok(None);
        };
        // A `Terminated` execution reports its failure exactly like `wait_for_execution` does — the
        // reason carries the error name + output surfaced to the remote client.
        let (error_name, error_output) = match &exec.status {
            crate::ExecutionStatus::Terminated(reason) => {
                let err = reason.to_execution_error();
                (Some(err.error_name().to_string()), err.error_output())
            }
            _ => (None, None),
        };
        Ok(Some(crate::types::result::ExecutionStatusSnapshot {
            status: exec.status.clone(),
            output: exec.output.clone(),
            error_name,
            error_output,
        }))
    }
}

/// The engine-hosted inbound job API a worker talks to (Zeebe's job gRPC API, locally).
///
/// Every method funnels the worker's claim/settle through the engine's controlled write entry — an
/// ordered inbound `Command` appended to the log, later read back and dispatched by the
/// StreamProcessor, which is the authoritative single writer that validates each transition (e.g.
/// `CompleteTask` is only honored for the leasing worker). The worker never writes the log itself.
#[async_trait::async_trait]
impl TaskApi for EngineInner {
    async fn poll_tasks(
        &self,
        worker_id: &str,
        resource: &str,
        max_tasks: usize,
        lease_seconds: u64,
    ) -> Result<Vec<ActivatedTask>, ExecutionError> {
        // Read-first gate (finding #12): an idle poll — nothing claimable **right now** — is a strict
        // idempotent pure query and must not write a durable `ClaimTasks` entry (the worker busy-polls
        // every few ms, so idle polls would otherwise bury the causal chain in no-op commands). This
        // discovery is best-effort: it only decides whether to bother appending; the authoritative
        // allotment still happens in the serialized dispatch. A task that appeared since this read is
        // picked up by the next poll; one that raced away only costs a single empty command write. A
        // discovery read failure likewise means "nothing claimable now" — the worker just polls again.
        let claimable_now = {
            let storage = self.storage.lock().await;
            match storage.activatable_tasks(resource, max_tasks).await {
                Ok(t) => {
                    let now = Timestamp::now();
                    t.into_iter()
                        .any(|t| !t.retry_state.next_available_at.is_some_and(|at| now < at))
                }
                Err(_) => false,
            }
        };
        if !claimable_now {
            return Ok(Vec::new());
        }
        // Defer allocation to the StreamProcessor: append ONE `ClaimTasks` command, whose handler
        // (running in the serialized, lock-holding command arm) discovers `Pending` tasks of
        // `resource`, leases each to this worker, and returns the granted set via our ack channel.
        // This is the fix that removes the old allocation-at-API-time bug — a projection read + one
        // `ClaimTasks` raced concurrent pulls (TOCTOU) and could allocate already-cancelled
        // tasks. Now allocation is decided in the same snapshot the fold writes, exactly-once-wise,
        // and the grant travels the same "register an ack, then await its channel" path every other
        // blocking Engine method uses. Register our one-shot *before* appending so the handler's
        // delivery is guaranteed to find a channel.
        let request_id = RequestId::new();
        let rx = self.register_ack(request_id).await;
        self.append_command(Command::ClaimTasks {
            request_id,
            worker_id: worker_id.to_string(),
            resource: resource.to_string(),
            max_tasks,
            lease_seconds,
        })
        .await?;
        match Self::await_ack_tasks(rx).await {
            Ok(tasks) => Ok(tasks),
            Err(failure) => Err(Self::ack_failure_error(failure)),
        }
    }

    async fn complete(
        &self,
        worker_id: &str,
        task: ObjectName,
        request_id: RequestId,
        output: Value,
    ) -> Result<(), ExecutionError> {
        // A request/response report (mirroring `poll_tasks` below): register a one-shot ack under the
        // worker-supplied `request_id` *before* appending, append the `CompleteTask` command, then
        // await the echoed outcome. The worker learns whether its settlement was actually applied
        // (the `TaskCompleted` matching its id) or refused (a `Reject`), instead of fire-and-forget.
        let rx = self.register_ack(request_id).await;
        // The worker addresses a task by its **canonical name** (finding #13), so build the
        // `ObjectReference` straight from it — `uid` is irrelevant to the name-keyed lookup, so carry
        // nil. No re-derivation from a uid needed (the old `for_uid`/`child-<uid>` bridge is gone).
        let task_ref = ObjectReference::new(
            crate::types::meta::ObjectKind::Task,
            task,
            ulid::Ulid::nil(),
        );
        self.append_command(Command::CompleteTask {
            request_id,
            task: task_ref,
            worker_id: worker_id.to_string(),
            output,
        })
        .await?;
        match Self::await_ack(rx).await {
            // The awaited `TaskCompleted` was applied — the settlement is durable. Its payload is
            // not needed here (the task's terminal state is already projected into Storage).
            Ok(_) => Ok(()),
            Err(failure) => Err(Self::ack_failure_error(failure)),
        }
    }

    async fn fail(
        &self,
        worker_id: &str,
        task: ObjectName,
        error: ExecutionError,
    ) -> Result<(), ExecutionError> {
        // Same name→`ObjectReference` bridge as `complete` (see above, finding #13).
        let task_ref = ObjectReference::new(
            crate::types::meta::ObjectKind::Task,
            task,
            ulid::Ulid::nil(),
        );
        self.append_command(Command::FailTask {
            task: task_ref,
            worker_id: worker_id.to_string(),
            error,
        })
        .await
    }
}

impl EngineInner {
    /// The engine's controlled write entry for an **inbound worker report**: append one command
    /// (a claim/complete/fail) to the log, enveloped like `trigger_timer` (placeholders; the log
    /// stamps position + its own stream id). `cause_id` is `None` — the report has no causal parent
    /// in the log — but the resume watermark still advances correctly: the handler output causally
    /// re-links to the command's *own* entry id, so applying its events raises the watermark past
    /// this entry. Single-writer preserved: the append is the one ordered funnel.
    async fn append_command(&self, command: Command) -> Result<(), ExecutionError> {
        // `?` discards the log-stamped `EntryId`; the append's `LogError` maps into `ExecutionError`.
        self.log
            .append(vec![Entry {
                stream_id: StreamId::nil(), // the log stamps its own id at append.
                entry_id: EntryId::nil(),   // placeholder — the log assigns the real position.
                cause_id: None,             // worker-initiated: no causal parent.
                timestamp: Timestamp::now(),
                payload: EntryPayload::Command(command),
            }])
            .await?;
        Ok(())
    }
}
