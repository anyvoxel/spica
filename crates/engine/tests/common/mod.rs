//! Shared helpers for the engine integration test suites.
//!
//! The one-shot `Engine::run` / `Engine::run_with_task_handlers` conveniences were removed together
//! with the in-process CLI (they had no production callers once `spica` became a pure-remote client
//! and `spica-server` drove the explicit lifecycle). Tests that still want a one-shot "create an
//! anonymous flow and run it" therefore reproduce that sequence here, built only on the retained
//! public API — `Engine::start` → `create_flow` → `start_for_revision` → `wait_for_execution` — so
//! the helpers exercise the same canonical path a server would.

#![allow(dead_code)] // a given suite uses only some helpers; that is expected of a shared module

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use spica_asl::StateMachine;
use spica_client::worker::{
    ClaimedTask, InMemoryTaskService, TaskApi as WorkerTaskApi, TaskApiError, TaskFailure,
    TaskHandler, TaskService,
};
use spica_engine::{
    ActivatedTask, Command, Engine, EngineBuilder, Entry, EntryId, EntryPayload, Event,
    ExecutionError, ExecutionResult, FlowName, Hook, LogStream, ObjectKind, ObjectName,
    ObjectReference, PlainName, Reject, RequestId, RuntimeError, StreamId, Task, Timestamp,
};
use spica_scheduler::{InMemoryScheduler, Scheduler, TimerSink};
use spica_storage::InMemoryStorage;
use tokio::sync::{Mutex, oneshot};
use tokio_util::sync::CancellationToken;

/// An in-process adapter presenting the engine's inbound [`spica_engine::TaskApi`] as the
/// worker-facing [`spica_client::worker::TaskApi`], so an [`InMemoryTaskService`] can be driven against
/// a running engine in tests. This is the engine-side half of the boundary, owning the
/// `ClaimedTask ↔ ActivatedTask` and `TaskFailure ↔ ExecutionError` mappings — the mirror image of
/// `GrpcTaskApi`, which owns the wire half. It lives in test code (not a crate) because it is the only
/// place the engine-linked worker contract and the engine's own inbound trait meet in-process.
pub(crate) struct EngineTaskApi {
    inner: Arc<dyn spica_engine::TaskApi>,
}

impl EngineTaskApi {
    pub(crate) fn new(inner: Arc<dyn spica_engine::TaskApi>) -> Self {
        Self { inner }
    }

    /// Reconstruct an engine [`spica_engine::ObjectName`] from the worker's scalar String task name
    /// (its canonical name, as returned by `poll_tasks`).
    fn task_name(s: &str, op: &str) -> Result<spica_engine::ObjectName, TaskApiError> {
        spica_engine::ObjectName::from_parsed(s)
            .map_err(|_| TaskApiError(format!("{op}: invalid task name: {s:?}")))
    }

    /// Reconstruct an engine [`spica_engine::RequestId`] from the worker-supplied String (its ULID).
    fn request_id(s: &str, op: &str) -> Result<spica_engine::RequestId, TaskApiError> {
        s.parse::<ulid::Ulid>()
            .map(spica_engine::RequestId::from)
            .map_err(|_| TaskApiError(format!("{op}: invalid request_id ULID: {s:?}")))
    }
}

#[async_trait::async_trait]
impl WorkerTaskApi for EngineTaskApi {
    async fn poll_tasks(
        &self,
        worker_id: &str,
        resource: &str,
        max_tasks: usize,
        lease_seconds: u64,
    ) -> Result<Vec<ClaimedTask>, TaskApiError> {
        let tasks = self
            .inner
            .poll_tasks(worker_id, resource, max_tasks, lease_seconds)
            .await
            .map_err(|e| TaskApiError(e.to_string()))?;
        Ok(tasks
            .into_iter()
            .map(|t| ClaimedTask {
                task_name: t.task.as_str().to_string(),
                resource: t.resource,
                arguments: t.arguments,
            })
            .collect())
    }

    async fn complete(
        &self,
        worker_id: &str,
        task_name: &str,
        request_id: &str,
        output: Value,
    ) -> Result<(), TaskApiError> {
        let name = Self::task_name(task_name, "CompleteTask")?;
        let request_id = Self::request_id(request_id, "CompleteTask")?;
        self.inner
            .complete(worker_id, name, request_id, output)
            .await
            .map_err(|e| TaskApiError(e.to_string()))
    }

    async fn fail(
        &self,
        worker_id: &str,
        task_name: &str,
        error: TaskFailure,
    ) -> Result<(), TaskApiError> {
        let name = Self::task_name(task_name, "FailTask")?;
        let TaskFailure { error_name, output } = error;
        // The worker reports ASL error *semantics*; map them onto the engine's `StateFailed`. The
        // `state` field is Display-only and the worker can't know it, so it stays empty — Retry/Catch
        // match on `error_name`/`output`, never on `state`.
        let exec_err = ExecutionError::Runtime(RuntimeError::StateFailed {
            state: String::new(),
            error: error_name,
            output: Box::new(output),
        });
        self.inner
            .fail(worker_id, name, exec_err)
            .await
            .map_err(|e| TaskApiError(e.to_string()))
    }
}

/// A builder wired to in-memory log + storage backends — the M1 test default. `EngineBuilder` itself
/// no longer fabricates backends (it takes caller-supplied trait objects; see
/// [`EngineBuilder::with_backends`](spica_engine::EngineBuilder::with_backends)), so the in-memory
/// log/store pair is assembled here from the seam crates and injected. A scheduler is **not** part of
/// `EngineBuilder` anymore — the consumer owns timer scheduling (see [`LocalClient::start`], which
/// builds the scheduler + sink around the booted engine).
pub fn in_memory_builder() -> EngineBuilder {
    EngineBuilder::with_backends(
        Box::new(spica_engine::InMemoryLogStream::<EntryPayload>::new()),
        Box::new(InMemoryStorage::new()),
    )
}

/// Seed a `CreateExecution` command directly onto a caller-supplied log — the raw CCES seam that
/// used to be `Engine::submit`. Raw-seam drivers (which build their own log + StreamProcessor and never
/// call [`EngineBuilder::start`](spica_engine::EngineBuilder::start)) still need to mint a birth
/// command, so the removed free function's body lives here, built only on the public log/entry
/// types. The seeded execution uses a generated name (the raw seam has no user name to supply); the
/// handler mints the durable `uid` at dispatch, so the seed is fire-and-forget — callers read the
/// resulting rows by stream position, never by a pre-known reference. (There is no per-execution
/// stream — a LogStream is one stream, so stream identity lives on the log, not the caller.)
pub async fn submit_seed(
    flow_version: ObjectReference,
    input: Value,
    logstream: &(impl LogStream<EntryPayload> + ?Sized),
) -> Result<(), ExecutionError> {
    // A static base + u64 suffix is never invalid.
    let name = PlainName::new("seed")
        .expect("static literal is a valid segment")
        .generated_from_key(ulid::Ulid::new().0 as u64);
    logstream
        .append(vec![Entry {
            stream_id: StreamId::nil(), // placeholder — the log stamps the stream on append.
            entry_id: EntryId::nil(),   // placeholder — the log assigns the position on append.
            cause_id: None,
            timestamp: Timestamp::now(),
            payload: EntryPayload::Command(Command::CreateExecution {
                // Seed is fire-and-forget: nothing awaits this execution's terminal ack, so we mint
                // a throwaway request id (no registry entry routes to it).
                request_id: RequestId::new(),
                name,
                flow_version,
                input,
            }),
        }])
        .await?;
    Ok(())
}

/// Create `sm` under an anonymous name and run one execution with `input` against `builder`'s own
/// backends — the explicit equivalent of the removed `Engine::run`. Consumes `builder` (starting it
/// boots the one long-lived StreamProcessor) and, on completion, drops the running `Engine`, so the
/// result is the single execution's output. **This does not call `Engine::stop`** — the StreamProcessor
/// task is left to be torn down when the engine drops, which suits a one-shot test run.
pub async fn create_and_run(
    builder: EngineBuilder,
    sm: StateMachine,
    input: Value,
) -> Result<ExecutionResult, ExecutionError> {
    create_and_run_with_handlers(builder, sm, input, HashMap::new()).await
}

/// Like [`create_and_run`], but boots the engine with `task_handlers` (the handlers are fixed for
/// the engine's lifetime) — the explicit equivalent of the removed `Engine::run_with_task_handlers`.
pub async fn create_and_run_with_handlers(
    builder: EngineBuilder,
    sm: StateMachine,
    input: Value,
    task_handlers: HashMap<String, Arc<dyn TaskHandler>>,
) -> Result<ExecutionResult, ExecutionError> {
    // `LocalClient::start` injects a local `AckHook` (so the blocking calls below can await their
    // outcomes) and boots the single long-lived StreamProcessor.
    let client = LocalClient::start(builder).await?;

    // The worker is a separate role (no longer spawned by the engine): boot it against the client's
    // inbound TaskApi (adapted to the worker-facing trait), and own its lifecycle here — cancel it
    // before returning so the strong `Arc` (a reference to the engine's inner state) it holds is
    // released as the engine drops.
    let cancel = CancellationToken::new();
    let worker = {
        let api = Arc::new(EngineTaskApi::new(Arc::new(client.clone())));
        let cancel = cancel.clone();
        let service = InMemoryTaskService::spawn(task_handlers);
        tokio::spawn(async move { service.run(api, cancel).await })
    };

    // Persist the definition the way a string-supplying caller would — the durable record is the raw
    // string, never the transient struct.
    let definition = serde_json::to_string(&sm).map_err(|e| {
        ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
            "serialize state machine: {e}"
        )))
    })?;
    let flow_version = client.create_flow(anonymous_name(), &definition).await?;
    let execution_id = client
        .start_for_revision(execution_name(), flow_version, input)
        .await?;
    let result = client.wait_for_execution(&execution_id).await;

    // Shut the worker down before the engine drops (worker-precedes-engine, the `task_api` contract).
    cancel.cancel();
    let _ = worker.await;
    result
}

/// A throwaway [`FlowName`] so each one-shot run never collides with a user-created flow; the
/// name's charset (`[A-Za-z0-9_]`) admits the `anon_` + ULID form.
pub fn anonymous_name() -> FlowName {
    FlowName::new(&format!("anon_{}", ulid::Ulid::new()))
        .expect("a ULID-suffixed anonymous name always satisfies FlowName's charset")
}

/// A throwaway, collision-free execution name for tests that don't care about the (now required)
/// user-supplied execution name. Uses the plain (**user**) form — a `CreateExecution` execution is
/// user-named by contract, and a generated child (e.g. the ExecutionTimeout timer) derives its own
/// name from this *plain* base, so it must not itself be generated. The random `_<ulid>` tail keeps
/// it collision-free without `-`.
pub fn execution_name() -> ObjectName {
    ObjectName::plain(&format!("run_{}", ulid::Ulid::new()))
        .expect("a ULID-suffixed user name is always valid")
}

// ── local blocking client ───────────────────────────────────────────────────────
//
// The engine is append + observe only; the blocking request/response API (`create_flow`, start,
// task poll/settle) now lives in the consumer (spica-server). Integration tests reimplement that
// consumer here as a `LocalClient`: an `AckHook` observer injected at boot correlates an awaited
// command's outcome to its caller via the echoed `request_id`, and `LocalClient` exposes the same
// shapes the engine's blocking API used to. `LocalClient` derefs to the `Engine` so the retained
// read/lifecycle methods (`wait_for_execution`, `stop`, `get_object`, …) resolve unchanged.

/// The awaiting outcome-event variant a pending entry expects. Gating on the variant matters because
/// a `CreateFlow` emits two request-id-bearing events (`FlowCreated`, then `FlowVersionCreated`)
/// sharing one `request_id`; only the awaited variant may wake its caller.
#[derive(Clone, Copy, PartialEq, Eq)]
enum AckTarget {
    FlowVersionCreated,
    ExecutionCreated,
    TaskCompleted,
    Grant,
}

/// The payload an acknowledgement delivers: the applied [`Event`], a [`Reject`], or a granted task set.
enum AckOutcome {
    Applied(Box<Event>),
    Rejected(Reject),
    Granted(Vec<ActivatedTask>),
}

/// Failure modes of awaiting an acknowledgement.
enum AckFailure {
    Dropped,
    Rejected(Reject),
}

/// The [`Hook`] this client injects into the engine: correlates applied events / rejections / grants
/// to awaiting callers by the echoed `request_id`. `register` must precede the command's append.
struct AckHook {
    pending: Mutex<HashMap<RequestId, (AckTarget, oneshot::Sender<AckOutcome>)>>,
}

impl AckHook {
    fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    async fn register(&self, key: RequestId, target: AckTarget) -> oneshot::Receiver<AckOutcome> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(key, (target, tx));
        rx
    }

    /// The `request_id` an event echoes and its awaited [`AckTarget`], when the variant is an awaited
    /// outcome. `FlowCreated` (same `CreateFlow` request_id) is deliberately not an awaited target.
    fn resolves(event: &Event) -> Option<(RequestId, AckTarget)> {
        match event {
            Event::FlowVersionCreated { request_id, .. } => {
                Some((*request_id, AckTarget::FlowVersionCreated))
            }
            Event::ExecutionCreated { request_id, .. } => {
                Some((*request_id, AckTarget::ExecutionCreated))
            }
            Event::TaskCompleted { request_id, .. } => {
                Some((*request_id, AckTarget::TaskCompleted))
            }
            Event::TasksClaimed { request_id, .. } => Some((*request_id, AckTarget::Grant)),
            _ => None,
        }
    }

    /// Rebuild the worker-facing grant from a durable `TasksClaimed` (see the server's `AckHook`).
    fn granted_from(tasks: &[Task]) -> Vec<ActivatedTask> {
        tasks
            .iter()
            .map(|t| ActivatedTask {
                task: t.meta.name.clone(),
                resource: t.resource.clone(),
                arguments: t.arguments.clone(),
            })
            .collect()
    }
}

#[async_trait]
impl Hook for AckHook {
    async fn on_event_applied(&self, event: &Event) {
        let Some((request_id, target)) = Self::resolves(event) else {
            return;
        };
        let mut pending = self.pending.lock().await;
        let Some((t, tx)) = pending.remove(&request_id) else {
            return;
        };
        if t != target {
            return;
        }
        // A Grant is answered by the durable `TasksClaimed` itself; every other awaited outcome is
        // Applied. A Grant target only ever aligns with a `TasksClaimed`.
        let outcome = match target {
            AckTarget::Grant => match event {
                Event::TasksClaimed { tasks, .. } => AckOutcome::Granted(Self::granted_from(tasks)),
                _ => return,
            },
            _ => AckOutcome::Applied(Box::new(event.clone())),
        };
        let _ = tx.send(outcome);
    }

    async fn on_command_rejected(&self, request_id: RequestId, reject: &Reject) {
        let mut pending = self.pending.lock().await;
        if let Some((_, tx)) = pending.remove(&request_id) {
            let _ = tx.send(AckOutcome::Rejected(reject.clone()));
        }
    }
}

/// The composite [`Hook`] a [`LocalClient`] injects: routes the acknowledgement facts to its
/// [`AckHook`], and re-derives physical timer arms/cancels from the durable
/// `TimerActivated`/`TimerCancelled` events — the consumer-owned side of timer scheduling (see
/// [`LocalClient::start`]).
struct CompositeHook {
    ack: Arc<AckHook>,
    scheduler: Arc<dyn Scheduler>,
}

#[async_trait]
impl Hook for CompositeHook {
    async fn on_event_applied(&self, event: &Event) {
        match event {
            Event::TimerActivated { timer } => {
                // The durable event carries the timer's absolute deadline; re-arm the physical
                // schedule from that persisted moment.
                self.scheduler.schedule(&timer.reference(), timer.deadline);
            }
            Event::TimerCancelled { timer } => self.scheduler.cancel(&timer.reference()),
            _ => {}
        }
        self.ack.on_event_applied(event).await;
    }

    async fn on_command_rejected(&self, request_id: RequestId, reject: &Reject) {
        self.ack.on_command_rejected(request_id, reject).await;
    }
}

/// The [`TimerSink`] a [`LocalClient`] attaches to its scheduler: routes an expired timer's
/// `TriggerTimer` back into the engine's append path. Holds the engine behind a **weak** slot filled
/// at boot (see [`LocalClient::start`]): the slot is owned by the observer chain the engine itself
/// holds (hook → scheduler → sink), so a strong reference there would keep the engine alive forever
/// and defeat `LocalClient::stop`'s `Arc::try_unwrap`. A fire happens only while the engine is alive
/// (post-start), so the weak upgrade succeeds right up to teardown.
struct EngineTimerSink {
    engine: Arc<Mutex<Option<std::sync::Weak<Engine>>>>,
}

#[async_trait]
impl TimerSink for EngineTimerSink {
    async fn trigger(&self, timer: &ObjectReference) {
        let Some(engine) = self
            .engine
            .lock()
            .await
            .clone()
            .and_then(|weak| weak.upgrade())
        else {
            return; // engine not booted or already dropped; nothing to resume.
        };
        // Fire-and-forget: a dropped append (engine shutting down) is not this consumer's fault.
        let _ = engine
            .append_command(Command::TriggerTimer {
                timer: timer.clone(),
            })
            .await;
    }
}

/// The blocking convenience client over a running [`Engine`] — the engine's removed blocking API,
/// reimplemented as a consumer for integration tests. Derefs to the `Engine` so retained methods
/// resolve unchanged; `create_flow`/`start_for_revision` are inherent, and the task claim/settle API
/// is implemented via [`spica_engine::TaskApi`].
#[derive(Clone)]
pub(crate) struct LocalClient {
    /// The running engine this client drives and reads from.
    pub(crate) engine: Arc<Engine>,
    ack: Arc<AckHook>,
}

impl std::ops::Deref for LocalClient {
    type Target = Engine;

    fn deref(&self) -> &Engine {
        &self.engine
    }
}

impl LocalClient {
    /// Inject the composite observer and boot the engine, then wrap it.
    ///
    /// Timer scheduling is **consumer-owned** (the engine no longer holds a scheduler): this boots
    /// an [`InMemoryScheduler`], injects a comosite [`Hook`] that (a) routes the `AckHook` facts and
    /// (b) re-derives physical timer arms from durable `TimerActivated`/`TimerCancelled` events, and
    /// attaches a [`TimerSink`] that routes an expired timer's `TriggerTimer` back into the engine's
    /// append path.
    pub(crate) async fn start(builder: EngineBuilder) -> Result<Self, ExecutionError> {
        let ack = Arc::new(AckHook::new());
        let scheduler: Arc<dyn Scheduler> = InMemoryScheduler::spawn();
        // The sink can only append once the engine exists, but the engine needs the hook at boot; a
        // fire happens strictly post-start, so a slot filled here after boot closes the gap. Held as
        // a `Weak` to keep the observer chain (hook → scheduler → sink) from keeping the engine alive
        // (see `EngineTimerSink`).
        let engine_slot: Arc<Mutex<Option<std::sync::Weak<Engine>>>> = Arc::new(Mutex::new(None));
        scheduler.attach_sink(Arc::new(EngineTimerSink {
            engine: engine_slot.clone(),
        }));
        let hook = CompositeHook {
            ack: ack.clone(),
            scheduler: scheduler.clone(),
        };
        let engine = Arc::new(builder.with_hook(Arc::new(hook)).start().await?);
        *engine_slot.lock().await = Some(Arc::downgrade(&engine));
        Ok(Self { engine, ack })
    }

    /// Controlled shutdown, forwarding to [`Engine::stop`] when this client is the engine's sole
    /// remaining `Arc` holder (a worker that cloned the engine must be dropped first — the engine's
    /// own `stop` refuses to consume it otherwise).
    pub(crate) async fn stop(self) {
        if Arc::strong_count(&self.engine) != 1 {
            // A sibling `Arc<Engine>` still lives (e.g. a worker cloned it); we cannot consume it.
            // Dropping `self` releases this reference; a cleaner shutdown requires dropping the
            // sibling first.
            return;
        }
        if let Ok(engine) = Arc::try_unwrap(self.engine) {
            engine.stop().await;
        }
    }

    async fn await_event(rx: oneshot::Receiver<AckOutcome>) -> Result<Box<Event>, AckFailure> {
        match rx.await {
            Ok(AckOutcome::Applied(ev)) => Ok(ev),
            Ok(AckOutcome::Rejected(reject)) => Err(AckFailure::Rejected(reject)),
            Ok(AckOutcome::Granted(_)) => {
                unreachable!("a task grant is awaited via await_tasks, not await_event")
            }
            Err(_) => Err(AckFailure::Dropped),
        }
    }

    async fn await_tasks(
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

    fn ack_failure_error(failure: AckFailure) -> ExecutionError {
        match failure {
            AckFailure::Dropped => ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "no acknowledgement received (engine not started, or its StreamProcessor never applied \
                 the awaited outcome)"
                    .to_string(),
            )),
            AckFailure::Rejected(reject) => ExecutionError::Rejected(reject),
        }
    }

    /// Create a new flow version and return its created version's [`ObjectReference`].
    pub(crate) async fn create_flow(
        &self,
        name: FlowName,
        definition: &str,
    ) -> Result<ObjectReference, ExecutionError> {
        // Fail fast: an unparseable definition can never enter the log or Storage.
        if serde_json::from_str::<spica_asl::StateMachine>(definition).is_err() {
            return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "malformed flow definition: does not parse as a StateMachine".to_string(),
            )));
        }
        if self
            .engine
            .get_object(ObjectKind::Flow, &ObjectName::Plain(name.clone()))
            .await?
            .is_some()
        {
            return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                format!("flow {name} already exists"),
            )));
        }
        let request_id = RequestId::new();
        let rx = self
            .ack
            .register(request_id, AckTarget::FlowVersionCreated)
            .await;
        self.engine
            .append_command(Command::CreateFlow {
                request_id,
                name: name.clone(),
                definition: definition.to_owned(),
            })
            .await?;
        let event = match Self::await_event(rx).await {
            Ok(ev) => *ev,
            Err(failure) => return Err(Self::ack_failure_error(failure)),
        };
        let Event::FlowVersionCreated { flow_version, .. } = event else {
            unreachable!(
                "AckHook routes CreateFlow's ack only to a FlowVersionCreated event; got {event:?}"
            );
        };
        Ok(flow_version.reference())
    }

    /// Start an execution against `flow_version`, returning the execution's id at birth.
    pub(crate) async fn start_for_revision(
        &self,
        name: ObjectName,
        flow_version: ObjectReference,
        input: Value,
    ) -> Result<ObjectReference, ExecutionError> {
        // Boundary pre-check: the name is the execution's storage primary key (per-scope unique).
        if self
            .engine
            .get_object(ObjectKind::Execution, &name)
            .await?
            .is_some()
        {
            return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                format!("execution {name} already exists"),
            )));
        }
        let request_id = RequestId::new();
        let rx = self
            .ack
            .register(request_id, AckTarget::ExecutionCreated)
            .await;
        self.engine
            .append_command(Command::CreateExecution {
                request_id,
                name,
                flow_version,
                input,
            })
            .await?;
        let event = match Self::await_event(rx).await {
            Ok(ev) => *ev,
            Err(failure) => return Err(Self::ack_failure_error(failure)),
        };
        match event {
            Event::ExecutionCreated { execution, .. } => Ok(execution.reference()),
            _ => unreachable!("AckHook only delivers ExecutionCreated to this ack"),
        }
    }
}

#[async_trait]
impl spica_engine::TaskApi for LocalClient {
    async fn poll_tasks(
        &self,
        worker_id: &str,
        resource: &str,
        max_tasks: usize,
        lease_seconds: u64,
    ) -> Result<Vec<ActivatedTask>, ExecutionError> {
        // Read-first gate (an idle poll stays a pure query and appends nothing).
        let claimable_now = {
            let now = Timestamp::now();
            match self.engine.activatable_tasks(resource, max_tasks).await {
                Ok(ts) => ts
                    .into_iter()
                    .any(|t| !t.retry_state.next_available_at.is_some_and(|at| now < at)),
                Err(_) => false,
            }
        };
        if !claimable_now {
            return Ok(Vec::new());
        }
        let request_id = RequestId::new();
        let rx = self.ack.register(request_id, AckTarget::Grant).await;
        self.engine
            .append_command(Command::ClaimTasks {
                request_id,
                worker_id: worker_id.to_string(),
                resource: resource.to_string(),
                max_tasks,
                lease_seconds,
            })
            .await?;
        match Self::await_tasks(rx).await {
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
        let rx = self
            .ack
            .register(request_id, AckTarget::TaskCompleted)
            .await;
        let task_ref = ObjectReference::new(ObjectKind::Task, task, ulid::Ulid::nil());
        self.engine
            .append_command(Command::CompleteTask {
                request_id,
                task: task_ref,
                worker_id: worker_id.to_string(),
                output,
            })
            .await?;
        match Self::await_event(rx).await {
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
        let task_ref = ObjectReference::new(ObjectKind::Task, task, ulid::Ulid::nil());
        self.engine
            .append_command(Command::FailTask {
                task: task_ref,
                worker_id: worker_id.to_string(),
                error,
            })
            .await
    }
}
