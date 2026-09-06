//! The engine's **consumer layer** in spica-server.
//!
//! The engine is append + observe only ([`Engine::append_command`] + an injected [`Hook`]); the
//! blocking request/response convenience API — `create_flow`, `start_for_revision`, and the task
//! `poll_tasks`/`complete`/`fail` — lives here as a consumer. [`AckHook`] is the [`Hook`] the server
//! injects at boot: it correlates an awaited command's outcome to its awaiting caller via the echoed
//! `request_id` (Zeebe's `requestId` → future model, owned by the consumer rather than the engine).
//! [`Facade`] wraps the engine + [`AckHook`] and exposes the same shapes the engine's blocking API
//! used to, so the tonic service handlers are thin forwarders.

use std::collections::HashMap;
use std::sync::Arc;

use spica_engine::{
    ActivatedTask, Command, Engine, Event, ExecutionError, FlowName, Hook, ObjectKind, ObjectName,
    ObjectReference, Reject, RequestId, RuntimeError, Task, Timestamp,
};
use spica_scheduler::{Scheduler, TimerSink};
use tokio::sync::{Mutex, oneshot};

/// The awaited outcome event variant a pending entry expects (see [`AckHook`]). Gating on the variant
/// matters because a `CreateFlow` emits **two** request-id-bearing events (`FlowCreated`, then
/// `FlowVersionCreated`) sharing one `request_id`; only the awaited variant must wake its caller.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckTarget {
    FlowVersionCreated,
    ExecutionCreated,
    TaskCompleted,
    Grant,
}

/// The payload an acknowledgement delivers to an awaiting operation: the applied [`Event`] (boxed — an
/// `Event` is large and this is a one-per-awaited-op send), a [`Reject`] if the command was refused, or
/// the granted task set of a `ClaimTasks` poll.
pub(crate) enum AckOutcome {
    Applied(Box<Event>),
    Rejected(Reject),
    Granted(Vec<ActivatedTask>),
}

/// Failure modes of awaiting an acknowledgement (the other side of [`AckOutcome`]).
enum AckFailure {
    Dropped,
    Rejected(Reject),
}

/// The correlation registry: expected outcome per in-flight `request_id`.
type PendingAck = HashMap<RequestId, (AckTarget, oneshot::Sender<AckOutcome>)>;

/// The observer injected into the engine via [`EngineBuilder`](spica_engine::EngineBuilder)`::with_hook`:
/// correlates observed applied events / rejections to awaiting callers by the echoed
/// `request_id`. `register` must precede the command's append — the processor only learns of a command
/// (and thus arrives at its ack) *after* the append lands, so an early register guarantees the ack is
/// never missed.
#[derive(Clone)]
pub(crate) struct AckHook {
    pending: Arc<Mutex<PendingAck>>,
}

impl AckHook {
    pub(crate) fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a one-shot receiver keyed by an opaque `request_id` (never the target entity id), so
    /// concurrent operations on the same flow/execution never alias in the registry.
    pub(crate) async fn register(
        &self,
        key: RequestId,
        target: AckTarget,
    ) -> oneshot::Receiver<AckOutcome> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(key, (target, tx));
        rx
    }

    /// The `request_id` an event echoes and the [`AckTarget`] it resolves, when the variant is an
    /// awaited outcome. `FlowCreated` (which shares a `CreateFlow`'s request_id) is deliberately
    /// **not** an awaited target.
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

    /// Rebuild the worker-facing grant set from a durable `TasksClaimed`. The `Task` records the
    /// event embeds are the *discovery-time* snapshots the handler leased (status/worker/lease set on
    /// it), so the awaiting poll gets the exact set decided at dispatch — no storage re-read, hence
    /// no racing-resolves-to-a-different-set hazard.
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

#[tonic::async_trait]
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
        // A Grant is answered by the durable `TasksClaimed` itself (`granted_from` rebuilds the exact
        // discovery-time set); every other awaited outcome resolves as Applied. A Grant target only
        // ever aligns with a `TasksClaimed` — anything else is a wiring mismatch, dropped.
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

/// The blocking convenience facade over an [`Engine`] — the consumer that owns acknowledgement
/// correlation and the request/response shapes removed from the engine. Every write funnels through
/// [`Engine::append_command`]; the engine stays the single writer.
#[derive(Clone)]
pub(crate) struct Facade {
    engine: Arc<Engine>,
    ack: Arc<AckHook>,
}

/// The engine slot a [`TimerSink`] fires through once booted (see [`build_observer`]). Held as a
/// **weak** slot: it is owned by the observer chain the engine itself holds (hook → scheduler →
/// sink), so a strong reference there would keep the engine alive forever.
type EngineSlot = Arc<Mutex<Option<std::sync::Weak<Engine>>>>;

/// Build the composite [`Hook`] injected at boot and the sink slot the booted engine fills in. The
/// engine no longer holds a scheduler — timer scheduling is consumer-owned: the hook re-derives
/// physical arms/cancels from durable `TimerActivated`/`TimerCancelled` events, and the attached
/// [`TimerSink`] routes an expired timer's `TriggerTimer` back through the engine's append path (the
/// slot closes the boot-time cycle: the sink needs the engine, the engine needs the hook).
pub(crate) fn build_observer(
    ack: Arc<AckHook>,
    scheduler: Arc<dyn Scheduler>,
) -> (Arc<dyn Hook>, EngineSlot) {
    let engine_slot: EngineSlot = Arc::new(Mutex::new(None));
    scheduler.attach_sink(Arc::new(EngineTimerSink {
        engine: engine_slot.clone(),
    }));
    (Arc::new(CompositeHook { ack, scheduler }), engine_slot)
}

/// The composite [`Hook`]: forwards the acknowledgement facts to [`AckHook`] and re-derives the
/// physical timer schedule from the durable timer events (the engine declares no effects in band).
struct CompositeHook {
    ack: Arc<AckHook>,
    scheduler: Arc<dyn Scheduler>,
}

#[tonic::async_trait]
impl Hook for CompositeHook {
    async fn on_event_applied(&self, event: &Event) {
        match event {
            Event::TimerActivated { timer } => {
                // The durable event carries the timer's absolute deadline; re-arm from that moment.
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

/// The [`TimerSink`] attached to the scheduler: routes an expired timer's `TriggerTimer` into the
/// engine's append path. Holds the engine behind a slot filled at boot (a fire happens only
/// post-start, so by then the slot is set).
struct EngineTimerSink {
    engine: EngineSlot,
}

#[tonic::async_trait]
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

impl Facade {
    pub(crate) fn new(engine: Arc<Engine>, ack: Arc<AckHook>) -> Self {
        Self { engine, ack }
    }

    /// Await an event-acking operation's outcome on `rx`, yielding the applied event.
    async fn await_event(rx: oneshot::Receiver<AckOutcome>) -> Result<Box<Event>, AckFailure> {
        match rx.await {
            Ok(AckOutcome::Applied(ev)) => Ok(ev),
            Ok(AckOutcome::Rejected(reject)) => Err(AckFailure::Rejected(reject)),
            // `await_event` is only reached by event-acking commands; a grant is awaited via `await_tasks`.
            Ok(AckOutcome::Granted(_)) => {
                unreachable!("a task grant is awaited via await_tasks, not await_event")
            }
            Err(_) => Err(AckFailure::Dropped),
        }
    }

    /// Await a `ClaimTasks` poll's granted task set on `rx`.
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

    /// Map an [`AckFailure`] into the [`ExecutionError`] surfaced to a caller: a dropped channel is an
    /// engine/StreamProcessor liveness problem; a rejected command becomes [`ExecutionError::Rejected`].
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

    /// Create a new flow version from `definition` and return its created version's [`ObjectReference`].
    /// Reproduces the engine's former boundary: definition validated up front, duplicate name pre-checked
    /// (the handler re-checks atomically at dispatch — the authoritative, serialized point).
    pub(crate) async fn create_flow(
        &self,
        name: FlowName,
        definition: &str,
    ) -> Result<ObjectReference, ExecutionError> {
        // Fail fast: a definition that doesn't parse can never enter the log or Storage.
        if serde_json::from_str::<spica_engine::StateMachine>(definition).is_err() {
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
        tracing::info!(name = %name, flow_version = %flow_version.reference(), version = flow_version.version, "flow created, acked");
        Ok(flow_version.reference())
    }

    /// Start an execution against `flow_version`, returning the execution's id **at birth** (settling
    /// is observed by polling the projection, never by blocking here).
    pub(crate) async fn start_for_revision(
        &self,
        name: ObjectName,
        flow_version: ObjectReference,
        input: serde_json::Value,
    ) -> Result<ObjectReference, ExecutionError> {
        // Boundary pre-check: the name is the execution's storage primary key (per-scope unique); the
        // handler re-checks atomically at dispatch (authoritative).
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

    /// Pull up to `max_tasks` claimable tasks of `resource` for `worker_id`, leasing each
    /// `lease_seconds`. An idle poll (nothing claimable right now) is a pure read and appends nothing.
    pub(crate) async fn poll_tasks(
        &self,
        worker_id: &str,
        resource: &str,
        max_tasks: usize,
        lease_seconds: u64,
    ) -> Result<Vec<ActivatedTask>, ExecutionError> {
        // Read-first gate (a busy-poller's idle polls must not bury the causal chain in no-op write
        // commands). Best-effort: it only decides whether to bother appending; the authoritative
        // allotment still happens in the serialized dispatch.
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

    /// Report a task completed with `output`; returns when the settlement's outcome is durable.
    pub(crate) async fn complete(
        &self,
        worker_id: &str,
        task: ObjectName,
        request_id: RequestId,
        output: serde_json::Value,
    ) -> Result<(), ExecutionError> {
        let rx = self
            .ack
            .register(request_id, AckTarget::TaskCompleted)
            .await;
        // The worker addresses a task by its canonical name; the `uid` is nil by convention (the
        // lookup is name-keyed).
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
            // The awaited `TaskCompleted` was applied — the settlement is durable.
            Ok(_) => Ok(()),
            Err(failure) => Err(Self::ack_failure_error(failure)),
        }
    }

    /// Report a task failed with `error`. Mirrors `complete`'s name→`ObjectReference` bridge.
    pub(crate) async fn fail(
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
