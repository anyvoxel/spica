use std::ops::Deref;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::hook::Hook;
use crate::log::{Entry, EntryPayload, LogStream, Timestamp};
use crate::query::{QueryListPage, QueryObject, ref_for};
use crate::storage::Storage;
use crate::stream_processor::StreamProcessor;
use crate::types::command::Command;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::id::{EntryId, FlowName, StreamId};
use crate::types::meta::{ObjectKind, ObjectName, ObjectReference};
use crate::types::result::ExecutionResult;
use crate::types::task::Task;

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
/// compiler enforces a linear lifecycle — an unstarted state has **no** `append_command` /
/// `stop` methods (calling them before `start` is a compile error, not a
/// runtime check), an `Engine` cannot be started twice (its builder is already consumed), and a
/// stopped `Engine` is gone (reusing it would replay already-dispatched Commands — see
/// [`Engine::stop`]).
///
/// The backends are chosen once at construction via [`EngineBuilder::with_backends`], which takes
/// the log and storage as type-erased trait objects that the *caller* has already built (see the
/// builder's docs for why the engine no longer fabricates concrete backends itself).
///
/// The running [`Engine`] drives **one long-lived StreamProcessor** for its whole lifetime. That single
/// StreamProcessor tails the log and folds every Command/Event into Storage in strict log order. The
/// engine itself is deliberately thin — it **appends Commands** ([`EngineInner::append_command`]) and
/// **reports durable facts** (applied events, rejections) through the injected
/// [`Hook`](crate::Hook) observer — every response, including a task grant, is a durable event. It does
/// **not** own acknowledgement correlation: waiting for a
/// command's outcome event (`FlowVersionCreated`, `ExecutionCreated`, `TaskCompleted`) is a
/// **consumer concern** — a `Hook` implementation (e.g. the server's `AckHook`) registers a one-shot
/// per request id before appending and is completed by the observer when the matching event is
/// reported (Zeebe's `requestId` model), so any number of operations can be in flight concurrently.
///
/// Definitions are **created** (persisted by id) then **executed by that id**: appending a
/// `CreateFlow` command folds a new immutable [`FlowVersion`](crate::FlowVersion) into Storage;
/// appending `CreateExecution` spawns executions that bind to a created version.
///
/// Execution cancellation is a fire-and-forget append: [`Engine::cancel_execution`] emits
/// `TerminateExecution{Cancelled}` on the engine's own log and returns once durable.
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
    /// The injected [`Hook`] observer the StreamProcessor reports durable facts to. Defaults to a
    /// no-op; callers that want to react (e.g. the server's `AckHook` correlating awaited commands)
    /// inject one via [`with_hook`](Self::with_hook). The engine itself never fabricates a concrete
    /// observer — it only publishes observations.
    hook: Arc<dyn Hook>,
}

/// Executes ASL state machines via the CCES architecture (Causal Command Event Sourcing).
///
/// A running `Engine` — obtained **only** from [`EngineBuilder::start`], which consumes the
/// unstarted builder (see the module docs for why this makes misuse a compile error).
///
/// `Engine` is a thin **single-owner wrapper** over `Arc<EngineInner>`. Because `Engine` is deliberately
/// **not** `Clone`, the wrapper remains the one strong reference — so `stop` can `Arc::try_unwrap`
/// it by construction. Every operation method lives on [`EngineInner`] and is reached through this
/// wrapper via `Deref` (so `engine.create_flow(..)` still reads naturally).
pub struct Engine {
    /// The running engine's state. `Engine` holds the only strong reference — see the type doc.
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
/// through its `Deref`). All operation methods live here and are reached through [`Engine`]'s `Deref`,
/// so callers keep writing `engine.create_flow(..)`; the type is `pub` only so that `Deref` target does
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
}

/// Handle to the engine's internal [`StreamProcessor`] run loop, so `Engine::stop` can request its
/// controlled shutdown (cancel token) and await the task to drain its current iteration.
struct StreamProcessorTask {
    cancel: CancellationToken,
    handle: tokio::task::JoinHandle<Result<(), ExecutionError>>,
}

/// The default [`Hook`] observer: reports nothing. An engine built without [`EngineBuilder::with_hook`]
/// is append + query only — durable facts are still logged, just not observed by any injected consumer.
pub(crate) struct NoopHook;

#[async_trait::async_trait]
impl Hook for NoopHook {}

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
            hook: Arc::new(NoopHook),
        }
    }

    /// Inject the [`Hook`] observer the StreamProcessor reports durable facts to. Callers that want
    /// to react to applied events / rejections — most often an acknowledgement
    /// correlator (e.g. the server's `AckHook`) or a notification sink — implement [`Hook`] and hand
    /// it over here. Defaults to a no-op when omitted, so an append/query-only engine works without
    /// one. This is the **assembly point**: the engine never fabricates a concrete observer, it only
    /// publishes observations.
    pub fn with_hook(mut self, hook: Arc<dyn Hook>) -> Self {
        self.hook = hook;
        self
    }

    /// Boot the Engine: spawns its **single long-lived StreamProcessor** on these log+storage backends and
    /// returns the running [`Engine`].
    ///
    /// This is the **only** way to obtain an [`Engine`], and it consumes the builder (`self`), so a
    /// given set of backends can be booted at most once and the returned `Engine` is *guaranteed*
    /// running — its `processor` is never `None`.
    ///
    /// Non-blocking: `start` spawns the StreamProcessor task and returns; the StreamProcessor processes appends
    /// as they land. Consumers append a Command (see [`EngineInner::append_command`]) and, when they
    /// await its outcome, correlate the acknowledgement themselves through the injected [`Hook`]; the
    /// engine never coordinates a StreamProcessor session.
    ///
    /// NOTE(recovery+design): one long-lived StreamProcessor is exactly the model the recovery-watermark
    /// TODO in `processor.rs` anticipates — one worker tailing the whole log and applying each entry
    /// once. Persisting that watermark is still TODO; because this method *consumes* the builder the
    /// type cannot express "stop then restart on the same Engine," which is the safe reading of the
    /// old convention (boot once, run many commands, stop once, drop).
    pub async fn start(self) -> Result<Engine, ExecutionError> {
        let mut processor = StreamProcessor::new();
        let log = Arc::clone(&self.log);
        let storage = Arc::clone(&self.storage);
        // Hand the StreamProcessor the injected observer as the `Hook` it reports facts to. The
        // engine never fabricates a concrete observer (default is the no-op); consumers that await
        // acknowledgements (e.g. the server's `AckHook`) inject their own via `with_hook`.
        let hook = self.hook;
        // Controlled-shutdown token for the StreamProcessor loop: on `stop` we `cancel()` it and await
        // the task, letting the loop drain its current iteration and return cleanly — rather than
        // `JoinHandle::abort`, which hard-kills the spawned future mid-iteration with no teardown.
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            // `&*log` is `&Box<dyn LogStream<EntryPayload>>` — a Sized implementor of `LogStream` via
            // `#[auto_impl(Box)]`, which `StreamProcessor::run`'s `L: LogStream` accepts. The StreamProcessor is
            // handed the storage `Arc` and acquires the `Mutex` per entry (releasing it between
            // entries), so it never holds the lock for the whole run; it also owns the shared
            // response registry (`ack`), completing each awaiter's channel when it applies the
            // matching event.
            processor.run(&*log, storage, hook, task_cancel).await
        });
        // Assemble the engine's run state.
        let inner = Arc::new(EngineInner {
            log: self.log,
            storage: self.storage,
            processor: StreamProcessorTask { cancel, handle },
        });
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
        // `inner` must be the engine's sole strong owner here for `Arc::try_unwrap` below. A consumer
        // that spawned workers holding a strong reference back (e.g. an `Engine::task_api`-style
        // handle) must release them before stopping the engine.
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
}

impl EngineInner {
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

    /// Abort a running execution by appending `TerminateExecution{Cancelled}` on the Engine's own log,
    /// so the Server's `Arc<Engine>` can cancel by `name` (with an optional `uid` incarnation guard)
    /// without holding a caller-supplied log or knowing the execution's stream up front. This is the
    /// engine's sole cancellation entry point; the StreamProcessor drives the unwind:
    /// `ExecutionTerminating`, then child cleanup, then `ExecutionTerminated{Cancelled}`.
    ///
    /// Non-blocking: appends the command and returns once it is durable; settlement is observed by
    /// polling [`wait_for_execution`](Self::wait_for_execution) / [`execution_status`](Self::execution_status),
    /// which surface it as `Terminated(Cancelled)`. The termination handler resolves the target by
    /// `name` from Storage (not by stream), and the log stamps its own id at append — so the
    /// cancellation needs no stream chosen here.
    pub async fn cancel_execution(
        &self,
        name: ObjectName,
        uid: Option<ulid::Ulid>,
    ) -> Result<(), ExecutionError> {
        // The handler ignores which stream the command lands on, resolving the execution by name (and
        // its optional incarnation guard); the log stamps its own stream id and entry position at append.
        self.log
            .append(vec![Entry {
                stream_id: StreamId::nil(),
                entry_id: crate::types::id::EntryId::nil(),
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
    /// to the client's Query read rather than blocked here.
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

    /// Read one persisted object of any kind from the current projection by `(kind, name)` — the
    /// engine side of the k8s-style `Query.GetObject` RPC (`name` is the addressing key the row is
    /// stored under; the returned row carries its real uid). Non-blocking: `None` when no row exists
    /// for `(kind, name)`.
    pub async fn get_object(
        &self,
        kind: ObjectKind,
        name: &ObjectName,
    ) -> Result<Option<QueryObject>, ExecutionError> {
        let storage = self.storage.lock().await;
        match kind {
            // A flow is keyed by its immutable plain `name`; a generated (non-plain) name can never
            // name a flow, so it reads as "not found" without touching storage.
            ObjectKind::Flow => {
                let Some(flow_name) = name.as_plain() else {
                    return Ok(None);
                };
                Ok(storage
                    .get_flow_by_name(flow_name.clone())
                    .await?
                    .map(QueryObject::Flow))
            }
            ObjectKind::FlowVersion => Ok(storage
                .get_flow_version(&ref_for(kind, name))
                .await?
                .map(QueryObject::FlowVersion)),
            ObjectKind::Execution => Ok(storage
                .get_execution(&ref_for(kind, name))
                .await?
                .map(QueryObject::Execution)),
            ObjectKind::Thread => Ok(storage
                .get_thread(&ref_for(kind, name))
                .await?
                .map(QueryObject::Thread)),
            ObjectKind::Activity => Ok(storage
                .get_activity(&ref_for(kind, name))
                .await?
                .map(QueryObject::Activity)),
            ObjectKind::Timer => Ok(storage
                .get_timer(&ref_for(kind, name))
                .await?
                .map(QueryObject::Timer)),
            ObjectKind::Task => Ok(storage
                .get_task(&ref_for(kind, name))
                .await?
                .map(QueryObject::Task)),
        }
    }

    /// List one kind's persisted objects in storage-key order — the engine side of the k8s-style
    /// `Query.ListObjects` RPC. `limit` caps the page (the server may substitute a default), and
    /// `continue_token` (the previous page's last addressing name, opaque on the wire) resumes the
    /// next page. Reads the current projection only — no watch/change semantics.
    pub async fn list_objects(
        &self,
        kind: ObjectKind,
        limit: usize,
        continue_token: Option<&str>,
    ) -> Result<QueryListPage, ExecutionError> {
        let storage = self.storage.lock().await;
        crate::query::list_kind(&*storage, kind, limit, continue_token).await
    }

    /// Pure read: scan up to `limit` `Pending` tasks of `resource` from the durable projection — the
    /// discovery query behind a worker pull. The worker-facing consumer (spica-server) uses it as a
    /// **read-first gate** before appending a `ClaimTasks`, so an idle poll (nothing claimable now)
    /// stays a pure query and never writes to the log.
    pub async fn activatable_tasks(
        &self,
        resource: &str,
        limit: usize,
    ) -> Result<Vec<Task>, ExecutionError> {
        let storage = self.storage.lock().await;
        Ok(storage
            .activatable_tasks(resource, limit)
            .await?
            .into_iter()
            .map(|r| r.value)
            .collect())
    }
}

impl EngineInner {
    /// The engine's single public write face: append one `Command` to the log. The StreamProcessor
    /// (the authoritative single writer) later reads it back and dispatches it, so callers — the
    /// server's consumer facade — never reason about streams: placeholders carry the log's position,
    /// and `cause_id` is `None` (such commands have no causal parent in the log; the resume watermark
    /// still advances because the handler output causally re-links to the command's *own* entry id).
    pub async fn append_command(&self, command: Command) -> Result<(), ExecutionError> {
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
