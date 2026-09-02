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

use serde_json::Value;
use spica_asl::StateMachine;
use spica_client::worker::{
    ClaimedTask, InMemoryTaskService, TaskApi as WorkerTaskApi, TaskApiError, TaskFailure,
    TaskHandler, TaskService,
};
use spica_engine::{
    Command, EngineBuilder, Entry, EntryId, EntryPayload, ExecutionError, ExecutionResult,
    FlowName, LogStream, ObjectName, ObjectReference, RequestId, RuntimeError, StreamId, Timestamp,
};
use spica_scheduler::InMemoryScheduler;
use spica_storage::InMemoryStorage;
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

/// A builder wired to in-memory log + storage backends and an in-memory scheduler — the M1 test
/// default. `EngineBuilder` itself no longer fabricates backends (it takes caller-supplied trait
/// objects; see [`EngineBuilder::with_backends`](spica_engine::EngineBuilder::with_backends)), so the
/// in-memory log/store/scheduler/worker trio is assembled here from the seam crates and
/// injected.
pub fn in_memory_builder() -> EngineBuilder {
    EngineBuilder::with_backends(
        Box::new(spica_engine::InMemoryLogStream::<EntryPayload>::new()),
        Box::new(InMemoryStorage::new()),
    )
    .with_scheduler(InMemoryScheduler::spawn())
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
    // A ULID-suffixed generated name is statically in the valid charset and cannot collide.
    let name = ObjectName::generated_with_suffix("seed", &ulid::Ulid::new().to_string())
        .expect("a ULID-suffixed generated name is always valid");
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
    // `EngineBuilder::start` consumes the builder (typestate: an unstarted engine has no operation
    // methods), so this drops the unstarted builder and returns the running engine — the one whose
    // single long-lived StreamProcessor folds the commands below.
    let engine = builder.start().await?;

    // The worker is a separate role (no longer spawned by the engine): boot it against the engine's
    // inbound TaskApi (adapted to the worker-facing trait), and own its lifecycle here — cancel it
    // before returning so the strong `Arc` (a reference to the engine's inner state) it holds is
    // released as the engine drops.
    let cancel = CancellationToken::new();
    let worker = {
        let api = Arc::new(EngineTaskApi::new(engine.task_api()));
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
    let flow_version = engine.create_flow(anonymous_name(), &definition).await?;
    let execution_id = engine
        .start_for_revision(execution_name(), flow_version, input)
        .await?;
    let result = engine.wait_for_execution(&execution_id).await;

    // Shut the worker down before the engine drops (worker-precedes-engine, the `task_api` contract).
    cancel.cancel();
    let _ = worker.await;
    result
}

/// A throwaway [`FlowName`] so each one-shot run never collides with a user-created flow; the
/// name's charset (`[A-Za-z0-9_]`) admits the `anon_` + ULID form.
fn anonymous_name() -> FlowName {
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
