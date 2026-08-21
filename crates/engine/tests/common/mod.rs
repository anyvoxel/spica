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
use spica_engine::{
    Command, EngineBuilder, Entry, EntryId, EntryPayload, ExecutionError, ExecutionId,
    ExecutionResult, FlowName, FlowVersionId, LogStream, RequestId, StreamId, TaskHandler,
    Timestamp,
};
use spica_scheduler::InMemoryScheduler;
use spica_storage::InMemoryStorage;
use spica_task_service::InMemoryTaskService;

/// A builder wired to in-memory log + storage backends and an in-memory scheduler — the M1 test
/// default. `EngineBuilder` itself no longer fabricates backends (it takes caller-supplied trait
/// objects; see [`EngineBuilder::with_backends`](spica_engine::EngineBuilder::with_backends)), so the
/// in-memory log/store/scheduler/task-service trio is assembled here from the seam crates and
/// injected.
pub fn in_memory_builder() -> EngineBuilder {
    EngineBuilder::with_backends(
        Box::new(spica_engine::InMemoryLogStream::<EntryPayload>::new()),
        Box::new(InMemoryStorage::new()),
    )
    .with_scheduler(InMemoryScheduler::spawn())
    .with_task_service(InMemoryTaskService::spawn(HashMap::new()))
}

/// Seed a `CreateExecution` command directly onto a caller-supplied log — the raw CCES seam that
/// used to be `Engine::submit`. Raw-seam drivers (which build their own log + StreamProcessor and never
/// call [`EngineBuilder::start`](spica_engine::EngineBuilder::start)) still need to mint a birth
/// execution, so the removed free function's body lives here, built only on the public log/entry
/// types. Returns the new `ExecutionId` the seeded birth event belongs to. (There is no per-execution
/// stream — a LogStream is one stream, so stream identity lives on the log, not the caller.)
pub async fn submit_seed(
    flow_version_id: FlowVersionId,
    input: Value,
    logstream: &(impl LogStream<EntryPayload> + ?Sized),
) -> Result<ExecutionId, ExecutionError> {
    let execution_id = ExecutionId::new();
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
                id: execution_id,
                flow_version_id,
                input,
            }),
        }])
        .await?;
    Ok(execution_id)
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
    // single long-lived StreamProcessor folds the commands below. The handlers are injected as the engine's
    // concrete task service (they are fixed for the engine's lifetime).
    let engine = builder
        .with_task_service(InMemoryTaskService::spawn(task_handlers))
        .start()
        .await?;
    // Persist the definition the way a string-supplying caller would — the durable record is the raw
    // string, never the transient struct.
    let definition = serde_json::to_string(&sm)
        .map_err(|e| ExecutionError::InvalidDefinition(format!("serialize state machine: {e}")))?;
    let flow_version_id = engine.create_flow(anonymous_name(), &definition).await?;
    let execution_id = engine.start_for_revision(flow_version_id, input).await?;
    engine.wait_for_execution(execution_id).await
}

/// A throwaway [`FlowName`] so each one-shot run never collides with a user-created flow; the
/// name's charset (`[A-Za-z0-9_]`) admits the `anon_` + ULID form.
fn anonymous_name() -> FlowName {
    FlowName::new(&format!("anon_{}", ulid::Ulid::new()))
        .expect("a ULID-suffixed anonymous name always satisfies FlowName's charset")
}
