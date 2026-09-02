//! The engine-hosted inbound **task API** a worker talks to — the spica analogue of Zeebe's job
//! gRPC API (ActivateJobs / CompleteJob / FailJob), realized locally for M1.
//!
//! A worker *pulls* available work by `resource` ([`TaskApi::poll_tasks`]), executes it, then
//! *reports* the outcome ([`TaskApi::complete`] / [`TaskApi::fail`]). Every call is validated by the
//! engine and funnelled into the log as an ordered [`Command`](crate::Command) — the engine stays
//! the single writer that owns the task lifecycle. This is deliberately distinct from the timer path:
//! the inbound report is an **external claim the engine must
//! validate**, not an engine-internal push.
//!
//! [`EngineInner`](crate::engine::EngineInner) implements this trait; [`Engine::task_api`]
//! (crate::Engine::task_api) hands a worker an `Arc<dyn TaskApi>` pointing at the running engine.
//! An out-of-process worker later reaches the same trait over a network server; the local
//! `spica_client::worker::InMemoryTaskService` realizes it in-process.

use serde_json::Value;

use crate::types::error::ExecutionError;
use crate::types::id::RequestId;
use crate::types::meta::ObjectName;

/// A task handed to a worker by [`TaskApi::poll_tasks`] — the unit of work a worker claims and later
/// completes/fails. Carries the durable facts needed to execute the handler without re-reading the
/// engine: which `resource` to call and with what projected `arguments`. The task's **canonical name**
/// (`task`, an `ObjectName`) is the handle a worker binds `complete`/`fail` on — derived from the
/// owning execution's name base (finding #13), not an opaque uid.
#[derive(Debug, Clone, PartialEq)]
pub struct ActivatedTask {
    pub task: ObjectName,
    pub resource: String,
    pub arguments: Value,
}

/// The engine-hosted inbound task API a worker talks to (Zeebe's job gRPC API, locally).
#[async_trait::async_trait]
pub trait TaskApi: Send + Sync {
    /// Pull up to `max_tasks` available (`Pending`) tasks of `resource`, leasing each to `worker_id`
    /// for `lease_seconds` (Zeebe `ActivateJobs`). Returns the claimed work so the worker can
    /// execute it; a later `complete`/`fail` is accepted only from the same `worker_id`.
    ///
    /// An idle poll (nothing claimable **right now**) returns immediately with no durable entry — the
    /// read-first gate in `poll_tasks` treats an empty discovery as a pure query and skips the append.
    ///
    /// When there is claimable work, allocation is **deferred to the StreamProcessor**: this appends a
    /// single `ClaimTasks` command and awaits its `Granted` ack, so discovery + leasing are decided in
    /// the processor's serialized, lock-holding dispatch (no allocation-at-API-time race, no claim of
    /// already-settled tasks). The returned set is the handler's *discovery-time* grant (direct
    /// return): a narrow racing pull can hand a task to two workers, which the conditional
    /// `TasksClaimed` applier settles to exactly-once *state* — so the caller's work may be
    /// at-least-once and handlers must be idempotent.
    async fn poll_tasks(
        &self,
        worker_id: &str,
        resource: &str,
        max_tasks: usize,
        lease_seconds: u64,
    ) -> Result<Vec<ActivatedTask>, ExecutionError>;

    /// Report a task completed with `output` (Zeebe `CompleteJob`). Validated by the engine: the
    /// task must be `Running` (leased) to this `worker_id`.
    ///
    /// A **request/response** boundary (mirroring `create_flow`/`start_for_revision`): `request_id`
    /// is the worker-supplied correlation key the engine registers and awaits on. The call returns
    /// only after the settlement's outcome is durable — `Ok(())` once the applied `TaskCompleted`
    /// echoes the id back, or an [`ExecutionError::Rejected`] if the settlement guard refuses (wrong
    /// lease holder, task not Running, …). The worker thus learns the *actual* result of its report
    /// rather than fire-and-forgetting it into the log.
    async fn complete(
        &self,
        worker_id: &str,
        task: ObjectName,
        request_id: RequestId,
        output: Value,
    ) -> Result<(), ExecutionError>;

    /// Report a task failed with `error` (Zeebe `FailJob`). Validated by the engine: when
    /// `worker_id` is non-empty it must match the leasing worker; an empty `worker_id` is reserved
    /// for engine-authoritative failures (e.g. the `TimeoutSeconds` backstop).
    async fn fail(
        &self,
        worker_id: &str,
        task: ObjectName,
        error: ExecutionError,
    ) -> Result<(), ExecutionError>;
}
