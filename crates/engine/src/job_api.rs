//! The engine-hosted inbound **job API** a worker talks to — the spica analogue of Zeebe's job
//! gRPC API (ActivateJobs / CompleteJob / FailJob), realized locally for M1.
//!
//! A worker *pulls* available work by `resource` ([`TaskApi::activate`]), executes it, then
//! *reports* the outcome ([`TaskApi::complete`] / [`TaskApi::fail`]). Every call is validated by the
//! engine and funnelled into the log as an ordered [`Command`](crate::Command) — the engine stays
//! the single writer that owns the task lifecycle. This is deliberately distinct from the timer
//! path (see `crate::task_service`): the inbound report is an **external claim the engine must
//! validate**, not an engine-internal push.
//!
//! [`EngineInner`](crate::engine::EngineInner) implements this trait; `start()` hands the injected
//! [`TaskService`](crate::task_service::TaskService) worker an `Arc<dyn TaskApi>` pointing at it.
//! An out-of-process worker later reaches the same trait over a network server; the local
//! `InMemoryTaskService` realizes it in-process.

use serde_json::Value;

use crate::error::ExecutionError;
use crate::id::TaskId;

/// A task handed to a worker by [`TaskApi::activate`] — the unit of work a worker claims and later
/// completes/fails. Carries the durable facts needed to execute the handler without re-reading the
/// engine: which `resource` to call and with what projected `arguments`.
#[derive(Debug, Clone, PartialEq)]
pub struct ActivatedTask {
    pub task: TaskId,
    pub resource: String,
    pub arguments: Value,
}

/// The engine-hosted inbound job API a worker talks to (Zeebe's job gRPC API, locally).
#[async_trait::async_trait]
pub trait TaskApi: Send + Sync {
    /// Pull up to `max_tasks` available (`Pending`) tasks of `resource`, leasing each to `worker_id`
    /// for `lease_seconds` (Zeebe `ActivateJobs`). Returns the claimed work so the worker can
    /// execute it; a later `complete`/`fail` is accepted only from the same `worker_id`.
    ///
    /// Allocation is **deferred to the StreamProcessor**: this appends a single `PullTasks` command
    /// and awaits its `Granted` ack, so discovery + leasing are decided in the processor's serialized,
    /// lock-holding dispatch (no allocation-at-API-time race, no claim of already-settled tasks). The
    /// returned set is the handler's *discovery-time* grant (direct return): a narrow racing pull can
    /// hand a task to two workers, which the conditional `TaskLeased` applier settles to exactly-once
    /// *state* — so the caller's work may be at-least-once and handlers must be idempotent.
    async fn activate(
        &self,
        worker_id: &str,
        resource: &str,
        max_tasks: usize,
        lease_seconds: u64,
    ) -> Result<Vec<ActivatedTask>, ExecutionError>;

    /// Report a task completed with `output` (Zeebe `CompleteJob`). Validated by the engine: the
    /// task must be `Running` (leased) to this `worker_id`.
    async fn complete(
        &self,
        worker_id: &str,
        task: TaskId,
        output: Value,
    ) -> Result<(), ExecutionError>;

    /// Report a task failed with `error` (Zeebe `FailJob`). Validated by the engine: when
    /// `worker_id` is non-empty it must match the leasing worker; an empty `worker_id` is reserved
    /// for engine-authoritative failures (e.g. the `TimeoutSeconds` backstop).
    async fn fail(
        &self,
        worker_id: &str,
        task: TaskId,
        error: ExecutionError,
    ) -> Result<(), ExecutionError>;
}
