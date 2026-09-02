//! External-resource task execution — the **worker** half of the task split, colocated with the
//! [`Client`](crate::Client) in `spica-client` (the single SDK crate a consumer links), but kept in its
//! own module so the worker's parsed types stay distinct from the client's bytes types.
//!
//! `spica-client` is one crate with two module-scoped faces:
//!
//! - the **control plane** — the thin [`Client`](crate::Client), whose methods return plain wire data
//!   (scalar ids, `Vec<u8>` JSON) and are also the raw surface the `spica` CLI binds to;
//! - this **worker** module — the task *consumer* (Zeebe's job worker), typed in the module's own
//!   parsed forms: [`ClaimedTask`]/[`TaskFailure`] here hold JSON `Value`s, while the same names on
//!   `crate` hold the opaque wire bytes. The two are deliberately same-named, distinguished only by
//!   module (`worker::ClaimedTask` vs `crate::ClaimedTask`).
//!
//! The worker owns a set of [`TaskHandler`]s keyed by `resource`, and runs a claim/settle loop against
//! the engine-free [`TaskApi`] below — pulling available work, executing the registered handler, and
//! reporting `complete`/`fail`. The engine never calls the handler itself — the worker does, so it can
//! live in a separate process.
//!
//! This module links **no `spica-engine`** — it depends only on `crate` (transport) + `serde_json`. It
//! is realized over the network by [`GrpcTaskApi`] (the out-of-process worker, via `crate::Client`'s
//! `Task` service), and in-process by an adapter in `spica-engine`'s test suite, which wraps the
//! engine's own inbound `spica_engine::TaskApi` so the same claim/settle loop runs against a running
//! engine without a network (the mirror image of [`GrpcTaskApi`]). The crate graph stays acyclic:
//! `client → proto`, never `→ engine`.

mod builder;
mod grpc;
mod in_memory;

pub use builder::{Worker, WorkerBuilder};
pub use grpc::GrpcTaskApi;
pub use in_memory::InMemoryTaskService;

use std::sync::Arc;

use serde_json::Value;
use tokio_util::sync::CancellationToken;

/// A task failure a [`TaskHandler`] reports — the **engine-free**, ASL-grade error semantics the
/// owning state's `Retry`/`Catch` policy consumes. It carries the error name and the error-output
/// object, which is all the failure policy needs; the worker never constructs an engine-side
/// `ExecutionError` (the server / in-process adapter map this onto one).
pub struct TaskFailure {
    /// The ASL error name (e.g. `"States.TaskFailed"` or a user-defined name), matched by
    /// `Retry`/`Catch` `ErrorEquals`.
    pub error_name: String,
    /// The error-output object bound to `$states.errorOutput` by a `Catch` (may be `null`).
    pub output: Value,
}

/// A task handed to a worker by [`TaskApi::poll_tasks`] — the worker-facing form: id as a scalar
/// `String` (its ULID) and `arguments` already parsed to JSON (only the transport adapters touch the
/// opaque-bytes wire form).
pub struct ClaimedTask {
    /// The task's canonical name (String) — the handle to bind `complete`/`fail` on.
    pub task_name: String,
    /// The `Resource` URI the worker dispatched under.
    pub resource: String,
    /// The projected arguments (parsed JSON).
    pub arguments: Value,
}

/// A claim/settle *operation* itself failed (transport/protocol), as opposed to a [`TaskFailure`],
/// which is a task's business failure reported by a handler. The worker only logs these — it re-polls
/// and never branches on them — so it is a flat owned string rather than a taxonomy.
#[derive(Debug, Clone)]
pub struct TaskApiError(pub String);

impl std::fmt::Display for TaskApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for TaskApiError {}

/// A caller-registered handler for one `Resource` URI. A worker dispatches a claimed [`ClaimedTask`]
/// to the handler registered under its `resource`, and routes the returned `Result` back to the
/// engine as [`TaskApi::complete`]/[`TaskApi::fail`].
///
/// `Ok(output)` resumes the owning state (its `complete`); `Err(error)` drives the failure through
/// the owning state's `Retry`/`Catch`/terminate policy. Handlers must be **idempotent**: a task can
/// be claimed more than once (at-least-once, lease expiry / worker stall).
#[async_trait::async_trait]
pub trait TaskHandler: Send + Sync {
    /// Execute the external call for `resource` with the projected `arguments`. Return `Ok` on
    /// success (the value becoming the task's output / the owning state's result) or `Err` on
    /// failure (the state's policy then decides Retry/Catch/terminate).
    async fn run(&self, resource: &str, arguments: &Value) -> Result<Value, TaskFailure>;
}

/// The **worker-facing task API** a worker consumes to claim and settle work — the consumer-side
/// contract, typed in this module's engine-free types. (The engine separately exposes its *inbound*
/// `spica_engine::TaskApi`; the two meet only at the adapters — [`GrpcTaskApi`] over the wire, and the
/// in-process adapter in the engine's tests.)
#[async_trait::async_trait]
pub trait TaskApi: Send + Sync {
    /// Pull up to `max_tasks` available tasks of `resource`, leasing each to `worker_id` for
    /// `lease_seconds`. Returns whatever is claimable now (possibly empty).
    async fn poll_tasks(
        &self,
        worker_id: &str,
        resource: &str,
        max_tasks: usize,
        lease_seconds: u64,
    ) -> Result<Vec<ClaimedTask>, TaskApiError>;

    /// Report a task completed with `output` (Zeebe `CompleteJob`).
    ///
    /// `request_id` is the worker-minted correlation key for this settlement (a fresh ULID string,
    /// minted per call by the caller). The engine echoes it back on the outcome, so this call is a
    /// **request/response**: it returns only after the settlement's actual result is known — `Ok` once
    /// the completion is applied, or a refused report surfaces as a `TaskApiError` from `complete`'s
    /// backing transport. The worker learns whether its settle landed instead of fire-and-forgetting it.
    async fn complete(
        &self,
        worker_id: &str,
        task_name: &str,
        request_id: &str,
        output: Value,
    ) -> Result<(), TaskApiError>;

    /// Report a task failed with `error`.
    async fn fail(
        &self,
        worker_id: &str,
        task_name: &str,
        error: TaskFailure,
    ) -> Result<(), TaskApiError>;
}

/// Contract for a **task worker** — the task consumer, the analogue of a Zeebe job worker. The
/// caller — not the engine — constructs one and drives `run`, so it also owns the worker's
/// lifecycle: it holds a strong [`TaskApi`] back to the engine's inner state, so the caller must
/// stop the worker (via `cancel`) **before** stopping the engine it polls against.
#[async_trait::async_trait]
pub trait TaskService: Send + Sync {
    /// Run this worker's loop until `cancel` is triggered. The worker assigns itself a `worker_id`,
    /// then repeatedly pulls available work ([`TaskApi::poll_tasks`]), executes the registered
    /// [`TaskHandler`], and reports `complete`/`fail`. The engine never calls the handler itself —
    /// the worker does, so it can later live in a separate process.
    async fn run(&self, api: Arc<dyn TaskApi>, cancel: CancellationToken);
}
