//! External-resource task execution, decoupled from the serial dispatch loop.
//!
//! A `Task` state's `Resource` is recorded in the stream as the durable fact
//! `Event::TaskActivated`. Unlike M1 (where the engine *invoked* an in-process handler as a side
//! effect of that fact), the engine now only makes the task **available**: a **worker** (a separate
//! role, later a separate process — Zeebe's job-worker split) claims it via the engine-hosted
//! [`TaskApi`], executes the [`TaskHandler`] registered for its `resource`, and reports the outcome
//! back through [`TaskApi::complete`]/[`TaskApi::fail`]. The engine validates each inbound report
//! (lease ownership) and funnels it into the log as an ordered `CompleteTask`/`FailTask` command —
//! the engine remains the single writer that owns the task lifecycle.
//!
//! This is deliberately **not** the scheduler/timer path: a timer is an engine-internal,
//! deterministic side effect (the engine itself schedules a resumption); a task is an **inbound
//! report from an external authority** (the worker) carrying non-deterministic result data, so the
//! engine must validate a foreign *claim* before applying it. Two different operations, two
//! different seams — there is no shared "push a command into the engine" channel.
//!
//! ## Contract vs implementation
//!
//! This module holds the **worker-side contract** ([`TaskService`], what a task *consumer*
//! implements) and the caller-registered [`TaskHandler`]; the engine-hosted inbound job API
//! ([`TaskApi`]) lives in `crate::job_api`. Mirrors `Storage`/`Scheduler`: the engine holds
//! `Arc<dyn TaskService>` and never fabricates a runtime; the M1 local worker lives in the
//! downstream `spica-task-service` crate. That keeps the crate graph acyclic
//! (`spica-task-service → spica-engine`) and lets a distributed / remote worker swap in behind the
//! same trait later.
//!
//! # At-least-once
//! A worker may claim a task, stall past its lease, and be re-claimed — Zeebe's at-least-once
//! contract (a `TaskHandler` must be idempotent). The engine's idempotency guards
//! (`CompleteTask`/`FailTask` settle only a task that is still `Running` to the reporting worker)
//! make the *state* advance exactly once even if the handler ran twice.

use std::sync::Arc;

use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::error::ExecutionError;
use crate::job_api::TaskApi;

/// A caller-registered handler for one `Resource` URI. A worker dispatches a claimed `ActivatedTask`
/// to the handler registered under its `resource`, and routes the returned `Result` back to the
/// engine as `TaskApi::complete`/`TaskApi::fail`.
///
/// `Ok(output)` resumes the owning state (its `complete`); `Err(error)` drives the failure through
/// the owning state's `Retry`/`Catch`/terminate policy. Handlers must be **idempotent**: a task can
/// be claimed more than once (at-least-once, lease expiry / worker stall).
#[async_trait::async_trait]
pub trait TaskHandler: Send + Sync {
    /// Execute the external call for `resource` with the projected `arguments`. Return `Ok` on
    /// success (the value becoming the task's output / the owning state's result) or `Err` on
    /// failure (the state terminates with that error).
    async fn run(&self, resource: &str, arguments: &Value) -> Result<Value, ExecutionError>;
}

/// Contract for a **task worker** — the task consumer the engine drives, the analogue of a Zeebe
/// job worker. The engine boots one (`with_task_service`) and hands it its `Arc<dyn TaskApi>`; the
/// worker then owns its own claim/settle loop against that API until the engine's `cancel` token
/// fires.
#[async_trait::async_trait]
pub trait TaskService: Send + Sync {
    /// Run this worker's loop until `cancel` is triggered. The worker assigns itself a `worker_id`,
    /// declares the `resource`s it serves, then repeatedly pulls available work
    /// ([`TaskApi::activate`]), executes the registered [`TaskHandler`], and reports
    /// `complete`/`fail`. The engine never calls the handler itself — the worker does, so it can
    /// later live in a separate process.
    async fn run(&self, api: Arc<dyn TaskApi>, cancel: CancellationToken);
}
