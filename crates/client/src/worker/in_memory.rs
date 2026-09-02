//! The M1 in-memory implementation of the task **worker** contract
//! ([`TaskService`](super::TaskService)).
//!
//! A worker is the consumer half of the task split (Zeebe's job worker): it owns a set of
//! [`TaskHandler`](super::TaskHandler)s keyed by `resource`, and runs a claim/settle loop against the
//! engine-free worker [`TaskApi`](super::TaskApi) until it is shut down. The engine only makes a task
//! available; this worker pulls it, executes the handler, and reports `complete`/`fail` back — which
//! is exactly the shape a worker running in a **separate process** takes (the same trait reached over
//! a transport via [`GrpcTaskApi`](super::GrpcTaskApi)).
//!
//! # At-least-once
//! A claim is leased for `LEASE_SECONDS`; if the handler outlives the lease (stall / crash) the task
//! is re-queued and re-claimed, so the same handler may run more than once for one logical task.
//! Handlers must be idempotent (Zeebe's contract); the engine's settlement guard
//! (`CompleteTask`/`FailTask` only honor the current lease holder) makes the *state* advance once.
//!
//! # Shutdown
//! The claim/settle loop observes the engine's `cancel` token (see [`TaskService::run`]); it returns
//! on cancel, leaving any in-flight handler futures to drain (spawned, not joined — same no-join
//! convention as the rest of M1).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::{ClaimedTask, TaskApi, TaskFailure, TaskHandler, TaskService};

/// How many tasks a worker claims per `resource` per poll.
pub(crate) const MAX_TASKS: usize = 10;
/// Lease length (seconds) for each claimed task — the window in which the handler must settle before
/// the engine re-queues it. ~60s matches Zeebe's default activation timeout.
pub(crate) const LEASE_SECONDS: u64 = 60;
/// Idle poll interval when no work is available. Short enough for prompt pickup, long enough not to
/// hammer the engine's discovery read.
pub(crate) const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// The M1 in-process worker: a pull/dispatch/settle loop over [`TaskApi`], backed by a fixed set of
/// [`TaskHandler`](super::TaskHandler)s.
///
/// The handlers are fixed for the worker's lifetime. `spawn` only constructs the worker — it does
/// not start a loop; the loop is [`TaskService::run`], driven by the caller (not the engine), which
/// hands it a worker [`TaskApi`](super::TaskApi). `InMemoryTaskService` is therefore a *stateless*
/// holder of the handler map.
pub struct InMemoryTaskService {
    handlers: HashMap<String, Arc<dyn TaskHandler>>,
    /// Identity prefix for this worker's engine-side `worker_id`; a fresh ULID is appended per
    /// `run`, so concurrent runs of the same service never collide on leases.
    name: String,
    /// Pull batch size per `resource` per poll.
    max_tasks: usize,
    /// Claim lease length (seconds).
    lease_seconds: u64,
    /// Idle poll interval.
    poll_interval: Duration,
}

impl InMemoryTaskService {
    /// Construct a worker serving `handlers` (a `resource` URI → [`TaskHandler`] map, fixed for the
    /// worker's lifetime), with default tuning and the `"inmem"` identity prefix. Requires no runtime
    /// context at construction; the loop runs only when the caller [`run`](TaskService::run)s it.
    pub fn spawn(handlers: HashMap<String, Arc<dyn TaskHandler>>) -> Arc<Self> {
        Self::spawn_named("inmem", handlers)
    }

    /// Like [`spawn`](InMemoryTaskService::spawn), but with a caller-chosen identity `name` (the
    /// `worker_id` becomes `{name}-{ulid}`).
    pub fn spawn_named(
        name: impl Into<String>,
        handlers: HashMap<String, Arc<dyn TaskHandler>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handlers,
            name: name.into(),
            max_tasks: MAX_TASKS,
            lease_seconds: LEASE_SECONDS,
            poll_interval: POLL_INTERVAL,
        })
    }

    /// Fully-tuned constructor used by the [`Worker`](super::Worker) builder — the low-level seam's
    /// public surface stays at [`spawn`](InMemoryTaskService::spawn)/[`spawn_named`](InMemoryTaskService::spawn_named).
    pub(crate) fn spawn_configured(
        name: String,
        handlers: HashMap<String, Arc<dyn TaskHandler>>,
        max_tasks: usize,
        lease_seconds: u64,
        poll_interval: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            handlers,
            name,
            max_tasks,
            lease_seconds,
            poll_interval,
        })
    }
}

#[async_trait::async_trait]
impl TaskService for InMemoryTaskService {
    async fn run(&self, api: Arc<dyn TaskApi>, cancel: CancellationToken) {
        // Pull available work for each resource we serve, dispatch to the handler, and report the
        // outcome back through the worker's claim/settle API.
        let resources: Vec<String> = self.handlers.keys().cloned().collect();
        tracing::info!(worker = %self.name, resources = ?resources, "task worker started");
        // A unique identity for this worker instance, carried on every claim/settle so the engine can
        // validate lease ownership.
        let worker_id = format!("{}-{}", self.name, ulid::Ulid::new());

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("task worker cancelled");
                    return;
                }
                _ = tokio::time::sleep(self.poll_interval) => {
                    self.poll(&api, &worker_id, &resources).await;
                }
            }
        }
    }
}

impl InMemoryTaskService {
    /// One pull round: claim up to the configured batch (`self.max_tasks`) per served resource,
    /// dispatch each claim to its handler on a fresh task, and report `complete`/`fail`. A claim
    /// returning no work just falls through to the next poll.
    async fn poll(&self, api: &Arc<dyn TaskApi>, worker_id: &str, resources: &[String]) {
        for resource in resources {
            // Best-effort claim: a race with another worker yields fewer/no tasks (the engine's
            // exactly-one guard settles any conflict).
            let claimed = match api
                .poll_tasks(worker_id, resource, self.max_tasks, self.lease_seconds)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(%resource, error = %e, "worker poll_tasks failed");
                    continue;
                }
            };
            for task in claimed {
                // Each handler runs on its own task, so one slow call never blocks the others; the
                // settle is reported once it returns. Spawned, not joined: a settle that races lease
                // expiry is exactly the re-claim the at-least-once contract expects.
                let api = Arc::clone(api);
                let worker_id = worker_id.to_string();
                let handlers = self.handlers.clone();
                let ClaimedTask {
                    task_name,
                    resource,
                    arguments,
                } = task;
                tokio::spawn(async move {
                    // Resolve the handler for the claimed task's resource. It is always present (we
                    // only pull the resources we serve), so `None` is an internal guard against a
                    // poll/claim race rather than a normal path.
                    let result = match handlers.get(&resource) {
                        Some(handler) => handler.run(&resource, &arguments).await,
                        None => Err(TaskFailure {
                            error_name: "States.Runtime".to_string(),
                            output: serde_json::Value::Null,
                        }),
                    };
                    let report = match result {
                        // A fresh request id per settle: `complete` is a request/response report, and
                        // the engine echoes this id back to resolve the settle's actual outcome.
                        Ok(output) => {
                            let request_id = ulid::Ulid::new().to_string();
                            api.complete(&worker_id, &task_name, &request_id, output)
                                .await
                        }
                        Err(error) => api.fail(&worker_id, &task_name, error).await,
                    };
                    if let Err(e) = report {
                        tracing::error!(task = %task_name, error = %e, "worker settlement report failed");
                    }
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::{ClaimedTask, TaskApiError};
    use serde_json::Value;
    use std::collections::VecDeque;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    fn task_id() -> String {
        ulid::Ulid::new().to_string()
    }

    /// A test double for the worker's claim/settle API: hands out seeded available tasks and records
    /// the worker's settlements, so the worker loop can be driven without a real engine/transport.
    #[derive(Clone, Default)]
    struct MockApi {
        state: Arc<tokio::sync::Mutex<MockState>>,
    }

    #[derive(Default)]
    struct MockState {
        queue: VecDeque<ClaimedTask>,
        completions: Vec<(String, Value)>,
        failures: Vec<(String, TaskFailure)>,
    }

    #[async_trait::async_trait]
    impl TaskApi for MockApi {
        async fn poll_tasks(
            &self,
            _worker_id: &str,
            _resource: &str,
            max_tasks: usize,
            _lease_seconds: u64,
        ) -> Result<Vec<ClaimedTask>, TaskApiError> {
            let mut st = self.state.lock().await;
            let n = st.queue.len().min(max_tasks);
            Ok(st.queue.drain(..n).collect())
        }

        async fn complete(
            &self,
            _worker_id: &str,
            task: &str,
            _request_id: &str,
            output: Value,
        ) -> Result<(), TaskApiError> {
            self.state
                .lock()
                .await
                .completions
                .push((task.to_string(), output));
            Ok(())
        }

        async fn fail(
            &self,
            _worker_id: &str,
            task: &str,
            error: TaskFailure,
        ) -> Result<(), TaskApiError> {
            self.state
                .lock()
                .await
                .failures
                .push((task.to_string(), error));
            Ok(())
        }
    }

    /// Returns `Ok(json!("pong"))` for any call — a trivial always-succeeding handler.
    #[derive(Default)]
    struct EchoHandler;

    #[async_trait::async_trait]
    impl TaskHandler for EchoHandler {
        async fn run(&self, _resource: &str, _arguments: &Value) -> Result<Value, TaskFailure> {
            Ok(serde_json::json!("pong"))
        }
    }

    /// Returns `Err` for any call — a trivial always-failing handler.
    #[derive(Default)]
    struct FailingHandler;

    #[async_trait::async_trait]
    impl TaskHandler for FailingHandler {
        async fn run(&self, _resource: &str, _arguments: &Value) -> Result<Value, TaskFailure> {
            Err(TaskFailure {
                error_name: "boom".to_string(),
                output: serde_json::json!({ "Error": "boom" }),
            })
        }
    }

    /// Run the worker against `api`, polling just long enough for it to settle the seeded tasks.
    async fn drive(api: Arc<dyn TaskApi>, cancel: CancellationToken) {
        let mut handlers: HashMap<String, Arc<dyn TaskHandler>> = HashMap::new();
        handlers.insert("urn:echo".to_string(), Arc::new(EchoHandler));
        handlers.insert("urn:fail".to_string(), Arc::new(FailingHandler));
        let worker = InMemoryTaskService::spawn(handlers);
        let h = tokio::spawn({
            let api = Arc::clone(&api);
            let cancel = cancel.clone();
            async move { worker.run(api, cancel).await }
        });
        // Let the loop claim + settle, then stop it.
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel.cancel();
        let _ = h.await;
    }

    fn claimed(id: &str, resource: &str) -> ClaimedTask {
        ClaimedTask {
            task_name: id.to_string(),
            resource: resource.to_string(),
            arguments: serde_json::json!({}),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn worker_completes_claimed_task() {
        let api = MockApi::default();
        let id = task_id();
        api.state
            .lock()
            .await
            .queue
            .push_back(claimed(&id, "urn:echo"));
        let cancel = CancellationToken::new();
        drive(Arc::new(api.clone()), cancel.clone()).await;

        let st = api.state.lock().await;
        // The echoed output reached `complete`.
        let (_t, out) = st
            .completions
            .iter()
            .find(|(task, _)| *task == id)
            .expect("the claimed task was completed");
        assert_eq!(*out, serde_json::json!("pong"));
        assert!(st.failures.is_empty(), "no task should have failed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn worker_fails_claimed_task_through_api() {
        let api = MockApi::default();
        let id = task_id();
        api.state
            .lock()
            .await
            .queue
            .push_back(claimed(&id, "urn:fail"));
        let cancel = CancellationToken::new();
        drive(Arc::new(api.clone()), cancel.clone()).await;

        let st = api.state.lock().await;
        let (_t, err) = st
            .failures
            .iter()
            .find(|(task, _)| *task == id)
            .expect("the claimed task was failed");
        assert_eq!(err.error_name, "boom");
        assert!(st.completions.is_empty(), "no task should have completed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn worker_reclaims_a_task_that_was_released() {
        // A claim that returns the same task again (simulating a lease expiry re-queue) re-runs the
        // handler, then completes — exercising the at-least-once retry of the worker loop.
        let api = MockApi::default();
        let id = task_id();
        api.state
            .lock()
            .await
            .queue
            .push_back(claimed(&id, "urn:echo"));
        let cancel = CancellationToken::new();
        drive(Arc::new(api.clone()), cancel.clone()).await;

        let st = api.state.lock().await;
        assert_eq!(
            st.completions.len(),
            1,
            "the re-claimed task completes once per loop pass"
        );
    }
}
