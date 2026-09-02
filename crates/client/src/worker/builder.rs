//! The [`Worker`] — the user-facing *worker object* that owns the entire task-consumption flow:
//! pulling available tasks, dispatching each to its registered [`TaskHandler`], and reporting
//! `complete`/`fail` back to the engine, until shut down.
//!
//! This is the spica analogue of Zeebe's `JobWorker`
//! (`client.newWorker().jobType(...).handler(...).open()`) and Temporal's worker
//! (`worker.New(c, taskQueue)` + `Register*`): a single object built *from* a
//! [`Client`](crate::Client) (via [`Client::new_worker`](crate::Client::new_worker)) that registers
//! the `resource → handler` pairs and then runs the loop against that client's transport. The loop
//! itself is shared with the lower-level [`InMemoryTaskService`] (over an engine-free [`TaskApi`]);
//! [`Worker`] is the ergonomic façade that wires a `Client`'s transport to it and adds a Zeebe-style
//! builder.
//!
//! # Lifecycle
//! [`Worker::run`] blocks until `cancel` fires — the same contract as the low-level seam: the caller
//! owns the worker's lifetime and must stop it before stopping the server/engine it polls (a worker
//! holds a strong transport/API reference, so worker-precedes-engine matters).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::Client;

use super::in_memory::{LEASE_SECONDS, MAX_TASKS, POLL_INTERVAL};
use super::{GrpcTaskApi, InMemoryTaskService, TaskApi, TaskHandler, TaskService};

/// Zeebe-style fluent builder for a [`Worker`]: start from `Client::new_worker()` (or
/// [`WorkerBuilder::from_api`] for an in-process transport), register `resource → handler` pairs,
/// then [`build`](WorkerBuilder::build).
pub struct WorkerBuilder {
    /// The claim/settle transport the worker drives — a [`GrpcTaskApi`] over `Client`, or any
    /// in-process [`TaskApi`] adapter.
    api: Arc<dyn TaskApi>,
    handlers: HashMap<String, Arc<dyn TaskHandler>>,
    /// Identity prefix; the engine-side `worker_id` becomes `{name}-{ulid}`.
    name: String,
    /// Pull batch size per resource per poll.
    max_tasks: usize,
    /// Claim lease length (seconds).
    lease_seconds: u64,
    /// Idle poll interval.
    poll_interval: Duration,
}

impl WorkerBuilder {
    /// Begin building a worker from a dialed [`Client`] (Zeebe `newWorker()` / Temporal
    /// `worker.New(c, …)`). The worker owns a cheap clone of the client's transport.
    pub(crate) fn new(client: Client) -> Self {
        Self {
            api: Arc::new(GrpcTaskApi::new(client)),
            handlers: HashMap::new(),
            name: "spica-worker".to_string(),
            max_tasks: MAX_TASKS,
            lease_seconds: LEASE_SECONDS,
            poll_interval: POLL_INTERVAL,
        }
    }

    /// Begin building a worker over an existing engine-free [`TaskApi`] — the in-process form (an
    /// adapter wrapping the engine's inbound `TaskApi`), for when the worker and the engine share a
    /// process rather than a network.
    pub fn from_api(api: Arc<dyn TaskApi>) -> Self {
        Self {
            api,
            handlers: HashMap::new(),
            name: "spica-worker".to_string(),
            max_tasks: MAX_TASKS,
            lease_seconds: LEASE_SECONDS,
            poll_interval: POLL_INTERVAL,
        }
    }

    /// Register `handler` for `resource`. Call repeatedly for each resource this worker serves; a
    /// claimed task is dispatched to the handler registered under its `resource`.
    pub fn resource(mut self, resource: impl Into<String>, handler: Arc<dyn TaskHandler>) -> Self {
        self.handlers.insert(resource.into(), handler);
        self
    }

    /// Set the worker's identity prefix (default `"spica-worker"`); the engine-side `worker_id`
    /// becomes `{name}-{ulid}`. Useful for disambiguating instances in logs.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Maximum tasks claimed per resource per poll (default 10).
    pub fn max_tasks(mut self, n: usize) -> Self {
        self.max_tasks = n;
        self
    }

    /// Claim lease length in seconds (default 60).
    pub fn lease_seconds(mut self, s: u64) -> Self {
        self.lease_seconds = s;
        self
    }

    /// Idle poll interval (default 10ms).
    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// Assemble the [`Worker`]. Construction is cheap — the loop starts only on [`Worker::run`].
    pub fn build(self) -> Worker {
        let service = InMemoryTaskService::spawn_configured(
            self.name,
            self.handlers,
            self.max_tasks,
            self.lease_seconds,
            self.poll_interval,
        );
        Worker {
            api: self.api,
            service,
        }
    }
}

/// A task worker: a pull → dispatch → settle loop over an engine-free [`TaskApi`], carrying a fixed
/// set of `resource → handler` pairs. Build one with a [`WorkerBuilder`] (from
/// `Client::new_worker()`) and drive it with [`Worker::run`].
pub struct Worker {
    api: Arc<dyn TaskApi>,
    service: Arc<InMemoryTaskService>,
}

impl Worker {
    /// Run the worker's loop until `cancel` fires: repeatedly pull available tasks, execute the
    /// registered handler for each, and report `complete`/`fail`. Blocks; the caller owns the
    /// lifecycle and must `cancel` before stopping the server/engine this worker polls.
    pub async fn run(&self, cancel: CancellationToken) {
        self.service.run(Arc::clone(&self.api), cancel).await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::time::Duration;

    use serde_json::Value;
    use tokio_util::sync::CancellationToken;

    use super::WorkerBuilder;
    use crate::worker::{ClaimedTask, TaskApi, TaskApiError, TaskFailure, TaskHandler};

    /// A minimal claim/settle double: hands out one seeded task per poll round and records the
    /// settlement, so the `Worker` façade can be driven without a network/engine.
    #[derive(Clone, Default)]
    struct MockApi {
        state: std::sync::Arc<tokio::sync::Mutex<MockState>>,
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

    /// Echoes `{"pong": <resource>}` for any call — an always-succeeding handler.
    #[derive(Default)]
    struct EchoHandler;

    #[async_trait::async_trait]
    impl TaskHandler for EchoHandler {
        async fn run(&self, resource: &str, _arguments: &Value) -> Result<Value, TaskFailure> {
            Ok(serde_json::json!({ "pong": resource }))
        }
    }

    /// Build a worker over `api` via the builder, run it just long enough to settle one seeded task,
    /// and return the recorded settlements. Exercises the full façade path: `build` wiring + one
    /// `run` pull→dispatch→settle round.
    #[tokio::test(flavor = "multi_thread")]
    async fn worker_builder_registers_and_settles_a_claimed_task() {
        let api = MockApi::default();
        let id = ulid::Ulid::new().to_string();
        api.state.lock().await.queue.push_back(ClaimedTask {
            task_name: id.clone(),
            resource: "urn:echo".to_string(),
            arguments: serde_json::json!({}),
        });

        let worker = WorkerBuilder::from_api(std::sync::Arc::new(api.clone()))
            .name("test-worker")
            .resource("urn:echo", std::sync::Arc::new(EchoHandler))
            .poll_interval(Duration::from_millis(1))
            .build();

        let cancel = CancellationToken::new();
        let h = tokio::spawn({
            let cancel = cancel.clone();
            async move { worker.run(cancel).await }
        });
        // Let the loop claim + settle, then stop it.
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();
        let _ = h.await;

        let st = api.state.lock().await;
        let (task, out) = st
            .completions
            .iter()
            .find(|(task, _)| *task == id)
            .expect("the claimed task was completed");
        assert_eq!(task, &id);
        assert_eq!(*out, serde_json::json!({ "pong": "urn:echo" }));
        assert!(st.failures.is_empty(), "no task should have failed");
    }
}
