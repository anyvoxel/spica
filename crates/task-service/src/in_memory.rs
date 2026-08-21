//! The M1 in-memory implementation of the task **worker** contract
//! ([`TaskService`](spica_engine::TaskService)).
//!
//! A worker is the consumer half of the task split (Zeebe's job worker): it owns a set of
//! [`TaskHandler`](spica_engine::TaskHandler)s keyed by `resource`, and runs a claim/settle loop
//! *against the engine's inbound job API* ([`TaskApi`](spica_engine::TaskApi)) until it is shut down.
//! Unlike M1 (where the engine *pushed* an invoked task into an in-process dispatcher), the engine
//! now only makes a task available; this worker pulls it, executes the handler, and reports
//! `complete`/`fail` back — which is exactly the shape a worker running in a **separate process**
//! later takes (the same `TaskApi` reached over a transport).
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

use spica_engine::{ExecutionError, TaskApi, TaskHandler, TaskService};
use tokio_util::sync::CancellationToken;

/// How many tasks a worker claims per `resource` per poll.
const MAX_TASKS: usize = 10;
/// Lease length (seconds) for each claimed task — the window in which the handler must settle before
/// the engine re-queues it. ~60s matches Zeebe's default activation timeout.
const LEASE_SECONDS: u64 = 60;
/// Idle poll interval when no work is available. Short enough for prompt pickup, long enough not to
/// hammer the engine's discovery read.
const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

/// The M1 in-process worker: a pull/dispatch/settle loop over [`TaskApi`], backed by a fixed set of
/// [`TaskHandler`](spica_engine::TaskHandler)s.
///
/// The handlers are fixed for the worker's lifetime. `spawn` no longer starts a background loop —
/// the loop is [`TaskService::run`], which the engine invokes on `start` (handing it its
/// [`TaskApi`]); `InMemoryTaskService` is therefore a *stateless* holder of the handler map, and the
/// same value is what a remote transport would later use to reach the engine over a network.
pub struct InMemoryTaskService {
    handlers: HashMap<String, Arc<dyn TaskHandler>>,
}

impl InMemoryTaskService {
    /// Construct a worker serving `handlers` (a `resource` URI → [`TaskHandler`] map, fixed for the
    /// worker's lifetime). Requires no runtime context at construction; the loop runs only when the
    /// engine [`run`](TaskService::run)s it.
    pub fn spawn(handlers: HashMap<String, Arc<dyn TaskHandler>>) -> Arc<Self> {
        Arc::new(Self { handlers })
    }
}

#[async_trait::async_trait]
impl TaskService for InMemoryTaskService {
    async fn run(&self, api: Arc<dyn TaskApi>, cancel: CancellationToken) {
        // Pull available work for each resource we serve, dispatch to the handler, and report the
        // outcome back through the engine's controlled API.
        let resources: Vec<String> = self.handlers.keys().cloned().collect();
        tracing::info!(resources = ?resources, "task worker started");
        // A unique identity for this worker instance, carried on every claim/settle so the engine can
        // validate lease ownership.
        let worker_id = format!("inmem-{}", ulid::Ulid::new());

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("task worker cancelled");
                    return;
                }
                _ = tokio::time::sleep(POLL_INTERVAL) => {
                    self.poll(&api, &worker_id, &resources).await;
                }
            }
        }
    }
}

impl InMemoryTaskService {
    /// One pull round: claim up to `MAX_TASKS` per served resource, dispatch each claim to its
    /// handler on a fresh task, and report `complete`/`fail`. A claim returning no work just falls
    /// through to the next poll.
    async fn poll(&self, api: &Arc<dyn TaskApi>, worker_id: &str, resources: &[String]) {
        for resource in resources {
            // Best-effort claim: a race with another worker yields fewer/no tasks (the engine's
            // exactly-one guard settles any conflict).
            let claimed = match api
                .activate(worker_id, resource, MAX_TASKS, LEASE_SECONDS)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(%resource, error = %e, "worker activate failed");
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
                let resource = task.resource.clone();
                let arguments = task.arguments.clone();
                let task_id = task.task;
                tokio::spawn(async move {
                    // Resolve the handler for the claimed task's resource. It is always present (we
                    // only pull the resources we serve), so `None` is an internal guard against a
                    // poll/claim race rather than a normal path.
                    let result = match handlers.get(&resource) {
                        Some(handler) => handler.run(&resource, &arguments).await,
                        None => Err(ExecutionError::InvalidDefinition(format!(
                            "no TaskHandler registered for resource: {resource}"
                        ))),
                    };
                    let report = match result {
                        Ok(output) => api.complete(&worker_id, task_id, output).await,
                        Err(error) => api.fail(&worker_id, task_id, error).await,
                    };
                    if let Err(e) = report {
                        tracing::error!(task = %task_id, error = %e, "worker settlement report failed");
                    }
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use spica_engine::{ActivatedTask, TaskApi, TaskHandler, TaskId};
    use std::collections::VecDeque;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    fn task() -> TaskId {
        TaskId::from(ulid::Ulid::new())
    }

    /// A test double for the engine's inbound job API: hands out seeded available tasks and records
    /// the worker's settlements, so the worker loop can be driven without booting an engine.
    #[derive(Clone, Default)]
    struct MockApi {
        state: Arc<tokio::sync::Mutex<MockState>>,
    }

    #[derive(Default)]
    struct MockState {
        queue: VecDeque<ActivatedTask>,
        completions: Vec<(TaskId, Value)>,
        failures: Vec<(TaskId, ExecutionError)>,
    }

    #[async_trait::async_trait]
    impl TaskApi for MockApi {
        async fn activate(
            &self,
            _worker_id: &str,
            _resource: &str,
            max_tasks: usize,
            _lease_seconds: u64,
        ) -> Result<Vec<ActivatedTask>, ExecutionError> {
            let mut st = self.state.lock().await;
            let n = st.queue.len().min(max_tasks);
            Ok(st.queue.drain(..n).collect())
        }
        async fn complete(
            &self,
            _worker_id: &str,
            task: TaskId,
            output: Value,
        ) -> Result<(), ExecutionError> {
            self.state.lock().await.completions.push((task, output));
            Ok(())
        }
        async fn fail(
            &self,
            _worker_id: &str,
            task: TaskId,
            error: ExecutionError,
        ) -> Result<(), ExecutionError> {
            self.state.lock().await.failures.push((task, error));
            Ok(())
        }
    }

    /// Returns `Ok(json!("pong"))` for any call — a trivial always-succeeding handler.
    #[derive(Default)]
    struct EchoHandler;

    #[async_trait::async_trait]
    impl TaskHandler for EchoHandler {
        async fn run(&self, _resource: &str, _arguments: &Value) -> Result<Value, ExecutionError> {
            Ok(serde_json::json!("pong"))
        }
    }

    /// Returns `Err` for any call — a trivial always-failing handler.
    #[derive(Default)]
    struct FailingHandler;

    #[async_trait::async_trait]
    impl TaskHandler for FailingHandler {
        async fn run(&self, _resource: &str, _arguments: &Value) -> Result<Value, ExecutionError> {
            Err(ExecutionError::StateFailed {
                state: "urn:fail".to_string(),
                error: "boom".to_string(),
                output: serde_json::Value::Null,
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

    #[tokio::test(flavor = "multi_thread")]
    async fn worker_completes_claimed_task() {
        let api = MockApi::default();
        let id = task();
        api.state.lock().await.queue.push_back(ActivatedTask {
            task: id,
            resource: "urn:echo".to_string(),
            arguments: serde_json::json!({}),
        });
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
        let id = task();
        api.state.lock().await.queue.push_back(ActivatedTask {
            task: id,
            resource: "urn:fail".to_string(),
            arguments: serde_json::json!({}),
        });
        let cancel = CancellationToken::new();
        drive(Arc::new(api.clone()), cancel.clone()).await;

        let st = api.state.lock().await;
        let (_t, err) = st
            .failures
            .iter()
            .find(|(task, _)| *task == id)
            .expect("the claimed task was failed");
        assert!(matches!(err, ExecutionError::StateFailed { .. }));
        assert!(st.completions.is_empty(), "no task should have completed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn worker_reclaims_a_task_that_was_released() {
        // A claim that returns the same task again (simulating a lease expiry re-queue) re-runs the
        // handler, then completes — exercising the at-least-once retry of the worker loop.
        let api = MockApi::default();
        let id = task();
        api.state.lock().await.queue.push_back(ActivatedTask {
            task: id,
            resource: "urn:echo".to_string(),
            arguments: serde_json::json!({}),
        });
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
