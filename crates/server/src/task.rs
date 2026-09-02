//! `Task` service handlers — the out-of-process worker's inbound claim/settle API: `PollTasks`
//! pulls claimable work, `CompleteTask` / `FailTask` settle a claim.
//!
//! Each handler forwards its request to the engine's in-process [`TaskApi`](spica_engine::TaskApi) —
//! the *same* trait an in-process worker ([`spica_client::worker::InMemoryTaskService`]) talks to — so
//! a network worker and a local worker are validated identically: every call is funnelled into the
//! log as a [`Command`](spica_engine::Command) and checked by the StreamProcessor's single-writer
//! dispatch (a `CompleteTask`/`FailTask` only honors the current lease holder). `arguments`/`output`
//! travel as opaque JSON bytes; a worker *failure* travels as the structured
//! [`TaskFailure`](spica_proto::v1::TaskFailure) (ASL error name + error-output object), which this
//! handler maps onto the engine's internal error representation — so the worker side never carries an
//! engine type across the boundary.

use spica_engine::{ActivatedTask, ObjectName, RuntimeError};
use spica_proto::v1::{
    CompleteTaskRequest, CompleteTaskResponse, FailTaskRequest, FailTaskResponse, PollTasksRequest,
    PollTasksResponse, task_server::Task as TaskService,
};
use tonic::{Request, Response, Status};

use crate::common::{Svc, parse_ulid, to_status};

/// Map an engine-side [`ActivatedTask`] onto its wire `ActivatedTask`: the task's **canonical name**
/// (the handle a worker binds `complete`/`fail` on) and the projected `arguments` to opaque JSON
/// bytes (the same `arguments`-as-JSON convention used elsewhere for `definition`/`input`).
fn to_wire_task(task: ActivatedTask) -> spica_proto::v1::ActivatedTask {
    spica_proto::v1::ActivatedTask {
        task_name: task.task.as_str().to_string(),
        resource: task.resource,
        // A projected `arguments` is always a JSON value (built by the engine's task handler), so the
        // re-serialization cannot fail; on the off chance it does, degrade to empty bytes rather than
        // fail the whole poll.
        arguments: serde_json::to_vec(&task.arguments).unwrap_or_default(),
    }
}

/// Parse the wire task-name (the canonical `ObjectName` returned by a prior `PollTasks`) back into a
/// typed `ObjectName`, failing the RPC as `INVALID_ARGUMENT` on a malformed name.
#[allow(clippy::result_large_err)]
fn parse_task_name(s: &str, what: &str) -> Result<ObjectName, Status> {
    ObjectName::from_parsed(s).map_err(|e| Status::invalid_argument(format!("{what}: {e}")))
}

#[tonic::async_trait]
impl TaskService for Svc {
    /// Claim up to `max_tasks` available tasks of `resource` for `worker_id`, leasing each
    /// `lease_seconds`. A non-blocking, point-in-time discovery read (mirroring `GetExecution`): it
    /// returns whatever is claimable now — possibly empty — and the worker polls at its own cadence.
    async fn poll_tasks(
        &self,
        request: Request<PollTasksRequest>,
    ) -> Result<Response<PollTasksResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!(
            worker = %req.worker_id,
            resource = %req.resource,
            max = req.max_tasks,
            "PollTasks"
        );
        let tasks = self
            .engine
            .task_api()
            .poll_tasks(
                &req.worker_id,
                &req.resource,
                req.max_tasks as usize,
                req.lease_seconds,
            )
            .await
            .map_err(to_status)?;
        Ok(Response::new(PollTasksResponse {
            tasks: tasks.into_iter().map(to_wire_task).collect(),
        }))
    }

    /// Report a task completed with `output` (Zeebe `CompleteJob`). A **request/response** settlement:
    /// it blocks until the engine has processed the report, returning the result — the empty success
    /// confirmation when the completion was applied, or a `Status` error when the settlement guard
    /// refused it (e.g. the task is not leased to this worker). The worker-correlated `request_id`
    /// (minted by the worker, a ULID string) is what the engine echoes to resolve the ack.
    async fn complete_task(
        &self,
        request: Request<CompleteTaskRequest>,
    ) -> Result<Response<CompleteTaskResponse>, Status> {
        let req = request.into_inner();
        let task = parse_task_name(&req.task_name, "task_name")?;
        let request_id = parse_ulid::<spica_engine::RequestId>(&req.request_id, "request_id")?;
        let output: serde_json::Value = serde_json::from_slice(&req.output)
            .map_err(|e| Status::invalid_argument(format!("output is not valid JSON: {e}")))?;

        tracing::debug!(task = %task, "CompleteTask");
        self.engine
            .task_api()
            .complete(&req.worker_id, task, request_id, output)
            .await
            .map_err(to_status)?;
        Ok(Response::new(CompleteTaskResponse {}))
    }

    /// Report a task failed with a structured [`TaskFailure`](spica_proto::v1::TaskFailure) (Zeebe
    /// `FailJob`). The worker sends the ASL error *semantics* — the error name and error-output
    /// object — and this handler maps them onto the engine's internal `RuntimeError::StateFailed`,
    /// the same shape an in-process `TaskApi::fail` carries. (The `state` field is Display-only; the
    /// worker cannot know the owning state's name, so it stays empty — Retry/Catch match on the error
    /// name and output, never on `state`.)
    async fn fail_task(
        &self,
        request: Request<FailTaskRequest>,
    ) -> Result<Response<FailTaskResponse>, Status> {
        let req = request.into_inner();
        let task = parse_task_name(&req.task_name, "task_name")?;
        let failure = req
            .error
            .ok_or_else(|| Status::invalid_argument("error is required"))?;
        let output: serde_json::Value = if failure.output.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&failure.output).map_err(|e| {
                Status::invalid_argument(format!("error.output is not valid JSON: {e}"))
            })?
        };

        tracing::debug!(task = %task, "FailTask");
        self.engine
            .task_api()
            .fail(
                &req.worker_id,
                task,
                spica_engine::ExecutionError::Runtime(RuntimeError::StateFailed {
                    state: String::new(),
                    error: failure.error_name,
                    output: Box::new(output),
                }),
            )
            .await
            .map_err(to_status)?;
        Ok(Response::new(FailTaskResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use serde_json::{Value, json};
    use spica_client::worker::{
        GrpcTaskApi, InMemoryTaskService, TaskFailure, TaskHandler, TaskService,
    };
    use spica_engine::{EngineBuilder, EntryPayload, FlowName, StateMachine};
    use spica_proto::v1::task_server::TaskServer;
    use spica_scheduler::InMemoryScheduler;
    use spica_storage::InMemoryStorage;
    use tokio_util::sync::CancellationToken;

    use crate::common::Svc;

    /// A [`TaskHandler`] that echoes the projected `arguments` back as the task output.
    #[derive(Default)]
    struct EchoHandler;

    #[tonic::async_trait]
    impl TaskHandler for EchoHandler {
        async fn run(&self, _resource: &str, arguments: &Value) -> Result<Value, TaskFailure> {
            Ok(arguments.clone())
        }
    }

    /// End-to-end: an out-of-process worker reaches the engine's `TaskApi` over gRPC — poll, claim,
    /// execute, and complete a Task, driving its execution to `COMPLETED`. This exercises the full
    /// wire path (`PollTasks` / `CompleteTask`) plus the `ActivatedTask` ↔ proto and
    /// `arguments`-as-JSON mappings, which the in-process worker tests (never crossing the network)
    /// do not cover.
    #[tokio::test]
    async fn remote_worker_claims_and_completes_task_over_grpc() {
        // An in-memory engine is enough — the point is the network boundary, not the storage backend.
        let engine = Arc::new(
            EngineBuilder::with_backends(
                Box::new(spica_engine::InMemoryLogStream::<EntryPayload>::new()),
                Box::new(InMemoryStorage::new()),
            )
            .with_scheduler(InMemoryScheduler::spawn())
            .start()
            .await
            .expect("engine boots"),
        );

        // Serve the `Task` service on a loopback port, holding the server for the test's duration.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("listener addr");
        let svc = Svc {
            engine: engine.clone(),
        };
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TaskServer::new(svc))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("serve Task");
        });

        // Boot a worker against the remote engine through the same claim/settle loop the in-process
        // worker uses — only the `TaskApi` transport differs (a gRPC client instead of `Arc<Engine>`).
        let mut handlers: HashMap<String, Arc<dyn TaskHandler>> = HashMap::new();
        handlers.insert(
            "arn:aws:states:::lambda:invoke".to_string(),
            Arc::new(EchoHandler),
        );
        let api = GrpcTaskApi::connect(&addr.to_string())
            .await
            .expect("dial Task service");
        let cancel = CancellationToken::new();
        let worker = tokio::spawn({
            let service = InMemoryTaskService::spawn(handlers);
            let cancel = cancel.clone();
            async move { service.run(Arc::new(api), cancel).await }
        });

        // Create a flow + execution whose single Task state the remote worker claims and completes.
        let sm: StateMachine = serde_json::from_value(json!({
            "StartAt": "T",
            "States": {
                "T": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Arguments": { "message": "hello" },
                    "End": true
                }
            }
        }))
        .expect("parse state machine");
        let flow = FlowName::new(&format!("grpc_{}", ulid::Ulid::new())).expect("valid name");
        let fvid = engine
            .create_flow(flow, &serde_json::to_string(&sm).unwrap())
            .await
            .expect("create flow");
        let execution_id = engine
            .start_for_revision(
                spica_engine::ObjectName::generated_with_suffix(
                    "exec",
                    &ulid::Ulid::new().to_string(),
                )
                .expect("a ULID-suffixed generated name is always valid"),
                fvid,
                Value::Null,
            )
            .await
            .expect("start execution");

        // The remote worker claims the task, echoes its arguments, and completes it; the execution
        // settles once that completion reports back over the wire.
        let result = engine
            .wait_for_execution(&execution_id)
            .await
            .expect("execution completes");
        assert_eq!(result.output, json!({ "message": "hello" }));

        // Tear down the worker before the engine drops, then the server.
        cancel.cancel();
        let _ = worker.await;
        server.abort();
        let _ = server.await;
    }
}
