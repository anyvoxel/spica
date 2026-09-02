//! The out-of-process worker's transport: [`GrpcTaskApi`] — a [`TaskApi`](super::TaskApi)
//! implementation that reaches the engine over the network (via the `spica-client` [`Client`]
//! `Task` service) instead of holding an in-process engine reference.
//!
//! This is the *wire half* of the worker/engine boundary: it converts the client's plain
//! [`crate::ClaimedTask`] (scalar id + opaque JSON bytes) into this module's parsed
//! [`ClaimedTask`](super::ClaimedTask), and serializes this module's [`TaskFailure`](super::TaskFailure)
//! back onto the wire. Crucially the worker *loop*
//! ([`InMemoryTaskService::run`](super::InMemoryTaskService)) depends only on the trait, so the same
//! claim/settle loop runs unchanged against a remote engine: build a worker, dial a [`GrpcTaskApi`],
//! and drive `run` — a worker lives in its own process, talking to the server the way `spica` CLI does
//! (the mirror image of how an in-process worker talks to an in-process engine through the test-suite
//! adapter).
//!
//! Nothing here (or anywhere in this module) references `spica-engine`: the engine-free worker
//! contract is the whole point of the crate-graph split.

use crate::{ClaimedTask as WireClaimedTask, Client, TaskFailure as WireFailure};

use super::{ClaimedTask, TaskApi, TaskApiError, TaskFailure};

/// A [`TaskApi`](super::TaskApi) that reaches the engine over the network — the out-of-process worker
/// half of the task split (the analogue of a Zeebe job worker's gRPC connection to its broker). It
/// wraps a [`crate::Client`] and translates each `TaskApi` call into the corresponding `Task` RPC,
/// carrying `arguments`/`output`/`error_output` as opaque JSON bytes; parsing/serialization happens
/// here, so the rest of the worker stays on JSON `Value`s.
pub struct GrpcTaskApi {
    client: Client,
}

impl GrpcTaskApi {
    /// Wrap an already-constructed [`Client`] — the worker shares the caller's dialed channel. This
    /// is how the [`Worker`](super::Worker) builder binds to a client ([`crate::Client::new_worker`]);
    /// [`connect`](GrpcTaskApi::connect) is the standalone address-dialing form.
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Dial the server's `Task` service at `address` (a bare `host:port` or a full `http(s)://` URL).
    /// Constructing only dials the channel; the first RPC performs the actual connection.
    pub async fn connect(address: &str) -> Result<Self, crate::ConnectError> {
        Ok(Self {
            client: Client::connect(address).await?,
        })
    }
}

/// Map a gRPC call failure onto a [`TaskApiError`]. The worker only *logs* these (it polls again and
/// never branches on the error), so we deliberately do not reconstruct a structured error here: the
/// RPC itself failed, rather than the engine rejecting or the task failing — a flat diagnostic string
/// is all the worker needs.
fn to_rpc_error(op: &str, status: crate::Status) -> TaskApiError {
    TaskApiError(format!("{op} gRPC call failed: {status}"))
}

/// Reconstruct a parsed worker [`ClaimedTask`] from its wire form (ULID string + opaque JSON
/// arguments). A malformed response is a server/protocol fault, surfaced as `TaskApiError` — the
/// worker treats it like any other poll failure (log + retry the next round).
fn from_wire_task(t: WireClaimedTask) -> Result<ClaimedTask, TaskApiError> {
    let arguments = serde_json::from_slice(&t.arguments)
        .map_err(|e| TaskApiError(format!("server returned invalid arguments JSON: {e}")))?;
    Ok(ClaimedTask {
        task_name: t.task_name,
        resource: t.resource,
        arguments,
    })
}

#[async_trait::async_trait]
impl TaskApi for GrpcTaskApi {
    async fn poll_tasks(
        &self,
        worker_id: &str,
        resource: &str,
        max_tasks: usize,
        lease_seconds: u64,
    ) -> Result<Vec<ClaimedTask>, TaskApiError> {
        let tasks = self
            .client
            .poll_tasks(worker_id, resource, max_tasks as u32, lease_seconds)
            .await
            .map_err(|s| to_rpc_error("PollTasks", s))?;
        tasks.into_iter().map(from_wire_task).collect()
    }

    async fn complete(
        &self,
        worker_id: &str,
        task_name: &str,
        request_id: &str,
        output: serde_json::Value,
    ) -> Result<(), TaskApiError> {
        self.client
            .complete_task(
                worker_id,
                task_name,
                request_id,
                &serde_json::to_vec(&output).unwrap_or_default(),
            )
            .await
            .map_err(|s| to_rpc_error("CompleteTask", s))
    }

    async fn fail(
        &self,
        worker_id: &str,
        task_name: &str,
        error: TaskFailure,
    ) -> Result<(), TaskApiError> {
        self.client
            .fail_task(
                worker_id,
                task_name,
                &WireFailure {
                    error_name: error.error_name,
                    output: serde_json::to_vec(&error.output).unwrap_or_default(),
                },
            )
            .await
            .map_err(|s| to_rpc_error("FailTask", s))
    }
}
