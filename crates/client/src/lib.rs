//! `spica-client` — the single SDK crate every spica *consumer* links to reach a `spica-server`.
//!
//! This crate is the one place that knows the transport. It dials an endpoint, owns the `spica-proto`
//! wire contract, and exposes two module-scoped faces:
//!
//! - the **control plane** — the thin [`Client`], whose write/action methods return plain wire data
//!   (`String` ids, [`ClaimedTask`], [`ObjectReference`]), and whose **read** methods
//!   ([`Client::get_object`] / [`Client::list_objects`]) return the wire `spica_proto::v1::Object`
//!   mirror directly — the k8s-style Query API is a generic read over any kind, so the one wire type
//!   covers every object and needs no per-kind client-side shadow;
//! - the **worker** ([`worker`]) — the task *consumer* (Zeebe's job worker), typed in its own parsed
//!   forms (JSON `Value`s) rather than the client's opaque `Vec<u8>`, and engine-free.
//!
//! Cross-cutting transport concerns (retry, auth, timeouts, load balancing) have exactly one place to
//! land: here.
//!
//! # Why not depend on `spica-engine`?
//!
//! The `spica` CLI is a *pure-remote* client — it must not transitively link the engine. Had
//! [`Client`] returned engine types, every consumer (CLI included) would carry the engine in. So this
//! crate stops at plain wire / proto types — engine types never cross its boundary. The one carve-out
//! is the Query read face, which returns the `spica_proto::v1::Object` mirror verbatim (the user's
//! explicit call): mirroring all seven kinds by hand would duplicate the proto contract for no
//! consumer benefit, and the CLI reads by matching one kind out of the generic `Object`.

use spica_proto::v1::{
    CompleteTaskRequest, CreateFlowRequest, FailTaskRequest, GetObjectRequest, ListObjectsRequest,
    PollTasksRequest, ResolveFlowVersionRequest, StartExecutionRequest, StopExecutionRequest,
    execution_service_client::ExecutionServiceClient, query_client::QueryClient,
    start_execution_request::Target as WireTarget, task_service_client::TaskServiceClient,
    workflow_service_client::WorkflowServiceClient,
};
use tonic::transport::{Channel, Endpoint};

// The task *consumer* half, colocated here (Zeebe-style "one SDK") but kept in its own engine-free,
// `Value`-typed module so its types don't collide with the bytes types above. See `worker/mod.rs`.
pub mod worker;

// Re-exported so consumers need not depend on `tonic`/`transport` themselves — the error types a
// client call produces, and the gRPC code used to branch on them (e.g. `NotFound` from a Query read),
// are part of this crate's public surface.
/// The error returned when [`Client::connect`] fails to dial the endpoint.
pub use tonic::transport::Error as ConnectError;
pub use tonic::{Code, Status};

/// The coarse lifecycle state a client settles on, collapsed from a wire `Execution`.
/// (Wire `UNSPECIFIED`/`RUNNING`/`COMPLETING`/`TERMINATING` all fold into [`ExecutionState::Active`]
/// — still in flight.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionState {
    /// Still in flight (Running / Completing / Terminating).
    Active,
    /// Settled successfully; `output` carries the result.
    Completed,
    /// Settled abnormally; `error_name` / `error_output` describe the failure.
    Terminated,
}

/// The client-side interpretation of a wire `Execution` mirror — produced by
/// [`Client::get_execution`] from a Query read. `state` is the coarse lifecycle; `output` /
/// `error_name` / `error_output` carry the terminal facts.
pub struct ExecutionSnapshot {
    pub state: ExecutionState,
    /// Terminal output (JSON bytes), present once `state == Completed`.
    pub output: Vec<u8>,
    /// ASL error name, present once `state == Terminated`.
    pub error_name: String,
    /// Error output (JSON bytes), present once `state == Terminated`.
    pub error_output: Vec<u8>,
}

/// A page of `ListObjects` results — the wire `Object` mirrors plus the opaque resume token.
pub struct ListObjectsPage {
    /// The page of objects, in storage key order (at most the requested `limit`).
    pub objects: Vec<spica_proto::v1::Object>,
    /// Non-empty when more rows remain; pass it back as the next request's `continue_token`.
    pub continue_token: String,
}

/// A structured reference to a flow version (mirrors the wire `ObjectReference`): the `kind`
/// (currently always `flowversion`), the version's generated `{flow_name}-{version}` `name`, and its
/// never-reused `uid`. Rendered as `kind/name/uid` so it round-trips through the CLI as one copyable
/// token (`flows create` prints it, `executions start --flow-version` accepts it back).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectReference {
    /// The referenced object's kind (e.g. `flowversion`).
    pub kind: String,
    /// The referenced object's name (a version's generated `{flow_name}-{version}`).
    pub name: String,
    /// The referenced object's never-reused incarnation id (ULID string).
    pub uid: String,
}

impl std::fmt::Display for ObjectReference {
    // `kind/name/uid` — none of the three segments contains `/` (names are `[A-Za-z0-9_-]`, a kind is
    // a lowercase token, an uid is hex), so the triple is unambiguous to re-parse.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.kind, self.name, self.uid)
    }
}

impl std::str::FromStr for ObjectReference {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut it = s.splitn(3, '/');
        let kind = it.next().filter(|k| !k.is_empty()).ok_or("missing kind")?;
        let name = it.next().filter(|n| !n.is_empty()).ok_or("missing name")?;
        let uid = it.next().filter(|u| !u.is_empty()).ok_or("missing uid")?;
        Ok(Self {
            kind: kind.to_string(),
            name: name.to_string(),
            uid: uid.to_string(),
        })
    }
}

/// How an execution's target revision is addressed (mirrors `StartExecutionRequest`'s oneof).
pub enum Target {
    /// A concrete revision already returned by `create_flow`.
    ByVersion(ObjectReference),
    /// A flow name plus an ordinal `version` (0 = latest).
    ByName { name: String, version: u32 },
}

/// The arguments to [`Client::start_execution`].
pub struct StartExecution {
    pub target: Target,
    /// The execution input (JSON bytes, opaque to the wire).
    pub input: Vec<u8>,
    /// The execution's user-supplied name; must satisfy `ObjectName::plain` (no `-`), enforced
    /// server-side.
    pub name: String,
}

/// One task handed to a worker by [`Client::poll_tasks`] — the wire form of the engine's
/// `ActivatedTask`, kept scalar/bytes (no engine types) so this crate stays transport-scoped.
pub struct ClaimedTask {
    /// The task's canonical name (String) — the handle to bind `complete_task` / `fail_task` on.
    pub task_name: String,
    /// The `Resource` URI the worker dispatched under.
    pub resource: String,
    /// The projected arguments (JSON bytes, opaque to the wire).
    pub arguments: Vec<u8>,
}

/// A task failure reported to the server by [`Client::fail_task`] — the wire form of the worker's
/// failure (ASL error name + error-output JSON), kept scalar/bytes so this crate stays
/// transport-scoped (callers construct no engine `ExecutionError`).
pub struct TaskFailure {
    /// The ASL error name (matched by `Retry`/`Catch` `ErrorEquals`).
    pub error_name: String,
    /// The error-output object (JSON bytes), bound to `$states.errorOutput` by a `Catch`.
    pub output: Vec<u8>,
}

/// One gRPC client for every `spica.v1` service, sharing a single dialed connection.
///
/// All methods take `&self` (each clone-then-calls the underlying tonic client), so a `Client` is
/// cheaply shareable behind an `Arc` — and, since every field is a cloneable tonic client over one
/// shared `Channel`, cheaply `Clone`-able too (a worker's own transport is just a channel clone).
#[derive(Clone)]
pub struct Client {
    workflow: WorkflowServiceClient<Channel>,
    execution: ExecutionServiceClient<Channel>,
    task: TaskServiceClient<Channel>,
    query: QueryClient<Channel>,
}

// The RPC methods deliberately return `tonic::Status` (re-exported above) so callers can branch on
// the wire gRPC code — boxing it would only add `.as_ref()` noise without shrinking what actually
// crosses the transport, so the `result_large_err` lint is allowed here.
#[allow(clippy::result_large_err)]
impl Client {
    /// Dial the server at `address` (a bare `host:port` or a full `http(s)://` URL). Constructing
    /// only dials the channel; the first RPC performs the actual connection.
    pub async fn connect(address: &str) -> Result<Self, ConnectError> {
        let endpoint = if address.contains("://") {
            address.to_string()
        } else {
            format!("http://{address}")
        };
        let channel = Endpoint::from_shared(endpoint)?.connect().await?;
        Ok(Self::from_channel(channel))
    }

    /// Build from an already-connected [`Channel`] — lets an embedding process that already owns a
    /// dialed channel construct every service client on the one connection.
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            workflow: WorkflowServiceClient::new(channel.clone()),
            execution: ExecutionServiceClient::new(channel.clone()),
            task: TaskServiceClient::new(channel.clone()),
            query: QueryClient::new(channel),
        }
    }

    /// Start building a task [`Worker`](worker::Worker) bound to this client — the Zeebe
    /// `newWorker()` / Temporal `worker.New(c, …)` analogue. The worker owns a cheap clone of this
    /// client's transport; register `resource → handler` pairs on the returned builder and call
    /// [`WorkerBuilder::build`](worker::WorkerBuilder::build) to obtain the worker object.
    pub fn new_worker(&self) -> worker::WorkerBuilder {
        worker::WorkerBuilder::new(self.clone())
    }

    /// Persist a new flow version (`CreateFlow`) and return its created version's `ObjectReference`.
    pub async fn create_flow(
        &self,
        name: &str,
        definition: &[u8],
    ) -> Result<ObjectReference, Status> {
        let mut client = self.workflow.clone();
        let resp = client
            .create_flow(CreateFlowRequest {
                name: name.to_string(),
                definition: definition.to_vec(),
            })
            .await?
            .into_inner();
        Ok(Self::to_ref(resp.flow_version.ok_or_else(|| {
            Status::internal("CreateFlow response missing flow_version")
        })?))
    }

    /// Resolve a flow name (+ optional ordinal version) to its concrete version `ObjectReference`.
    pub async fn resolve_flow_version(
        &self,
        name: &str,
        version: u32,
    ) -> Result<ObjectReference, Status> {
        let mut client = self.workflow.clone();
        let resp = client
            .resolve_flow_version(ResolveFlowVersionRequest {
                flow_name: name.to_string(),
                version,
            })
            .await?
            .into_inner();
        Ok(Self::to_ref(resp.flow_version.ok_or_else(|| {
            Status::internal("ResolveFlowVersion response missing flow_version")
        })?))
    }

    /// Start an execution and return its user-supplied `name` — the per-scope-unique handle later
    /// `get_execution` / `stop_execution` resolve by (non-blocking).
    pub async fn start_execution(&self, req: StartExecution) -> Result<String, Status> {
        // Split the target into the wire oneof + ordinal version (the latter only meaningful for the
        // name form; the explicit version-reference form ignores it).
        let (target, version) = match req.target {
            Target::ByVersion(r) => (Some(WireTarget::FlowVersion(Self::to_wire(&r))), 0),
            Target::ByName { name, version } => (Some(WireTarget::FlowName(name)), version),
        };
        let mut client = self.execution.clone();
        let resp = client
            .start_execution(StartExecutionRequest {
                target,
                version,
                input: req.input,
                name: req.name,
            })
            .await?
            .into_inner();
        Ok(resp.name)
    }

    /// Convert a wire `ObjectReference` message into the client's public form.
    fn to_ref(w: spica_proto::v1::ObjectReference) -> ObjectReference {
        ObjectReference {
            kind: w.kind,
            name: w.name,
            uid: w.uid,
        }
    }

    /// Convert the client's public reference into the wire `ObjectReference` message.
    fn to_wire(r: &ObjectReference) -> spica_proto::v1::ObjectReference {
        spica_proto::v1::ObjectReference {
            kind: r.kind.clone(),
            name: r.name.clone(),
            uid: r.uid.clone(),
        }
    }

    /// Read one persisted object of `kind` by `name` — the k8s-style Query `GetObject`, non-blocking.
    /// Returns the wire `Object` mirror for that `(kind, name)`; a missing row surfaces as a non-ok
    /// `Status` with gRPC code `NotFound`.
    pub async fn get_object(
        &self,
        kind: &str,
        name: &str,
    ) -> Result<spica_proto::v1::Object, Status> {
        let mut client = self.query.clone();
        let resp = client
            .get_object(GetObjectRequest {
                kind: kind.to_string(),
                name: name.to_string(),
            })
            .await?
            .into_inner();
        resp.object
            .ok_or_else(|| Status::internal("GetObject response missing object"))
    }

    /// List a page of one `kind` within the current scope — the k8s-style Query `ListObjects`:
    /// `limit` (0 = server default) rows plus the opaque `continue_token` to resume from (empty =
    /// first page).
    pub async fn list_objects(
        &self,
        kind: &str,
        limit: u32,
        continue_token: &str,
    ) -> Result<ListObjectsPage, Status> {
        let mut client = self.query.clone();
        let resp = client
            .list_objects(ListObjectsRequest {
                kind: kind.to_string(),
                limit,
                continue_token: continue_token.to_string(),
            })
            .await?
            .into_inner();
        Ok(ListObjectsPage {
            objects: resp.objects,
            continue_token: resp.continue_token,
        })
    }

    /// Read a point-in-time status snapshot of one execution, addressed by its user-supplied `name`
    /// (non-blocking). Thin wrapper over [`Client::get_object`] on kind `execution` that pulls the
    /// `Execution` out of the generic `Object` and collapses it — kept for the CLI's convenience; a
    /// missing execution surfaces as a non-ok `Status` with gRPC code `NotFound`.
    pub async fn get_execution(&self, name: &str) -> Result<ExecutionSnapshot, Status> {
        match self.get_object("execution", name).await?.object {
            Some(spica_proto::v1::object::Object::Execution(e)) => Ok(interpret_execution(&e)),
            Some(_) => Err(Status::internal(
                "GetObject kind=execution returned a non-execution",
            )),
            None => Err(Status::internal("GetObject returned an empty object")),
        }
    }

    /// Abort a running execution by `name` (with an optional `uid` incarnation guard — `None` skips
    /// it). Non-blocking; echoes the `name` as confirmation. Returns a non-ok `Status` on an invalid
    /// name or an engine refusal (e.g. the named execution is not running / a different incarnation).
    pub async fn stop_execution(&self, name: &str, uid: Option<&str>) -> Result<String, Status> {
        let mut client = self.execution.clone();
        let resp = client
            .stop_execution(StopExecutionRequest {
                name: name.to_string(),
                uid: uid.unwrap_or("").to_string(),
            })
            .await?
            .into_inner();
        Ok(resp.name)
    }

    /// Pull up to `max_tasks` available tasks of `resource`, leasing each to `worker_id` for
    /// `lease_seconds`. Returns whatever is claimable now (possibly empty).
    pub async fn poll_tasks(
        &self,
        worker_id: &str,
        resource: &str,
        max_tasks: u32,
        lease_seconds: u64,
    ) -> Result<Vec<ClaimedTask>, Status> {
        let mut client = self.task.clone();
        let resp = client
            .poll_tasks(PollTasksRequest {
                worker_id: worker_id.to_string(),
                resource: resource.to_string(),
                max_tasks,
                lease_seconds,
            })
            .await?
            .into_inner();
        Ok(resp
            .tasks
            .into_iter()
            .map(|t| ClaimedTask {
                task_name: t.task_name,
                resource: t.resource,
                arguments: t.arguments,
            })
            .collect())
    }

    /// Report a task completed with `output` (JSON bytes).
    ///
    /// `request_id` is this settlement's request/response correlation key (a worker-minted ULID
    /// string): the server echoes it back on the outcome, so this returns only after the engine has
    /// applied (or refused) the completion — the worker learns the actual result rather than polling
    /// for it later.
    pub async fn complete_task(
        &self,
        worker_id: &str,
        task_name: &str,
        request_id: &str,
        output: &[u8],
    ) -> Result<(), Status> {
        let mut client = self.task.clone();
        client
            .complete_task(CompleteTaskRequest {
                worker_id: worker_id.to_string(),
                task_name: task_name.to_string(),
                output: output.to_vec(),
                request_id: request_id.to_string(),
            })
            .await?;
        Ok(())
    }

    /// Report a task failed with `failure` (the ASL error name + error-output object). The server
    /// maps this structured failure onto the engine's internal error representation.
    pub async fn fail_task(
        &self,
        worker_id: &str,
        task_name: &str,
        failure: &TaskFailure,
    ) -> Result<(), Status> {
        let mut client = self.task.clone();
        client
            .fail_task(FailTaskRequest {
                worker_id: worker_id.to_string(),
                task_name: task_name.to_string(),
                error: Some(spica_proto::v1::TaskFailure {
                    error_name: failure.error_name.clone(),
                    output: failure.output.clone(),
                }),
            })
            .await?;
        Ok(())
    }
}

/// Collapse a wire `Execution` mirror onto this crate's client-facing snapshot. Wire
/// `UNSPECIFIED`/`RUNNING`/`COMPLETING`/`TERMINATING` are all "still in flight" from the client's
/// perspective (`Active`); `COMPLETED` carries `output`; `TERMINATED` carries the ASL `error_name` /
/// `error_output` from the termination reason.
pub fn interpret_execution(e: &spica_proto::v1::Execution) -> ExecutionSnapshot {
    let state = match spica_proto::v1::ExecutionStatus::try_from(e.status) {
        Ok(spica_proto::v1::ExecutionStatus::Completed) => ExecutionState::Completed,
        Ok(spica_proto::v1::ExecutionStatus::Terminated) => ExecutionState::Terminated,
        _ => ExecutionState::Active,
    };
    let (error_name, error_output) = match &e.termination_reason {
        Some(spica_proto::v1::TerminationReason {
            failed: Some(f), ..
        }) => (f.error_name.clone(), f.output.clone()),
        _ => (String::new(), Vec::new()),
    };
    ExecutionSnapshot {
        state,
        output: e.output.clone(),
        error_name,
        error_output,
    }
}
