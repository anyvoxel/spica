//! `Execution` service handlers — running and observing executions: `StartExecution` (binds a
//! revision and returns the id at birth, non-blocking), `GetExecution` (point-in-time snapshot,
//! polled to settlement), and `StopExecution` (issue an abort, non-blocking).

use spica_engine::{ExecutionId, ExecutionStatus, FlowName, ObjectKind, ObjectReference};
use spica_proto::v1::{
    ExecutionState, GetExecutionRequest, GetExecutionResponse, StartExecutionRequest,
    StartExecutionResponse, StopExecutionRequest, StopExecutionResponse,
    execution_server::Execution as ExecutionService,
};
use tonic::{Request, Response, Status};

use crate::common::{Svc, parse_ref, parse_ulid, to_status};

#[tonic::async_trait]
impl ExecutionService for Svc {
    /// Bind a revision and start a run, returning the execution id **at birth** — the client settles
    /// by polling [`GetExecution`](Self::get_execution), not by blocking here.
    async fn start_execution(
        &self,
        request: Request<StartExecutionRequest>,
    ) -> Result<Response<StartExecutionResponse>, Status> {
        let req = request.into_inner();
        let input: serde_json::Value = serde_json::from_slice(&req.input)
            .map_err(|e| Status::invalid_argument(format!("input is not valid JSON: {e}")))?;

        // Resolve the revision to a concrete version reference: either the explicit reference
        // CreateFlow returned, or a (name, version) lookup resolved non-blocking against the projection.
        let flow_version = match req.target {
            Some(spica_proto::v1::start_execution_request::Target::FlowVersion(r)) => parse_ref(r)?,
            Some(spica_proto::v1::start_execution_request::Target::FlowName(name)) => {
                let name = FlowName::new(&name)
                    .map_err(|e| Status::invalid_argument(format!("invalid flow name: {e}")))?;
                self.engine
                    .resolve_version_id(name, req.version)
                    .await
                    .map_err(to_status)?
            }
            None => {
                return Err(Status::invalid_argument(
                    "StartExecution: no target (flow_version or flow_name) set",
                ));
            }
        };

        tracing::debug!(flow_version = %flow_version, "StartExecution");
        // The engine requires a user-supplied execution name; validate it here at the gRPC boundary
        // (`ObjectName::plain` bans the reserved `-`, among other invalids) so a bad or missing name is
        // rejected before any command is appended.
        let name = spica_engine::ObjectName::plain(&req.name)
            .map_err(|e| Status::invalid_argument(format!("invalid execution name: {e}")))?;
        let execution_id = self
            .engine
            .start_for_revision(name, flow_version, input)
            .await
            .map_err(to_status)?;
        Ok(Response::new(StartExecutionResponse {
            execution_id: execution_id.uid.to_string(),
            // Echo the validated name back as the primary handle — the key later GetExecution /
            // StopExecution calls resolve by.
            name: req.name,
        }))
    }

    /// Return a **point-in-time** snapshot of one execution (non-blocking); the client polls this at
    /// its own cadence until `COMPLETED` / `TERMINATED`.
    async fn get_execution(
        &self,
        request: Request<GetExecutionRequest>,
    ) -> Result<Response<GetExecutionResponse>, Status> {
        let req = request.into_inner();
        // The execution's user-supplied name (validated here exactly as StartExecution does) — the
        // per-scope-unique key the projection is indexed by.
        let name = spica_engine::ObjectName::plain(&req.name)
            .map_err(|e| Status::invalid_argument(format!("invalid execution name: {e}")))?;

        // Never blocks on settlement: it's a single projection read (name-keyed) under the engine's
        // internal storage lock. The uid is left nil — storage indexes executions by name, so the
        // lookup resolves regardless of incarnation.
        let snapshot = self
            .engine
            .execution_status(&ObjectReference::new(
                ObjectKind::Execution,
                name,
                ulid::Ulid::nil(),
            ))
            .await
            .map_err(to_status)?;
        let Some(snap) = snapshot else {
            return Ok(Response::new(GetExecutionResponse {
                state: ExecutionState::NotFound as i32,
                output: Vec::new(),
                error_name: String::new(),
                error_output: Vec::new(),
            }));
        };

        // Map the engine's lifecycle status onto the coarse wire state the client settles on.
        let resp = match snap.status {
            ExecutionStatus::Completed => GetExecutionResponse {
                state: ExecutionState::Completed as i32,
                output: serde_json::to_vec(&snap.output.unwrap_or(serde_json::Value::Null))
                    .unwrap_or_default(),
                error_name: String::new(),
                error_output: Vec::new(),
            },
            ExecutionStatus::Terminated(_) => GetExecutionResponse {
                state: ExecutionState::Terminated as i32,
                output: Vec::new(),
                error_name: snap.error_name.unwrap_or_default(),
                error_output: snap
                    .error_output
                    .map(|v| serde_json::to_vec(&v).unwrap_or_default())
                    .unwrap_or_default(),
            },
            // Running / Completing / Terminating: still in flight.
            _ => GetExecutionResponse {
                state: ExecutionState::Active as i32,
                output: Vec::new(),
                error_name: String::new(),
                error_output: Vec::new(),
            },
        };
        Ok(Response::new(resp))
    }

    /// Abort a running execution by issuing `TerminateExecution{Cancelled}` against the engine's own
    /// log. Non-blocking (appends and returns immediately): the empty confirmation only means the
    /// termination command was *accepted*, not that the execution has settled — the client observes
    /// `TERMINATED` by polling [`GetExecution`](Self::get_execution), mirroring the fire-and-forget
    /// model of `StartExecution`.
    async fn stop_execution(
        &self,
        request: Request<StopExecutionRequest>,
    ) -> Result<Response<StopExecutionResponse>, Status> {
        let req = request.into_inner();
        // The execution's user-supplied name (validated here at the gRPC boundary exactly as
        // StartExecution does) — the per-scope-unique key the termination resolves by.
        let name = spica_engine::ObjectName::plain(&req.name)
            .map_err(|e| Status::invalid_argument(format!("invalid execution name: {e}")))?;
        // Optional incarnation guard: an empty uid skips it; a present one is parsed and passed through
        // so the termination is refused unless the named execution is exactly this incarnation.
        let uid = if req.uid.is_empty() {
            None
        } else {
            Some(parse_ulid::<ExecutionId>(&req.uid, "uid")?.into())
        };

        tracing::debug!(name = %name, "StopExecution");
        self.engine
            .cancel_execution(name, uid)
            .await
            .map_err(to_status)?;
        Ok(Response::new(StopExecutionResponse { name: req.name }))
    }
}
