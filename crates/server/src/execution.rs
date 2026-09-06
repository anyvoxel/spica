//! `ExecutionService` handlers — running and aborting executions: `StartExecution` (binds a revision
//! and returns the id at birth, non-blocking) and `StopExecution` (issue an abort, non-blocking).
//! Settlement (and any point-in-time snapshot) is read through the Query API, not a dedicated
//! per-kind `GetExecution` RPC.

use spica_engine::ExecutionId;
use spica_proto::v1::{
    StartExecutionRequest, StartExecutionResponse, StopExecutionRequest, StopExecutionResponse,
    execution_service_server::ExecutionService as ExecutionServiceTrait,
};
use tonic::{Request, Response, Status};

use crate::common::{Svc, parse_ref, parse_ulid, to_status};

#[tonic::async_trait]
impl ExecutionServiceTrait for Svc {
    /// Bind a revision and start a run, returning the execution id **at birth** — the client settles
    /// by polling [`Query.GetObject`](crate::query::QueryService::get_object) on kind `execution`,
    /// not by blocking here.
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
                let name = spica_engine::FlowName::new(&name)
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
            .facade
            .start_for_revision(name, flow_version, input)
            .await
            .map_err(to_status)?;
        Ok(Response::new(StartExecutionResponse {
            execution_id: execution_id.uid.to_string(),
            // Echo the validated name back as the primary handle — the key later Query reads /
            // StopExecution calls resolve by.
            name: req.name,
        }))
    }

    /// Abort a running execution by issuing `TerminateExecution{Cancelled}` against the engine's own
    /// log. Non-blocking (appends and returns immediately): the empty confirmation only means the
    /// termination command was *accepted*, not that the execution has settled — the client observes
    /// `TERMINATED` by polling the Query API on kind `execution`, mirroring the fire-and-forget
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
