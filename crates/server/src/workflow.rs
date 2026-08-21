//! `Workflow` service handlers — definition versioning: `CreateFlow` persists a definition and
//! returns its system-minted `FlowVersionId`; `ResolveFlowVersion` resolves a name (+ ordinal
//! version) to its concrete `FlowVersionId` without starting anything.

use spica_engine::FlowName;
use spica_proto::v1::{
    CreateFlowRequest, CreateFlowResponse, ResolveFlowVersionRequest, ResolveFlowVersionResponse,
    workflow_server::Workflow as WorkflowService,
};
use tonic::{Request, Response, Status};

use crate::common::{Svc, to_status};

#[tonic::async_trait]
impl WorkflowService for Svc {
    /// Persist a new flow version and return the system-minted `FlowVersionId`.
    async fn create_flow(
        &self,
        request: Request<CreateFlowRequest>,
    ) -> Result<Response<CreateFlowResponse>, Status> {
        let req = request.into_inner();
        let name = FlowName::new(&req.name)
            .map_err(|e| Status::invalid_argument(format!("invalid flow name: {e}")))?;
        let definition = std::str::from_utf8(&req.definition)
            .map_err(|e| Status::invalid_argument(format!("definition is not UTF-8: {e}")))?;

        tracing::debug!(flow = %name, bytes = req.definition.len(), "CreateFlow");
        let fvid = self
            .engine
            .create_flow(name, definition)
            .await
            .map_err(to_status)?;
        Ok(Response::new(CreateFlowResponse {
            flow_version_id: fvid.0.to_string(),
        }))
    }

    /// Resolve a flow name (+ optional ordinal version) to its concrete `FlowVersionId` — a
    /// non-blocking read of the persisted projection, so a client can capture the id (for scripting,
    /// or to bind `StartExecution` by explicit revision later) without starting anything.
    async fn resolve_flow_version(
        &self,
        request: Request<ResolveFlowVersionRequest>,
    ) -> Result<Response<ResolveFlowVersionResponse>, Status> {
        let req = request.into_inner();
        let name = FlowName::new(&req.flow_name)
            .map_err(|e| Status::invalid_argument(format!("invalid flow name: {e}")))?;

        tracing::debug!(flow = %name, version = req.version, "ResolveFlowVersion");
        let fvid = self
            .engine
            .resolve_version_id(name, req.version)
            .await
            .map_err(to_status)?;
        Ok(Response::new(ResolveFlowVersionResponse {
            flow_version_id: fvid.0.to_string(),
        }))
    }
}
