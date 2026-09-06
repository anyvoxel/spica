//! `QueryService` handlers — the k8s-style read API over the persisted projection: `GetObject`
//! (one object of any kind by name) and `ListObjects` (a kind, paged by `limit` + continue-token).
//! Both are non-blocking point-in-time reads delegated to the engine's read facade; this module is
//! the single place engine rows cross into their wire mirrors.

use spica_engine::{ObjectKind, ObjectName, ObjectReference, QueryObject, TerminationReason};
use spica_proto::v1::{
    GetObjectRequest, GetObjectResponse, ListObjectsRequest, ListObjectsResponse,
    query_server::Query as QueryServiceTrait,
};
use tonic::{Request, Response, Status};

use crate::common::{Svc, to_status};

/// Default page size when a `ListObjects` request omits `limit`.
const DEFAULT_LIST_LIMIT: usize = 100;

#[tonic::async_trait]
impl QueryServiceTrait for Svc {
    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectResponse>, Status> {
        let req = request.into_inner();
        let kind = parse_kind(&req.kind)?;
        let name = parse_name(&req.name)?;
        let Some(obj) = self
            .engine
            .get_object(kind, &name)
            .await
            .map_err(to_status)?
        else {
            return Err(Status::not_found(format!(
                "{} {} not found",
                kind.as_str(),
                name.as_str()
            )));
        };
        Ok(Response::new(GetObjectResponse {
            object: Some(to_proto_object(&obj)),
        }))
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let req = request.into_inner();
        let kind = parse_kind(&req.kind)?;
        let limit = if req.limit == 0 {
            DEFAULT_LIST_LIMIT
        } else {
            req.limit as usize
        };
        let continue_token = if req.continue_token.is_empty() {
            None
        } else {
            Some(req.continue_token.clone())
        };
        let page = self
            .engine
            .list_objects(kind, limit, continue_token.as_deref())
            .await
            .map_err(to_status)?;
        Ok(Response::new(ListObjectsResponse {
            objects: page.objects.iter().map(to_proto_object).collect(),
            continue_token: page.continue_token.unwrap_or_default(),
        }))
    }
}

// These return `Result<_, tonic::Status>` (a `Status` is inherently larger than the small `Ok`
// kinds), the same allowance the client crate applies to its service-returning methods.
#[allow(clippy::result_large_err)]
fn parse_kind(raw: &str) -> Result<ObjectKind, Status> {
    ObjectKind::parse(raw)
        .ok_or_else(|| Status::invalid_argument(format!("unknown object kind: {raw:?}")))
}

#[allow(clippy::result_large_err)]
fn parse_name(raw: &str) -> Result<ObjectName, Status> {
    ObjectName::from_parsed(raw).map_err(|e| Status::invalid_argument(format!("invalid name: {e}")))
}

/// Map an engine query row onto its wire mirror. The one place a persisted row crosses into a
/// `spica_proto` `Object`; JSON values and the nested serde bookkeeping travel as bytes.
fn to_proto_object(obj: &QueryObject) -> spica_proto::v1::Object {
    use spica_proto::v1::object::Object as O;
    let inner = match obj {
        QueryObject::Flow(f) => O::Flow(to_proto_flow(f)),
        QueryObject::FlowVersion(v) => O::FlowVersion(to_proto_flow_version(v)),
        QueryObject::Execution(e) => O::Execution(to_proto_execution(e)),
        QueryObject::Thread(t) => O::Thread(to_proto_thread(t)),
        QueryObject::Activity(a) => O::Activity(to_proto_activity(a)),
        QueryObject::Timer(t) => O::Timer(to_proto_timer(t)),
        QueryObject::Task(t) => O::Task(to_proto_task(t)),
    };
    spica_proto::v1::Object {
        object: Some(inner),
    }
}

/// The `meta` shared by every mirror — kind/tenant/namespace/name/uid + timing + owner.
fn proto_meta(meta: &spica_engine::ObjectMeta) -> spica_proto::v1::ObjectMeta {
    spica_proto::v1::ObjectMeta {
        kind: meta.kind.as_str().to_string(),
        tenant: meta.tenant.as_str().to_string(),
        namespace: meta.namespace.as_str().to_string(),
        name: meta.name.as_str(),
        uid: meta.uid.to_string(),
        created_at_millis: meta.created_at.as_millis() as i64,
        updated_at_millis: meta.updated_at.as_millis() as i64,
        owner: meta.owner.as_ref().map(proto_ref),
    }
}

fn proto_ref(r: &ObjectReference) -> spica_proto::v1::ObjectReference {
    spica_proto::v1::ObjectReference {
        kind: r.kind.as_str().to_string(),
        name: r.name.as_str(),
        uid: r.uid.to_string(),
    }
}

fn proto_termination_reason(r: &TerminationReason) -> spica_proto::v1::TerminationReason {
    use spica_engine::TerminationReason as R;
    let mut out = spica_proto::v1::TerminationReason {
        failed: None,
        timed_out: false,
        cancelled: false,
    };
    match r {
        R::Failed { error } => {
            out.failed = Some(spica_proto::v1::ErrorFailure {
                error_name: error.error_name().to_string(),
                output: error
                    .error_output()
                    .map(|v| serde_json::to_vec(&v).unwrap_or_default())
                    .unwrap_or_default(),
            });
        }
        R::TimedOut => out.timed_out = true,
        R::Cancelled => out.cancelled = true,
    }
    out
}

fn to_proto_flow(f: &spica_engine::Flow) -> spica_proto::v1::Flow {
    spica_proto::v1::Flow {
        meta: Some(proto_meta(&f.meta)),
        status: match f.status {
            spica_engine::FlowStatus::Active => spica_proto::v1::FlowStatus::Active as i32,
            spica_engine::FlowStatus::Deleted => spica_proto::v1::FlowStatus::Deleted as i32,
        },
        latest_version: f.latest_version,
    }
}

fn to_proto_flow_version(v: &spica_engine::FlowVersion) -> spica_proto::v1::FlowVersion {
    spica_proto::v1::FlowVersion {
        meta: Some(proto_meta(&v.meta)),
        version: v.version,
        definition: v.definition.as_bytes().to_vec(),
        checksum: v.checksum,
    }
}

fn to_proto_execution(e: &spica_engine::ExecutionRecord) -> spica_proto::v1::Execution {
    use spica_proto::v1::ExecutionStatus as S;
    let (status, reason) = match &e.value.status {
        spica_engine::ExecutionStatus::Running => (S::Running as i32, None),
        spica_engine::ExecutionStatus::Completing => (S::Completing as i32, None),
        spica_engine::ExecutionStatus::Terminating(r) => (S::Terminating as i32, Some(r)),
        spica_engine::ExecutionStatus::Completed => (S::Completed as i32, None),
        spica_engine::ExecutionStatus::Terminated(r) => (S::Terminated as i32, Some(r)),
    };
    spica_proto::v1::Execution {
        meta: Some(proto_meta(&e.value.meta)),
        flow_version: Some(proto_ref(&e.value.flow_version)),
        status,
        termination_reason: reason.map(proto_termination_reason),
        input: serde_json::to_vec(&e.value.input).unwrap_or_default(),
        output: e
            .value
            .output
            .as_ref()
            .map(|v| serde_json::to_vec(v).unwrap_or_default())
            .unwrap_or_default(),
        variables: e
            .variables
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::to_vec(v).unwrap_or_default()))
            .collect(),
    }
}

fn to_proto_thread(t: &spica_engine::ThreadRecord) -> spica_proto::v1::Thread {
    use spica_proto::v1::ThreadStatus as S;
    let (status, reason) = match &t.value.status {
        spica_engine::ThreadStatus::Running => (S::Running as i32, None),
        spica_engine::ThreadStatus::Completing => (S::Completing as i32, None),
        spica_engine::ThreadStatus::Terminating(r) => (S::Terminating as i32, Some(r)),
        spica_engine::ThreadStatus::Completed => (S::Completed as i32, None),
        spica_engine::ThreadStatus::Terminated(r) => (S::Terminated as i32, Some(r)),
    };
    spica_proto::v1::Thread {
        meta: Some(proto_meta(&t.value.meta)),
        execution: Some(proto_ref(&t.value.execution)),
        state_path: t.value.state_path.to_string(),
        index: t.value.index as u64,
        status,
        termination_reason: reason.map(proto_termination_reason),
        input: serde_json::to_vec(&t.value.input).unwrap_or_default(),
        output: t
            .value
            .output
            .as_ref()
            .map(|v| serde_json::to_vec(v).unwrap_or_default())
            .unwrap_or_default(),
        variables: t
            .variables
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::to_vec(v).unwrap_or_default()))
            .collect(),
    }
}

fn to_proto_activity(a: &spica_engine::ActivityRecord) -> spica_proto::v1::Activity {
    use spica_proto::v1::ActivityStatus as S;
    let (status, reason) = match &a.value.status {
        spica_engine::ActivityStatus::Running => (S::Running as i32, None),
        spica_engine::ActivityStatus::Completing => (S::Completing as i32, None),
        spica_engine::ActivityStatus::Terminating(r) => (S::Terminating as i32, Some(r)),
        spica_engine::ActivityStatus::Completed => (S::Completed as i32, None),
        spica_engine::ActivityStatus::Terminated(r) => (S::Terminated as i32, Some(r)),
    };
    spica_proto::v1::Activity {
        meta: Some(proto_meta(&a.value.meta)),
        execution: Some(proto_ref(&a.value.execution)),
        state_path: a.value.state_path.to_string(),
        status,
        termination_reason: reason.map(proto_termination_reason),
        raw_input: serde_json::to_vec(&a.value.raw_input).unwrap_or_default(),
        input: serde_json::to_vec(&a.value.input).unwrap_or_default(),
        raw_output: serde_json::to_vec(&a.value.raw_output).unwrap_or_default(),
        output: serde_json::to_vec(&a.value.output).unwrap_or_default(),
        activity_state: a
            .value
            .activity_state
            .as_ref()
            .map(|s| serde_json::to_vec(s).unwrap_or_default())
            .unwrap_or_default(),
        retry_state: a
            .value
            .retry_state
            .as_ref()
            .map(|s| serde_json::to_vec(s).unwrap_or_default())
            .unwrap_or_default(),
    }
}

fn to_proto_timer(t: &spica_engine::TimerRecord) -> spica_proto::v1::Timer {
    use spica_proto::v1::{TimerPurpose as P, TimerStatus as S};
    spica_proto::v1::Timer {
        meta: Some(proto_meta(&t.value.meta)),
        execution: Some(proto_ref(&t.value.execution)),
        purpose: match t.value.purpose {
            spica_engine::TimerPurpose::ExecutionTimeout => P::ExecutionTimeout as i32,
            spica_engine::TimerPurpose::WaitResume => P::WaitResume as i32,
            spica_engine::TimerPurpose::TaskTimeout => P::TaskTimeout as i32,
            spica_engine::TimerPurpose::DeliveryLease => P::DeliveryLease as i32,
        },
        status: match t.value.status {
            spica_engine::TimerStatus::Active => S::Active as i32,
            spica_engine::TimerStatus::Completed => S::Completed as i32,
            spica_engine::TimerStatus::Cancelled => S::Cancelled as i32,
        },
        deadline_millis: t.value.deadline.as_millis() as i64,
    }
}

fn to_proto_task(t: &spica_engine::TaskRecord) -> spica_proto::v1::Task {
    use spica_proto::v1::TaskStatus as S;
    spica_proto::v1::Task {
        meta: Some(proto_meta(&t.value.meta)),
        execution: Some(proto_ref(&t.value.execution)),
        resource: t.value.resource.clone(),
        arguments: serde_json::to_vec(&t.value.arguments).unwrap_or_default(),
        status: match t.value.status {
            spica_engine::TaskStatus::Pending => S::Pending as i32,
            spica_engine::TaskStatus::Running => S::Running as i32,
            spica_engine::TaskStatus::Completed => S::Completed as i32,
            spica_engine::TaskStatus::Failed => S::Failed as i32,
            spica_engine::TaskStatus::Cancelled => S::Cancelled as i32,
        },
        deadline_millis: t
            .value
            .deadline
            .map(|d| d.as_millis() as i64)
            .unwrap_or_default(),
        worker_id: t.value.worker_id.clone().unwrap_or_default(),
        lease_until_millis: t
            .value
            .lease_until
            .map(|d| d.as_millis() as i64)
            .unwrap_or_default(),
        retry_plan: serde_json::to_vec(&t.value.retry_plan).unwrap_or_default(),
        retry_state: serde_json::to_vec(&t.value.retry_state).unwrap_or_default(),
    }
}
