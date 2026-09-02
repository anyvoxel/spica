//! Shared service state and error-mapping helpers for the three tonic gRPC services
//! ([`crate::workflow`], [`crate::execution`], [`crate::task`]).

use std::sync::Arc;

use spica_engine::{
    Engine, ExecutionError, ObjectKind, ObjectName, ObjectReference, RejectionType, RuntimeError,
};
use tonic::Status;

/// Shared service state: the one [`Engine`] — the single authority for every RPC.
///
/// There is intentionally **no outer mutex**: `Engine` is internally synchronized. Every public
/// method the RPCs touch (`create_flow`, `start_for_revision`, `resolve_version_id`,
/// `execution_status`, `cancel_execution`) takes `&self` and guards its mutable state
/// (`storage`/`ids`/`ack`) behind per-field `tokio::sync::Mutex`s, so an `Arc<Engine>` shared across
/// the three tonic services is `Sync` and safe to call concurrently. Only `Engine::start` — which boots
/// the StreamProcessor — takes `&mut self`, and the server calls it on the owned Engine before wrapping it
/// in the `Arc` (see `main`). Calls that await an ack (birth of an execution / creation of a flow) do
/// not block on settlement, so the server stays responsive to concurrent `GetExecution` polls.
/// `Clone` is derived because each tonic service is handed its own clone of this handle.
#[derive(Clone)]
pub(crate) struct Svc {
    /// The running engine every RPC handler reads from / writes to. `pub(crate)` so the sibling
    /// service modules ([`crate::workflow`], [`crate::execution`], [`crate::task`]) can access
    /// `self.engine`.
    pub(crate) engine: Arc<Engine>,
}

/// Parse a ULID-string handle into a typed engine id, failing the RPC as `INVALID_ARGUMENT`.
///
/// `tonic::Status` is the error type the handler trait requires we return, so boxing it here would
/// only add a deref at every call site — the `result_large_err` size-worthiness is inherent to
/// propagating the gRPC status, not a leak we can shape away.
#[allow(clippy::result_large_err)]
pub(crate) fn parse_ulid<T>(s: &str, what: &str) -> Result<T, Status>
where
    T: From<ulid::Ulid>,
{
    s.parse::<ulid::Ulid>()
        .map(T::from)
        .map_err(|_| Status::invalid_argument(format!("invalid {what} ULID: {s:?}")))
}

/// Map an engine [`ObjectReference`] onto the wire [`spica_proto::v1::ObjectReference`] — the one
/// place a version reference crosses into the gRPC boundary (structured kind/name/uid on the wire).
pub(crate) fn proto_ref(r: &ObjectReference) -> spica_proto::v1::ObjectReference {
    spica_proto::v1::ObjectReference {
        kind: r.kind.as_str().to_string(),
        name: r.name.as_str(),
        uid: r.uid.to_string(),
    }
}

/// Parse a wire [`spica_proto::v1::ObjectReference`] back into an engine [`ObjectReference`],
/// failing the RPC as `INVALID_ARGUMENT` on an unknown kind or a malformed name/uid.
#[allow(clippy::result_large_err)]
pub(crate) fn parse_ref(r: spica_proto::v1::ObjectReference) -> Result<ObjectReference, Status> {
    let kind = ObjectKind::parse(&r.kind)
        .ok_or_else(|| Status::invalid_argument(format!("unknown object kind: {:?}", r.kind)))?;
    let name = ObjectName::from_parsed(&r.name)
        .map_err(|e| Status::invalid_argument(format!("invalid object name: {e}")))?;
    let uid = r
        .uid
        .parse::<ulid::Ulid>()
        .map_err(|_| Status::invalid_argument(format!("invalid uid ULID: {:?}", r.uid)))?;
    Ok(ObjectReference::new(kind, name, uid))
}

/// Map a spica engine error onto a gRPC status — the one place engine errors cross into a
/// wire-level `Status`, mirroring Restate's "map the already-stable code at the boundary" model.
///
/// The engine keeps type/code separate: [`RejectionType`] is a stable rejection taxonomy (the
/// Zeebe-style `ALREADY_EXISTS`/`INVALID_ARGUMENT`/… set) and [`RuntimeError`] distinguishes
/// structural faults from execution outcomes. We surface those here instead of flattening every
/// error to `INTERNAL`, so a client can tell "duplicate name" from "malformed definition" at the
/// gRPC code level rather than by scraping message text.
pub(crate) fn to_status(e: ExecutionError) -> Status {
    match e {
        // A well-formed command the engine refused to apply. The `RejectionType` is the stable code
        // taxonomy, mapped onto the nearest canonical gRPC code; the reason string is kept whole.
        ExecutionError::Rejected(reject) => match reject.rejection_type {
            RejectionType::AlreadyExists => Status::already_exists(reject.rejection_reason),
            RejectionType::NotFound => Status::not_found(reject.rejection_reason),
            RejectionType::InvalidArgument => Status::invalid_argument(reject.rejection_reason),
            // The remaining types are "wrong state / nothing to do / concurrent conflict" — all
            // preconditions of the request that are not satisfied, so failed_precondition is the
            // honest fit (Zeebe INVALID_STATE / STATE_CONFLICT equivalents).
            RejectionType::InvalidState
            | RejectionType::NotApplicable
            | RejectionType::StateConflict => Status::failed_precondition(reject.rejection_reason),
            RejectionType::ProcessingError => Status::internal(reject.rejection_reason),
        },
        // A runtime/domain failure. Structural faults the caller can fix (a bad definition, a
        // missing state) are INVALID_ARGUMENT; an execution *outcome* (Fail/Timeout/Cancel —
        // something that happened to an accepted run) stays INTERNAL so we never misrepresent an
        // execution result as a request error.
        ExecutionError::Runtime(err) => match err {
            RuntimeError::InvalidDefinition(_)
            | RuntimeError::StateNotFound(_)
            | RuntimeError::NoTerminal => Status::invalid_argument(err.to_string()),
            _ => Status::internal(err.to_string()),
        },
        // Infrastructure (log/storage) fault — always an internal condition.
        ExecutionError::Infra(_) => Status::internal(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::to_status;
    use spica_engine::{
        ExecutionError, InfraError, Reject, RejectionType, RequestId, RuntimeError,
    };
    use tonic::Code;

    fn rejected(kind: RejectionType) -> ExecutionError {
        ExecutionError::Rejected(Reject {
            request_id: RequestId::nil(),
            rejection_type: kind,
            rejection_reason: "boom".to_string(),
        })
    }

    #[test]
    fn rejection_types_map_to_the_canonical_grpc_codes() {
        assert_eq!(
            to_status(rejected(RejectionType::AlreadyExists)).code(),
            Code::AlreadyExists
        );
        assert_eq!(
            to_status(rejected(RejectionType::NotFound)).code(),
            Code::NotFound
        );
        assert_eq!(
            to_status(rejected(RejectionType::InvalidArgument)).code(),
            Code::InvalidArgument
        );
        // Wrong-state / conflict class → failed_precondition, not a masked INTERNAL.
        for kind in [
            RejectionType::InvalidState,
            RejectionType::NotApplicable,
            RejectionType::StateConflict,
        ] {
            assert_eq!(to_status(rejected(kind)).code(), Code::FailedPrecondition);
        }
        assert_eq!(
            to_status(rejected(RejectionType::ProcessingError)).code(),
            Code::Internal
        );
    }

    #[test]
    fn structural_runtime_faults_are_invalid_argument_but_outcomes_stay_internal() {
        assert_eq!(
            to_status(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "bad def".into()
            )))
            .code(),
            Code::InvalidArgument
        );
        assert_eq!(
            to_status(ExecutionError::Runtime(RuntimeError::StateNotFound(
                "s".into()
            )))
            .code(),
            Code::InvalidArgument
        );
        // A runtime outcome of an accepted execution must not read as a request error.
        assert_eq!(
            to_status(ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "s".into(),
                error: "E".into(),
                output: Box::new(serde_json::json!({})),
            }))
            .code(),
            Code::Internal
        );
        assert_eq!(
            to_status(ExecutionError::Infra(InfraError::Log("disk".into()))).code(),
            Code::Internal
        );
    }

    #[test]
    fn the_end_to_end_reject_carries_the_reason() {
        // `Status::already_exists`→ `already_exists(msg)` puts the message on the Status; check
        // the exact code and that the reason text survives on the wire.
        let s = to_status(rejected(RejectionType::AlreadyExists));
        assert_eq!(s.code(), Code::AlreadyExists);
        assert!(s.message().contains("boom"));
    }
}
