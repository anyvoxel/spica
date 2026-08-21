//! Shared service state and error-mapping helpers for the two tonic gRPC services
//! ([`crate::workflow`], [`crate::execution`]).

use std::sync::Arc;

use spica_engine::Engine;
use tonic::Status;

/// Shared service state: the one [`Engine`] — the single authority for every RPC.
///
/// There is intentionally **no outer mutex**: `Engine` is internally synchronized. Every public
/// method the RPCs touch (`create_flow`, `start_for_revision`, `resolve_version_id`,
/// `execution_status`, `cancel_execution`) takes `&self` and guards its mutable state
/// (`storage`/`ids`/`ack`) behind per-field `tokio::sync::Mutex`s, so an `Arc<Engine>` shared across
/// the two tonic services is `Sync` and safe to call concurrently. Only `Engine::start` — which boots
/// the StreamProcessor — takes `&mut self`, and the server calls it on the owned Engine before wrapping it
/// in the `Arc` (see `main`). Calls that await an ack (birth of an execution / creation of a flow) do
/// not block on settlement, so the server stays responsive to concurrent `GetExecution` polls.
/// `Clone` is derived because each tonic service is handed its own clone of this handle.
#[derive(Clone)]
pub(crate) struct Svc {
    /// The running engine every RPC handler reads from / writes to. `pub(crate)` so the sibling
    /// service modules ([`crate::workflow`], [`crate::execution`]) can access `self.engine`.
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

/// Map a spica engine error onto a gRPC status. Structural/validation faults carry messages that
/// help the client correct its request; there is no finer-grained code taxonomy wired yet, so they
/// land as `INTERNAL` with the engine's message preserved.
pub(crate) fn to_status(e: spica_engine::ExecutionError) -> Status {
    Status::internal(e.to_string())
}
