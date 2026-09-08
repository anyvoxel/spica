use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::types::reject::Reject;

/// The ASL **runtime/domain** failure set of an execution — the errors `Retry`/`Catch` can
/// intercept via [`RuntimeError::error_name`] / [`RuntimeError::error_output`], and the terminal
/// outcome surfaced from a terminated execution. Kept as its own narrow type (per the TiKV-style
/// per-concern split) so the domain branch that genuinely needs exhaustive matching on error kinds
/// — `error_name`/`error_output`, Retry/Catch matching — does so over a small, cohesive set instead
/// of an umbrella that also carries infrastructure and command-layer concerns.
///
/// These are the domain failures a `Fail` state, an unhandled runtime error, a timeout, or a cancel
/// produce; a terminated execution carries one as `TerminationReason::Failed { error }`.
///
/// Note: `Engine::wait_for_execution` returns the whole terminal [`Execution`](crate::Execution) for
/// the caller to inspect, so a domain failure surfaces there as `status == Terminated`, not as the
/// wait call's error value.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum RuntimeError {
    /// A state referenced by `StartAt`/`Next`/`Default` is not present in `States`.
    #[error("state not found: {0}")]
    StateNotFound(String),

    /// A non-terminal state specified neither `Next` nor `End`, so execution cannot continue.
    #[error("no terminal state reached (a state is missing Next/End)")]
    NoTerminal,

    /// A `Choice` state matched no rule and has no `Default`.
    #[error("choice state {state} matched no rule and has no Default")]
    NoChoiceMatched { state: String },

    /// A `Fail` state terminated the execution.
    #[error("Fail state {state} reported error {error}")]
    StateFailed {
        state: String,
        error: String,
        /// Boxed: `Value` is the largest payload in the enum (pushing `RuntimeError` over the
        /// clippy `result_large_err` threshold); the error output is rarely inspected in hot paths.
        output: Box<Value>,
    },

    /// A JSONata expression failed to parse or evaluate.
    #[error("JSONata error in {field}: {message}")]
    Jsonata { field: String, message: String },

    /// The state machine uses a feature outside the supported subset, or a submitted definition /
    /// command resolution failed structurally (e.g. a flow/version no longer exists).
    #[error("invalid state definition: {0}")]
    InvalidDefinition(String),

    /// The execution ran past the state machine's `TimeoutSeconds`.
    #[error("execution timed out: {message}")]
    TimedOut { message: String },

    /// The execution was cancelled externally (engine `terminate`).
    #[error("execution cancelled: {message}")]
    Cancelled { message: String },
}

impl RuntimeError {
    /// The ASL reserved error name, used by `Retry`/`Catch` matching.
    ///
    /// Runtime errors (`Fail`, `NoChoiceMatched`, JSONata failures) carry ASL-defined names;
    /// structural faults are reported as `States.Runtime`.
    pub fn error_name(&self) -> &str {
        match self {
            RuntimeError::StateFailed { error, .. } => error,
            RuntimeError::NoChoiceMatched { .. } => "States.NoChoiceMatched",
            RuntimeError::Jsonata { .. } => "States.Runtime",
            RuntimeError::StateNotFound(_) => "States.Runtime",
            RuntimeError::NoTerminal => "States.Runtime",
            RuntimeError::InvalidDefinition(_) => "States.Runtime",
            RuntimeError::TimedOut { .. } => "States.Timeout",
            RuntimeError::Cancelled { .. } => "States.Cancelled",
        }
    }

    /// The error-output object bound to `$states.errorOutput` by `Catch` entries. Returns `None`
    /// for structural faults that have no ASL error output.
    pub fn error_output(&self) -> Option<Value> {
        match self {
            RuntimeError::StateFailed { output, .. } => Some((**output).clone()),
            RuntimeError::NoChoiceMatched { state } => Some(serde_json::json!({
                "Error": "States.NoChoiceMatched",
                "Cause": format!("Choice state '{state}' matched no rule and has no Default"),
            })),
            RuntimeError::Jsonata { field, message } => Some(serde_json::json!({
                "Error": "States.Runtime",
                "Cause": format!("JSONata error in {field}: {message}"),
            })),
            RuntimeError::TimedOut { message } => Some(serde_json::json!({
                "Error": "States.Timeout",
                "Cause": message,
            })),
            RuntimeError::Cancelled { .. } => None,
            RuntimeError::StateNotFound(_)
            | RuntimeError::NoTerminal
            | RuntimeError::InvalidDefinition(_) => None,
        }
    }
}

/// Engine-**infrastructure** failures — the durable log or storage backend 故障 when the engine
/// cannot continue its own machinery. These are not ASL-catchable and never carry an ASL
/// `error_name`; a caller is expected to **bubble** them (type-erased to the bug-facing config/
/// retry path) rather than branch on them. Kept separate from [`RuntimeError`] so the domain
/// exhaustive matches above stay small and infra failures don't force a `States.Runtime` arm everywhere.
///
/// `Log` carries the [`LogError`](spica_logstream::LogError)'s message as a `String`, not the error
/// value itself: the error type is non-`Clone`/non-`Serialize`, while `ExecutionError` (in which this
/// is embedded, and which flows across the engine/API boundary) is both.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum InfraError {
    /// A log/stream protocol violation or backend fault — e.g. an out-of-order, non-contiguous,
    /// or duplicate `entry_id` passed to [`LogStream::append`](crate::LogStream::append), or a
    /// durable read/write failure.
    #[error("log/storage error: {0}")]
    Log(String),
}

impl From<spica_logstream::LogError> for InfraError {
    fn from(e: spica_logstream::LogError) -> Self {
        InfraError::Log(e.to_string())
    }
}

/// The public error surfaced by the engine API. This is a thin **facade** over the per-concern
/// error types ([`RuntimeError`], [`InfraError`], and the command-layer [`Reject`]) so callers get
/// a single `Result<_, ExecutionError>` — while each concern keeps its own narrow type and the
/// domain branch (Runtime) preserves exhaustiveness over a small set.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum ExecutionError {
    /// A runtime/domain failure of an accepted execution — see [`RuntimeError`].
    #[error(transparent)]
    Runtime(#[from] RuntimeError),

    /// An engine-infrastructure failure (durable log / storage) — see [`InfraError`].
    #[error(transparent)]
    Infra(#[from] InfraError),

    /// The command was **rejected** — the engine refused to apply it (see [`Reject`]) and surfaced
    /// the reason to the awaiting caller. This is the *command-layer* outcome of a well-formed
    /// command whose preconditions failed (e.g. a malformed definition, or a duplicate name),
    /// distinct from a runtime *execution* failure (which reaches the caller via
    /// `ExecutionError::Runtime` on a terminated execution). Carries the same [`Reject`] the engine
    /// already appends to the log, so the two never drift.
    #[error("command rejected: {0}")]
    Rejected(Reject),
}

impl ExecutionError {
    /// The ASL reserved error name, used by `Retry`/`Catch` matching. Delegates to the embedded
    /// [`RuntimeError`]; infrastructure and command-layer errors report the generic structural name.
    pub fn error_name(&self) -> &str {
        match self {
            ExecutionError::Runtime(r) => r.error_name(),
            ExecutionError::Infra(_) | ExecutionError::Rejected(_) => "States.Runtime",
        }
    }

    /// The error-output object bound to `$states.errorOutput` by `Catch` entries. Delegates to the
    /// embedded [`RuntimeError`]; structural faults have no ASL error output.
    pub fn error_output(&self) -> Option<Value> {
        match self {
            ExecutionError::Runtime(r) => r.error_output(),
            ExecutionError::Infra(_) | ExecutionError::Rejected(_) => None,
        }
    }
}

// An infra (log/storage) fault reached directly with `?` in an engine-internal fn returning
// `Result<_, ExecutionError>` — routed through `InfraError` so the facade stays the single public
// error surface. (`From` does not chain, hence this explicit hop.)
impl From<spica_logstream::LogError> for ExecutionError {
    fn from(e: spica_logstream::LogError) -> Self {
        ExecutionError::Infra(InfraError::from(e))
    }
}

// A name-validation failure from the leaf kernel (`spica_machinery`) reached with `?` in an
// engine-internal fn returning `Result<_, ExecutionError>`. The kernel's own `NameError` is thin/no
// categorization; it lands here as a `RuntimeError` so name defects surface through the same domain
// branch as the rest of the definition-validation concern.
impl From<spica_machinery::NameError> for ExecutionError {
    fn from(e: spica_machinery::NameError) -> Self {
        ExecutionError::Runtime(RuntimeError::InvalidDefinition(e.to_string()))
    }
}
