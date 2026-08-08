use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// A failure produced while executing a state machine.
///
/// The `Ok` branch of [`crate::Engine::start`] is reserved for successful executions; every
/// failure — a `Fail` state, an unhandled runtime error, a timeout, a cancel, or a structural
/// problem with the definition — is reported via this type.
///
/// The [`error_name`](Self::error_name) and [`error_output`](Self::error_output) accessors expose
/// the ASL reserved error name and error-output object so that a later milestone's `Retry`/`Catch`
/// can intercept runtime errors before they propagate.
#[derive(Debug, Clone, PartialEq, Error, Serialize, Deserialize)]
pub enum ExecutionError {
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
        output: Value,
    },

    /// A JSONata expression failed to parse or evaluate.
    #[error("JSONata error in {field}: {message}")]
    Jsonata { field: String, message: String },

    /// The state machine uses a feature outside the M1 subset (e.g. `Task`, `Map`, `Parallel`,
    /// or JSONata `Wait` fields).
    #[error("invalid state definition: {0}")]
    InvalidDefinition(String),

    /// The execution ran past the state machine's `TimeoutSeconds`.
    #[error("execution timed out: {message}")]
    TimedOut { message: String },

    /// The execution was cancelled externally (engine `terminate`).
    #[error("execution cancelled: {message}")]
    Cancelled { message: String },

    /// A log/stream protocol violation — e.g. an out-of-order, non-contiguous, or duplicate
    /// `entry_id` passed to [`LogStream::append`](crate::LogStream::append). These
    /// are engine-internal faults (not ASL-catchable).
    #[error("log protocol error: {0}")]
    Log(String),
}

impl ExecutionError {
    /// The ASL reserved error name, used by `Retry`/`Catch` matching in a later milestone.
    ///
    /// Runtime errors (`Fail`, `NoChoiceMatched`, JSONata failures) carry ASL-defined names;
    /// structural faults are reported as `States.Runtime`.
    pub fn error_name(&self) -> &str {
        match self {
            ExecutionError::StateFailed { error, .. } => error,
            ExecutionError::NoChoiceMatched { .. } => "States.NoChoiceMatched",
            ExecutionError::Jsonata { .. } => "States.Runtime",
            ExecutionError::StateNotFound(_) => "States.Runtime",
            ExecutionError::NoTerminal => "States.Runtime",
            ExecutionError::InvalidDefinition(_) => "States.Runtime",
            ExecutionError::TimedOut { .. } => "States.Timeout",
            ExecutionError::Cancelled { .. } => "States.Cancelled",
            ExecutionError::Log(_) => "States.Runtime",
        }
    }

    /// The error-output object bound to `$states.errorOutput` by `Catch` entries in a later
    /// milestone. Returns `None` for structural faults that have no ASL error output.
    pub fn error_output(&self) -> Option<Value> {
        match self {
            ExecutionError::StateFailed { output, .. } => Some(output.clone()),
            ExecutionError::NoChoiceMatched { state } => Some(serde_json::json!({
                "Error": "States.NoChoiceMatched",
                "Cause": format!("Choice state '{state}' matched no rule and has no Default"),
            })),
            ExecutionError::Jsonata { field, message } => Some(serde_json::json!({
                "Error": "States.Runtime",
                "Cause": format!("JSONata error in {field}: {message}"),
            })),
            ExecutionError::TimedOut { message } => Some(serde_json::json!({
                "Error": "States.Timeout",
                "Cause": message,
            })),
            ExecutionError::Cancelled { .. } => None,
            ExecutionError::StateNotFound(_)
            | ExecutionError::NoTerminal
            | ExecutionError::InvalidDefinition(_)
            | ExecutionError::Log(_) => None,
        }
    }
}
