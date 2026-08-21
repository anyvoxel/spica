use serde_json::Value;

use crate::ExecutionStatus;

/// The successful outcome of a state-machine execution.
///
/// `Ok(ExecutionResult)` means the machine reached a terminal success state (`Succeed` or
/// `End: true`); `output` is the final state output. A failed execution is reported as
/// `Err(ExecutionError)` instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionResult {
    /// The output of the terminal state.
    pub output: Value,
}

/// A non-blocking, point-in-time view of an execution's lifecycle state.
///
/// Served by [`Engine::execution_status`](crate::engine::Engine::execution_status) for the Server's
/// `GetExecution`: a remote client polls this until the snapshot is terminal (`Completed` /
/// `Terminated`) instead of blocking on a live ack or an internal poll loop. Unlike
/// [`ExecutionResult`] (success-only), it also encodes the in-flight and failed states.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionStatusSnapshot {
    /// The execution's current lifecycle status (Running / Completing / Completed / Terminating /
    /// Terminated).
    pub status: ExecutionStatus,
    /// The terminal state output, present once `status == Completed`.
    pub output: Option<Value>,
    /// The error name when `status == Terminated(reason)`, mirroring `ExecutionError::error_name`.
    pub error_name: Option<String>,
    /// The error output when `status == Terminated(reason)`, mirroring
    /// `ExecutionError::error_output`.
    pub error_output: Option<Value>,
}
