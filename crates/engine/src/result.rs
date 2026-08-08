use serde_json::Value;

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
