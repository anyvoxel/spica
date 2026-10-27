mod activate_state;
mod complete_state;
mod continue_complete;
mod continue_terminate;
mod dispatch;
#[cfg(test)]
pub(crate) mod fixtures;
mod terminate_state;

pub use activate_state::ActivateStateHandler;
pub use complete_state::CompleteStateHandler;
pub use continue_complete::ContinueCompleteHandler;
pub use continue_terminate::ContinueTerminateHandler;
pub use terminate_state::TerminateStateHandler;

pub(crate) use dispatch::{build_state_handlers, dispatch_command};

use serde_json::Value;

use crate::eval_env::EvalEnv;
use crate::types::error::ExecutionError;

// ── Shared helpers ───────────────────────────────────────────────────────────

/// Evaluates a string that may be a literal or a `{% ... %}` JSONata expression.
pub(super) fn eval_string_or_expr(
    env: &mut EvalEnv,
    s: &str,
    states: &Value,
    variables: &crate::types::variables::Variables,
) -> Result<Value, ExecutionError> {
    match crate::eval_env::extract_jsonata(s) {
        Some(inner) => env.eval_expr(inner, states, variables),
        None => Ok(Value::String(s.to_string())),
    }
}
