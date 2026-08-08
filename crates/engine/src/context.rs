use serde_json::{Value, json};

/// Builds the `$states` object exposed to JSONata expressions during a state's evaluation.
///
/// Shape (M1 subset):
/// ```json
/// {
///   "input":   <per-state input>,
///   "result":  <state result, or null>,
///   "context": {
///     "Execution":     { "Input": <original execution input> },
///     "State":         { "Name": <state name> },
///     "StateMachine":  {}
///   }
/// }
/// ```
///
/// `assign_ctx` distinguises two moments within a state's lifecycle:
/// - the **activate** step (`$states.input` = input): pass `None` — the state's own `Assign` has
///   not yet been applied; and
/// - the **output** projection during `complete` (runs *after* `Assign` mutates scope): pass the
///   input *with* scope already updated, so derived values read consistently.
///
/// `errorOutput` is omitted in M1 (it is only bound by `Catch` entries, added in a later
/// milestone). `Execution.Input` is the original top-level execution input, not the per-state
/// input. `Map.Item` and `State.RetryCount` are populated in later milestones.
pub fn build_states(
    input: &Value,
    result: Option<&Value>,
    state_name: &str,
    exec_input: &Value,
    assign_ctx: Option<&Value>,
) -> Value {
    let input = match assign_ctx {
        Some(ctx) => ctx.clone(),
        None => input.clone(),
    };
    json!({
        "input": input,
        "result": result.unwrap_or(&Value::Null),
        "context": {
            "Execution": { "Input": exec_input },
            "State": { "Name": state_name },
            "StateMachine": {}
        }
    })
}
