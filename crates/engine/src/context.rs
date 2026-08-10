use serde_json::{Value, json};

/// Builds the `$states` object exposed to JSONata expressions during a state's evaluation.
///
/// Shape (M2 subset):
/// ```json
/// {
///   "input":   <per-state input>,
///   "result":  <state result, or null>,
///   "errorOutput": <error output object, only when a Catch is being evaluated>,
///   "context": {
///     "Execution":     { "Input": <original execution input> },
///     "State":         { "Name": <state name>, "RetryCount": <retry attempt count> },
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
/// `error_output` is `Some` only when a `Catch` entry is being evaluated — it binds
/// `$states.errorOutput` (and the `Error`/`Cause` inside `context.Execution`) so the catcher's
/// `Assign`/`Output` can reference the failure. It is omitted (`null`) in every other phase.
///
/// `retry_count` is the number of retry attempts already made for the current state (0 on first
/// entry), bound to `$states.context.State.RetryCount` so a `Retry`'s `IntervalSeconds`/`BackoffRate`
/// expressions can reference it.
///
/// `Execution.Input` is the original top-level execution input, not the per-state input.
///
/// `map_item` is `Some((index, value))` only while a `Map` state is computing an item's input
/// (projecting `ItemSelector`) — it binds `context.Map.Item.{Index,Value}` so the selector can
/// reference the current item. It is `None` in every other phase, and the 10 non-Map call sites pass
/// `None` unchanged.
///
/// # TODO(M3) — `Map.Item` inside item-processor states / `State.Machine` stats
/// `context.Map.Item` is bound *here* when the Map projects an item's input, but it is **not yet
/// threaded down** into the item-processor child execution's own states: a state inside the
/// processor referencing `$states.context.Map.Item` will resolve it as absent. Threading it would
/// require carrying the current item on the child execution/activity row; deferred. See
/// `handlers/states/map.rs`.
#[allow(clippy::too_many_arguments)]
pub fn build_states(
    input: &Value,
    result: Option<&Value>,
    state_name: &str,
    exec_input: &Value,
    assign_ctx: Option<&Value>,
    retry_count: u32,
    error_output: Option<&Value>,
    map_item: Option<(usize, &Value)>,
) -> Value {
    let input = match assign_ctx {
        Some(ctx) => ctx.clone(),
        None => input.clone(),
    };
    // `errorOutput` is only present for a Catch evaluation; elsewhere the field is a JSON `null` so
    // a `{% $states.errorOutput %}` reference degrades to `null` instead of erroring.
    let error_output = error_output.unwrap_or(&Value::Null);
    let mut exec_ctx = serde_json::Map::new();
    exec_ctx.insert("Input".to_string(), exec_input.clone());
    if error_output != &Value::Null {
        // A Catch's `errorOutput` is mirrored into the Execution context as `Error`/`Cause` — the
        // shape a catcher's `Assign`/`Output` references when it reads the failing error fields
        // directly off `context.Execution` (the top-level `$states.errorOutput` form is also bound).
        exec_ctx.insert(
            "Error".to_string(),
            error_output.get("Error").cloned().unwrap_or(Value::Null),
        );
        exec_ctx.insert(
            "Cause".to_string(),
            error_output.get("Cause").cloned().unwrap_or(Value::Null),
        );
    }
    // `context.Map` is present only while projecting a Map item's input; elsewhere it is omitted so
    // a `$states.context.Map.Item` reference degrades to a JSON `null` / absent field instead of an
    // error. Its `Item.Index` is the item's position in the `Items` array (0-based); `Item.Value` is
    // the item itself.
    let mut map_ctx = json!({});
    if let Some((index, value)) = map_item {
        map_ctx = json!({
            "Item": { "Index": index, "Value": value }
        });
    }
    json!({
        "input": input,
        "result": result.unwrap_or(&Value::Null),
        "errorOutput": error_output,
        "context": {
            "Execution": exec_ctx,
            "State": { "Name": state_name, "RetryCount": retry_count },
            "StateMachine": {},
            "Map": map_ctx
        }
    })
}
