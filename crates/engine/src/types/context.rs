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
///     "Execution":     { /* "Error"/"Cause" only when a Catch is being evaluated */ },
///     "State":         { "Name": <state name>, "RetryCount": <retry attempt count> },
///     "StateMachine":  {}
///   }
/// }
/// ```
///
/// The phase-common inputs go to [`States::new`]; the per-phase optionals default to `None`
/// and are set with the `with_*` setters before [`States::build`].
///
/// # TODO(M3) — `Map.Item` inside item-processor states / `State.Machine` stats
/// `context.Map.Item` is bound *here* when the Map projects an item's input, but it is **not yet
/// threaded down** into the item-processor child execution's own states: a state inside the
/// processor referencing `$states.context.Map.Item` will resolve it as absent. Threading it would
/// require carrying the current item on the child execution/activity row; deferred. See
/// `handlers/states/map.rs`.
pub struct States<'a> {
    input: &'a Value,
    result: Option<&'a Value>,
    state_name: &'a str,
    assign_ctx: Option<&'a Value>,
    retry_count: u32,
    error_output: Option<&'a Value>,
    map_item: Option<(usize, &'a Value)>,
}

impl<'a> States<'a> {
    pub fn new(input: &'a Value, state_name: &'a str, retry_count: u32) -> Self {
        Self {
            input,
            result: None,
            state_name,
            assign_ctx: None,
            retry_count,
            error_output: None,
            map_item: None,
        }
    }

    /// `$states.result` — the raw state result before any complete-step `Output` projection; `None`
    /// while the state is only being activated.
    pub fn with_result(mut self, result: Option<&'a Value>) -> Self {
        self.result = result;
        self
    }

    /// Distinguishes the two lifecycle moments: the **activate** step (`$states.input` = input) passes
    /// `None` — the state's own `Assign` has not yet been applied; the **output** projection during
    /// `complete` (runs *after* `Assign` mutates scope) passes the input *with* scope already updated,
    /// so derived values read consistently.
    pub fn with_assign_ctx(mut self, assign_ctx: Option<&'a Value>) -> Self {
        self.assign_ctx = assign_ctx;
        self
    }

    /// `Some` only when a `Catch` entry is being evaluated — binds `$states.errorOutput` (and the
    /// `Error`/`Cause` inside `context.Execution`) so the catcher's `Assign`/`Output` can reference
    /// the failure. Omitted (`null`) in every other phase.
    pub fn with_error_output(mut self, error_output: Option<&'a Value>) -> Self {
        self.error_output = error_output;
        self
    }

    /// `Some((index, value))` only while a `Map` state is computing an item's input (projecting
    /// `ItemSelector`) — binds `context.Map.Item.{Index,Value}` so the selector can reference the
    /// current item. `None` in every other phase. Unused until the Map threading in the doc above
    /// lands, so `with_map_item` is a reserved hook rather than a live path.
    #[allow(dead_code)]
    pub fn with_map_item(mut self, map_item: Option<(usize, &'a Value)>) -> Self {
        self.map_item = map_item;
        self
    }

    /// Materialize the `$states` JSON value.
    pub fn build(self) -> Value {
        let input = match self.assign_ctx {
            Some(ctx) => ctx.clone(),
            None => self.input.clone(),
        };
        // `errorOutput` is only present for a Catch evaluation; elsewhere the field is a JSON `null` so
        // a `{% $states.errorOutput %}` reference degrades to `null` instead of erroring.
        let error_output = self.error_output.unwrap_or(&Value::Null);
        let mut exec_ctx = serde_json::Map::new();
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
        if let Some((index, value)) = self.map_item {
            map_ctx = json!({
                "Item": { "Index": index, "Value": value }
            });
        }
        json!({
            "input": input,
            "result": self.result.unwrap_or(&Value::Null),
            "errorOutput": error_output,
            "context": {
                "Execution": exec_ctx,
                "State": { "Name": self.state_name, "RetryCount": self.retry_count },
                "StateMachine": {},
                "Map": map_ctx
            }
        })
    }
}
