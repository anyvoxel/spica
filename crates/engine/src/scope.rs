use std::collections::HashMap;

use serde_json::Value;

/// The variable scope of a state-machine execution.
///
/// Each top-level state's `Assign` mutates this map in place, and the result is visible to
/// subsequent states. When entering a `Map` iteration or `Parallel` branch (later milestones),
/// the parent scope is cloned so that child `Assign`s do not leak back.
///
/// Variable names are stored without the leading `$`; a binding `("outer", v)` is referenced from
/// JSONata as `$outer`.
pub type Scope = HashMap<String, Value>;
