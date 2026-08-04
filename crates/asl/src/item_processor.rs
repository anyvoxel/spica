use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::State;

/// The item processor defines the state machine that processes each item (or batch of items) of
/// a `Map` state's input array. It is referenced by a `Map` state's `item_processor` field.
///
/// Per the Amazon States Language, an item processor has `StartAt` (required) and `States`
/// (required). Unlike the top-level `StateMachine`, it does not allow `Comment`, `Version`,
/// `TimeoutSeconds`, or `QueryLanguage`.
///
/// This learning subset is **Inline-only**: the AWS-specific `ProcessorConfig` (Distributed
/// mode, execution type) is not modeled.
///
/// See:
/// - https://states-language.net/spec.html#map-state
/// - https://docs.aws.amazon.com/step-functions/latest/dg/state-map.html
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct ItemProcessor {
    /// Required. A string that must exactly match (case sensitive) the name of one of the state
    /// objects in `states`. This is the state executed first for each iteration.
    pub start_at: String,

    /// Required. An object containing the set of states for this item processor. States can
    /// occur in any order; their `Next`/`End` transitions determine the run order.
    pub states: HashMap<String, State>,
}
