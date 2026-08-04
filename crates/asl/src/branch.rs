use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::State;

/// A branch is an element of a `Parallel` state's `branches` array. It is a self-contained
/// sub-state-machine executed concurrently with the other branches; states within a branch
/// may only transition to each other.
///
/// Per the Amazon States Language, a branch is a distinct type that allows only `States`
/// (required) and `StartAt` (required). Unlike the top-level `StateMachine`, a branch does not
/// allow `Comment`, `Version`, `TimeoutSeconds`, or `QueryLanguage`.
///
/// See:
/// - https://states-language.net/spec.html#parallel-state
/// - https://docs.aws.amazon.com/step-functions/latest/dg/state-parallel.html
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct Branch {
    /// Required. A string that must exactly match (case sensitive) the name of one of the
    /// state objects in `states`. This is the state executed first when the branch starts.
    pub start_at: String,

    /// Required. An object containing the set of states for this branch. States can occur in
    /// any order; their `Next`/`End` transitions determine the run order, and may only target
    /// states within this same branch.
    pub states: HashMap<String, State>,
}

#[cfg(test)]
mod tests {
    use crate::Branch;

    #[test]
    fn test_branch_rejects_comment() {
        let content = r#"{
          "Comment": "Lookup contact details in parallel",
          "StartAt": "LookupAddress",
          "States": {
            "LookupAddress": {
              "Type": "Pass",
              "End": true
            }
          }
        }"#;

        let err = serde_json::from_str::<Branch>(content).expect_err("expected parse failure");
        assert!(
            err.to_string().contains("unknown field `Comment`")
                || err.to_string().contains("unknown field `comment`"),
            "unexpected error: {err}"
        );
    }
}
