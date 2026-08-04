use serde::{Deserialize, Serialize};

use crate::AssignObject;
use serde_with::skip_serializing_none;

/// A catcher is an entry in a state's `Catch` array (available to `Task`, `Parallel`, and
/// `Map` states). It defines a fallback state that is executed when the state reports a
/// runtime error whose name appears in `error_equals` and the retry policy (if any) is
/// exhausted or undefined.
///
/// See:
/// - https://states-language.net/spec.html#fallback-states
/// - https://docs.aws.amazon.com/step-functions/latest/dg/concepts-error-handling.html
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct Catcher {
    /// Required. A non-empty array of strings matching error names, specified exactly as with
    /// the retrier `error_equals` field. The reserved name `States.ALL` is a wildcard matching
    /// any error name and must appear alone in the array and in the last catcher of the
    /// `Catch` array; `States.TaskFailed` matches any error except `States.Timeout`.
    pub error_equals: Vec<String>,

    /// Required. A string that must exactly match one of the state machine's state names.
    /// The state machine transitions to this state when the catcher matches a reported error.
    pub next: String,

    /// Optional. A collection of key-value pairs to assign data to variables, working exactly
    /// like a state's top-level `Assign`. If a catcher matches, its `Assign` (if any) is
    /// evaluated instead of the state's top-level `Assign`.
    pub assign: Option<AssignObject>,

    /// Optional. Used to specify and transform output from the catcher. Works exactly like a
    /// state's top-level `Output`. Accepts any JSON value; strings surrounded by `{% %}` are
    /// evaluated as JSONata. If not provided, the state output is the error output.
    pub output: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::{AssignObject, Catcher};

    fn assign(v: serde_json::Value) -> Option<AssignObject> {
        Some(AssignObject(v.as_object().expect("assign must be object").clone()))
    }

    #[test]
    fn test_catcher_minimal() -> Result<(), Box<dyn Error>> {
        // Minimal catcher: only the required `ErrorEquals` and `Next` fields.
        let content = r#"{
          "ErrorEquals": ["States.ALL"],
          "Next": "EndState"
        }"#;

        let catcher: Catcher = serde_json::from_str(content)?;
        assert_eq!(catcher.error_equals, ["States.ALL"]);
        assert_eq!(catcher.next, "EndState");
        assert_eq!(catcher.assign, None);
        assert_eq!(catcher.output, None);

        // Round-trip: omitted optional fields must not reappear on serialization.
        let reserialized = serde_json::to_string(&catcher)?;
        assert!(
            !reserialized.contains("Assign"),
            "Assign should be absent, got: {reserialized}"
        );
        let reparsed: Catcher = serde_json::from_str(&reserialized)?;
        assert_eq!(catcher, reparsed);

        Ok(())
    }

    #[test]
    fn test_catcher_assign() -> Result<(), Box<dyn Error>> {
        // A catcher that sets a variable via `Assign` before transitioning to the fallback state.
        let content = r#"{
          "ErrorEquals": ["java.lang.Exception"],
          "Assign": {
            "error-info": "{% $states.errorOutput %}"
          },
          "Next": "RecoveryState"
        }"#;

        let catcher: Catcher = serde_json::from_str(content)?;
        assert_eq!(catcher.error_equals, ["java.lang.Exception"]);
        assert_eq!(
            catcher.assign,
            assign(serde_json::json!({
                "error-info": "{% $states.errorOutput %}"
            }))
        );
        assert_eq!(catcher.next, "RecoveryState");

        // Round-trip: the Assign must be preserved.
        let reserialized = serde_json::to_string(&catcher)?;
        let reparsed: Catcher = serde_json::from_str(&reserialized)?;
        assert_eq!(catcher, reparsed);

        Ok(())
    }

    #[test]
    fn test_catcher_jsonata_output() -> Result<(), Box<dyn Error>> {
        // JSONata catcher: uses `Output` to transform the error output and `Assign` to set a
        // variable before transitioning to the fallback state.
        let content = r#"{
          "ErrorEquals": ["States.ALL"],
          "Next": "Fallback",
          "Assign": {
            "error": "{% $states.errorOutput %}"
          },
          "Output": "{% $states.errorOutput.Cause %}"
        }"#;

        let catcher: Catcher = serde_json::from_str(content)?;
        assert_eq!(catcher.error_equals, ["States.ALL"]);
        assert_eq!(catcher.next, "Fallback");
        assert_eq!(
            catcher.assign,
            assign(serde_json::json!({
                "error": "{% $states.errorOutput %}"
            }))
        );
        assert_eq!(
            catcher.output,
            Some(serde_json::json!("{% $states.errorOutput.Cause %}"))
        );

        // Round-trip: the Assign and Output must be preserved.
        let reserialized = serde_json::to_string(&catcher)?;
        let reparsed: Catcher = serde_json::from_str(&reserialized)?;
        assert_eq!(catcher, reparsed);

        Ok(())
    }
}
