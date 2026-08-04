use serde::{Deserialize, Serialize};

/// A `Fail` state (`"Type": "Fail"`) stops the execution of the state machine and marks it as
/// a failure, unless it is caught by a `Catch` block. Because `Fail` states always exit the
/// state machine, they have no `Next` field and don't require an `End` field.
///
/// See:
/// - https://docs.aws.amazon.com/step-functions/latest/dg/state-fail.html
/// - https://states-language.net/spec.html#fail-state
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FailState {
    /// Optional. A human-readable description of the state.
    #[serde(rename = "Comment", skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,

    /// Optional. A custom string that describes the cause of the error, for operational or
    /// diagnostic purposes. A JSONata expression is also accepted.
    #[serde(rename = "Cause", skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,

    /// Optional. An error name used for error handling via `Retry`/`Catch`, or for
    /// operational/diagnostic purposes. A JSONata expression is also accepted.
    #[serde(rename = "Error", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::State;

    #[test]
    fn test_fail_state_static() -> Result<(), Box<dyn Error>> {
        // Static `Error` and `Cause` field values.
        let content = r#"{
          "Type": "Fail",
          "Cause": "Invalid response.",
          "Error": "ErrorA"
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Fail(ref fail) = value else {
            panic!("expected Fail state, got {:?}", value);
        };

        assert_eq!(fail.cause.as_deref(), Some("Invalid response."));
        assert_eq!(fail.error.as_deref(), Some("ErrorA"));

        // Round-trip.
        let reserialized = serde_json::to_string(&value)?;
        assert!(reserialized.contains(r#""Cause":"Invalid response.""#));
        assert!(reserialized.contains(r#""Error":"ErrorA""#));
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_fail_state_jsonata_expressions() -> Result<(), Box<dyn Error>> {
        // The `Error` and `Cause` fields accept JSONata expressions directly, so the error name
        // and cause are resolved dynamically from the state input.
        let content = r#"{
          "Type": "Fail",
          "Comment": "my error comment",
          "Error": "{% $states.input.Error %}",
          "Cause": "{% $states.input.Cause %}"
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Fail(ref fail) = value else {
            panic!("expected Fail state, got {:?}", value);
        };

        assert_eq!(fail.comment.as_deref(), Some("my error comment"));
        assert_eq!(fail.error.as_deref(), Some("{% $states.input.Error %}"));
        assert_eq!(fail.cause.as_deref(), Some("{% $states.input.Cause %}"));

        // Round-trip: the JSONata expressions must be preserved.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }
}
