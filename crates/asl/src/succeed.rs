use serde::{Deserialize, Serialize};

/// A `Succeed` state (`"Type": "Succeed"`) terminates a state machine successfully, ends a
/// branch of a `Parallel` state, or ends an iteration of a `Map` state. It is a useful target
/// for `Choice` state branches that don't do anything except terminate the state machine.
///
/// Because `Succeed` states are terminal states, they have no `Next` field and don't need an
/// `End` field.
///
/// See:
/// - https://docs.aws.amazon.com/step-functions/latest/dg/state-succeed.html
/// - https://states-language.net/spec.html#succeed-state
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SucceedState {
    /// Optional. A human-readable description of the state.
    #[serde(rename = "Comment", skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,

    /// Optional. Used to specify and transform output from the state. When specified, the value
    /// overrides the state output default. Accepts any JSON value; strings surrounded by
    /// `{% %}` are evaluated as JSONata.
    #[serde(rename = "Output", skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::State;

    #[test]
    fn test_succeed_state() -> Result<(), Box<dyn Error>> {
        // Minimal Succeed state from the documentation: a terminal state with no `Next` or
        // `End` field.
        let content = r#"{
          "Type": "Succeed"
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Succeed(ref succeed) = value else {
            panic!("expected Succeed state, got {:?}", value);
        };

        assert_eq!(succeed.comment, None);
        assert_eq!(succeed.output, None);

        // Round-trip: omitted fields must not reappear on serialization.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_succeed_state_output_jsonata() -> Result<(), Box<dyn Error>> {
        // A Succeed state that uses `Output` to specify and transform the state output. The
        // output value overrides the state output default, and strings surrounded by `{% %}` are
        // evaluated as JSONata.
        let content = r#"{
          "Type": "Succeed",
          "Output": "{% $states.input %}"
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Succeed(ref succeed) = value else {
            panic!("expected Succeed state, got {:?}", value);
        };

        assert_eq!(
            succeed.output,
            Some(serde_json::json!("{% $states.input %}"))
        );

        // Round-trip: the JSONata Output must be preserved.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }
}
