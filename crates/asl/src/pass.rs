use serde::{Deserialize, Serialize};

use crate::AssignObject;

/// A `Pass` state (`"Type": "Pass"`) passes its input to its output without performing work.
/// `Pass` states are useful when constructing and debugging state machines, and can also be
/// used to transform JSON state input using JSONata before passing it to the next state.
///
/// See:
/// - https://docs.aws.amazon.com/step-functions/latest/dg/state-pass.html
/// - https://states-language.net/spec.html#pass-state
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PassState {
    /// Optional. A human-readable description of the state.
    #[serde(rename = "Comment", skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,

    /// Optional. Used to specify and transform output from the state. When specified, the value
    /// overrides the state output default. Accepts any JSON value (object, array, string,
    /// number, boolean, null); strings surrounded by `{% %}` (and values inside objects/arrays)
    /// are evaluated as JSONata.
    #[serde(rename = "Output", skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,

    /// Optional. A collection of key-value pairs to assign data to variables. Any string value
    /// surrounded by `{% %}` is evaluated as JSONata.
    #[serde(rename = "Assign", skip_serializing_if = "Option::is_none")]
    pub assign: Option<AssignObject>,

    /// Optional. The name of the next state that is run when the current state finishes. Only
    /// one of `next` or `end` may be used.
    #[serde(rename = "Next", skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,

    /// Optional. Designates this state as a terminal state (ends the execution) when `true`.
    /// Only one of `next` or `end` may be used.
    #[serde(rename = "End", skip_serializing_if = "Option::is_none")]
    pub end: Option<bool>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::{AssignObject, State};

    fn assign(v: serde_json::Value) -> Option<AssignObject> {
        Some(AssignObject(
            v.as_object().expect("assign must be object").clone(),
        ))
    }

    #[test]
    fn test_pass_state_assign_jsonata() -> Result<(), Box<dyn Error>> {
        // JSONata example: assigns a `coords` variable whose `additional` field is a JSONata
        // expression that conditionally pulls `moreData` from the state input, then transitions
        // to the `End` state via `Next`.
        let content = r#"{
          "Type": "Pass",
          "Assign": {
            "coords": {
              "x-datum": 0.381018,
              "y-datum": 622.2269926397355,
              "additional": "{% $exists($states.input.moreData) ? $states.input.moreData : {} %}"
            }
          },
          "Next": "End"
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Pass(ref pass) = value else {
            panic!("expected Pass state, got {:?}", value);
        };

        assert_eq!(pass.next.as_deref(), Some("End"));
        assert_eq!(pass.end, None);
        assert_eq!(
            pass.assign,
            assign(serde_json::json!({
                "coords": {
                    "x-datum": 0.381018,
                    "y-datum": 622.2269926397355,
                    "additional": "{% $exists($states.input.moreData) ? $states.input.moreData : {} %}"
                }
            }))
        );

        // Round-trip: the JSONata Assign must be preserved.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_pass_state_output_jsonata() -> Result<(), Box<dyn Error>> {
        // JSONata example: injects fixed coordinate data via `Output` (replacing the prior
        // JSONPath `Result` + `ResultPath` combination) and terminates with `End`.
        let content = r#"{
          "Type": "Pass",
          "Output": {
            "coords": {
              "x-datum": 0.381018,
              "y-datum": 622.2269926397355
            }
          },
          "End": true
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Pass(pass) = value else {
            panic!("expected Pass state, got {:?}", value);
        };

        assert_eq!(pass.comment, None);
        assert_eq!(pass.next, None);
        assert_eq!(pass.end, Some(true));
        assert_eq!(
            pass.output,
            Some(serde_json::json!({
                "coords": {
                    "x-datum": 0.381018,
                    "y-datum": 622.2269926397355
                }
            }))
        );

        // Round-trip.
        let reserialized = serde_json::to_string(&State::Pass(pass))?;
        assert!(reserialized.contains(r#""End":true"#));

        Ok(())
    }
}
