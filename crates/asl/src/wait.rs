use serde::{Deserialize, Serialize};

use crate::{
    AssignObject,
    utils::{IntOrExpr, JsonataExpr, parse_int_or_expr},
};

/// The value of a `Wait` state's `Seconds` field in the JSONata-only subset.
///
/// Per the spec, a JSONata Wait State MAY specify either:
/// - a non-negative integer literal, or
/// - a JSONata string whose evaluated value must be a non-negative integer.
pub type WaitSeconds = IntOrExpr;

fn deserialize_wait_seconds_option<'de, D>(deserializer: D) -> Result<Option<WaitSeconds>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        Some(value) => parse_int_or_expr(value, "Seconds").map(Some),
        None => Ok(None),
    }
}

/// The value of a `Wait` state's `Timestamp` field in the JSONata-only subset.
///
/// Per the spec, a JSONata Wait State MAY specify either:
/// - a string literal containing a valid timestamp, or
/// - a JSONata string whose evaluated value must be a string containing a valid timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum WaitTimestamp {
    Literal(String),
    Expr(JsonataExpr),
}

impl<'de> Deserialize<'de> for WaitTimestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match JsonataExpr::parse::<D::Error>(s.clone(), "Timestamp") {
            Ok(expr) => Ok(WaitTimestamp::Expr(expr)),
            Err(_) => Ok(WaitTimestamp::Literal(s)),
        }
    }
}

/// A `Wait` state (`"Type": "Wait"`) delays the state machine from continuing for a specified
/// time. The wait can be relative (a number of seconds from when the state begins) or absolute
/// (a timestamp). Exactly one of `seconds` or `timestamp` must be specified; both accept JSONata
/// expressions.
///
/// See:
/// - https://docs.aws.amazon.com/step-functions/latest/dg/state-wait.html
/// - https://states-language.net/spec.html#wait-state
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WaitState {
    /// Optional. A human-readable description of the state.
    #[serde(rename = "Comment", skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,

    /// Optional. Used to specify and transform output from the state. When specified, the value
    /// overrides the state output default. Accepts any JSON value; strings surrounded by
    /// `{% %}` are evaluated as JSONata.
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

    /// Optional. A time, in seconds, to wait before beginning the state specified in `next`. An
    /// integer value from 0 to 99,999,999. A JSONata expression evaluating to an integer in that
    /// range is also accepted.
    #[serde(
        rename = "Seconds",
        default,
        deserialize_with = "deserialize_wait_seconds_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub seconds: Option<WaitSeconds>,

    /// Optional. An absolute time to wait until before beginning the state specified in `next`.
    /// Must conform to the RFC3339 profile of ISO 8601 (e.g. `2024-08-18T17:33:00Z`). A JSONata
    /// expression evaluating to such a string is also accepted.
    #[serde(rename = "Timestamp", skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<WaitTimestamp>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::{JsonataExpr, State, WaitSeconds, WaitTimestamp};

    #[test]
    fn test_wait_state_seconds() -> Result<(), Box<dyn Error>> {
        // A 10-second delay before transitioning to `NextState`.
        let content = r#"{
          "Type": "Wait",
          "Seconds": 10,
          "Next": "NextState"
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Wait(ref wait) = value else {
            panic!("expected Wait state, got {:?}", value);
        };

        assert_eq!(wait.seconds, Some(WaitSeconds::Int(10)));
        assert_eq!(wait.next.as_deref(), Some("NextState"));
        assert_eq!(wait.end, None);

        // Round-trip.
        let reserialized = serde_json::to_string(&value)?;
        assert!(reserialized.contains(r#""Seconds":10"#));
        assert!(reserialized.contains(r#""Next":"NextState""#));
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_wait_state_seconds_jsonata_expr() -> Result<(), Box<dyn Error>> {
        // A JSONata Wait State may specify a JSONata string for `Seconds` whose value, when
        // evaluated, must be a non-negative integer.
        let content = r#"{
          "Type": "Wait",
          "Seconds": "{% $states.input.delaySeconds %}",
          "Next": "NextState"
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Wait(ref wait) = value else {
            panic!("expected Wait state, got {:?}", value);
        };

        assert_eq!(
            wait.seconds,
            Some(WaitSeconds::Expr(
                JsonataExpr::new("{% $states.input.delaySeconds %}").unwrap()
            ))
        );
        assert_eq!(wait.next.as_deref(), Some("NextState"));
        assert_eq!(wait.end, None);

        // Round-trip.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_wait_state_timestamp() -> Result<(), Box<dyn Error>> {
        // Waits until the absolute time March 14, 2016, 1:59 AM UTC.
        let content = r#"{
          "Type": "Wait",
          "Timestamp": "2016-03-14T01:59:00Z",
          "Next": "NextState"
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Wait(ref wait) = value else {
            panic!("expected Wait state, got {:?}", value);
        };

        assert_eq!(
            wait.timestamp,
            Some(WaitTimestamp::Literal("2016-03-14T01:59:00Z".to_string()))
        );
        assert_eq!(wait.next.as_deref(), Some("NextState"));
        assert_eq!(wait.end, None);

        // Round-trip.
        let reserialized = serde_json::to_string(&value)?;
        assert!(reserialized.contains(r#""Timestamp":"2016-03-14T01:59:00Z""#));
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_wait_state_rejects_non_jsonata_seconds_string() {
        let content = r#"{
          "Type": "Wait",
          "Seconds": "ten",
          "Next": "NextState"
        }"#;

        let err = serde_json::from_str::<State>(content).expect_err("expected parse failure");
        assert!(
            err.to_string()
                .contains("Seconds string must be a JSONata expression wrapped in {% %}"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_wait_state_assign_jsonata() -> Result<(), Box<dyn Error>> {
        // In JSONata states the wait time fields accept JSONata expressions directly, so
        // `Timestamp` reads the absolute wait time from the state input.
        let content = r#"{
          "Type": "Wait",
          "Timestamp": "{% $states.input.expirydate %}",
          "Next": "NextState"
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Wait(ref wait) = value else {
            panic!("expected Wait state, got {:?}", value);
        };

        assert_eq!(wait.next.as_deref(), Some("NextState"));
        assert_eq!(wait.end, None);
        assert_eq!(
            wait.timestamp,
            Some(WaitTimestamp::Expr(
                JsonataExpr::new("{% $states.input.expirydate %}").unwrap()
            ))
        );

        // Round-trip: the JSONata Timestamp expression must be preserved.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }
}
