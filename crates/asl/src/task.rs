use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::{
    utils::{parse_int_or_expr, IntOrExpr},
    AssignObject, Catcher, Retrier,
};

/// The value of a `Task` state's `TimeoutSeconds` field in the JSONata-only subset.
///
/// Per the spec, a JSONata Task State MAY specify either:
/// - a positive integer literal, or
/// - a JSONata string whose evaluated value must be a positive integer.
pub type TaskTimeoutSeconds = IntOrExpr;

fn deserialize_task_timeout_seconds_option<'de, D>(
    deserializer: D,
) -> Result<Option<TaskTimeoutSeconds>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        Some(value) => parse_int_or_expr(value, "TimeoutSeconds").map(Some),
        None => Ok(None),
    }
}

/// The value of a `Task` state's `HeartbeatSeconds` field in the JSONata-only subset.
///
/// Per the spec, a JSONata Task State MAY specify either:
/// - a positive integer literal, or
/// - a JSONata string whose evaluated value must be a positive integer.
pub type TaskHeartbeatSeconds = IntOrExpr;

fn deserialize_task_heartbeat_seconds_option<'de, D>(
    deserializer: D,
) -> Result<Option<TaskHeartbeatSeconds>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        Some(value) => parse_int_or_expr(value, "HeartbeatSeconds").map(Some),
        None => Ok(None),
    }
}

/// A `Task` state (`"Type": "Task"`) represents a single unit of work performed by a state
/// machine, identified by the URI in `resource`.
///
/// A `Task` state must set either `end` to `true` (if it ends the execution) or provide a
/// `next` state that is run when the task completes.
///
/// See:
/// - https://docs.aws.amazon.com/step-functions/latest/dg/state-task.html
/// - https://states-language.net/spec.html#task-state
/// - https://docs.aws.amazon.com/step-functions/latest/dg/concepts-error-handling.html
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct TaskState {
    /// Optional. A human-readable description of the state.
    pub comment: Option<String>,

    /// Optional. Used to specify and transform output from the state. When specified, the value
    /// overrides the state output default. Accepts any JSON value (object, array, string,
    /// number, boolean, null); strings surrounded by `{% %}` (and values inside objects/arrays)
    /// are evaluated as JSONata.
    pub output: Option<serde_json::Value>,

    /// Optional. A collection of key-value pairs to assign data to variables. Any string value
    /// surrounded by `{% %}` is evaluated as JSONata.
    pub assign: Option<AssignObject>,

    /// Optional. The name of the next state that is run when the task completes. One of `next`
    /// or `end` must be set.
    pub next: Option<String>,

    /// Optional. Designates this state as a terminal state (ends the execution) when `true`.
    /// One of `next` or `end` must be set.
    pub end: Option<bool>,

    /// Optional. Used to pass information to the API actions of connected resources. Values can
    /// include JSONata expressions.
    pub arguments: Option<serde_json::Value>,

    /// Optional. An array of retrier objects that define a retry policy if the state encounters
    /// runtime errors. Each retrier has an `ErrorEquals` array plus optional `IntervalSeconds`,
    /// `MaxAttempts`, `BackoffRate`, `MaxDelaySeconds`, and `JitterStrategy` fields.
    pub retry: Option<Vec<Retrier>>,

    /// Optional. An array of catcher objects that define a fallback state, executed if the
    /// state encounters runtime errors and its retry policy is exhausted or undefined. Each
    /// catcher has an `ErrorEquals` array, a required `Next`, and optional `Assign` and
    /// `Output` fields.
    pub catch: Option<Vec<Catcher>>,

    /// Required. A URI that uniquely identifies the specific task to execute.
    pub resource: String,

    /// Optional. The maximum number of seconds a task can run before it times out with a
    /// `States.Timeout` error (default 99,999,999). A JSONata string is also accepted and must
    /// evaluate to a positive integer.
    #[serde(default, deserialize_with = "deserialize_task_timeout_seconds_option")]
    pub timeout_seconds: Option<TaskTimeoutSeconds>,

    /// Optional. The frequency (in seconds) of heartbeat signals an activity worker sends during
    /// task execution (must be less than `timeout_seconds`, default 99,999,999). A JSONata
    /// string is also accepted and must evaluate to a positive integer.
    #[serde(
        default,
        deserialize_with = "deserialize_task_heartbeat_seconds_option"
    )]
    pub heartbeat_seconds: Option<TaskHeartbeatSeconds>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::{AssignObject, JsonataExpr, State, TaskHeartbeatSeconds, TaskTimeoutSeconds};

    fn assign(v: serde_json::Value) -> Option<AssignObject> {
        Some(AssignObject(
            v.as_object().expect("assign must be object").clone(),
        ))
    }

    #[test]
    fn test_task_state_lambda_jsonata() -> Result<(), Box<dyn Error>> {
        // JSONata example from the Task state documentation: invokes a Lambda function, passing
        // JSONata expressions in `Arguments` and assigning the task result via `Assign`.
        let content = r#"{
          "Type": "Task",
          "Resource": "arn:aws:states:::lambda:invoke",
          "Next": "Check Price",
          "Arguments": {
            "Payload": {
              "product": "{% $states.context.Execution.Input.product %}"
            },
            "FunctionName": "arn:aws:lambda:us-east-1:123456789012:function:priceWatcher:$LATEST"
          },
          "Assign": {
            "currentPrice": "{% $states.result.Payload.current_price %}"
          }
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Task(ref task) = value else {
            panic!("expected Task state, got {:?}", value);
        };

        assert_eq!(task.resource, "arn:aws:states:::lambda:invoke");
        assert_eq!(task.next.as_deref(), Some("Check Price"));
        assert_eq!(task.end, None);
        assert_eq!(
            task.arguments,
            Some(serde_json::json!({
                "Payload": {
                    "product": "{% $states.context.Execution.Input.product %}"
                },
                "FunctionName": "arn:aws:lambda:us-east-1:123456789012:function:priceWatcher:$LATEST"
            }))
        );
        assert_eq!(
            task.assign,
            assign(serde_json::json!({
                "currentPrice": "{% $states.result.Payload.current_price %}"
            }))
        );

        // Round-trip: the JSONata definition must be preserved.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_task_state_end() -> Result<(), Box<dyn Error>> {
        // A minimal Task that terminates with `End`.
        let content = r#"{
          "Type": "Task",
          "Resource": "arn:aws:states:::lambda:invoke",
          "End": true
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Task(ref task) = value else {
            panic!("expected Task state, got {:?}", value);
        };

        assert_eq!(task.next, None);
        assert_eq!(task.end, Some(true));

        // Round-trip.
        let reserialized = serde_json::to_string(&value)?;
        assert!(reserialized.contains(r#""End":true"#));
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_task_state_retry_and_catch() -> Result<(), Box<dyn Error>> {
        // Error-handling example from the documentation: a Task with both `Retry` (two
        // retriers) and `Catch` (a fallback to the `Z` state).
        let content = r#"{
          "Type": "Task",
          "Resource": "arn:aws:states:us-east-1:123456789012:task:X",
          "Next": "Y",
          "Retry": [
            {
              "ErrorEquals": ["ErrorA", "ErrorB"],
              "IntervalSeconds": 1,
              "BackoffRate": 2.0,
              "MaxAttempts": 2
            },
            {
              "ErrorEquals": ["ErrorC"],
              "IntervalSeconds": 5
            }
          ],
          "Catch": [
            {
              "ErrorEquals": ["States.ALL"],
              "Next": "Z"
            }
          ]
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Task(ref task) = value else {
            panic!("expected Task state, got {:?}", value);
        };

        assert_eq!(task.next.as_deref(), Some("Y"));
        let retry = task.retry.as_ref().expect("retry should be present");
        assert_eq!(retry.len(), 2);
        assert_eq!(retry[0].error_equals, ["ErrorA", "ErrorB"]);
        assert_eq!(retry[0].max_attempts, Some(2));
        assert_eq!(retry[0].interval_seconds, Some(1));
        assert_eq!(retry[1].error_equals, ["ErrorC"]);
        assert_eq!(retry[1].interval_seconds, Some(5));

        let catch = task.catch.as_ref().expect("catch should be present");
        assert_eq!(catch.len(), 1);
        assert_eq!(catch[0].error_equals, ["States.ALL"]);
        assert_eq!(catch[0].next, "Z");

        // Round-trip: the Retry/Catch arrays must be preserved.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_task_state_timeouts_and_heartbeat() -> Result<(), Box<dyn Error>> {
        // Timeout/heartbeat example from the documentation: static `TimeoutSeconds` and
        // `HeartbeatSeconds`.
        let content = r#"{
          "Type": "Task",
          "Resource": "arn:aws:states:us-east-1:123456789012:activity:HelloWorld",
          "TimeoutSeconds": 300,
          "HeartbeatSeconds": 60,
          "Next": "NextState"
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Task(ref task) = value else {
            panic!("expected Task state, got {:?}", value);
        };

        assert_eq!(task.timeout_seconds, Some(TaskTimeoutSeconds::Int(300)));
        assert_eq!(task.heartbeat_seconds, Some(TaskHeartbeatSeconds::Int(60)));
        assert_eq!(task.next.as_deref(), Some("NextState"));

        // Round-trip.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_task_state_timeout_jsonata_expr() -> Result<(), Box<dyn Error>> {
        // A JSONata Task State may specify a JSONata string for `TimeoutSeconds` whose value,
        // when evaluated, must be a positive integer.
        let content = r#"{
          "Type": "Task",
          "Resource": "arn:aws:states:::lambda:invoke",
          "TimeoutSeconds": "{% $states.input.timeoutSeconds %}",
          "Next": "NextState"
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Task(ref task) = value else {
            panic!("expected Task state, got {:?}", value);
        };

        assert_eq!(
            task.timeout_seconds,
            Some(TaskTimeoutSeconds::Expr(
                JsonataExpr::new("{% $states.input.timeoutSeconds %}").unwrap()
            ))
        );

        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_task_state_rejects_non_jsonata_timeout_string() {
        let content = r#"{
          "Type": "Task",
          "Resource": "arn:aws:states:::lambda:invoke",
          "TimeoutSeconds": "slow",
          "Next": "NextState"
        }"#;

        let err = serde_json::from_str::<State>(content).expect_err("expected parse failure");
        assert!(
            err.to_string()
                .contains("TimeoutSeconds string must be a JSONata expression wrapped in {% %}"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_task_state_heartbeat_jsonata_expr() -> Result<(), Box<dyn Error>> {
        // A JSONata Task State may specify a JSONata string for `HeartbeatSeconds` whose value,
        // when evaluated, must be a positive integer.
        let content = r#"{
          "Type": "Task",
          "Resource": "arn:aws:states:::lambda:invoke",
          "HeartbeatSeconds": "{% $states.input.heartbeatSeconds %}",
          "Next": "NextState"
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Task(ref task) = value else {
            panic!("expected Task state, got {:?}", value);
        };

        assert_eq!(
            task.heartbeat_seconds,
            Some(TaskHeartbeatSeconds::Expr(
                JsonataExpr::new("{% $states.input.heartbeatSeconds %}").unwrap()
            ))
        );

        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }
}
