use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::{AssignObject, Branch, Catcher, Retrier};

/// A `Parallel` state (`"Type": "Parallel"`) executes multiple branches of a state machine
/// concurrently. Each branch is a self-contained state machine (with its own `StartAt` and
/// `States`); the interpreter starts each branch and waits until all branches reach a terminal
/// state before transitioning.
///
/// Each branch receives a copy of the state's input. The state's output is an array with one
/// element per branch, containing that branch's output. A `Parallel` state must set either
/// `next` or `end`. If any branch fails (unhandled error or a `Fail` state), the entire
/// `Parallel` state fails and all branches are stopped.
///
/// See:
/// - https://docs.aws.amazon.com/step-functions/latest/dg/state-parallel.html
/// - https://states-language.net/spec.html#parallel-state
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct ParallelState {
    /// Optional. A human-readable description of the state.
    pub comment: Option<String>,

    /// Optional. Used to specify and transform output from the state. When specified, the value
    /// overrides the state output default. Accepts any JSON value; strings surrounded by
    /// `{% %}` are evaluated as JSONata.
    pub output: Option<serde_json::Value>,

    /// Optional. A collection of key-value pairs to assign data to variables. Any string value
    /// surrounded by `{% %}` is evaluated as JSONata.
    pub assign: Option<AssignObject>,

    /// Optional. The name of the next state that is run when all branches terminate. One of
    /// `next` or `end` must be set.
    pub next: Option<String>,

    /// Optional. Designates this state as a terminal state (ends the execution) when `true`.
    /// One of `next` or `end` must be set.
    pub end: Option<bool>,

    /// Required. An array of branch objects to execute in parallel. Each branch is a
    /// self-contained sub-state-machine (with its own `StartAt` and `States`); states within a
    /// branch may only transition to each other.
    pub branches: Vec<Branch>,

    /// Optional. Used to pass information to the state machines defined in `branches`. Values
    /// can include JSONata expressions.
    pub arguments: Option<serde_json::Value>,

    /// Optional. An array of retrier objects that define a retry policy if the state encounters
    /// runtime errors.
    pub retry: Option<Vec<Retrier>>,

    /// Optional. An array of catcher objects that define a fallback state, executed if the
    /// state encounters runtime errors and its retry policy is exhausted or undefined.
    pub catch: Option<Vec<Catcher>>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::State;

    #[test]
    fn test_parallel_state_branches() -> Result<(), Box<dyn Error>> {
        // Two branches (LookupAddress and LookupPhone) executed concurrently, each a
        // self-contained state machine.
        let content = r#"{
          "Type": "Parallel",
          "End": true,
          "Branches": [
            {
              "StartAt": "LookupAddress",
              "States": {
                "LookupAddress": {
                  "Type": "Task",
                  "Resource": "arn:aws:lambda:us-east-1:123456789012:function:AddressFinder",
                  "End": true
                }
              }
            },
            {
              "StartAt": "LookupPhone",
              "States": {
                "LookupPhone": {
                  "Type": "Task",
                  "Resource": "arn:aws:lambda:us-east-1:123456789012:function:PhoneFinder",
                  "End": true
                }
              }
            }
          ]
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Parallel(ref parallel) = value else {
            panic!("expected Parallel state, got {:?}", value);
        };

        assert_eq!(parallel.next, None);
        assert_eq!(parallel.end, Some(true));
        assert_eq!(parallel.branches.len(), 2);

        let addr = &parallel.branches[0];
        assert_eq!(addr.start_at, "LookupAddress");
        assert!(addr.states.contains_key("LookupAddress"));

        let phone = &parallel.branches[1];
        assert_eq!(phone.start_at, "LookupPhone");
        assert!(phone.states.contains_key("LookupPhone"));

        // Round-trip: branches preserved.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }

    #[test]
    fn test_parallel_state_result_path_and_catch() -> Result<(), Box<dyn Error>> {
        // A Parallel state with a `Catch` fallback.
        let content = r#"{
          "Type": "Parallel",
          "Next": "MergeResults",
          "Retry": [
            {
              "ErrorEquals": ["States.Timeout"],
              "MaxAttempts": 2
            }
          ],
          "Catch": [
            {
              "ErrorEquals": ["States.ALL"],
              "Next": "HandleError"
            }
          ],
          "Branches": [
            {
              "StartAt": "Add",
              "States": {
                "Add": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:us-east-1:123456789012:activity:Add",
                  "End": true
                }
              }
            }
          ]
        }"#;

        let value = serde_json::from_str::<State>(content)?;
        let State::Parallel(ref parallel) = value else {
            panic!("expected Parallel state, got {:?}", value);
        };

        assert_eq!(parallel.next.as_deref(), Some("MergeResults"));
        assert_eq!(parallel.end, None);

        let retry = parallel.retry.as_ref().expect("retry should be present");
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].error_equals, ["States.Timeout"]);
        assert_eq!(retry[0].max_attempts, Some(2));

        let catch = parallel.catch.as_ref().expect("catch should be present");
        assert_eq!(catch.len(), 1);
        assert_eq!(catch[0].error_equals, ["States.ALL"]);
        assert_eq!(catch[0].next, "HandleError");

        // Round-trip.
        let reserialized = serde_json::to_string(&value)?;
        let reparsed: State = serde_json::from_str(&reserialized)?;
        assert_eq!(value, reparsed);

        Ok(())
    }
}
