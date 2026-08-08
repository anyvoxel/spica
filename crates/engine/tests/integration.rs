//! Integration tests for the M1 engine: pure-dataflow state machines using `Pass`, `Succeed`,
//! `Fail`, `Choice`, and `Wait`.

use serde_json::{Value, json};
use spica_asl::StateMachine;
use spica_engine::{Engine, ExecutionError};

fn parse_sm(definition: &str) -> StateMachine {
    serde_json::from_str(definition).expect("state machine should parse")
}

async fn run(sm: &StateMachine, input: Value) -> Result<Value, ExecutionError> {
    Engine::start(sm.clone(), input).await.map(|r| r.output)
}

#[tokio::test]
async fn pass_output_projection() {
    let sm = parse_sm(
        r#"{
          "StartAt": "Project",
          "States": {
            "Project": {
              "Type": "Pass",
              "Output": { "total": "{% $states.input.transaction.total %}" },
              "End": true
            }
          }
        }"#,
    );
    let output = run(&sm, json!({ "transaction": { "total": 42 } }))
        .await
        .expect("execution should succeed");
    // jsonata-core numbers are f64, so 42 round-trips as 42.0.
    assert_eq!(output, json!({ "total": 42.0 }));
}

#[tokio::test]
async fn assign_propagates_to_next_state() {
    let sm = parse_sm(
        r#"{
          "StartAt": "Set",
          "States": {
            "Set": { "Type": "Pass", "Assign": { "greeting": "hello" }, "Next": "Read" },
            "Read": { "Type": "Pass", "Output": "{% $greeting %}", "End": true }
          }
        }"#,
    );
    let output = run(&sm, Value::Null)
        .await
        .expect("execution should succeed");
    assert_eq!(output, json!("hello"));
}

#[tokio::test]
async fn choice_routing() {
    let sm = parse_sm(
        r#"{
          "StartAt": "Pick",
          "States": {
            "Pick": {
              "Type": "Choice",
              "Choices": [
                { "Condition": "{% $states.input.type = 'A' %}", "Next": "A" },
                { "Condition": "{% $states.input.type = 'B' %}", "Next": "B" }
              ],
              "Default": "Other"
            },
            "A": { "Type": "Succeed", "Output": { "branch": "A" } },
            "B": { "Type": "Succeed", "Output": { "branch": "B" } },
            "Other": { "Type": "Succeed", "Output": { "branch": "other" } }
          }
        }"#,
    );
    let output = run(&sm, json!({ "type": "B" }))
        .await
        .expect("execution should succeed");
    assert_eq!(output, json!({ "branch": "B" }));

    let output = run(&sm, json!({ "type": "Z" }))
        .await
        .expect("execution should succeed");
    assert_eq!(output, json!({ "branch": "other" }));
}

#[tokio::test]
async fn choice_rule_assign_and_output_override_state_level() {
    let sm = parse_sm(
        r#"{
          "StartAt": "Pick",
          "States": {
            "Pick": {
              "Type": "Choice",
              "Choices": [
                {
                  "Condition": "{% $states.input.v >= 20 %}",
                  "Assign": { "range": "twenties" },
                  "Output": { "r": "{% $range %}" },
                  "Next": "Done"
                }
              ],
              "Default": "Done"
            },
            "Done": { "Type": "Succeed" }
          }
        }"#,
    );
    let output = run(&sm, json!({ "v": 25 }))
        .await
        .expect("execution should succeed");
    // The matched rule's Assign binds `range`, its Output projects it; Done (Succeed, no Output)
    // passes its input through.
    assert_eq!(output, json!({ "r": "twenties" }));
}

#[tokio::test]
async fn choice_no_match_without_default_errors() {
    let sm = parse_sm(
        r#"{
          "StartAt": "Pick",
          "States": {
            "Pick": {
              "Type": "Choice",
              "Choices": [ { "Condition": false, "Next": "X" } ]
            },
            "X": { "Type": "Succeed" }
          }
        }"#,
    );
    let err = run(&sm, Value::Null)
        .await
        .expect_err("should fail with no match");
    assert!(matches!(err, ExecutionError::NoChoiceMatched { .. }));
    assert_eq!(err.error_name(), "States.NoChoiceMatched");
}

#[tokio::test]
async fn fail_state_terminates_with_error_output() {
    let sm = parse_sm(
        r#"{
          "StartAt": "Boom",
          "States": {
            "Boom": { "Type": "Fail", "Error": "ErrorA", "Cause": "Invalid response." }
          }
        }"#,
    );
    let err = run(&sm, Value::Null)
        .await
        .expect_err("Fail should produce an error");
    match err {
        ExecutionError::StateFailed {
            ref error,
            ref output,
            ..
        } => {
            assert_eq!(error, "ErrorA");
            assert_eq!(
                output,
                &json!({ "Error": "ErrorA", "Cause": "Invalid response." })
            );
        }
        other => panic!("expected StateFailed, got {other:?}"),
    }
    assert_eq!(err.error_name(), "ErrorA");
}

#[tokio::test]
async fn wait_seconds_zero_then_pass() {
    let sm = parse_sm(
        r#"{
          "StartAt": "W",
          "States": {
            "W": { "Type": "Wait", "Seconds": 0, "Next": "P" },
            "P": { "Type": "Pass", "Output": { "done": true }, "End": true }
          }
        }"#,
    );
    let output = run(&sm, Value::Null)
        .await
        .expect("execution should succeed");
    assert_eq!(output, json!({ "done": true }));
}

#[tokio::test]
async fn wait_timestamp_absolute_is_supported() {
    // A Wait may hold until an absolute RFC3339 Timestamp. A timestamp in the past (relative to
    // the engine's clock) fires immediately, so the execution proceeds straight to `P`. This
    // verifies the absolute timestamp is parsed into a deadline (rather than rejected as M1
    // unsupported).
    let sm = parse_sm(
        r#"{
          "StartAt": "W",
          "States": {
            "W": { "Type": "Wait", "Timestamp": "2016-03-14T01:59:00Z", "Next": "P" },
            "P": { "Type": "Pass", "Output": { "done": true }, "End": true }
          }
        }"#,
    );
    let output = run(&sm, Value::Null)
        .await
        .expect("absolute-Timestamp Wait should succeed (past deadline fires immediately)");
    assert_eq!(output, json!({ "done": true }));
}

#[tokio::test]
async fn succeed_output_expression_string() {
    let sm = parse_sm(
        r#"{
          "StartAt": "S",
          "States": { "S": { "Type": "Succeed", "Output": "{% $states.input %}" } }
        }"#,
    );
    let output = run(&sm, json!({ "x": 1 }))
        .await
        .expect("execution should succeed");
    assert_eq!(output, json!({ "x": 1.0 }));
}

#[tokio::test]
async fn end_to_end_dataflow() {
    let sm = parse_sm(
        r#"{
          "StartAt": "Init",
          "States": {
            "Init": { "Type": "Pass", "Assign": { "score": "{% $states.input.score %}" }, "Next": "Branch" },
            "Branch": {
              "Type": "Choice",
              "Choices": [
                { "Condition": "{% $score >= 90 %}", "Next": "High" },
                { "Condition": "{% $score < 90 %}", "Next": "Low" }
              ],
              "Default": "Low"
            },
            "High": { "Type": "Pass", "Output": { "grade": "A" }, "Next": "Finish" },
            "Low": { "Type": "Pass", "Output": { "grade": "B" }, "Next": "Finish" },
            "Finish": { "Type": "Succeed", "Output": { "grade": "{% $states.input.grade %}" } }
          }
        }"#,
    );
    let output = run(&sm, json!({ "score": 95 }))
        .await
        .expect("execution should succeed");
    assert_eq!(output, json!({ "grade": "A" }));

    let output = run(&sm, json!({ "score": 50 }))
        .await
        .expect("execution should succeed");
    assert_eq!(output, json!({ "grade": "B" }));
}

#[tokio::test]
async fn unsupported_state_types_are_rejected() {
    let sm = parse_sm(
        r#"{
          "StartAt": "T",
          "States": { "T": { "Type": "Task", "Resource": "arn:aws:lambda:::f", "End": true } }
        }"#,
    );
    let err = run(&sm, Value::Null)
        .await
        .expect_err("Task should be unsupported in M1");
    assert!(matches!(err, ExecutionError::InvalidDefinition(_)));
}

#[tokio::test]
async fn handler_error_is_recorded_as_failure() {
    // A Pass with a malformed JSONata `Output`: the handler's `decide` errors (eval failure), and
    // the default `handle` records it as a failure (ActivityFailed + FailExecution) into the same
    // collector — preserving the already-emitted ActivityStarted — rather than the Processor
    // synthesizing a fresh failure.
    let sm = parse_sm(
        r#"{ "StartAt": "P", "States": { "P": { "Type": "Pass", "Output": "{% $states.input.. %}", "End": true } } }"#,
    );
    let err = run(&sm, Value::Null)
        .await
        .expect_err("malformed JSONata should fail the execution");
    assert!(
        matches!(err, ExecutionError::Jsonata { .. }),
        "expected Jsonata error, got {err:?}"
    );
}

// ── Fixture smoke tests ──────────────────────────────────────────────────────
//
// These parse a real ASL fixture from the `spica-asl` corpus (verified pure-JSONata, no JSONPath
// fields) and execute it end-to-end.

fn fixture(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("asl")
        .join("tests")
        .join("resources")
        .join("valid")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read fixture {name}: {e}"))
}

#[tokio::test]
async fn fixture_basic_pass() {
    let sm: StateMachine = serde_json::from_str(&fixture("basic_pass.json")).unwrap();
    let output = run(&sm, json!({ "k": "v" }))
        .await
        .expect("execution should succeed");
    // A Pass with no Output passes its input through unchanged.
    assert_eq!(output, json!({ "k": "v" }));
}

#[tokio::test]
async fn fixture_choice_with_boolean_condition() {
    let sm: StateMachine =
        serde_json::from_str(&fixture("choice-with-boolean-condition.json")).unwrap();
    // The first rule's Condition is `true`, routing to the `Matched` Succeed state (no Output),
    // so the input passes through.
    let output = run(&sm, json!({ "hello": "world" }))
        .await
        .expect("execution should succeed");
    assert_eq!(output, json!({ "hello": "world" }));
}
