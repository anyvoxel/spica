//! Integration tests for the M1 engine: pure-dataflow state machines using `Pass`, `Succeed`,
//! `Fail`, `Choice`, and `Wait`.

mod common;

use serde_json::{Value, json};
use spica_asl::StateMachine;
use spica_engine::ExecutionError;

fn parse_sm(definition: &str) -> StateMachine {
    serde_json::from_str(definition).expect("state machine should parse")
}

/// One-shot try: create an anonymous flow and run it once, returning the output. This is the explicit
/// lifecycle the removed `Engine::run` used to sugar (see `tests/common`).
async fn run(sm: &StateMachine, input: Value) -> Result<Value, ExecutionError> {
    common::create_and_run(common::in_memory_builder(), sm.clone(), input)
        .await
        .map(|r| r.output)
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
async fn wait_seconds_jsonata_expression() {
    // A Wait `Seconds` may be a JSONata expression (rather than a literal). It is evaluated against
    // `$states.input` at activation, and the resulting number is normalized into an absolute
    // deadline exactly like a literal `Seconds`. A 0-second delay fires immediately.
    let sm = parse_sm(
        r#"{
          "StartAt": "W",
          "States": {
            "W": { "Type": "Wait", "Seconds": "{% $states.input.delay %}", "Next": "P" },
            "P": { "Type": "Pass", "Output": { "done": true }, "End": true }
          }
        }"#,
    );
    let output = run(&sm, json!({ "delay": 0 }))
        .await
        .expect("JSONata Wait Seconds should succeed");
    assert_eq!(output, json!({ "done": true }));
}

#[tokio::test]
async fn wait_timestamp_jsonata_expression() {
    // A Wait `Timestamp` may be a JSONata expression (rather than a literal). It is evaluated
    // against `$states.input` at activation, and the resulting RFC3339 string becomes the absolute
    // deadline. A timestamp in the past fires immediately.
    let sm = parse_sm(
        r#"{
          "StartAt": "W",
          "States": {
            "W": { "Type": "Wait", "Timestamp": "{% $states.input.at %}", "Next": "P" },
            "P": { "Type": "Pass", "Output": { "done": true }, "End": true }
          }
        }"#,
    );
    let output = run(&sm, json!({ "at": "2016-03-14T01:59:00Z" }))
        .await
        .expect("JSONata Wait Timestamp should succeed");
    assert_eq!(output, json!({ "done": true }));
}

#[tokio::test]
async fn wait_seconds_jsonata_non_integer_fails() {
    // A `Seconds` expression must evaluate to an integer in the ASL range; a non-integer result is
    // an invalid definition and terminates the execution at activation.
    let sm = parse_sm(
        r#"{
          "StartAt": "W",
          "States": {
            "W": { "Type": "Wait", "Seconds": "{% $states.input.delay %}", "Next": "P" },
            "P": { "Type": "Pass", "End": true }
          }
        }"#,
    );
    let err = run(&sm, json!({ "delay": "soon" }))
        .await
        .expect_err("non-integer JSONata Seconds should fail");
    assert!(
        matches!(err, ExecutionError::InvalidDefinition(_)),
        "expected InvalidDefinition, got {err:?}"
    );
}

#[tokio::test]
async fn wait_timestamp_jsonata_invalid_rfc3339_fails() {
    // A `Timestamp` expression must evaluate to a valid RFC3339 string; anything else is an invalid
    // definition and terminates the execution at activation.
    let sm = parse_sm(
        r#"{
          "StartAt": "W",
          "States": {
            "W": { "Type": "Wait", "Timestamp": "{% $states.input.at %}", "Next": "P" },
            "P": { "Type": "Pass", "End": true }
          }
        }"#,
    );
    let err = run(&sm, json!({ "at": "not-a-timestamp" }))
        .await
        .expect_err("invalid JSONata Timestamp should fail");
    assert!(
        matches!(err, ExecutionError::InvalidDefinition(_)),
        "expected InvalidDefinition, got {err:?}"
    );
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
async fn unserved_task_fails_via_timeout_not_definition() {
    // With no worker for its Resource, a Task is *not* rejected up front — Zeebe semantics: an
    // unclaimed task simply stays queued/claimable (Pending). If the state sets `TimeoutSeconds`, the
    // engine's `TaskTimeout` backstop fails it after that window, so the terminal outcome is a
    // deterministic `TimedOut` — not the M1 `InvalidDefinition` "no worker" guard, which is gone.
    let sm = parse_sm(
        r#"{
          "StartAt": "T",
          "States": {
            "T": { "Type": "Task", "Resource": "arn:aws:lambda:::f",
                   "TimeoutSeconds": 1, "End": true }
          }
        }"#,
    );
    let err = run(&sm, Value::Null)
        .await
        .expect_err("an unserved Task with TimeoutSeconds should eventually time out");
    assert!(
        matches!(err, ExecutionError::TimedOut { .. }),
        "expected TimedOut, got {err:?}"
    );
}

#[tokio::test]
async fn handler_error_is_recorded_as_failure() {
    // A Pass with a malformed JSONata `Output`: the handler's `decide` errors (eval failure), and
    // the default `handle` records it as a failure (ActivityFailed + FailExecution) into the same
    // collector — preserving the already-emitted ActivityStarted — rather than the StreamProcessor
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

// ── Task (external-resource call) ────────────────────────────────────────────

/// A `TaskHandler` that echoes its `arguments` back as the task's output.
#[derive(Default)]
struct EchoHandler;

#[async_trait::async_trait]
impl spica_engine::TaskHandler for EchoHandler {
    async fn run(&self, _resource: &str, arguments: &Value) -> Result<Value, ExecutionError> {
        Ok(arguments.clone())
    }
}

/// A `TaskHandler` that always fails with a fixed `ExecutionError`.
#[derive(Default)]
struct FailingHandler;

#[async_trait::async_trait]
impl spica_engine::TaskHandler for FailingHandler {
    async fn run(&self, _resource: &str, _arguments: &Value) -> Result<Value, ExecutionError> {
        Err(ExecutionError::StateFailed {
            state: "T".to_string(),
            error: "TaskBoom".to_string(),
            output: json!({ "Error": "TaskBoom" }),
        })
    }
}

async fn run_task(
    sm: &StateMachine,
    input: Value,
    handler: Box<dyn spica_engine::TaskHandler>,
) -> Result<Value, ExecutionError> {
    let mut handlers = std::collections::HashMap::new();
    handlers.insert(
        "arn:aws:states:::lambda:invoke".to_string(),
        std::sync::Arc::from(handler),
    );
    common::create_and_run_with_handlers(common::in_memory_builder(), sm.clone(), input, handlers)
        .await
        .map(|r| r.output)
}

#[tokio::test]
async fn task_success_outputs_the_arguments() {
    // A Task echoes its (projected) arguments back as the execution's output.
    let sm = parse_sm(
        r#"{
          "StartAt": "T",
          "States": {
            "T": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Arguments": { "product": "{% $states.input.product %}", "qty": 2 },
              "End": true
            }
          }
        }"#,
    );
    let output = run_task(&sm, json!({ "product": "widget" }), Box::new(EchoHandler))
        .await
        .expect("task should succeed");
    assert_eq!(output, json!({ "product": "widget", "qty": 2 }));
}

#[tokio::test]
async fn task_assigns_result_and_routes_to_next() {
    // A Task whose result is captured via Assign and routed to a following state. `$states.result`
    // is the task's output (the echoed arguments).
    let sm = parse_sm(
        r#"{
          "StartAt": "T",
          "States": {
            "T": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Arguments": { "v": "{% $states.input.v %}" },
              "Assign": { "result_copy": "{% $states.result.v %}" },
              "Next": "Done"
            },
            "Done": { "Type": "Pass", "Output": "{% $result_copy %}", "End": true }
          }
        }"#,
    );
    let output = run_task(&sm, json!({ "v": 7 }), Box::new(EchoHandler))
        .await
        .expect("task should succeed");
    assert_eq!(output, json!(7.0));
}

#[tokio::test]
async fn task_failure_terminates_execution() {
    // A Task whose handler fails terminates the execution with the failure, propagating up through
    // the terminate cascade.
    let sm = parse_sm(
        r#"{
          "StartAt": "T",
          "States": {
            "T": { "Type": "Task", "Resource": "arn:aws:states:::lambda:invoke", "End": true }
          }
        }"#,
    );
    let err = run_task(&sm, Value::Null, Box::new(FailingHandler))
        .await
        .expect_err("a failing task should terminate the execution");
    match err {
        ExecutionError::StateFailed { error, .. } => assert_eq!(error, "TaskBoom"),
        other => panic!("expected StateFailed, got {other:?}"),
    }
}

/// A `TaskHandler` that fails the first `failures` calls, then echoes `arguments`.
/// Used to drive a `Retry` that must re-invoke until the budget is exhausted.
#[derive(Default)]
struct FailThenSucceed {
    failures: u32,
}

#[async_trait::async_trait]
impl spica_engine::TaskHandler for FailThenSucceed {
    async fn run(&self, _resource: &str, arguments: &Value) -> Result<Value, ExecutionError> {
        // The handler runs on a spawned task; use interior mutability to count calls.
        static CALLS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let n = CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if n < self.failures {
            Err(ExecutionError::StateFailed {
                state: "T".to_string(),
                error: "TaskBoom".to_string(),
                output: json!({ "Error": "TaskBoom" }),
            })
        } else {
            Ok(arguments.clone())
        }
    }
}

#[tokio::test]
async fn task_retry_succeeds_after_retries() {
    // A Task that fails twice then succeeds: its `Retry` (MaxAttempts 3) re-invokes it until it
    // eventually succeeds, and the execution completes normally.
    let sm = parse_sm(
        r#"{
          "StartAt": "T",
          "States": {
            "T": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Arguments": { "v": "{% $states.input.v %}" },
              "Retry": [
                { "ErrorEquals": ["States.ALL"], "IntervalSeconds": 0, "MaxAttempts": 3 }
              ],
              "End": true
            }
          }
        }"#,
    );
    let output = run_task(
        &sm,
        json!({ "v": 5 }),
        Box::new(FailThenSucceed { failures: 2 }),
    )
    .await
    .expect("task should succeed after retries");
    // jsonata-core renders integers as f64, so 5 round-trips as 5.0.
    assert_eq!(output, json!({ "v": 5.0 }));
}

#[derive(Default)]
struct SequencedHandler {
    outcomes: std::sync::Mutex<std::collections::VecDeque<Result<Value, ExecutionError>>>,
}

impl SequencedHandler {
    fn new(outcomes: Vec<Result<Value, ExecutionError>>) -> Self {
        Self {
            outcomes: std::sync::Mutex::new(std::collections::VecDeque::from(outcomes)),
        }
    }
}

#[async_trait::async_trait]
impl spica_engine::TaskHandler for SequencedHandler {
    async fn run(&self, _resource: &str, arguments: &Value) -> Result<Value, ExecutionError> {
        self.outcomes
            .lock()
            .expect("sequence mutex should not be poisoned")
            .pop_front()
            .unwrap_or_else(|| Ok(arguments.clone()))
    }
}

#[tokio::test]
async fn task_multiple_retriers_keep_independent_attempt_counts() {
    // Two retriers with independent budgets: the first handles `ErrorA` once, the second handles
    // `ErrorB` twice. With only an activity-wide retry counter, the second retrier would inherit the
    // first retrier's history and exhaust early; the structured retry state keeps each retrier's own
    // attempt ladder separate, so the task reaches its eventual success.
    let sm = parse_sm(
        r#"{
          "StartAt": "T",
          "States": {
            "T": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Arguments": { "v": "{% $states.input.v %}" },
              "Retry": [
                { "ErrorEquals": ["ErrorA"], "IntervalSeconds": 0, "MaxAttempts": 1 },
                { "ErrorEquals": ["ErrorB"], "IntervalSeconds": 0, "MaxAttempts": 2 }
              ],
              "End": true
            }
          }
        }"#,
    );
    let output = run_task(
        &sm,
        json!({ "v": 9 }),
        Box::new(SequencedHandler::new(vec![
            Err(ExecutionError::StateFailed {
                state: "T".to_string(),
                error: "ErrorA".to_string(),
                output: json!({ "Error": "ErrorA" }),
            }),
            Err(ExecutionError::StateFailed {
                state: "T".to_string(),
                error: "ErrorB".to_string(),
                output: json!({ "Error": "ErrorB" }),
            }),
            Err(ExecutionError::StateFailed {
                state: "T".to_string(),
                error: "ErrorB".to_string(),
                output: json!({ "Error": "ErrorB" }),
            }),
            Ok(json!({ "v": 9 })),
        ])),
    )
    .await
    .expect("independent retrier budgets should allow the task to succeed");
    assert_eq!(output, json!({ "v": 9 }));
}

#[tokio::test]
async fn task_retry_exhausted_then_catch() {
    // A Task that always fails, with `Retry` exhausted (MaxAttempts 1) then a `Catch` that binds
    // `$states.errorOutput` and routes to a fallback. The fallback receives the error output.
    let sm = parse_sm(
        r#"{
          "StartAt": "T",
          "States": {
            "T": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "End": false,
              "Retry": [ { "ErrorEquals": ["States.ALL"], "IntervalSeconds": 0, "MaxAttempts": 0 } ],
              "Catch": [
                { "ErrorEquals": ["States.ALL"], "Next": "Recover",
                  "Assign": { "err": "{% $states.errorOutput %}" } }
              ]
            },
            "Recover": { "Type": "Pass", "Output": "{% $err %}", "End": true }
          }
        }"#,
    );
    let output = run_task(&sm, Value::Null, Box::new(FailingHandler))
        .await
        .expect("catch should route to the fallback");
    // The error output from `StateFailed{error: TaskBoom}` is the recorded `output`.
    assert_eq!(output, json!({ "Error": "TaskBoom" }));
}

#[tokio::test]
async fn task_retry_exhausted_no_catch_terminates() {
    // A Task that always fails with `Retry` budget 0 and no `Catch` terminates the execution.
    let sm = parse_sm(
        r#"{
          "StartAt": "T",
          "States": {
            "T": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "End": true,
              "Retry": [ { "ErrorEquals": ["States.ALL"], "IntervalSeconds": 0, "MaxAttempts": 0 } ]
            }
          }
        }"#,
    );
    let err = run_task(&sm, Value::Null, Box::new(FailingHandler))
        .await
        .expect_err("no catch should terminate");
    match err {
        ExecutionError::StateFailed { error, .. } => assert_eq!(error, "TaskBoom"),
        other => panic!("expected StateFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn task_timeout_with_catch() {
    // A Task with `TimeoutSeconds` that never settles (a handler sleeping past the deadline): the
    // `TaskTimeout` timer fails it with `States.Timeout`, which the `Catch` routes to a fallback.
    let sm = parse_sm(
        r#"{
          "StartAt": "T",
          "States": {
            "T": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "TimeoutSeconds": 1,
              "Catch": [ { "ErrorEquals": ["States.Timeout"], "Next": "Recover" } ]
            },
            "Recover": { "Type": "Pass", "Output": { "timed_out": true }, "End": true }
          }
        }"#,
    );
    let output = run_task(&sm, Value::Null, Box::new(SleepHandler))
        .await
        .expect("timeout should route to catch");
    assert_eq!(output, json!({ "timed_out": true }));
}

/// A `TaskHandler` that sleeps well past any reasonable test timeout, so only the `TimeoutSeconds`
/// timer terminates the task.
#[derive(Default)]
struct SleepHandler;

#[async_trait::async_trait]
impl spica_engine::TaskHandler for SleepHandler {
    async fn run(&self, _resource: &str, _arguments: &Value) -> Result<Value, ExecutionError> {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        Ok(Value::Null)
    }
}

// ── M3 Parallel ──────────────────────────────────────────────────────────────
//
// Each branch runs as a child execution carrying a `state_path` into the shared machine; the
// `Parallel` activity stays `Running` until every branch settles, then aggregates their outputs (in
// `Branches` order) into the array that becomes its result. A `Parallel` must set `next` or `end`.

#[tokio::test]
async fn parallel_two_branches_aggregates_output_array() {
    // Two independent branches, each a Succeed producing one field. The Parallel's result is the
    // ordered array of branch outputs, and (with `End: true`) that array is the execution's output.
    let sm = parse_sm(
        r#"{
          "StartAt": "P",
          "States": {
            "P": {
              "Type": "Parallel",
              "End": true,
              "Branches": [
                {
                  "StartAt": "A",
                  "States": { "A": { "Type": "Succeed", "Output": { "branch": "A", "v": 1 } } }
                },
                {
                  "StartAt": "B",
                  "States": { "B": { "Type": "Succeed", "Output": { "branch": "B", "v": 2 } } }
                }
              ]
            }
          }
        }"#,
    );
    let output = run(&sm, Value::Null)
        .await
        .expect("parallel should succeed");
    // Branches aggregate in declaration order, regardless of which child execution settles first.
    // The literal `v` values stay integers (a literal Output is not JSONata-projected).
    assert_eq!(
        output,
        json!([{ "branch": "A", "v": 1 }, { "branch": "B", "v": 2 }])
    );
}

#[tokio::test]
async fn parallel_branch_failure_terminates_execution() {
    // One branch fails; per the ASL spec the whole Parallel fails and the surviving branch is
    // stopped. The execution terminates with the branch's recorded failure.
    let sm = parse_sm(
        r#"{
          "StartAt": "P",
          "States": {
            "P": {
              "Type": "Parallel",
              "End": true,
              "Branches": [
                {
                  "StartAt": "Ok",
                  "States": { "Ok": { "Type": "Succeed", "Output": { "branch": "ok" } } }
                },
                {
                  "StartAt": "Boom",
                  "States": {
                    "Boom": { "Type": "Fail", "Error": "BranchBoom", "Cause": "nope" }
                  }
                }
              ]
            }
          }
        }"#,
    );
    let err = run(&sm, Value::Null)
        .await
        .expect_err("a failing branch should terminate the parallel");
    match err {
        ExecutionError::StateFailed { error, .. } => assert_eq!(error, "BranchBoom"),
        other => panic!("expected StateFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn parallel_routes_next_after_convergence() {
    // After both branches converge, the Parallel routes to a following state (`Next`), passing its
    // aggregated output array as that state's input — the non-terminal `next`, not `end`, form.
    let sm = parse_sm(
        r#"{
          "StartAt": "P",
          "States": {
            "P": {
              "Type": "Parallel",
              "Next": "Merge",
              "Branches": [
                {
                  "StartAt": "A",
                  "States": { "A": { "Type": "Succeed", "Output": { "branch": "A" } } }
                },
                {
                  "StartAt": "B",
                  "States": { "B": { "Type": "Succeed", "Output": { "branch": "B" } } }
                }
              ]
            },
            "Merge": {
              "Type": "Succeed",
              "Output": { "branches_count": "{% $count($states.input) %}" }
            }
          }
        }"#,
    );
    let output = run(&sm, Value::Null)
        .await
        .expect("parallel should route to next after convergence");
    assert_eq!(output, json!({ "branches_count": 2.0 }));
}

// ── Map state (M3) ───────────────────────────────────────────────────────────

#[tokio::test]
async fn map_array_items_aggregates_output_array() {
    // A Map over a literal items array, MaxConcurrency 1 (sequential). Each item runs the same
    // item processor child machine; the Map's result is the ordered array of per-item outputs.
    let sm = parse_sm(
        r#"{
          "StartAt": "M",
          "States": {
            "M": {
              "Type": "Map",
              "End": true,
              "Items": [ { "sku": "A-1" }, { "sku": "B-2" } ],
              "ItemProcessor": {
                "StartAt": "Emit",
                "States": { "Emit": { "Type": "Pass", "End": true } }
              }
            }
          }
        }"#,
    );
    let output = run(&sm, Value::Null)
        .await
        .expect("map over a literal items array should succeed");
    // The per-item input is the item itself (default, no ItemSelector), and a Pass echoes it; items
    // aggregate in array order. Literal values stay as-is.
    assert_eq!(output, json!([{ "sku": "A-1" }, { "sku": "B-2" }]));
}

#[tokio::test]
async fn map_jsonata_items() {
    // `Items` as a JSONata expression resolving from the state input, over a Succeed that emits a
    // per-item literal field. Result aggregates both outputs in order.
    let sm = parse_sm(
        r#"{
          "StartAt": "M",
          "States": {
            "M": {
              "Type": "Map",
              "End": true,
              "Items": "{% $states.input.list %}",
              "ItemProcessor": {
                "StartAt": "Tag",
                "States": {
                  "Tag": { "Type": "Succeed", "Output": { "tagged": "{% $states.input %}" } }
                }
              }
            }
          }
        }"#,
    );
    let output = run(&sm, json!({ "list": [1, 2, 3] }))
        .await
        .expect("map with JSONata items should succeed");
    // Each item `n` maps to `{ "tagged": n }`, in order. JSONata projection (`{% ... %}`) yields
    // numbers as f64, so the tagged values are `1.0`/`2.0`/`3.0` (same behavior as the Parallel
    // `$count()` result asserting `2.0`).
    assert_eq!(
        output,
        json!([
            { "tagged": 1.0 },
            { "tagged": 2.0 },
            { "tagged": 3.0 }
        ])
    );
}

#[tokio::test]
async fn map_bounded_concurrency_preserves_order() {
    // MaxConcurrency 2 over 5 items: the engine replenishes one slot per settle while keeping at
    // most 2 in flight. Ordering of the aggregated result must still follow the items array order.
    let sm = parse_sm(
        r#"{
          "StartAt": "M",
          "States": {
            "M": {
              "Type": "Map",
              "End": true,
              "Items": [ 1, 2, 3, 4, 5 ],
              "MaxConcurrency": 2,
              "ItemProcessor": {
                "StartAt": "Emit",
                "States": { "Emit": { "Type": "Pass", "End": true } }
              }
            }
          }
        }"#,
    );
    let output = run(&sm, Value::Null)
        .await
        .expect("bounded-concurrency map should succeed");
    assert_eq!(output, json!([1, 2, 3, 4, 5]));
}

#[tokio::test]
async fn map_empty_items_converges_immediately() {
    // An empty items array spawns no children, so the Map must converge immediately to an empty
    // array (otherwise it would wedge waiting on a settle that never comes).
    let sm = parse_sm(
        r#"{
          "StartAt": "M",
          "States": {
            "M": {
              "Type": "Map",
              "End": true,
              "Items": [],
              "ItemProcessor": {
                "StartAt": "Emit",
                "States": { "Emit": { "Type": "Pass", "End": true } }
              }
            }
          }
        }"#,
    );
    let output = run(&sm, Value::Null)
        .await
        .expect("empty map should succeed");
    assert_eq!(output, json!([]));
}

#[tokio::test]
async fn map_item_failure_fails_map() {
    // An item processor that ends in a Fail state: with the default failure tolerance (0), any item
    // failure fails the whole Map and terminates the execution.
    let sm = parse_sm(
        r#"{
          "StartAt": "M",
          "States": {
            "M": {
              "Type": "Map",
              "End": true,
              "Items": [ 1, 2, 3 ],
              "ItemProcessor": {
                "StartAt": "Boom",
                "States": { "Boom": { "Type": "Fail", "Error": "ItemBoom", "Cause": "nope" } }
              }
            }
          }
        }"#,
    );
    let err = run(&sm, Value::Null)
        .await
        .expect_err("a failing item should fail the map");
    match err {
        ExecutionError::StateFailed { error, .. } => {
            assert_eq!(
                error, "Map item failed",
                "the map surfaces its own item-failure error (child detail is deferred)"
            )
        }
        other => panic!("expected StateFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn map_routes_next_after_convergence() {
    // A non-terminal Map (`Next`) routes to a following state once all items converge, passing its
    // aggregated output array as that state's input.
    let sm = parse_sm(
        r#"{
          "StartAt": "M",
          "States": {
            "M": {
              "Type": "Map",
              "Next": "Merge",
              "Items": [ 7, 8 ],
              "ItemProcessor": {
                "StartAt": "Emit",
                "States": { "Emit": { "Type": "Pass", "End": true } }
              }
            },
            "Merge": {
              "Type": "Succeed",
              "Output": { "count": "{% $count($states.input) %}" }
            }
          }
        }"#,
    );
    let output = run(&sm, Value::Null)
        .await
        .expect("map should route to next after convergence");
    assert_eq!(output, json!({ "count": 2.0 }));
}

#[tokio::test]
async fn map_output_projection_reshapes_aggregated_array() {
    // A Map with an `Output` that reshapes the aggregated per-item output array instead of passing
    // it through verbatim.
    let sm = parse_sm(
        r#"{
          "StartAt": "M",
          "States": {
            "M": {
              "Type": "Map",
              "End": true,
              "Items": [ "a", "b" ],
              "Output": { "items": "{% $states.result %}" },
              "ItemProcessor": {
                "StartAt": "Emit",
                "States": { "Emit": { "Type": "Pass", "End": true } }
              }
            }
          }
        }"#,
    );
    let output = run(&sm, Value::Null)
        .await
        .expect("map output projection should succeed");
    assert_eq!(output, json!({ "items": ["a", "b"] }));
}
