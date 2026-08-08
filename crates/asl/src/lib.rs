// The ASL model types form a library intended for reuse across the Spica crates. As a
// standalone library crate the types are not yet consumed internally, so dead-code and
// unused-import (re-export) warnings are expected.

mod assign;
mod branch;
mod catch;
mod choice;
mod fail;
mod item_processor;
mod map;
mod parallel;
mod pass;
mod retry;
mod succeed;
mod task;
mod utils;
mod wait;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub use assign::AssignObject;
pub use branch::Branch;
pub use catch::Catcher;
pub use choice::{ChoiceCondition, ChoiceRule, ChoiceState};
pub use fail::FailState;
pub use item_processor::ItemProcessor;
pub use map::{
    MapItems, MapMaxConcurrency, MapState, MapToleratedFailureCount, MapToleratedFailurePercentage,
};
pub use parallel::ParallelState;
pub use pass::PassState;
pub use retry::Retrier;
pub use succeed::SucceedState;
pub use task::{TaskHeartbeatSeconds, TaskState, TaskTimeoutSeconds};
pub use utils::{IntOrExpr, JsonataExpr};
pub use wait::{WaitSeconds, WaitState, WaitTimestamp};

/// A state machine is defined by the states it contains and the relationships between them.
/// An execution begins at the state referenced by `start_at` and follows `Next`/`End`
/// transitions until it reaches a terminal state (Succeed, Fail, or `End: true`).
///
/// This model is a **JSONata-only learning subset** of the Amazon States Language: data
/// processing (input/output transformation, variable assignment, branching conditions) is
/// expressed via JSONata in the `Output`/`Arguments`/`Assign`/`Condition` fields, evaluated by
/// `jsonata-core`. JSONPath-specific fields (`InputPath`/`OutputPath`/`ResultPath`/`Parameters`/
/// `ResultSelector`/`*Path`), the `QueryLanguage` switch, and AWS Distributed-Map / service
/// integration fields are intentionally absent.
///
/// See:
/// - https://docs.aws.amazon.com/step-functions/latest/dg/statemachine-structure.html
/// - https://states-language.net/spec.html
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct StateMachine {
    /// Required. A string that must exactly match (case sensitive) the name of one of the
    /// state objects in `states`. This is the state executed first when the execution starts.
    pub start_at: String,

    /// Optional. A human-readable description of the state machine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,

    /// Optional. The version of the Amazon States Language used in the state machine
    /// (default is "1.0"). `None` indicates the field was omitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<StateLanguageVersion>,

    /// Optional. The maximum number of seconds an execution of the state machine can run. If
    /// it runs longer than the specified time, the execution fails with a `States.Timeout`
    /// error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i64>,

    /// Required. An object containing the set of states. States can occur in any order
    /// within this block; the order in which they're listed doesn't affect the order in
    /// which they're run — the contents of the states (their `Next`/`End` transitions)
    /// determines that.
    pub states: HashMap<String, State>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(untagged)]
pub enum StateLanguageVersion {
    #[default]
    #[serde(rename = "1.0")]
    V1,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "Type")]
pub enum State {
    Pass(PassState),
    Wait(WaitState),
    Succeed(SucceedState),
    Fail(FailState),
    Task(TaskState),
    Choice(ChoiceState),
    Parallel(ParallelState),
    Map(Box<MapState>),
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::{AssignObject, StateMachine};

    fn assign(v: serde_json::Value) -> Option<AssignObject> {
        Some(AssignObject(
            v.as_object().expect("assign must be object").clone(),
        ))
    }

    #[test]
    fn test_state_machine_minimal() -> Result<(), Box<dyn Error>> {
        // A simple minimal example of the States language: a single Task state that invokes a
        // Lambda function and terminates.
        let content = r#"{
          "Comment": "A simple minimal example of the States language",
          "StartAt": "Hello World",
          "States": {
            "Hello World": {
              "Type": "Task",
              "Resource": "arn:aws:lambda:us-east-1:123456789012:function:HelloWorld",
              "End": true
            }
          }
        }"#;

        let sm: StateMachine = serde_json::from_str(content)?;
        assert_eq!(sm.start_at, "Hello World");
        assert_eq!(
            sm.comment.as_deref(),
            Some("A simple minimal example of the States language")
        );
        assert_eq!(sm.version, None);
        assert_eq!(sm.timeout_seconds, None);
        assert_eq!(sm.states.len(), 1);
        assert!(sm.states.contains_key("Hello World"));

        // Round-trip.
        let reserialized = serde_json::to_string(&sm)?;
        let reparsed: StateMachine = serde_json::from_str(&reserialized)?;
        assert_eq!(sm, reparsed);

        Ok(())
    }

    #[test]
    fn test_state_machine_jsonata_output() -> Result<(), Box<dyn Error>> {
        // A JSONata state machine: a Pass state uses `Output` to project a field from the input.
        let content = r#"{
          "StartAt": "Project total",
          "States": {
            "Project total": {
              "Type": "Pass",
              "Output": {
                "total": "{% $states.input.transaction.total %}"
              },
              "End": true
            }
          }
        }"#;

        let sm: StateMachine = serde_json::from_str(content)?;
        assert_eq!(sm.start_at, "Project total");
        assert_eq!(sm.states.len(), 1);

        let crate::State::Pass(ref pass) = sm.states["Project total"] else {
            panic!("expected Pass state, got {:?}", sm.states["Project total"]);
        };
        assert_eq!(
            pass.output,
            Some(serde_json::json!({
                "total": "{% $states.input.transaction.total %}"
            }))
        );

        // Round-trip: the JSONata Output must be preserved.
        let reserialized = serde_json::to_string(&sm)?;
        let reparsed: StateMachine = serde_json::from_str(&reserialized)?;
        assert_eq!(sm, reparsed);

        Ok(())
    }

    #[test]
    fn test_state_machine_jsonata_variable_scoping() -> Result<(), Box<dyn Error>> {
        // A JSONata state machine demonstrating variable scoping across a nested Map: the outer
        // `Assign` sets `outer`, the Map's `Assign` rebinds `outer`, and the nested item
        // processor's `Assign` references the outer variable via `$outer`.
        let content = r#"{
          "StartAt": "Get Greeting",
          "States": {
            "Get Greeting": {
              "Type": "Pass",
              "Assign": {
                "outer": "hello"
              },
              "Next": "Greet Everyone"
            },
            "Greet Everyone": {
              "Type": "Map",
              "ItemProcessor": {
                "StartAt": "Begin",
                "States": {
                  "Begin": {
                    "Type": "Pass",
                    "Assign": {
                      "inner": "world",
                      "hi": "{% $outer %}"
                    },
                    "Next": "End"
                  },
                  "End": {
                    "Type": "Succeed",
                    "Output": "{% $hi %}"
                  }
                }
              },
              "Assign": {
                "outer": 2
              },
              "Next": "Goodbye"
            },
            "Goodbye": {
              "Type": "Succeed",
              "Output": "{% $outer %}"
            }
          }
        }"#;

        let sm: StateMachine = serde_json::from_str(content)?;
        assert_eq!(sm.start_at, "Get Greeting");
        assert_eq!(sm.states.len(), 3);

        // The first state assigns the outer variable `outer = "hello"`.
        let crate::State::Pass(ref greet) = sm.states["Get Greeting"] else {
            panic!("expected Pass state, got {:?}", sm.states["Get Greeting"]);
        };
        assert_eq!(
            greet.assign,
            assign(serde_json::json!({ "outer": "hello" }))
        );

        // The Map state rebinds `outer = 2` and defines a nested item processor.
        let crate::State::Map(ref map_state) = sm.states["Greet Everyone"] else {
            panic!("expected Map state, got {:?}", sm.states["Greet Everyone"]);
        };
        let map = map_state.as_ref();
        assert_eq!(map.assign, assign(serde_json::json!({ "outer": 2 })));
        let processor = map
            .item_processor
            .as_ref()
            .expect("item_processor should be present");
        assert_eq!(processor.start_at, "Begin");
        assert_eq!(processor.states.len(), 2);

        // The nested Pass state references the outer variable via `$outer`.
        let crate::State::Pass(ref begin) = processor.states["Begin"] else {
            panic!("expected Pass state, got {:?}", processor.states["Begin"]);
        };
        assert_eq!(
            begin.assign,
            assign(serde_json::json!({
                "inner": "world",
                "hi": "{% $outer %}"
            }))
        );

        // The terminal Succeed state outputs `outer`.
        let crate::State::Succeed(ref goodbye) = sm.states["Goodbye"] else {
            panic!("expected Succeed state, got {:?}", sm.states["Goodbye"]);
        };
        assert_eq!(goodbye.output, Some(serde_json::json!("{% $outer %}")));

        // Round-trip: the nested Map, item processor, and all JSONata expressions must be
        // preserved.
        let reserialized = serde_json::to_string(&sm)?;
        let reparsed: StateMachine = serde_json::from_str(&reserialized)?;
        assert_eq!(sm, reparsed);

        Ok(())
    }

    /// The model is intentionally a **lenient** round-trip representation: it does not use
    /// `#[serde(deny_unknown_fields)]`, so fields outside a state type's modeled set are silently
    /// dropped rather than causing a parse error.
    ///
    /// The resource corpus is split only by **parse behavior**:
    /// - `valid/` must parse into a `StateMachine` and round-trip losslessly.
    /// - `invalid/` contains fixtures documenting definitions the current model should reject at
    ///   parse time (`SERDE_REJECTED`) plus fixtures that are expected to still parse under the
    ///   lenient model (`PARSE_OK_IN_INVALID`).
    const SERDE_REJECTED: &[&str] = &[
        "bad_type.json",
        "choice-with-bare-string-condition.json",
        "map-with-bare-string-items.json",
        "map-with-null-items.json",
        "map-with-object-items.json",
        "map-with-null-tolerated-failure-count.json",
        "map-with-out-of-range-tolerated-failure-percentage.json",
    ];

    const PARSE_OK_IN_INVALID: &[&str] = &[
        "bad_unknown_state.json",
        "basic_choice.json",
        "builder.json",
        "step_deployer.json",
        "linked-parallel.json",
        "no-terminal.json",
        "empty-error-equals-on-catch.json",
        "empty-error-equals-on-retry.json",
        "app_decompose_for_parallelism_runner_simplewait.json",
        "aws_step_functions_waitable_pattern.json",
        "eventbridge_replay_events.json",
        "request_response.json",
        "smart_cron_job.json",
        "sync_buckets_state_machine.json",
        "bad_path.json",
    ];

    /// Collects and sorts the `.json` file paths directly under `dir`.
    fn resource_files(dir: &std::path::Path) -> Result<Vec<std::path::PathBuf>, Box<dyn Error>> {
        let mut entries: Vec<_> = std::fs::read_dir(dir)?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
            .collect();
        entries.sort();
        Ok(entries)
    }

    fn fixture_name(path: &std::path::Path) -> String {
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("?")
            .to_string()
    }

    /// Every fixture in `tests/resources/valid/` must parse into a `StateMachine` and round-trip
    /// losslessly: parse → serialize → re-parse yields an equal machine. A file appearing here
    /// means "this is a well-formed definition the lenient model fully represents."
    #[test]
    fn test_valid_resources_roundtrip() -> Result<(), Box<dyn Error>> {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/resources/valid");
        let mut parsed = 0usize;
        let mut failures: Vec<(String, String)> = Vec::new();

        for path in resource_files(&dir)? {
            let name = fixture_name(&path);
            let content = std::fs::read_to_string(&path)?;
            match serde_json::from_str::<StateMachine>(&content) {
                Err(err) => failures.push((name, err.to_string())),
                Ok(sm) => {
                    let reserialized = serde_json::to_string(&sm)?;
                    match serde_json::from_str::<StateMachine>(&reserialized) {
                        Err(err) => failures.push((name, format!("reparse failed: {err}"))),
                        Ok(reparsed) => {
                            if sm != reparsed {
                                failures.push((name, "round-trip mismatch".to_string()));
                            } else {
                                parsed += 1;
                            }
                        }
                    }
                }
            }
        }

        eprintln!(
            "valid round-trip: {parsed} parsed, {} failed",
            failures.len()
        );

        if !failures.is_empty() {
            let report = failures
                .iter()
                .map(|(name, err)| format!("  - {name}: {err}"))
                .collect::<Vec<_>>()
                .join("\n");
            panic!(
                "{} valid fixture(s) failed to round-trip:\n{report}",
                failures.len()
            );
        }

        assert!(parsed > 0, "expected at least one valid fixture to parse");
        Ok(())
    }

    /// Partitions the `invalid/` fixtures by how the lenient model handles them:
    ///
    /// - `SERDE_REJECTED` must fail to parse (structurally unrepresentable).
    /// - `PARSE_OK_IN_INVALID` must still parse successfully under the lenient model. These files
    ///   are kept under `invalid/` as known-bad examples, but parse behavior alone does not make
    ///   them fail.
    #[test]
    fn test_invalid_resources_partition() -> Result<(), Box<dyn Error>> {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/resources/invalid");
        let mut serde_rejected = 0usize;
        let mut parsed_ok = 0usize;
        let mut misclassified: Vec<String> = Vec::new();

        for path in resource_files(&dir)? {
            let name = fixture_name(&path);
            let content = std::fs::read_to_string(&path)?;
            let expects_reject = SERDE_REJECTED.contains(&name.as_str());
            let expects_parse = PARSE_OK_IN_INVALID.contains(&name.as_str());
            assert!(
                expects_reject ^ expects_parse,
                "{name} is not classified in SERDE_REJECTED or PARSE_OK_IN_INVALID; classify it"
            );
            match serde_json::from_str::<StateMachine>(&content) {
                Err(_) => {
                    if expects_reject {
                        serde_rejected += 1;
                    } else {
                        misclassified
                            .push(format!("{name}: expected to parse but was serde-rejected"));
                    }
                }
                Ok(_) => {
                    if expects_parse {
                        parsed_ok += 1;
                    } else {
                        misclassified.push(format!("{name}: expected serde-reject but parsed"));
                    }
                }
            }
        }

        eprintln!("invalid fixtures: {serde_rejected} serde-rejected, {parsed_ok} parsed-ok");

        if !misclassified.is_empty() {
            panic!(
                "{} invalid fixture(s) misclassified:\n{}",
                misclassified.len(),
                misclassified
                    .iter()
                    .map(|m| format!("  - {m}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }

        assert_eq!(
            serde_rejected,
            SERDE_REJECTED.len(),
            "expected {} serde-rejected fixtures, got {serde_rejected}",
            SERDE_REJECTED.len()
        );
        assert_eq!(
            parsed_ok,
            PARSE_OK_IN_INVALID.len(),
            "expected {} parse-ok fixtures, got {parsed_ok}",
            PARSE_OK_IN_INVALID.len()
        );
        Ok(())
    }
}
