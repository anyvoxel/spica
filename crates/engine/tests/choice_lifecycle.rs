//! The `Choice` state's lifecycle, pinned as a whole **entry chain** rather than as "which branch ran".
//!
//! Every case declares more than one rule whose condition is decided by the same input, so *which
//! rule* matched is evidence the chain holds rather than merely what the input was.

mod common;

use common::{
    TypedCase, flow_name, meta, name, path, pointer, ref_to, request, run_typed_case, uid, vars,
};
use serde_json::json;
use spica_engine::{
    ActivateState, Activity, ActivityStatus, Command, CompleteExecution, CompleteState,
    CompleteThread, CreateExecution, CreateFlow, EntryPayload, Event, Execution, ExecutionCreated,
    ExecutionError, ExecutionStatus, Flow, FlowCreated, FlowStatus, FlowVersion,
    FlowVersionCreated, ObjectKind, RuntimeError, StateTransitioned, TerminateExecution,
    TerminateState, TerminateThread, TerminationReason, Thread, ThreadStatus, VariablesAssigned,
};

#[rustfmt::skip]
#[tokio::test]
async fn choice_first_matching_rule_wins() {
    let definition = r#"{
    "StartAt": "Picker",
    "States": {
        "Picker": {
            "Type": "Choice",
            "Choices": [
                { "Condition": false, "Next": "Never" },
                { "Condition": "{% $states.input.n > 5 %}", "Next": "Big" },
                { "Condition": "{% $states.input.n > 0 %}", "Next": "Small" }
            ],
            "Default": "Other"
        },
        "Never": { "Type": "Succeed", "Output": { "branch": "never" } },
        "Big": { "Type": "Succeed", "Output": { "branch": "big" } },
        "Small": { "Type": "Succeed", "Output": { "branch": "small" } },
        "Other": { "Type": "Succeed", "Output": { "branch": "other" } }
    }
}"#;
    let case = TypedCase {
        definition,
        input: r#"{"n":10}"#,
        chain: vec![
            EntryPayload::Command(Command::CreateFlow(CreateFlow {
                request_id: request(0),
                name: flow_name("lifecycle_flow"),
                definition: definition.to_string(),
            })),
            EntryPayload::Event(Event::FlowCreated(FlowCreated {
                request_id: request(0),
                flow: Flow {
                    meta: meta(ObjectKind::Flow, uid(1), "lifecycle_flow"),
                    status: FlowStatus::Active,
                    latest_version: 1,
                },
            })),
            EntryPayload::Event(Event::FlowVersionCreated(FlowVersionCreated {
                request_id: request(0),
                flow_version: FlowVersion {
                    meta: meta(ObjectKind::FlowVersion, uid(2), "lifecycle_flow-1")
                        .with_owner(ref_to(ObjectKind::Flow, "lifecycle_flow", 1)),
                    version: 1,
                    definition: definition.to_string(),
                    checksum: 5394964661927106503,
                },
            })),
            EntryPayload::Command(Command::CreateExecution(CreateExecution {
                request_id: request(1),
                name: name("lifecycle_execution"),
                flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                input: json!({"n": 10}),
            })),
            EntryPayload::Event(Event::ExecutionCreated(ExecutionCreated {
                request_id: request(1),
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Running,
                    input: json!({"n": 10}),
                    output: None,
                },
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 10}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/Picker"),
                input: json!({"n": 10}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 10}),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 10}),
                    input: Some(json!({"n": 10})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                output: json!({"n": 10}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 10}),
                    input: Some(json!({"n": 10})),
                    raw_output: Some(json!({"n": 10})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 10}),
                    input: Some(json!({"n": 10})),
                    raw_output: Some(json!({"n": 10})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"n": 10})),
                },
            }),
            EntryPayload::Event(Event::StateTransitioned(StateTransitioned {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                next: pointer("/States/Big"),
            })),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/Big"),
                input: json!({"n": 10}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Big"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 10}),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Big"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 10}),
                    input: Some(json!({"n": 10})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-2", 6),
                output: json!({"n": 10}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Big"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 10}),
                    input: Some(json!({"n": 10})),
                    raw_output: Some(json!({"n": 10})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Big"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 10}),
                    input: Some(json!({"n": 10})),
                    raw_output: Some(json!({"n": 10})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"branch": "big"})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!({"branch": "big"}),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 10}),
                    output: Some(json!({"branch": "big"})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 10}),
                    output: Some(json!({"branch": "big"})),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!({"branch": "big"}),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 10}),
                    output: Some(json!({"branch": "big"})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 10}),
                    output: Some(json!({"branch": "big"})),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn choice_rule_assign_and_output_override_the_state_level() {
    let definition = r#"{
    "StartAt": "Picker",
    "States": {
        "Picker": {
            "Type": "Choice",
            "Choices": [
                {
                    "Condition": "{% $states.input.v >= 20 %}",
                    "Assign": { "range": "rule" },
                    "Output": { "chosen": "rule", "range": "{% $range %}" },
                    "Next": "Done"
                }
            ],
            "Default": "Done",
            "Assign": { "range": "state" },
            "Output": { "chosen": "state" }
        },
        "Done": { "Type": "Succeed" }
    }
}"#;
    let case = TypedCase {
        definition,
        input: r#"{"v":25}"#,
        chain: vec![
            EntryPayload::Command(Command::CreateFlow(CreateFlow {
                request_id: request(0),
                name: flow_name("lifecycle_flow"),
                definition: definition.to_string(),
            })),
            EntryPayload::Event(Event::FlowCreated(FlowCreated {
                request_id: request(0),
                flow: Flow {
                    meta: meta(ObjectKind::Flow, uid(1), "lifecycle_flow"),
                    status: FlowStatus::Active,
                    latest_version: 1,
                },
            })),
            EntryPayload::Event(Event::FlowVersionCreated(FlowVersionCreated {
                request_id: request(0),
                flow_version: FlowVersion {
                    meta: meta(ObjectKind::FlowVersion, uid(2), "lifecycle_flow-1")
                        .with_owner(ref_to(ObjectKind::Flow, "lifecycle_flow", 1)),
                    version: 1,
                    definition: definition.to_string(),
                    checksum: 10245798505564325824,
                },
            })),
            EntryPayload::Command(Command::CreateExecution(CreateExecution {
                request_id: request(1),
                name: name("lifecycle_execution"),
                flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                input: json!({"v": 25}),
            })),
            EntryPayload::Event(Event::ExecutionCreated(ExecutionCreated {
                request_id: request(1),
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Running,
                    input: json!({"v": 25}),
                    output: None,
                },
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"v": 25}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/Picker"),
                input: json!({"v": 25}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"v": 25}),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"v": 25}),
                    input: Some(json!({"v": 25})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                output: json!({"v": 25}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"v": 25}),
                    input: Some(json!({"v": 25})),
                    raw_output: Some(json!({"v": 25})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::VariablesAssigned(VariablesAssigned {
                scope: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                variables: vars(&[("range", json!("rule"))]),
            })),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"v": 25}),
                    input: Some(json!({"v": 25})),
                    raw_output: Some(json!({"v": 25})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"chosen": "rule", "range": "rule"})),
                },
            }),
            EntryPayload::Event(Event::StateTransitioned(StateTransitioned {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                next: pointer("/States/Done"),
            })),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/Done"),
                input: json!({"chosen": "rule", "range": "rule"}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Done"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"chosen": "rule", "range": "rule"}),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Done"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"chosen": "rule", "range": "rule"}),
                    input: Some(json!({"chosen": "rule", "range": "rule"})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-2", 6),
                output: json!({"chosen": "rule", "range": "rule"}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Done"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"chosen": "rule", "range": "rule"}),
                    input: Some(json!({"chosen": "rule", "range": "rule"})),
                    raw_output: Some(json!({"chosen": "rule", "range": "rule"})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Done"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"chosen": "rule", "range": "rule"}),
                    input: Some(json!({"chosen": "rule", "range": "rule"})),
                    raw_output: Some(json!({"chosen": "rule", "range": "rule"})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"chosen": "rule", "range": "rule"})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!({"chosen": "rule", "range": "rule"}),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"v": 25}),
                    output: Some(json!({"chosen": "rule", "range": "rule"})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"v": 25}),
                    output: Some(json!({"chosen": "rule", "range": "rule"})),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!({"chosen": "rule", "range": "rule"}),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"v": 25}),
                    output: Some(json!({"chosen": "rule", "range": "rule"})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"v": 25}),
                    output: Some(json!({"chosen": "rule", "range": "rule"})),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn choice_without_a_match_takes_the_default() {
    let definition = r#"{
    "StartAt": "Picker",
    "States": {
        "Picker": {
            "Type": "Choice",
            "Choices": [ { "Condition": "{% $states.input.v > 100 %}", "Next": "Big" } ],
            "Default": "Fallback",
            "Assign": { "range": "state" },
            "Output": { "chosen": "default", "range": "{% $range %}" }
        },
        "Big": { "Type": "Succeed" },
        "Fallback": { "Type": "Succeed" }
    }
}"#;
    let case = TypedCase {
        definition,
        input: r#"{"v":1}"#,
        chain: vec![
            EntryPayload::Command(Command::CreateFlow(CreateFlow {
                request_id: request(0),
                name: flow_name("lifecycle_flow"),
                definition: definition.to_string(),
            })),
            EntryPayload::Event(Event::FlowCreated(FlowCreated {
                request_id: request(0),
                flow: Flow {
                    meta: meta(ObjectKind::Flow, uid(1), "lifecycle_flow"),
                    status: FlowStatus::Active,
                    latest_version: 1,
                },
            })),
            EntryPayload::Event(Event::FlowVersionCreated(FlowVersionCreated {
                request_id: request(0),
                flow_version: FlowVersion {
                    meta: meta(ObjectKind::FlowVersion, uid(2), "lifecycle_flow-1")
                        .with_owner(ref_to(ObjectKind::Flow, "lifecycle_flow", 1)),
                    version: 1,
                    definition: definition.to_string(),
                    checksum: 14811692099016931934,
                },
            })),
            EntryPayload::Command(Command::CreateExecution(CreateExecution {
                request_id: request(1),
                name: name("lifecycle_execution"),
                flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                input: json!({"v": 1}),
            })),
            EntryPayload::Event(Event::ExecutionCreated(ExecutionCreated {
                request_id: request(1),
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Running,
                    input: json!({"v": 1}),
                    output: None,
                },
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"v": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/Picker"),
                input: json!({"v": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"v": 1}),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"v": 1}),
                    input: Some(json!({"v": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                output: json!({"v": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"v": 1}),
                    input: Some(json!({"v": 1})),
                    raw_output: Some(json!({"v": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::VariablesAssigned(VariablesAssigned {
                scope: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                variables: vars(&[("range", json!("state"))]),
            })),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"v": 1}),
                    input: Some(json!({"v": 1})),
                    raw_output: Some(json!({"v": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"chosen": "default", "range": "state"})),
                },
            }),
            EntryPayload::Event(Event::StateTransitioned(StateTransitioned {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                next: pointer("/States/Fallback"),
            })),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/Fallback"),
                input: json!({"chosen": "default", "range": "state"}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Fallback"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"chosen": "default", "range": "state"}),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Fallback"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"chosen": "default", "range": "state"}),
                    input: Some(json!({"chosen": "default", "range": "state"})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-2", 6),
                output: json!({"chosen": "default", "range": "state"}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Fallback"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"chosen": "default", "range": "state"}),
                    input: Some(json!({"chosen": "default", "range": "state"})),
                    raw_output: Some(json!({"chosen": "default", "range": "state"})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Fallback"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"chosen": "default", "range": "state"}),
                    input: Some(json!({"chosen": "default", "range": "state"})),
                    raw_output: Some(json!({"chosen": "default", "range": "state"})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"chosen": "default", "range": "state"})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!({"chosen": "default", "range": "state"}),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"v": 1}),
                    output: Some(json!({"chosen": "default", "range": "state"})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"v": 1}),
                    output: Some(json!({"chosen": "default", "range": "state"})),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!({"chosen": "default", "range": "state"}),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"v": 1}),
                    output: Some(json!({"chosen": "default", "range": "state"})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"v": 1}),
                    output: Some(json!({"chosen": "default", "range": "state"})),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn choice_without_a_match_or_default_fails() {
    let definition = r#"{
    "StartAt": "Picker",
    "States": {
        "Picker": {
            "Type": "Choice",
            "Choices": [ { "Condition": "{% $states.input.v > 100 %}", "Next": "Big" } ]
        },
        "Big": { "Type": "Succeed" }
    }
}"#;
    let case = TypedCase {
        definition,
        input: r#"{"v":1}"#,
        chain: vec![
            EntryPayload::Command(Command::CreateFlow(CreateFlow {
                request_id: request(0),
                name: flow_name("lifecycle_flow"),
                definition: definition.to_string(),
            })),
            EntryPayload::Event(Event::FlowCreated(FlowCreated {
                request_id: request(0),
                flow: Flow {
                    meta: meta(ObjectKind::Flow, uid(1), "lifecycle_flow"),
                    status: FlowStatus::Active,
                    latest_version: 1,
                },
            })),
            EntryPayload::Event(Event::FlowVersionCreated(FlowVersionCreated {
                request_id: request(0),
                flow_version: FlowVersion {
                    meta: meta(ObjectKind::FlowVersion, uid(2), "lifecycle_flow-1")
                        .with_owner(ref_to(ObjectKind::Flow, "lifecycle_flow", 1)),
                    version: 1,
                    definition: definition.to_string(),
                    checksum: 1956968342152518958,
                },
            })),
            EntryPayload::Command(Command::CreateExecution(CreateExecution {
                request_id: request(1),
                name: name("lifecycle_execution"),
                flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                input: json!({"v": 1}),
            })),
            EntryPayload::Event(Event::ExecutionCreated(ExecutionCreated {
                request_id: request(1),
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Running,
                    input: json!({"v": 1}),
                    output: None,
                },
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"v": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/Picker"),
                input: json!({"v": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"v": 1}),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"v": 1}),
                    input: Some(json!({"v": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                output: json!({"v": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"v": 1}),
                    input: Some(json!({"v": 1})),
                    raw_output: Some(json!({"v": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::NoChoiceMatched {
                        state: "Picker".to_string(),
                    }),
                },
            })),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::NoChoiceMatched {
                        state: "Picker".to_string(),
                    }),
                },
            })),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::NoChoiceMatched {
                            state: "Picker".to_string(),
                        }),
                    }),
                    raw_input: json!({"v": 1}),
                    input: Some(json!({"v": 1})),
                    raw_output: Some(json!({"v": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateTerminated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::NoChoiceMatched {
                            state: "Picker".to_string(),
                        }),
                    }),
                    raw_input: json!({"v": 1}),
                    input: Some(json!({"v": 1})),
                    raw_output: Some(json!({"v": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::NoChoiceMatched {
                            state: "Picker".to_string(),
                        }),
                    }),
                    input: json!({"v": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateExecution(TerminateExecution {
                name: name("lifecycle_execution"),
                uid: Some(uid(3)),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::NoChoiceMatched {
                        state: "Picker".to_string(),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::NoChoiceMatched {
                            state: "Picker".to_string(),
                        }),
                    }),
                    input: json!({"v": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ExecutionTerminating {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::NoChoiceMatched {
                            state: "Picker".to_string(),
                        }),
                    }),
                    input: json!({"v": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ExecutionTerminated {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::NoChoiceMatched {
                            state: "Picker".to_string(),
                        }),
                    }),
                    input: json!({"v": 1}),
                    output: None,
                },
            }),
        ],
        status: ExecutionStatus::Terminated(TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::NoChoiceMatched {
                state: "Picker".to_string(),
            }),
        }),
        acts: vec![],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn choice_condition_that_is_not_a_boolean_is_a_runtime_error() {
    let definition = r#"{
    "StartAt": "Picker",
    "States": {
        "Picker": {
            "Type": "Choice",
            "Choices": [ { "Condition": "{% $states.input.v + 1 %}", "Next": "Big" } ],
            "Default": "Other"
        },
        "Big": { "Type": "Succeed" },
        "Other": { "Type": "Succeed" }
    }
}"#;
    let case = TypedCase {
        definition,
        input: r#"{"v":1}"#,
        chain: vec![
            EntryPayload::Command(Command::CreateFlow(CreateFlow {
                request_id: request(0),
                name: flow_name("lifecycle_flow"),
                definition: definition.to_string(),
            })),
            EntryPayload::Event(Event::FlowCreated(FlowCreated {
                request_id: request(0),
                flow: Flow {
                    meta: meta(ObjectKind::Flow, uid(1), "lifecycle_flow"),
                    status: FlowStatus::Active,
                    latest_version: 1,
                },
            })),
            EntryPayload::Event(Event::FlowVersionCreated(FlowVersionCreated {
                request_id: request(0),
                flow_version: FlowVersion {
                    meta: meta(ObjectKind::FlowVersion, uid(2), "lifecycle_flow-1")
                        .with_owner(ref_to(ObjectKind::Flow, "lifecycle_flow", 1)),
                    version: 1,
                    definition: definition.to_string(),
                    checksum: 11158558825877593162,
                },
            })),
            EntryPayload::Command(Command::CreateExecution(CreateExecution {
                request_id: request(1),
                name: name("lifecycle_execution"),
                flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                input: json!({"v": 1}),
            })),
            EntryPayload::Event(Event::ExecutionCreated(ExecutionCreated {
                request_id: request(1),
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Running,
                    input: json!({"v": 1}),
                    output: None,
                },
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"v": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/Picker"),
                input: json!({"v": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"v": 1}),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"v": 1}),
                    input: Some(json!({"v": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                output: json!({"v": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"v": 1}),
                    input: Some(json!({"v": 1})),
                    raw_output: Some(json!({"v": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::Jsonata {
                        field: "{% $states.input.v + 1 %}".to_string(),
                        message: "Condition must evaluate to a boolean".to_string(),
                    }),
                },
            })),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::Jsonata {
                        field: "{% $states.input.v + 1 %}".to_string(),
                        message: "Condition must evaluate to a boolean".to_string(),
                    }),
                },
            })),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::Jsonata {
                            field: "{% $states.input.v + 1 %}".to_string(),
                            message: "Condition must evaluate to a boolean".to_string(),
                        }),
                    }),
                    raw_input: json!({"v": 1}),
                    input: Some(json!({"v": 1})),
                    raw_output: Some(json!({"v": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateTerminated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Picker"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::Jsonata {
                            field: "{% $states.input.v + 1 %}".to_string(),
                            message: "Condition must evaluate to a boolean".to_string(),
                        }),
                    }),
                    raw_input: json!({"v": 1}),
                    input: Some(json!({"v": 1})),
                    raw_output: Some(json!({"v": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::Jsonata {
                            field: "{% $states.input.v + 1 %}".to_string(),
                            message: "Condition must evaluate to a boolean".to_string(),
                        }),
                    }),
                    input: json!({"v": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateExecution(TerminateExecution {
                name: name("lifecycle_execution"),
                uid: Some(uid(3)),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::Jsonata {
                        field: "{% $states.input.v + 1 %}".to_string(),
                        message: "Condition must evaluate to a boolean".to_string(),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Picker".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::Jsonata {
                            field: "{% $states.input.v + 1 %}".to_string(),
                            message: "Condition must evaluate to a boolean".to_string(),
                        }),
                    }),
                    input: json!({"v": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ExecutionTerminating {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::Jsonata {
                            field: "{% $states.input.v + 1 %}".to_string(),
                            message: "Condition must evaluate to a boolean".to_string(),
                        }),
                    }),
                    input: json!({"v": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ExecutionTerminated {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::Jsonata {
                            field: "{% $states.input.v + 1 %}".to_string(),
                            message: "Condition must evaluate to a boolean".to_string(),
                        }),
                    }),
                    input: json!({"v": 1}),
                    output: None,
                },
            }),
        ],
        status: ExecutionStatus::Terminated(TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::Jsonata {
                field: "{% $states.input.v + 1 %}".to_string(),
                message: "Condition must evaluate to a boolean".to_string(),
            }),
        }),
        acts: vec![],
    };
    run_typed_case(&case).await;
}
