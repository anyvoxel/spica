//! The `Pass` state's lifecycle, pinned as a whole **entry chain** rather than as a terminal status.

mod common;

use common::{
    TypedCase, flow_name, meta, name, path, pointer, ref_to, request, run_typed_case, uid, vars,
};
use serde_json::json;
use spica_engine::{
    ActivateState, Activity, ActivityStatus, Command, CompleteExecution, CompleteState,
    CompleteThread, CreateExecution, CreateFlow, EntryPayload, Event, Execution, ExecutionCreated,
    ExecutionStatus, Flow, FlowCreated, FlowStatus, FlowVersion, FlowVersionCreated, ObjectKind,
    StateTransitioned, Thread, ThreadStatus, VariablesAssigned,
};

#[rustfmt::skip]
#[tokio::test]
async fn pass_hops_to_a_sibling_then_ends() {
    let definition = r#"{
    "StartAt": "P1",
    "States": {
        "P1": { "Type": "Pass", "Next": "P2" },
        "P2": { "Type": "Pass", "End": true }
    }
}"#;
    let case = TypedCase {
        definition,
        input: r#"{"n":1}"#,
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
                    checksum: 4486263596873975519,
                },
            })),
            EntryPayload::Command(Command::CreateExecution(CreateExecution {
                request_id: request(1),
                name: name("lifecycle_execution"),
                flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ExecutionCreated(ExecutionCreated {
                request_id: request(1),
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P1".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/P1"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P1"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
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
                    state_path: path("/States/P1"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P1"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
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
                    state_path: path("/States/P1"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"n": 1})),
                },
            }),
            EntryPayload::Event(Event::StateTransitioned(StateTransitioned {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                next: pointer("/States/P2"),
            })),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/P2"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P2"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
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
                    state_path: path("/States/P2"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-2", 6),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P2"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
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
                    state_path: path("/States/P2"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"n": 1})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P1".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"n": 1})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P1".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"n": 1})),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"n": 1})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"n": 1})),
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
async fn pass_without_output_passes_its_input_through() {
    let definition = r#"{
    "StartAt": "P",
    "States": { "P": { "Type": "Pass", "End": true } }
}"#;
    let case = TypedCase {
        definition,
        input: r#"{"n":1}"#,
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
                    checksum: 3446009112827175363,
                },
            })),
            EntryPayload::Command(Command::CreateExecution(CreateExecution {
                request_id: request(1),
                name: name("lifecycle_execution"),
                flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ExecutionCreated(ExecutionCreated {
                request_id: request(1),
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/P"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
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
                    state_path: path("/States/P"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
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
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"n": 1})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"n": 1})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"n": 1})),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"n": 1})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"n": 1})),
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
async fn pass_assign_then_jsonata_output() {
    let definition = r#"{
    "StartAt": "P",
    "States": {
        "P": {
            "Type": "Pass",
            "Assign": { "code": "E-42", "why": "bad input" },
            "Output": { "echo": "{% $states.input.n %}", "code": "{% $code %}" },
            "End": true
        }
    }
}"#;
    let case = TypedCase {
        definition,
        input: r#"{"n":1}"#,
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
                    checksum: 8155033810427974086,
                },
            })),
            EntryPayload::Command(Command::CreateExecution(CreateExecution {
                request_id: request(1),
                name: name("lifecycle_execution"),
                flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ExecutionCreated(ExecutionCreated {
                request_id: request(1),
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/P"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
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
                    state_path: path("/States/P"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::VariablesAssigned(VariablesAssigned {
                scope: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                variables: vars(&[("code", json!("E-42")), ("why", json!("bad input"))]),
            })),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"echo": 1.0, "code": "E-42"})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!({"echo": 1.0, "code": "E-42"}),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"echo": 1.0, "code": "E-42"})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"echo": 1.0, "code": "E-42"})),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!({"echo": 1.0, "code": "E-42"}),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"echo": 1.0, "code": "E-42"})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"echo": 1.0, "code": "E-42"})),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![],
    };
    run_typed_case(&case).await;
}
