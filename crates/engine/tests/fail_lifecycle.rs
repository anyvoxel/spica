//! The `Fail` state's lifecycle, pinned as a whole **entry chain** rather than as a terminal status.

mod common;

use common::{
    TypedCase, flow_name, meta, name, path, pointer, ref_to, request, run_typed_case, uid, vars,
};
use serde_json::json;
use spica_engine::{
    ActivateState, Activity, ActivityStatus, Command, CompleteState, CreateExecution, CreateFlow,
    EntryPayload, Event, Execution, ExecutionCreated, ExecutionError, ExecutionStatus, Flow,
    FlowCreated, FlowStatus, FlowVersion, FlowVersionCreated, ObjectKind, RuntimeError,
    StateTransitioned, TerminateExecution, TerminateThread, TerminationReason, Thread,
    ThreadStatus, VariablesAssigned,
};

#[rustfmt::skip]
#[tokio::test]
async fn fail_with_explicit_error_and_cause() {
    let definition = r#"{
    "StartAt": "F",
    "States": { "F": { "Type": "Fail", "Error": "E1", "Cause": "boom" } }
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
                    checksum: 2171183822638040358,
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
                    start_at: "F".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/F"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/F"),
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
                    state_path: path("/States/F"),
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
                    state_path: path("/States/F"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/F"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "E1".to_string(),
                            output: Box::new(json!({"Error": "E1", "Cause": "boom"})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
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
                    state_path: path("/States/F"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "E1".to_string(),
                            output: Box::new(json!({"Error": "E1", "Cause": "boom"})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "F".to_string(),
                        error: "E1".to_string(),
                        output: Box::new(json!({"Error": "E1", "Cause": "boom"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "F".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "E1".to_string(),
                            output: Box::new(json!({"Error": "E1", "Cause": "boom"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateExecution(TerminateExecution {
                name: name("lifecycle_execution"),
                uid: Some(uid(3)),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "F".to_string(),
                        error: "E1".to_string(),
                        output: Box::new(json!({"Error": "E1", "Cause": "boom"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "F".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "E1".to_string(),
                            output: Box::new(json!({"Error": "E1", "Cause": "boom"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ExecutionTerminating {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "E1".to_string(),
                            output: Box::new(json!({"Error": "E1", "Cause": "boom"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ExecutionTerminated {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "E1".to_string(),
                            output: Box::new(json!({"Error": "E1", "Cause": "boom"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
        ],
        status: ExecutionStatus::Terminated(TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "F".to_string(),
                error: "E1".to_string(),
                output: Box::new(json!({"Error": "E1", "Cause": "boom"})),
            }),
        }),
        acts: vec![],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn fail_without_error_or_cause_defaults() {
    let definition = r#"{
    "StartAt": "F",
    "States": { "F": { "Type": "Fail" } }
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
                    checksum: 5539617061879590914,
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
                    start_at: "F".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/F"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/F"),
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
                    state_path: path("/States/F"),
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
                    state_path: path("/States/F"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/F"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "States.Fail".to_string(),
                            output: Box::new(json!({})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
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
                    state_path: path("/States/F"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "States.Fail".to_string(),
                            output: Box::new(json!({})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "F".to_string(),
                        error: "States.Fail".to_string(),
                        output: Box::new(json!({})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "F".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "States.Fail".to_string(),
                            output: Box::new(json!({})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateExecution(TerminateExecution {
                name: name("lifecycle_execution"),
                uid: Some(uid(3)),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "F".to_string(),
                        error: "States.Fail".to_string(),
                        output: Box::new(json!({})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "F".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "States.Fail".to_string(),
                            output: Box::new(json!({})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ExecutionTerminating {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "States.Fail".to_string(),
                            output: Box::new(json!({})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ExecutionTerminated {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "States.Fail".to_string(),
                            output: Box::new(json!({})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
        ],
        status: ExecutionStatus::Terminated(TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "F".to_string(),
                error: "States.Fail".to_string(),
                output: Box::new(json!({})),
            }),
        }),
        acts: vec![],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn fail_error_and_cause_as_jsonata() {
    let definition = r#"{
    "StartAt": "Seed",
    "States": {
        "Seed": { "Type": "Pass", "Assign": { "code": "E-42", "why": "bad input" }, "Next": "F" },
        "F": { "Type": "Fail", "Error": "{% $code %}", "Cause": "{% $why %}" }
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
                    checksum: 8051900064570304716,
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
                    start_at: "Seed".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/Seed"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Seed"),
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
                    state_path: path("/States/Seed"),
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
                    state_path: path("/States/Seed"),
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
                    state_path: path("/States/Seed"),
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
                next: pointer("/States/F"),
            })),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/F"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/F"),
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
                    state_path: path("/States/F"),
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
                    state_path: path("/States/F"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/F"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "E-42".to_string(),
                            output: Box::new(json!({"Error": "E-42", "Cause": "bad input"})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateTerminated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/F"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "E-42".to_string(),
                            output: Box::new(json!({"Error": "E-42", "Cause": "bad input"})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "F".to_string(),
                        error: "E-42".to_string(),
                        output: Box::new(json!({"Error": "E-42", "Cause": "bad input"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Seed".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "E-42".to_string(),
                            output: Box::new(json!({"Error": "E-42", "Cause": "bad input"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateExecution(TerminateExecution {
                name: name("lifecycle_execution"),
                uid: Some(uid(3)),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "F".to_string(),
                        error: "E-42".to_string(),
                        output: Box::new(json!({"Error": "E-42", "Cause": "bad input"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Seed".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "E-42".to_string(),
                            output: Box::new(json!({"Error": "E-42", "Cause": "bad input"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ExecutionTerminating {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "E-42".to_string(),
                            output: Box::new(json!({"Error": "E-42", "Cause": "bad input"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ExecutionTerminated {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "F".to_string(),
                            error: "E-42".to_string(),
                            output: Box::new(json!({"Error": "E-42", "Cause": "bad input"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
        ],
        status: ExecutionStatus::Terminated(TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "F".to_string(),
                error: "E-42".to_string(),
                output: Box::new(json!({"Error": "E-42", "Cause": "bad input"})),
            }),
        }),
        acts: vec![],
    };
    run_typed_case(&case).await;
}
