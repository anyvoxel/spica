//! The `Wait` state's lifecycle, pinned as a whole **entry chain** rather than as "the timer fired".

mod common;

use std::time::Duration;

use common::{
    Act, Signal, TypedCase, VIRTUAL_EPOCH_MILLIS, epoch, flow_name, meta, meta_span, name, path,
    pointer, ref_to, request, run_typed_case, stamp, uid, vars,
};
use serde_json::json;
use spica_engine::{
    ActivateState, Activity, ActivityState, ActivityStatus, Command, CompleteExecution,
    CompleteState, CompleteThread, CreateExecution, CreateFlow, EntryPayload, Event, Execution,
    ExecutionCreated, ExecutionStatus, Flow, FlowCreated, FlowStatus, FlowVersion,
    FlowVersionCreated, ObjectKind, StateTransitioned, Thread, ThreadStatus, Timer, TimerPurpose,
    TimerStatus, VariablesAssigned, WaitActivityState,
};

#[rustfmt::skip]
#[tokio::test]
async fn wait_seconds_literal_routes_on_its_next() {
    let definition = r#"{
    "StartAt": "W",
    "States": {
        "W": { "Type": "Wait", "Seconds": 60, "Next": "P" },
        "P": { "Type": "Pass", "End": true }
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
                    checksum: 8462035664190107030,
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
                    start_at: "W".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/W"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/W"),
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
                    state_path: path("/States/W"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 60_000) })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::TimerActivated {
                timer: Timer {
                    meta: meta(ObjectKind::Timer, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::WaitResume,
                    status: TimerStatus::Active,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                },
            }),
            EntryPayload::Command(Command::TriggerTimer {
                timer: ref_to(ObjectKind::Timer, "lifecycle_execution-2", 6),
            }),
            EntryPayload::Event(Event::TimerTriggered {
                timer: Timer {
                    meta: meta_span(
                        ObjectKind::Timer,
                        uid(6),
                        "lifecycle_execution-2",
                        epoch(),
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                    )
                    .with_owner(ref_to(
                        ObjectKind::Activity,
                        "lifecycle_execution-1",
                        5,
                    )),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::WaitResume,
                    status: TimerStatus::Completed,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta_span(
                        ObjectKind::Activity,
                        uid(5),
                        "lifecycle_execution-1",
                        epoch(),
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                    )
                    .with_owner(ref_to(
                        ObjectKind::Thread,
                        "lifecycle_execution-0",
                        4,
                    )),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/W"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 60_000) })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta_span(
                        ObjectKind::Activity,
                        uid(5),
                        "lifecycle_execution-1",
                        epoch(),
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                    )
                    .with_owner(ref_to(
                        ObjectKind::Thread,
                        "lifecycle_execution-0",
                        4,
                    )),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/W"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 60_000) })),
                    retry_state: None,
                    output: Some(json!({"n": 1})),
                },
            }),
            EntryPayload::Event(Event::StateTransitioned(StateTransitioned {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                next: pointer("/States/P"),
            })),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/P"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta_span(
                        ObjectKind::Activity,
                        uid(7),
                        "lifecycle_execution-3",
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                    )
                    .with_owner(ref_to(
                        ObjectKind::Thread,
                        "lifecycle_execution-0",
                        4,
                    )),
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
                    meta: meta_span(
                        ObjectKind::Activity,
                        uid(7),
                        "lifecycle_execution-3",
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                    )
                    .with_owner(ref_to(
                        ObjectKind::Thread,
                        "lifecycle_execution-0",
                        4,
                    )),
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
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-3", 7),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta_span(
                        ObjectKind::Activity,
                        uid(7),
                        "lifecycle_execution-3",
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                    )
                    .with_owner(ref_to(
                        ObjectKind::Thread,
                        "lifecycle_execution-0",
                        4,
                    )),
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
                    meta: meta_span(
                        ObjectKind::Activity,
                        uid(7),
                        "lifecycle_execution-3",
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                    )
                    .with_owner(ref_to(
                        ObjectKind::Thread,
                        "lifecycle_execution-0",
                        4,
                    )),
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
                    meta: meta_span(
                        ObjectKind::Thread,
                        uid(4),
                        "lifecycle_execution-0",
                        epoch(),
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                    )
                    .with_owner(ref_to(
                        ObjectKind::Execution,
                        "lifecycle_execution",
                        3,
                    )),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "W".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"n": 1})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta_span(
                        ObjectKind::Thread,
                        uid(4),
                        "lifecycle_execution-0",
                        epoch(),
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                    )
                    .with_owner(ref_to(
                        ObjectKind::Execution,
                        "lifecycle_execution",
                        3,
                    )),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "W".to_string(),
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
                    meta: meta_span(
                        ObjectKind::Execution,
                        uid(3),
                        "lifecycle_execution",
                        epoch(),
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                    ),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"n": 1})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta_span(
                        ObjectKind::Execution,
                        uid(3),
                        "lifecycle_execution",
                        epoch(),
                        stamp(VIRTUAL_EPOCH_MILLIS + 60_000),
                    ),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"n": 1})),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![Act::Advance(Signal::TimerArmed, Duration::from_secs(60))],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn wait_until_a_literal_timestamp() {
    let definition = r#"{
    "StartAt": "W",
    "States": {
        "W": { "Type": "Wait", "Timestamp": "2023-11-14T22:14:50Z", "End": true }
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
                    checksum: 10749953455697590229,
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
                    start_at: "W".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/W"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/W"),
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
                    state_path: path("/States/W"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 90_000) })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::TimerActivated {
                timer: Timer {
                    meta: meta(ObjectKind::Timer, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::WaitResume,
                    status: TimerStatus::Active,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 90_000),
                },
            }),
            EntryPayload::Command(Command::TriggerTimer {
                timer: ref_to(ObjectKind::Timer, "lifecycle_execution-2", 6),
            }),
            EntryPayload::Event(Event::TimerTriggered {
                timer: Timer {
                    meta: meta_span(ObjectKind::Timer, uid(6), "lifecycle_execution-2", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 90_000))
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::WaitResume,
                    status: TimerStatus::Completed,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 90_000),
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(5), "lifecycle_execution-1", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 90_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/W"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 90_000) })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(5), "lifecycle_execution-1", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 90_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/W"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 90_000) })),
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
                    meta: meta_span(ObjectKind::Thread, uid(4), "lifecycle_execution-0", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 90_000))
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "W".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"n": 1})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(4), "lifecycle_execution-0", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 90_000))
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "W".to_string(),
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
                    meta: meta_span(ObjectKind::Execution, uid(3), "lifecycle_execution", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 90_000)),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"n": 1})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta_span(ObjectKind::Execution, uid(3), "lifecycle_execution", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 90_000)),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"n": 1})),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![Act::Advance(Signal::TimerArmed, Duration::from_secs(90))],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn wait_seconds_from_an_expression_with_assign_and_output() {
    let definition = r#"{
    "StartAt": "Seed",
    "States": {
        "Seed": { "Type": "Pass", "Assign": { "delay": 45 }, "Next": "W" },
        "W": {
            "Type": "Wait",
            "Seconds": "{% $delay %}",
            "Assign": { "waited": true },
            "Output": { "echo": "{% $states.input.n %}", "waited": "{% $waited %}" },
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
                    checksum: 6453348102086762282,
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
                variables: vars(&[("delay", json!(45))]),
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
                next: pointer("/States/W"),
            })),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/W"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/W"),
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
                    state_path: path("/States/W"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 45_000) })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::TimerActivated {
                timer: Timer {
                    meta: meta(ObjectKind::Timer, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::WaitResume,
                    status: TimerStatus::Active,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 45_000),
                },
            }),
            EntryPayload::Command(Command::TriggerTimer {
                timer: ref_to(ObjectKind::Timer, "lifecycle_execution-3", 7),
            }),
            EntryPayload::Event(Event::TimerTriggered {
                timer: Timer {
                    meta: meta_span(ObjectKind::Timer, uid(7), "lifecycle_execution-3", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 45_000))
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::WaitResume,
                    status: TimerStatus::Completed,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 45_000),
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-2", 6),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(6), "lifecycle_execution-2", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 45_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/W"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 45_000) })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::VariablesAssigned(VariablesAssigned {
                scope: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                variables: vars(&[("delay", json!(45)), ("waited", json!(true))]),
            })),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(6), "lifecycle_execution-2", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 45_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/W"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 45_000) })),
                    retry_state: None,
                    output: Some(json!({"echo": 1.0, "waited": true})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!({"echo": 1.0, "waited": true}),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(4), "lifecycle_execution-0", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 45_000))
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Seed".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"echo": 1.0, "waited": true})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(4), "lifecycle_execution-0", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 45_000))
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "Seed".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"echo": 1.0, "waited": true})),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!({"echo": 1.0, "waited": true}),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta_span(ObjectKind::Execution, uid(3), "lifecycle_execution", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 45_000)),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"echo": 1.0, "waited": true})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta_span(ObjectKind::Execution, uid(3), "lifecycle_execution", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 45_000)),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"echo": 1.0, "waited": true})),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![Act::Advance(Signal::TimerArmed, Duration::from_secs(45))],
    };
    run_typed_case(&case).await;
}
