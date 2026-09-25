//! The `Parallel` state's lifecycle, pinned as a whole **entry chain** rather than as "the output

mod common;

use std::time::Duration;

use common::{
    Act, Signal, TypedCase, VIRTUAL_EPOCH_MILLIS, epoch, flow_name, indexed_refs, meta, meta_span,
    name, path, pointer, ref_to, request, run_typed_case, stamp, uid,
};
use serde_json::json;
use spica_engine::{
    ActivateState, Activity, ActivityState, ActivityStatus, Command, CompleteExecution,
    CompleteState, CompleteThread, CreateExecution, CreateFlow, EntryPayload, Event, Execution,
    ExecutionCreated, ExecutionError, ExecutionStatus, Flow, FlowCreated, FlowStatus, FlowVersion,
    FlowVersionCreated, ObjectKind, ParallelActivityState, RuntimeError, SpawnThread,
    StateTransitioned, TerminateExecution, TerminateState, TerminateThread, TerminationReason,
    Thread, ThreadStatus, Timer, TimerPurpose, TimerStatus, WaitActivityState,
};

#[rustfmt::skip]
#[tokio::test]
async fn parallel_over_branches_aggregates_in_branch_order() {
    let definition = r#"{
    "StartAt": "P",
    "States": {
        "P": {
            "Type": "Parallel",
            "End": true,
            "Branches": [
                { "StartAt": "A", "States": { "A": { "Type": "Pass", "Output": "b0", "End": true } } },
                { "StartAt": "B", "States": { "B": { "Type": "Pass", "Output": "b1", "End": true } } }
            ]
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
                    checksum: 4278941812908819375,
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/0/States")),
                index: 0,
                start_at: "A".to_string(),
                input: json!({"n": 1}),
            })),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/1/States")),
                index: 1,
                start_at: "B".to_string(),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "A".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                state_path: path("/States/P/Branches/0/States/A"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "B".to_string(),
                    index: 1,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                state_path: path("/States/P/Branches/1/States/B"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
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
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
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
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-4", 8),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
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
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
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
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
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
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!("b0")),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                output: json!("b0"),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
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
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!("b1")),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                output: json!("b1"),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "A".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!("b0")),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "A".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!("b0")),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "B".to_string(),
                    index: 1,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!("b1")),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "B".to_string(),
                    index: 1,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!("b1")),
                },
            }),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[
                            (0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                            (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                        ]),
                    })),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[
                            (0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                            (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                        ]),
                    })),
                    retry_state: None,
                    output: Some(json!(["b0", "b1"])),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!(["b0", "b1"]),
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
                    output: Some(json!(["b0", "b1"])),
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
                    output: Some(json!(["b0", "b1"])),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!(["b0", "b1"]),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!(["b0", "b1"])),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!(["b0", "b1"])),
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
async fn parallel_one_branch_settles_later_than_the_other() {
    let definition = r#"{
    "StartAt": "P",
    "States": {
        "P": {
            "Type": "Parallel",
            "End": true,
            "Branches": [
                { "StartAt": "Fast", "States": { "Fast": { "Type": "Pass", "Output": "fast", "End": true } } },
                { "StartAt": "Slow", "States": { "Slow": { "Type": "Wait", "Seconds": 300, "Output": "slow", "End": true } } }
            ]
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
                    checksum: 17919816192777907792,
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/0/States")),
                index: 0,
                start_at: "Fast".to_string(),
                input: json!({"n": 1}),
            })),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/1/States")),
                index: 1,
                start_at: "Slow".to_string(),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "Fast".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                state_path: path("/States/P/Branches/0/States/Fast"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "Slow".to_string(),
                    index: 1,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                state_path: path("/States/P/Branches/1/States/Slow"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Fast"),
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
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Fast"),
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
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-4", 8),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/Slow"),
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
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/Slow"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 300_000) })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::TimerActivated {
                timer: Timer {
                    meta: meta(ObjectKind::Timer, uid(10), "lifecycle_execution-6")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::WaitResume,
                    status: TimerStatus::Active,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 300_000),
                },
            }),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Fast"),
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
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Fast"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!("fast")),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                output: json!("fast"),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "Fast".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!("fast")),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "Fast".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!("fast")),
                },
            }),
            EntryPayload::Command(Command::TriggerTimer {
                timer: ref_to(ObjectKind::Timer, "lifecycle_execution-6", 10),
            }),
            EntryPayload::Event(Event::TimerTriggered {
                timer: Timer {
                    meta: meta_span(ObjectKind::Timer, uid(10), "lifecycle_execution-6", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 300_000))
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::WaitResume,
                    status: TimerStatus::Completed,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 300_000),
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(9), "lifecycle_execution-5", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 300_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/Slow"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 300_000) })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(9), "lifecycle_execution-5", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 300_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/Slow"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 300_000) })),
                    retry_state: None,
                    output: Some(json!("slow")),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                output: json!("slow"),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(7), "lifecycle_execution-3", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 300_000))
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "Slow".to_string(),
                    index: 1,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!("slow")),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(7), "lifecycle_execution-3", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 300_000))
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "Slow".to_string(),
                    index: 1,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!("slow")),
                },
            }),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(5), "lifecycle_execution-1", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 300_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(5), "lifecycle_execution-1", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 300_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
                    retry_state: None,
                    output: Some(json!(["fast", "slow"])),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!(["fast", "slow"]),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(4), "lifecycle_execution-0", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 300_000))
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!(["fast", "slow"])),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(4), "lifecycle_execution-0", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 300_000))
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!(["fast", "slow"])),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!(["fast", "slow"]),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta_span(ObjectKind::Execution, uid(3), "lifecycle_execution", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 300_000)),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!(["fast", "slow"])),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta_span(ObjectKind::Execution, uid(3), "lifecycle_execution", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 300_000)),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!(["fast", "slow"])),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![Act::Advance(Signal::TimerArmed, Duration::from_secs(300))],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn parallel_arguments_project_the_input_each_branch_receives() {
    let definition = r#"{
    "StartAt": "P",
    "States": {
        "P": {
            "Type": "Parallel",
            "End": true,
            "Arguments": { "v": "{% $states.input.n + 1 %}" },
            "Branches": [
                { "StartAt": "A", "States": { "A": { "Type": "Pass", "End": true } } },
                { "StartAt": "B", "States": { "B": { "Type": "Pass", "End": true } } }
            ]
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
                    checksum: 16649147650872322773,
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
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
                    input: Some(json!({"v": 2.0})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/0/States")),
                index: 0,
                start_at: "A".to_string(),
                input: json!({"v": 2.0}),
            })),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/1/States")),
                index: 1,
                start_at: "B".to_string(),
                input: json!({"v": 2.0}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "A".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"v": 2.0}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                state_path: path("/States/P/Branches/0/States/A"),
                input: json!({"v": 2.0}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "B".to_string(),
                    index: 1,
                    status: ThreadStatus::Running,
                    input: json!({"v": 2.0}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                state_path: path("/States/P/Branches/1/States/B"),
                input: json!({"v": 2.0}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"v": 2.0}),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"v": 2.0}),
                    input: Some(json!({"v": 2.0})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-4", 8),
                output: json!({"v": 2.0}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"v": 2.0}),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"v": 2.0}),
                    input: Some(json!({"v": 2.0})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9),
                output: json!({"v": 2.0}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"v": 2.0}),
                    input: Some(json!({"v": 2.0})),
                    raw_output: Some(json!({"v": 2.0})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"v": 2.0}),
                    input: Some(json!({"v": 2.0})),
                    raw_output: Some(json!({"v": 2.0})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"v": 2.0})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                output: json!({"v": 2.0}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"v": 2.0}),
                    input: Some(json!({"v": 2.0})),
                    raw_output: Some(json!({"v": 2.0})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"v": 2.0}),
                    input: Some(json!({"v": 2.0})),
                    raw_output: Some(json!({"v": 2.0})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"v": 2.0})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                output: json!({"v": 2.0}),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "A".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"v": 2.0}),
                    output: Some(json!({"v": 2.0})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "A".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"v": 2.0}),
                    output: Some(json!({"v": 2.0})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "B".to_string(),
                    index: 1,
                    status: ThreadStatus::Completing,
                    input: json!({"v": 2.0}),
                    output: Some(json!({"v": 2.0})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "B".to_string(),
                    index: 1,
                    status: ThreadStatus::Completed,
                    input: json!({"v": 2.0}),
                    output: Some(json!({"v": 2.0})),
                },
            }),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"v": 2.0})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
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
                    input: Some(json!({"v": 2.0})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
                    retry_state: None,
                    output: Some(json!([{"v": 2.0}, {"v": 2.0}])),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!([{"v": 2.0}, {"v": 2.0}]),
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
                    output: Some(json!([{"v": 2.0}, {"v": 2.0}])),
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
                    output: Some(json!([{"v": 2.0}, {"v": 2.0}])),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!([{"v": 2.0}, {"v": 2.0}]),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!([{"v": 2.0}, {"v": 2.0}])),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!([{"v": 2.0}, {"v": 2.0}])),
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
async fn parallel_output_projects_the_branches_result_array() {
    let definition = r#"{
    "StartAt": "P",
    "States": {
        "P": {
            "Type": "Parallel",
            "End": true,
            "Output": { "second": "{% $states.result[1] %}", "first": "{% $states.result[0] %}" },
            "Branches": [
                { "StartAt": "A", "States": { "A": { "Type": "Pass", "Output": "b0", "End": true } } },
                { "StartAt": "B", "States": { "B": { "Type": "Pass", "Output": "b1", "End": true } } }
            ]
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
                    checksum: 7642041935803403012,
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/0/States")),
                index: 0,
                start_at: "A".to_string(),
                input: json!({"n": 1}),
            })),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/1/States")),
                index: 1,
                start_at: "B".to_string(),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "A".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                state_path: path("/States/P/Branches/0/States/A"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "B".to_string(),
                    index: 1,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                state_path: path("/States/P/Branches/1/States/B"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
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
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
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
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-4", 8),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
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
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
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
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
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
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!("b0")),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                output: json!("b0"),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
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
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!("b1")),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                output: json!("b1"),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "A".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!("b0")),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "A".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!("b0")),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "B".to_string(),
                    index: 1,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!("b1")),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "B".to_string(),
                    index: 1,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!("b1")),
                },
            }),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
                    retry_state: None,
                    output: Some(json!({"second": "b1", "first": "b0"})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!({"second": "b1", "first": "b0"}),
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
                    output: Some(json!({"second": "b1", "first": "b0"})),
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
                    output: Some(json!({"second": "b1", "first": "b0"})),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!({"second": "b1", "first": "b0"}),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"second": "b1", "first": "b0"})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"second": "b1", "first": "b0"})),
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
async fn parallel_with_a_next_successor_hands_the_array_on() {
    let definition = r#"{
    "StartAt": "P",
    "States": {
        "P": {
            "Type": "Parallel",
            "Next": "Done",
            "Branches": [
                { "StartAt": "A", "States": { "A": { "Type": "Pass", "Output": "b0", "End": true } } },
                { "StartAt": "B", "States": { "B": { "Type": "Pass", "Output": "b1", "End": true } } }
            ]
        },
        "Done": { "Type": "Pass", "End": true }
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
                    checksum: 9901388541278302510,
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/0/States")),
                index: 0,
                start_at: "A".to_string(),
                input: json!({"n": 1}),
            })),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/1/States")),
                index: 1,
                start_at: "B".to_string(),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "A".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                state_path: path("/States/P/Branches/0/States/A"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "B".to_string(),
                    index: 1,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                state_path: path("/States/P/Branches/1/States/B"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
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
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
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
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-4", 8),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
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
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
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
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
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
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/A"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!("b0")),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                output: json!("b0"),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
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
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/B"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!("b1")),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                output: json!("b1"),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "A".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!("b0")),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "A".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!("b0")),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "B".to_string(),
                    index: 1,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!("b1")),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "B".to_string(),
                    index: 1,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!("b1")),
                },
            }),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
                    retry_state: None,
                    output: Some(json!(["b0", "b1"])),
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
                input: json!(["b0", "b1"]),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(10), "lifecycle_execution-6")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Done"),
                    status: ActivityStatus::Running,
                    raw_input: json!(["b0", "b1"]),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(10), "lifecycle_execution-6")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Done"),
                    status: ActivityStatus::Running,
                    raw_input: json!(["b0", "b1"]),
                    input: Some(json!(["b0", "b1"])),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-6", 10),
                output: json!(["b0", "b1"]),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(10), "lifecycle_execution-6")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Done"),
                    status: ActivityStatus::Completing,
                    raw_input: json!(["b0", "b1"]),
                    input: Some(json!(["b0", "b1"])),
                    raw_output: Some(json!(["b0", "b1"])),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(10), "lifecycle_execution-6")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Done"),
                    status: ActivityStatus::Completed,
                    raw_input: json!(["b0", "b1"]),
                    input: Some(json!(["b0", "b1"])),
                    raw_output: Some(json!(["b0", "b1"])),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!(["b0", "b1"])),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!(["b0", "b1"]),
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
                    output: Some(json!(["b0", "b1"])),
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
                    output: Some(json!(["b0", "b1"])),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!(["b0", "b1"]),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!(["b0", "b1"])),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!(["b0", "b1"])),
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
async fn parallel_branch_failure_fails_the_run() {
    let definition = r#"{
    "StartAt": "P",
    "States": {
        "P": {
            "Type": "Parallel",
            "End": true,
            "Branches": [
                { "StartAt": "Boom", "States": { "Boom": { "Type": "Fail", "Error": "BranchBoom", "Cause": "nope" } } }
            ]
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
                    checksum: 4771441858248751746,
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/0/States")),
                index: 0,
                start_at: "Boom".to_string(),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "Boom".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                state_path: path("/States/P/Branches/0/States/Boom"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
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
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
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
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
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
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
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
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
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
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "Boom".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "Boom".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6))]),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateTerminated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6))]),
                    })),
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
                    start_at: "P".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
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
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
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
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
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
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
        ],
        status: ExecutionStatus::Terminated(TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "Boom".to_string(),
                error: "BranchBoom".to_string(),
                output: Box::new(json!({ "Error": "BranchBoom", "Cause": "nope" })),
            }),
        }),
        acts: vec![],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn parallel_failure_stops_a_sibling_still_in_flight() {
    let definition = r#"{
    "StartAt": "P",
    "States": {
        "P": {
            "Type": "Parallel",
            "End": true,
            "Branches": [
                { "StartAt": "Boom", "States": { "Boom": { "Type": "Fail", "Error": "BranchBoom", "Cause": "nope" } } },
                { "StartAt": "Slow", "States": { "Slow": { "Type": "Wait", "Seconds": 300, "End": true } } }
            ]
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
                    checksum: 3447312135218870189,
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/0/States")),
                index: 0,
                start_at: "Boom".to_string(),
                input: json!({"n": 1}),
            })),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/1/States")),
                index: 1,
                start_at: "Slow".to_string(),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "Boom".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                state_path: path("/States/P/Branches/0/States/Boom"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "Slow".to_string(),
                    index: 1,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                state_path: path("/States/P/Branches/1/States/Slow"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
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
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
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
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-4", 8),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/Slow"),
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
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/Slow"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 300_000) })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::TimerActivated {
                timer: Timer {
                    meta: meta(ObjectKind::Timer, uid(10), "lifecycle_execution-6")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::WaitResume,
                    status: TimerStatus::Active,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 300_000),
                },
            }),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
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
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
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
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
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
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "Boom".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "Boom".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
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
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "Slow".to_string(),
                    index: 1,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ExecutionTerminating {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/Slow"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 300_000) })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CancelTimer {
                timer: ref_to(ObjectKind::Timer, "lifecycle_execution-6", 10),
            }),
            EntryPayload::Event(Event::TimerCancelled {
                timer: Timer {
                    meta: meta(ObjectKind::Timer, uid(10), "lifecycle_execution-6")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::WaitResume,
                    status: TimerStatus::Cancelled,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 300_000),
                },
            }),
            EntryPayload::Command(Command::ContinueTerminate {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9),
            }),
            EntryPayload::Event(Event::StateTerminated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/Slow"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 300_000) })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ContinueTerminate {
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
            }),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "Slow".to_string(),
                    index: 1,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ContinueTerminate {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
            }),
            EntryPayload::Event(Event::StateTerminated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ContinueTerminate {
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
            }),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ContinueTerminate {
                owner: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
            }),
            EntryPayload::Event(Event::ExecutionTerminated {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
        ],
        status: ExecutionStatus::Terminated(TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "Boom".to_string(),
                error: "BranchBoom".to_string(),
                output: Box::new(json!({ "Error": "BranchBoom", "Cause": "nope" })),
            }),
        }),
        acts: vec![],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn parallel_two_failing_branches_absorb_a_duplicate_termination() {
    let definition = r#"{
    "StartAt": "P",
    "States": {
        "P": {
            "Type": "Parallel",
            "End": true,
            "Branches": [
                { "StartAt": "Boom", "States": { "Boom": { "Type": "Fail", "Error": "BranchBoom", "Cause": "nope" } } },
                { "StartAt": "Boom2", "States": { "Boom2": { "Type": "Fail", "Error": "BranchBoom2", "Cause": "nope" } } }
            ]
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
                    checksum: 18327526135807667144,
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/0/States")),
                index: 0,
                start_at: "Boom".to_string(),
                input: json!({"n": 1}),
            })),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/P/Branches/1/States")),
                index: 1,
                start_at: "Boom2".to_string(),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "Boom".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                state_path: path("/States/P/Branches/0/States/Boom"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "Boom2".to_string(),
                    index: 1,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                state_path: path("/States/P/Branches/1/States/Boom2"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
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
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
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
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-4", 8),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/Boom2"),
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
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/Boom2"),
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
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9),
                output: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
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
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
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
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States/Boom"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
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
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/Boom2"),
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
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/Boom2"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom2".to_string(),
                            error: "BranchBoom2".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom2", "Cause": "nope"})),
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
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States/Boom2"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom2".to_string(),
                            error: "BranchBoom2".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom2", "Cause": "nope"})),
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
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom2".to_string(),
                        error: "BranchBoom2".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom2", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "Boom".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/0/States"),
                    start_at: "Boom".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "Boom2".to_string(),
                    index: 1,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom2".to_string(),
                            error: "BranchBoom2".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom2", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P/Branches/1/States"),
                    start_at: "Boom2".to_string(),
                    index: 1,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom2".to_string(),
                            error: "BranchBoom2".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom2", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateTerminated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
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
                    start_at: "P".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
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
                        state: "Boom".to_string(),
                        error: "BranchBoom".to_string(),
                        output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "P".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateTerminated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
                    retry_state: None,
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
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
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
                            state: "Boom".to_string(),
                            error: "BranchBoom".to_string(),
                            output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
        ],
        status: ExecutionStatus::Terminated(TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "Boom".to_string(),
                error: "BranchBoom".to_string(),
                output: Box::new(json!({ "Error": "BranchBoom", "Cause": "nope" })),
            }),
        }),
        acts: vec![],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn parallel_with_no_branches_converges_to_an_empty_array() {
    let definition = r#"{
    "StartAt": "P",
    "States": {
        "P": { "Type": "Parallel", "End": true, "Branches": [] }
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
                    checksum: 14520603696728161513,
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
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
                    activity_state: Some(ActivityState::Parallel(ParallelActivityState {
                        branches: Default::default(),
                    })),
                    retry_state: None,
                    output: Some(json!([])),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!([]),
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
                    output: Some(json!([])),
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
                    output: Some(json!([])),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!([]),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!([])),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!([])),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![],
    };
    run_typed_case(&case).await;
}
