//! The `Map` state's lifecycle, pinned as a whole **entry chain** rather than as "the output array".

mod common;

use common::{
    TypedCase, VIRTUAL_EPOCH_MILLIS, flow_name, indexed_refs, meta, name, path, pointer, ref_to,
    request, run_typed_case, stamp, uid,
};
use serde_json::json;
use spica_engine::{
    ActivateState, Activity, ActivityState, ActivityStatus, Command, CompleteExecution,
    CompleteState, CompleteThread, CreateExecution, CreateFlow, EntryPayload, Event, Execution,
    ExecutionCreated, ExecutionError, ExecutionStatus, Flow, FlowCreated, FlowStatus, FlowVersion,
    FlowVersionCreated, MapActivityState, ObjectKind, RuntimeError, SpawnThread, StateTransitioned,
    TerminateExecution, TerminateState, TerminateThread, TerminationReason, Thread, ThreadStatus,
    Timer, TimerPurpose, TimerStatus, WaitActivityState,
};

#[rustfmt::skip]
#[tokio::test]
async fn map_over_items_aggregates_in_item_order() {
    let definition = r#"{
    "StartAt": "M",
    "States": {
        "M": {
            "Type": "Map",
            "End": true,
            "Items": [3, 1, 2],
            "ItemProcessor": {
                "StartAt": "Emit",
                "States": { "Emit": { "Type": "Pass", "End": true } }
            }
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
                    checksum: 14452615179516465290,
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
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/M"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: None,
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![],
                        total: 0,
                        max_concurrency: 0,
                        children: Default::default(),
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
                    state_path: path("/States/M"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(3), json!(1), json!(2)],
                        total: 3,
                        max_concurrency: 0,
                        children: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/M/ItemProcessor/States")),
                index: 0,
                start_at: "Emit".to_string(),
                input: json!(3),
            })),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/M/ItemProcessor/States")),
                index: 1,
                start_at: "Emit".to_string(),
                input: json!(1),
            })),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/M/ItemProcessor/States")),
                index: 2,
                start_at: "Emit".to_string(),
                input: json!(2),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!(3),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                state_path: path("/States/M/ItemProcessor/States/Emit"),
                input: json!(3),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 1,
                    status: ThreadStatus::Running,
                    input: json!(1),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                state_path: path("/States/M/ItemProcessor/States/Emit"),
                input: json!(1),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 2,
                    status: ThreadStatus::Running,
                    input: json!(2),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8),
                state_path: path("/States/M/ItemProcessor/States/Emit"),
                input: json!(2),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Running,
                    raw_input: json!(3),
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
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Running,
                    raw_input: json!(3),
                    input: Some(json!(3)),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9),
                output: json!(3),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(10), "lifecycle_execution-6")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Running,
                    raw_input: json!(1),
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
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Running,
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-6", 10),
                output: json!(1),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(11), "lifecycle_execution-7")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Running,
                    raw_input: json!(2),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(11), "lifecycle_execution-7")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Running,
                    raw_input: json!(2),
                    input: Some(json!(2)),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-7", 11),
                output: json!(2),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Completing,
                    raw_input: json!(3),
                    input: Some(json!(3)),
                    raw_output: Some(json!(3)),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Completed,
                    raw_input: json!(3),
                    input: Some(json!(3)),
                    raw_output: Some(json!(3)),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!(3)),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                output: json!(3),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(10), "lifecycle_execution-6")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Completing,
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: Some(json!(1)),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(10), "lifecycle_execution-6")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Completed,
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: Some(json!(1)),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!(1)),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                output: json!(1),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(11), "lifecycle_execution-7")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Completing,
                    raw_input: json!(2),
                    input: Some(json!(2)),
                    raw_output: Some(json!(2)),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(11), "lifecycle_execution-7")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Completed,
                    raw_input: json!(2),
                    input: Some(json!(2)),
                    raw_output: Some(json!(2)),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!(2)),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8),
                output: json!(2),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!(3),
                    output: Some(json!(3)),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!(3),
                    output: Some(json!(3)),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 1,
                    status: ThreadStatus::Completing,
                    input: json!(1),
                    output: Some(json!(1)),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 1,
                    status: ThreadStatus::Completed,
                    input: json!(1),
                    output: Some(json!(1)),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 2,
                    status: ThreadStatus::Completing,
                    input: json!(2),
                    output: Some(json!(2)),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 2,
                    status: ThreadStatus::Completed,
                    input: json!(2),
                    output: Some(json!(2)),
                },
            }),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(3), json!(1), json!(2)],
                        total: 3,
                        max_concurrency: 0,
                        children: indexed_refs(&[
                            (0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                            (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                            (2, ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8)),
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
                    state_path: path("/States/M"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(3), json!(1), json!(2)],
                        total: 3,
                        max_concurrency: 0,
                        children: indexed_refs(&[
                            (0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                            (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                            (2, ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8)),
                        ]),
                    })),
                    retry_state: None,
                    output: Some(json!([3, 1, 2])),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!([3, 1, 2]),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!([3, 1, 2])),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!([3, 1, 2])),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!([3, 1, 2]),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!([3, 1, 2])),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!([3, 1, 2])),
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
async fn map_with_max_concurrency_one_replenishes_per_settle() {
    let definition = r#"{
    "StartAt": "M",
    "States": {
        "M": {
            "Type": "Map",
            "End": true,
            "Items": [3, 1],
            "MaxConcurrency": 1,
            "ItemProcessor": {
                "StartAt": "Emit",
                "States": { "Emit": { "Type": "Pass", "End": true } }
            }
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
                    checksum: 17931075226976749563,
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
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/M"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: None,
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![],
                        total: 0,
                        max_concurrency: 0,
                        children: Default::default(),
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
                    state_path: path("/States/M"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(3), json!(1)],
                        total: 2,
                        max_concurrency: 1,
                        children: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/M/ItemProcessor/States")),
                index: 0,
                start_at: "Emit".to_string(),
                input: json!(3),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!(3),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                state_path: path("/States/M/ItemProcessor/States/Emit"),
                input: json!(3),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Running,
                    raw_input: json!(3),
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
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Running,
                    raw_input: json!(3),
                    input: Some(json!(3)),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-3", 7),
                output: json!(3),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Completing,
                    raw_input: json!(3),
                    input: Some(json!(3)),
                    raw_output: Some(json!(3)),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Completed,
                    raw_input: json!(3),
                    input: Some(json!(3)),
                    raw_output: Some(json!(3)),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!(3)),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                output: json!(3),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!(3),
                    output: Some(json!(3)),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!(3),
                    output: Some(json!(3)),
                },
            }),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/M/ItemProcessor/States")),
                index: 1,
                start_at: "Emit".to_string(),
                input: json!(1),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 1,
                    status: ThreadStatus::Running,
                    input: json!(1),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8),
                state_path: path("/States/M/ItemProcessor/States/Emit"),
                input: json!(1),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Running,
                    raw_input: json!(1),
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
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Running,
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9),
                output: json!(1),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Completing,
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: Some(json!(1)),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Completed,
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: Some(json!(1)),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!(1)),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8),
                output: json!(1),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 1,
                    status: ThreadStatus::Completing,
                    input: json!(1),
                    output: Some(json!(1)),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 1,
                    status: ThreadStatus::Completed,
                    input: json!(1),
                    output: Some(json!(1)),
                },
            }),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(3), json!(1)],
                        total: 2,
                        max_concurrency: 1,
                        children: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8))]),
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
                    state_path: path("/States/M"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(3), json!(1)],
                        total: 2,
                        max_concurrency: 1,
                        children: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-4", 8))]),
                    })),
                    retry_state: None,
                    output: Some(json!([3, 1])),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!([3, 1]),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!([3, 1])),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!([3, 1])),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!([3, 1]),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!([3, 1])),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!([3, 1])),
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
async fn map_with_no_items_converges_to_an_empty_array() {
    let definition = r#"{
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
                    checksum: 6483212456599020933,
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
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/M"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: None,
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![],
                        total: 0,
                        max_concurrency: 0,
                        children: Default::default(),
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
                    state_path: path("/States/M"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![],
                        total: 0,
                        max_concurrency: 0,
                        children: Default::default(),
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
                    state_path: path("/States/M"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![],
                        total: 0,
                        max_concurrency: 0,
                        children: Default::default(),
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
                    state_path: path("/States/M"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![],
                        total: 0,
                        max_concurrency: 0,
                        children: Default::default(),
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
                    start_at: "M".to_string(),
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
                    start_at: "M".to_string(),
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

#[rustfmt::skip]
#[tokio::test]
async fn map_defaults_its_items_to_its_array_input() {
    let definition = r#"{
    "StartAt": "M",
    "States": {
        "M": {
            "Type": "Map",
            "End": true,
            "ItemProcessor": {
                "StartAt": "Emit",
                "States": { "Emit": { "Type": "Pass", "End": true } }
            }
        }
    }
}"#;
    let case = TypedCase {
        definition,
        input: r#"[10, 20]"#,
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
                    checksum: 6277088080643968226,
                },
            })),
            EntryPayload::Command(Command::CreateExecution(CreateExecution {
                request_id: request(1),
                name: name("lifecycle_execution"),
                flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                input: json!([10, 20]),
            })),
            EntryPayload::Event(Event::ExecutionCreated(ExecutionCreated {
                request_id: request(1),
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Running,
                    input: json!([10, 20]),
                    output: None,
                },
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!([10, 20]),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/M"),
                input: json!([10, 20]),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M"),
                    status: ActivityStatus::Running,
                    raw_input: json!([10, 20]),
                    input: None,
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![],
                        total: 0,
                        max_concurrency: 0,
                        children: Default::default(),
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
                    state_path: path("/States/M"),
                    status: ActivityStatus::Running,
                    raw_input: json!([10, 20]),
                    input: Some(json!([10, 20])),
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(10), json!(20)],
                        total: 2,
                        max_concurrency: 0,
                        children: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/M/ItemProcessor/States")),
                index: 0,
                start_at: "Emit".to_string(),
                input: json!(10),
            })),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/M/ItemProcessor/States")),
                index: 1,
                start_at: "Emit".to_string(),
                input: json!(20),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!(10),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                state_path: path("/States/M/ItemProcessor/States/Emit"),
                input: json!(10),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 1,
                    status: ThreadStatus::Running,
                    input: json!(20),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                state_path: path("/States/M/ItemProcessor/States/Emit"),
                input: json!(20),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Running,
                    raw_input: json!(10),
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
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Running,
                    raw_input: json!(10),
                    input: Some(json!(10)),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-4", 8),
                output: json!(10),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Running,
                    raw_input: json!(20),
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
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Running,
                    raw_input: json!(20),
                    input: Some(json!(20)),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9),
                output: json!(20),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Completing,
                    raw_input: json!(10),
                    input: Some(json!(10)),
                    raw_output: Some(json!(10)),
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
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Completed,
                    raw_input: json!(10),
                    input: Some(json!(10)),
                    raw_output: Some(json!(10)),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!(10)),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                output: json!(10),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Completing,
                    raw_input: json!(20),
                    input: Some(json!(20)),
                    raw_output: Some(json!(20)),
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
                    state_path: path("/States/M/ItemProcessor/States/Emit"),
                    status: ActivityStatus::Completed,
                    raw_input: json!(20),
                    input: Some(json!(20)),
                    raw_output: Some(json!(20)),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!(20)),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                output: json!(20),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!(10),
                    output: Some(json!(10)),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!(10),
                    output: Some(json!(10)),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 1,
                    status: ThreadStatus::Completing,
                    input: json!(20),
                    output: Some(json!(20)),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Emit".to_string(),
                    index: 1,
                    status: ThreadStatus::Completed,
                    input: json!(20),
                    output: Some(json!(20)),
                },
            }),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M"),
                    status: ActivityStatus::Completing,
                    raw_input: json!([10, 20]),
                    input: Some(json!([10, 20])),
                    raw_output: Some(json!([10, 20])),
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(10), json!(20)],
                        total: 2,
                        max_concurrency: 0,
                        children: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
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
                    state_path: path("/States/M"),
                    status: ActivityStatus::Completed,
                    raw_input: json!([10, 20]),
                    input: Some(json!([10, 20])),
                    raw_output: Some(json!([10, 20])),
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(10), json!(20)],
                        total: 2,
                        max_concurrency: 0,
                        children: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
                    retry_state: None,
                    output: Some(json!([10, 20])),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!([10, 20]),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!([10, 20]),
                    output: Some(json!([10, 20])),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!([10, 20]),
                    output: Some(json!([10, 20])),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!([10, 20]),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!([10, 20]),
                    output: Some(json!([10, 20])),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!([10, 20]),
                    output: Some(json!([10, 20])),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![],
    };
    run_typed_case(&case).await;
}

// `MaxConcurrency: 1` is deliberate: with a sibling still in flight, the container's own failure
// path would also have to stop it, and that sweep is a different question from what a failed item
// does to the Map. One item in flight at a time makes this chain say only the latter.
#[rustfmt::skip]
#[tokio::test]
async fn map_item_failure_fails_the_run() {
    let definition = r#"{
    "StartAt": "M",
    "States": {
        "M": {
            "Type": "Map",
            "End": true,
            "Items": [1, 2],
            "MaxConcurrency": 1,
            "ItemProcessor": {
                "StartAt": "Boom",
                "States": { "Boom": { "Type": "Fail", "Error": "ItemBoom", "Cause": "nope" } }
            }
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
                    checksum: 6449115331599728925,
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
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/M"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: None,
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![],
                        total: 0,
                        max_concurrency: 0,
                        children: Default::default(),
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
                    state_path: path("/States/M"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(1), json!(2)],
                        total: 2,
                        max_concurrency: 1,
                        children: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/M/ItemProcessor/States")),
                index: 0,
                start_at: "Boom".to_string(),
                input: json!(1),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Boom".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!(1),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                state_path: path("/States/M/ItemProcessor/States/Boom"),
                input: json!(1),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Boom"),
                    status: ActivityStatus::Running,
                    raw_input: json!(1),
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
                    state_path: path("/States/M/ItemProcessor/States/Boom"),
                    status: ActivityStatus::Running,
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-3", 7),
                output: json!(1),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Boom"),
                    status: ActivityStatus::Completing,
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: Some(json!(1)),
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
                    state_path: path("/States/M/ItemProcessor/States/Boom"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "ItemBoom".to_string(),
                            output: Box::new(json!({"Error": "ItemBoom", "Cause": "nope"})),
                        }),
                    }),
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: Some(json!(1)),
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
                    state_path: path("/States/M/ItemProcessor/States/Boom"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "ItemBoom".to_string(),
                            output: Box::new(json!({"Error": "ItemBoom", "Cause": "nope"})),
                        }),
                    }),
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: Some(json!(1)),
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
                        error: "ItemBoom".to_string(),
                        output: Box::new(json!({"Error": "ItemBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Boom".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "ItemBoom".to_string(),
                            output: Box::new(json!({"Error": "ItemBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!(1),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Boom".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "ItemBoom".to_string(),
                            output: Box::new(json!({"Error": "ItemBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!(1),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "M".to_string(),
                        error: "Map item failed".to_string(),
                        output: Box::new(json!(null)),
                    }),
                },
            })),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "M".to_string(),
                        error: "Map item failed".to_string(),
                        output: Box::new(json!(null)),
                    }),
                },
            })),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(1), json!(2)],
                        total: 2,
                        max_concurrency: 1,
                        children: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6))]),
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
                    state_path: path("/States/M"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(1), json!(2)],
                        total: 2,
                        max_concurrency: 1,
                        children: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6))]),
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
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
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
                        state: "M".to_string(),
                        error: "Map item failed".to_string(),
                        output: Box::new(json!(null)),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
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
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
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
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
        ],
        status: ExecutionStatus::Terminated(TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "M".to_string(),
                error: "Map item failed".to_string(),
                output: Box::new(json!(null)),
            }),
        }),
        acts: vec![],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn map_failure_stops_a_sibling_still_in_flight() {
    let definition = r#"{
    "StartAt": "M",
    "States": {
        "M": {
            "Type": "Map",
            "End": true,
            "Items": [1, 2],
            "ItemProcessor": {
                "StartAt": "Pick",
                "States": {
                    "Pick": {
                        "Type": "Choice",
                        "Choices": [ { "Condition": "{% $states.input = 1 %}", "Next": "Boom" } ],
                        "Default": "Slow"
                    },
                    "Boom": { "Type": "Fail", "Error": "ItemBoom", "Cause": "nope" },
                    "Slow": { "Type": "Wait", "Seconds": 300, "End": true }
                }
            }
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
                    checksum: 9942292986466984382,
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
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/M"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: None,
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![],
                        total: 0,
                        max_concurrency: 0,
                        children: Default::default(),
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
                    state_path: path("/States/M"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(1), json!(2)],
                        total: 2,
                        max_concurrency: 0,
                        children: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/M/ItemProcessor/States")),
                index: 0,
                start_at: "Pick".to_string(),
                input: json!(1),
            })),
            EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                state_path: Some(path("/States/M/ItemProcessor/States")),
                index: 1,
                start_at: "Pick".to_string(),
                input: json!(2),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {   // Map.Thread-1
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Pick".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!(1),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                state_path: path("/States/M/ItemProcessor/States/Pick"),
                input: json!(1),
            })),
            EntryPayload::Event(Event::ThreadCreated {
                thread: Thread {  // Map.Thread-2
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Pick".to_string(),
                    index: 1,
                    status: ThreadStatus::Running,
                    input: json!(2),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                state_path: path("/States/M/ItemProcessor/States/Pick"),
                input: json!(2),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {   // Map.Thread-1
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Pick"),
                    status: ActivityStatus::Running,
                    raw_input: json!(1),
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
                    state_path: path("/States/M/ItemProcessor/States/Pick"),
                    status: ActivityStatus::Running,
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-4", 8),
                output: json!(1),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {   // Map.Thread-2
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Pick"),
                    status: ActivityStatus::Running,
                    raw_input: json!(2),
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
                    state_path: path("/States/M/ItemProcessor/States/Pick"),
                    status: ActivityStatus::Running,
                    raw_input: json!(2),
                    input: Some(json!(2)),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9),
                output: json!(2),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {   // Map.Thread-1
                    meta: meta(ObjectKind::Activity, uid(8), "lifecycle_execution-4")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Pick"),
                    status: ActivityStatus::Completing,
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: Some(json!(1)),
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
                    state_path: path("/States/M/ItemProcessor/States/Pick"),
                    status: ActivityStatus::Completed,
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: Some(json!(1)),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!(1)),
                },
            }),
            EntryPayload::Event(Event::StateTransitioned(StateTransitioned {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-4", 8),
                next: pointer("/States/M/ItemProcessor/States/Boom"),
            })),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6),
                state_path: path("/States/M/ItemProcessor/States/Boom"),
                input: json!(1),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity { // Map.Thread-2
                    meta: meta(ObjectKind::Activity, uid(9), "lifecycle_execution-5")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Pick"),
                    status: ActivityStatus::Completing,
                    raw_input: json!(2),
                    input: Some(json!(2)),
                    raw_output: Some(json!(2)),
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
                    state_path: path("/States/M/ItemProcessor/States/Pick"),
                    status: ActivityStatus::Completed,
                    raw_input: json!(2),
                    input: Some(json!(2)),
                    raw_output: Some(json!(2)),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!(2)),
                },
            }),
            EntryPayload::Event(Event::StateTransitioned(StateTransitioned {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-5", 9),
                next: pointer("/States/M/ItemProcessor/States/Slow"),
            })),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                state_path: path("/States/M/ItemProcessor/States/Slow"),
                input: json!(2),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity { // Map.Thread-1
                    meta: meta(ObjectKind::Activity, uid(10), "lifecycle_execution-6")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Boom"),
                    status: ActivityStatus::Running,
                    raw_input: json!(1),
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
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Boom"),
                    status: ActivityStatus::Running,
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-6", 10),
                output: json!(1),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity { // Map.Thread-2
                    meta: meta(ObjectKind::Activity, uid(11), "lifecycle_execution-7")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Slow"),
                    status: ActivityStatus::Running,
                    raw_input: json!(2),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(11), "lifecycle_execution-7")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Slow"),
                    status: ActivityStatus::Running,
                    raw_input: json!(2),
                    input: Some(json!(2)),
                    raw_output: None,
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 300_000) })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::TimerActivated {
                timer: Timer {
                    meta: meta(ObjectKind::Timer, uid(12), "lifecycle_execution-8")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-7", 11)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::WaitResume,
                    status: TimerStatus::Active,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 300_000),
                },
            }),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity { // Map.Thread-1
                    meta: meta(ObjectKind::Activity, uid(10), "lifecycle_execution-6")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Boom"),
                    status: ActivityStatus::Completing,
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: Some(json!(1)),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(10), "lifecycle_execution-6")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Boom"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "ItemBoom".to_string(),
                            output: Box::new(json!({"Error": "ItemBoom", "Cause": "nope"})),
                        }),
                    }),
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: Some(json!(1)),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateTerminated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(10), "lifecycle_execution-6")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Boom"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "ItemBoom".to_string(),
                            output: Box::new(json!({"Error": "ItemBoom", "Cause": "nope"})),
                        }),
                    }),
                    raw_input: json!(1),
                    input: Some(json!(1)),
                    raw_output: Some(json!(1)),
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
                        error: "ItemBoom".to_string(),
                        output: Box::new(json!({"Error": "ItemBoom", "Cause": "nope"})),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {  // Map.Thread-1
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Pick".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "ItemBoom".to_string(),
                            output: Box::new(json!({"Error": "ItemBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!(1),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Pick".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "Boom".to_string(),
                            error: "ItemBoom".to_string(),
                            output: Box::new(json!({"Error": "ItemBoom", "Cause": "nope"})),
                        }),
                    }),
                    input: json!(1),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "M".to_string(),
                        error: "Map item failed".to_string(),
                        output: Box::new(json!(null)),
                    }),
                },
            })),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {   // Root Thread
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "M".to_string(),
                        error: "Map item failed".to_string(),
                        output: Box::new(json!(null)),
                    }),
                },
            })),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(1), json!(2)],
                        total: 2,
                        max_concurrency: 0,
                        children: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {// Map.Thread-2
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "M".to_string(),
                        error: "Map item failed".to_string(),
                        output: Box::new(json!(null)),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
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
                        state: "M".to_string(),
                        error: "Map item failed".to_string(),
                        output: Box::new(json!(null)),
                    }),
                },
            })),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),   // 重复对 /States/M 进行终止
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "M".to_string(),
                        error: "Map item failed".to_string(),
                        output: Box::new(json!(null)),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {  // Map.Thread-2
                    meta: meta(ObjectKind::Thread, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Pick".to_string(),
                    index: 1,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
                        }),
                    }),
                    input: json!(2),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-7", 11),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "M".to_string(),
                        error: "Map item failed".to_string(),
                        output: Box::new(json!(null)),
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
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
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
                        state: "M".to_string(),
                        error: "Map item failed".to_string(),
                        output: Box::new(json!(null)),
                    }),
                },
            })),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(11), "lifecycle_execution-7")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Slow"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
                        }),
                    }),
                    raw_input: json!(2),
                    input: Some(json!(2)),
                    raw_output: None,
                    activity_state: Some(ActivityState::Wait(WaitActivityState { resume_at: stamp(VIRTUAL_EPOCH_MILLIS + 300_000) })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CancelTimer {
                timer: ref_to(ObjectKind::Timer, "lifecycle_execution-8", 12),
            }),
            EntryPayload::Event(Event::TimerCancelled {
                timer: Timer {
                    meta: meta(ObjectKind::Timer, uid(12), "lifecycle_execution-8")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-7", 11)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::WaitResume,
                    status: TimerStatus::Cancelled,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 300_000),
                },
            }),
            EntryPayload::Command(Command::ContinueTerminate {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-7", 11),
            }),
            EntryPayload::Event(Event::StateTerminated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(11), "lifecycle_execution-7")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M/ItemProcessor/States/Slow"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
                        }),
                    }),
                    raw_input: json!(2),
                    input: Some(json!(2)),
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
                    state_path: path("/States/M/ItemProcessor/States"),
                    start_at: "Pick".to_string(),
                    index: 1,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
                        }),
                    }),
                    input: json!(2),
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
                    state_path: path("/States/M"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![json!(1), json!(2)],
                        total: 2,
                        max_concurrency: 0,
                        children: indexed_refs(&[(0, ref_to(ObjectKind::Thread, "lifecycle_execution-2", 6)), (1, ref_to(ObjectKind::Thread, "lifecycle_execution-3", 7))]),
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
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::StateFailed {
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
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
                            state: "M".to_string(),
                            error: "Map item failed".to_string(),
                            output: Box::new(json!(null)),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
        ],
        status: ExecutionStatus::Terminated(TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "M".to_string(),
                error: "Map item failed".to_string(),
                output: Box::new(json!(null)),
            }),
        }),
        acts: vec![],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn map_with_a_negative_max_concurrency_is_a_definition_error() {
    let definition = r#"{
    "StartAt": "M",
    "States": {
        "M": {
            "Type": "Map",
            "End": true,
            "Items": [3, 1],
            "MaxConcurrency": "{% -1 %}",
            "ItemProcessor": {
                "StartAt": "Emit",
                "States": { "Emit": { "Type": "Pass", "End": true } }
            }
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
                    checksum: 5937166124419682522,
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
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/M"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: None,
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![],
                        total: 0,
                        max_concurrency: 0,
                        children: Default::default(),
                    })),
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Map MaxConcurrency expression must evaluate to a non-negative integer".to_string())),
                },
            })),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Map MaxConcurrency expression must evaluate to a non-negative integer".to_string())),
                },
            })),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/M"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Map MaxConcurrency expression must evaluate to a non-negative integer".to_string())),
                    }),
                    raw_input: json!({"n": 1}),
                    input: None,
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![],
                        total: 0,
                        max_concurrency: 0,
                        children: Default::default(),
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
                    state_path: path("/States/M"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Map MaxConcurrency expression must evaluate to a non-negative integer".to_string())),
                    }),
                    raw_input: json!({"n": 1}),
                    input: None,
                    raw_output: None,
                    activity_state: Some(ActivityState::Map(MapActivityState {
                        items: vec![],
                        total: 0,
                        max_concurrency: 0,
                        children: Default::default(),
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
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Map MaxConcurrency expression must evaluate to a non-negative integer".to_string())),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateExecution(TerminateExecution {
                name: name("lifecycle_execution"),
                uid: Some(uid(3)),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Map MaxConcurrency expression must evaluate to a non-negative integer".to_string())),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "M".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Map MaxConcurrency expression must evaluate to a non-negative integer".to_string())),
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
                        error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Map MaxConcurrency expression must evaluate to a non-negative integer".to_string())),
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
                        error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Map MaxConcurrency expression must evaluate to a non-negative integer".to_string())),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
        ],
        status: ExecutionStatus::Terminated(TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "Map MaxConcurrency expression must evaluate to a non-negative integer".to_string(),
            )),
        }),
        acts: vec![],
    };
    run_typed_case(&case).await;
}
