//! The `Task` state's lifecycle, pinned as a whole **entry chain** rather than as "which branch ran".

mod common;

use std::time::Duration;

use common::{
    Act, Call, Signal, TypedCase, VIRTUAL_EPOCH_MILLIS, epoch, flow_name, meta, meta_span, name,
    path, pointer, ref_to, request, run_typed_case, stamp, uid,
};
use serde_json::json;
use spica_engine::{
    ActivateState, ActivateTask, Activity, ActivityStatus, ClaimTasks, Command, CompleteExecution,
    CompleteState, CompleteTask, CompleteThread, CreateExecution, CreateFlow, EntryPayload, Event,
    Execution, ExecutionCreated, ExecutionError, ExecutionStatus, FailTask, Flow, FlowCreated,
    FlowStatus, FlowVersion, FlowVersionCreated, ObjectKind, Reject, RejectionType,
    RetrierAttemptState, RetryPolicy, RetryState, RuntimeError, StateTransitioned, Task,
    TaskCompleted, TaskFailed, TaskStatus, TasksClaimed, TerminateExecution, TerminateState,
    TerminateThread, TerminationReason, Thread, ThreadStatus, Timer, TimerPurpose, TimerStatus,
};

#[rustfmt::skip]
#[tokio::test]
async fn task_poll_then_complete_routes_on_its_next() {
    let definition = r#"{
    "StartAt": "T",
    "States": {
        "T": { "Type": "Task", "Resource": "r", "Next": "P" },
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
                    checksum: 5298509650561049483,
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
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/T"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
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
                    state_path: path("/States/T"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateTask(ActivateTask {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 6),
                resource: "r".to_string(),
                arguments: json!({"n": 1}),
                retry_plan: vec![],
                deadline: None,
            })),
            EntryPayload::Event(Event::TaskActivated {
                task: Task {
                    meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Pending,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
            }),
            EntryPayload::Command(Command::ClaimTasks(ClaimTasks {
                request_id: request(2),
                worker_id: "w1".to_string(),
                resource: "r".to_string(),
                max_tasks: 10,
                lease_seconds: 60,
            })),
            EntryPayload::Event(Event::TasksClaimed(TasksClaimed {
                request_id: request(2),
                tasks: vec![Task {
                    meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Running,
                    deadline: None,
                    worker_id: Some("w1".to_string()),
                    lease_expires_at: Some(stamp(VIRTUAL_EPOCH_MILLIS + 60_000)),
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                }],
            })),
            EntryPayload::Command(Command::CompleteTask(CompleteTask {
                request_id: request(3),
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 0),
                worker_id: "w1".to_string(),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::TaskCompleted(TaskCompleted {
                request_id: request(3),
                task: Task {
                    meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Completed,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
                output: json!({"ok": true}),
            })),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"ok": true})),
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
                    state_path: path("/States/T"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"ok": true})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"ok": true})),
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
                input: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"ok": true}),
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
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"ok": true}),
                    input: Some(json!({"ok": true})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-3", 7),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"ok": true}),
                    input: Some(json!({"ok": true})),
                    raw_output: Some(json!({"ok": true})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"ok": true}),
                    input: Some(json!({"ok": true})),
                    raw_output: Some(json!({"ok": true})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"ok": true})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"ok": true})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"ok": true})),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"ok": true})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"ok": true})),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![
            Act::Drive(
                Signal::TaskArmed,
                Call::Poll {
                    worker_id: "w1".to_string(),
                    resource: "r".to_string(),
                    max_tasks: 10,
                    lease_seconds: 60,
                    expect: 1,
                },
            ),
            Act::Drive(
                Signal::TasksClaimed,
                Call::Complete {
                    worker_id: "w1".to_string(),
                    task: name("lifecycle_execution-2"),
                    request_id: request(3),
                    output: json!({ "ok": true }),
                },
            ),
        ],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn task_settled_before_a_claim_is_refused() {
    let definition = r#"{
    "StartAt": "T",
    "States": {
        "T": { "Type": "Task", "Resource": "r", "Next": "P" },
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
                    checksum: 5298509650561049483,
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
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/T"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
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
                    state_path: path("/States/T"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateTask(ActivateTask {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 6),
                resource: "r".to_string(),
                arguments: json!({"n": 1}),
                retry_plan: vec![],
                deadline: None,
            })),
            EntryPayload::Event(Event::TaskActivated {
                task: Task {
                    meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Pending,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
            }),
            EntryPayload::Command(Command::CompleteTask(CompleteTask {
                request_id: request(3),
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 0),
                worker_id: "w1".to_string(),
                output: json!({"early": true}),
            })),
            EntryPayload::Reject(Reject {
                request_id: request(3),
                rejection_type: RejectionType::InvalidState,
                rejection_reason: "task task/lifecycle_execution-2 is not currently Running (status Pending); settlement refused".to_string(),
            }),
            EntryPayload::Command(Command::ClaimTasks(ClaimTasks {
                request_id: request(2),
                worker_id: "w1".to_string(),
                resource: "r".to_string(),
                max_tasks: 10,
                lease_seconds: 60,
            })),
            EntryPayload::Event(Event::TasksClaimed(TasksClaimed {
                request_id: request(2),
                tasks: vec![
                    Task {
                        meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                            .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                        execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                        resource: "r".to_string(),
                        arguments: json!({"n": 1}),
                        status: TaskStatus::Running,
                        deadline: None,
                        worker_id: Some("w1".to_string()),
                        lease_expires_at: Some(stamp(VIRTUAL_EPOCH_MILLIS + 60_000)),
                        retry_plan: vec![],
                        retry_state: RetryState {
                            attempts: 0,
                            retrier_attempts: vec![],
                            next_available_at: None,
                        },
                    },
                ],
            })),
            EntryPayload::Command(Command::CompleteTask(CompleteTask {
                request_id: request(4),
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 0),
                worker_id: "w1".to_string(),
                output: json!({"late": true}),
            })),
            EntryPayload::Event(Event::TaskCompleted(TaskCompleted {
                request_id: request(4),
                task: Task {
                    meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Completed,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
                output: json!({"late": true}),
            })),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                output: json!({"late": true}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"late": true})),
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
                    state_path: path("/States/T"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"late": true})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"late": true})),
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
                input: json!({"late": true}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"late": true}),
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
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"late": true}),
                    input: Some(json!({"late": true})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-3", 7),
                output: json!({"late": true}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"late": true}),
                    input: Some(json!({"late": true})),
                    raw_output: Some(json!({"late": true})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"late": true}),
                    input: Some(json!({"late": true})),
                    raw_output: Some(json!({"late": true})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"late": true})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!({"late": true}),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"late": true})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"late": true})),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!({"late": true}),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"late": true})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"late": true})),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![
            Act::Drive(
                Signal::TaskArmed,
                Call::Refused(Box::new(Call::Complete {
                    worker_id: "w1".to_string(),
                    task: name("lifecycle_execution-2"),
                    request_id: request(3),
                    output: json!({ "early": true }),
                })),
            ),
            Act::Drive(
                Signal::None,
                Call::Poll {
                    worker_id: "w1".to_string(),
                    resource: "r".to_string(),
                    max_tasks: 10,
                    lease_seconds: 60,
                    expect: 1,
                },
            ),
            Act::Drive(
                Signal::TasksClaimed,
                Call::Complete {
                    worker_id: "w1".to_string(),
                    task: name("lifecycle_execution-2"),
                    request_id: request(4),
                    output: json!({ "late": true }),
                },
            ),
        ],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn task_timeout_seconds_fails_the_run() {
    let definition = r#"{
    "StartAt": "T",
    "States": {
        "T": { "Type": "Task", "Resource": "r", "TimeoutSeconds": 30, "Next": "P" },
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
                    checksum: 11299113241886723525,
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
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/T"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
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
                    state_path: path("/States/T"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateTask(ActivateTask {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 6),
                resource: "r".to_string(),
                arguments: json!({"n": 1}),
                retry_plan: vec![],
                deadline: Some(stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
            })),
            EntryPayload::Event(Event::TimerActivated {
                timer: Timer {
                    meta: meta(ObjectKind::Timer, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::TaskTimeout,
                    status: TimerStatus::Active,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 30_000),
                },
            }),
            EntryPayload::Event(Event::TaskActivated {
                task: Task {
                    meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Pending,
                    deadline: Some(stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
            }),
            EntryPayload::Command(Command::TriggerTimer {
                timer: ref_to(ObjectKind::Timer, "lifecycle_execution-3", 7),
            }),
            EntryPayload::Event(Event::TimerTriggered {
                timer: Timer {
                    meta: meta_span(ObjectKind::Timer, uid(7), "lifecycle_execution-3", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::TaskTimeout,
                    status: TimerStatus::Completed,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 30_000),
                },
            }),
            EntryPayload::Command(Command::FailTask(FailTask {
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 6),
                worker_id: "".to_string(),
                error: ExecutionError::Runtime(RuntimeError::TimedOut {
                    message: "task ran past its TimeoutSeconds deadline (1700000030000)".to_string(),
                }),
            })),
            EntryPayload::Event(Event::TaskFailed(TaskFailed {
                task: Task {
                    meta: meta_span(ObjectKind::Task, uid(6), "lifecycle_execution-2", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Failed,
                    deadline: Some(stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
                error: ExecutionError::Runtime(RuntimeError::TimedOut {
                    message: "task ran past its TimeoutSeconds deadline (1700000030000)".to_string(),
                }),
            })),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(5), "lifecycle_execution-1", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::TimedOut {
                            message: "task ran past its TimeoutSeconds deadline (1700000030000)".to_string(),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateTerminated {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(5), "lifecycle_execution-1", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::TimedOut {
                            message: "task ran past its TimeoutSeconds deadline (1700000030000)".to_string(),
                        }),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::TimedOut {
                        message: "task ran past its TimeoutSeconds deadline (1700000030000)".to_string(),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(4), "lifecycle_execution-0", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::TimedOut {
                            message: "task ran past its TimeoutSeconds deadline (1700000030000)".to_string(),
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
                    error: ExecutionError::Runtime(RuntimeError::TimedOut {
                        message: "task ran past its TimeoutSeconds deadline (1700000030000)".to_string(),
                    }),
                },
            })),
            EntryPayload::Event(Event::ThreadTerminated {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(4), "lifecycle_execution-0", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::TimedOut {
                            message: "task ran past its TimeoutSeconds deadline (1700000030000)".to_string(),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ExecutionTerminating {
                execution: Execution {
                    deadline: None,
                    meta: meta_span(ObjectKind::Execution, uid(3), "lifecycle_execution", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::TimedOut {
                            message: "task ran past its TimeoutSeconds deadline (1700000030000)".to_string(),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::ExecutionTerminated {
                execution: Execution {
                    deadline: None,
                    meta: meta_span(ObjectKind::Execution, uid(3), "lifecycle_execution", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::TimedOut {
                            message: "task ran past its TimeoutSeconds deadline (1700000030000)".to_string(),
                        }),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
        ],
        status: ExecutionStatus::Terminated(TerminationReason::Failed {
            // The message names the instant the engine computed, so a deadline off by the arm's own
            // reading rather than the state's is a diff here and not merely "it timed out".
            error: ExecutionError::Runtime(RuntimeError::TimedOut {
                message: format!(
                    "task ran past its TimeoutSeconds deadline ({})",
                    VIRTUAL_EPOCH_MILLIS + 30_000
                ),
            }),
        }),
        acts: vec![Act::Advance(Signal::TimerArmed, Duration::from_secs(30))],
    };
    run_typed_case(&case).await;
}

// A deadline the engine *arms* is a failure like any other once it fires: a `Catch` naming
// `States.Timeout` takes the run down the catcher's `Next` and the execution completes, so the
// timeout cannot be told apart from a worker-reported failure by the routing it drives. The arm is
// also shown not to be reached early — the clock stops one second short of it first, and the chain
// has no room for a record that would have been written there.
#[rustfmt::skip]
#[tokio::test]
async fn task_timeout_caught_routes_on_its_catcher() {
    let definition = r#"{
    "StartAt": "T",
    "States": {
        "T": {
            "Type": "Task",
            "Resource": "late",
            "TimeoutSeconds": 30,
            "Catch": [ { "ErrorEquals": ["States.Timeout"], "Next": "Ok" } ],
            "Next": "Bad"
        },
        "Ok": { "Type": "Succeed", "Output": { "caught": true } },
        "Bad": { "Type": "Pass", "End": true }
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
                    checksum: 6109696600079096977,
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
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/T"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
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
                    state_path: path("/States/T"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateTask(ActivateTask {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 6),
                resource: "late".to_string(),
                arguments: json!({"n": 1}),
                retry_plan: vec![],
                deadline: Some(stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
            })),
            EntryPayload::Event(Event::TimerActivated {
                timer: Timer {
                    meta: meta(ObjectKind::Timer, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::TaskTimeout,
                    status: TimerStatus::Active,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 30_000),
                },
            }),
            EntryPayload::Event(Event::TaskActivated {
                task: Task {
                    meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "late".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Pending,
                    deadline: Some(stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
            }),
            EntryPayload::Command(Command::TriggerTimer {
                timer: ref_to(ObjectKind::Timer, "lifecycle_execution-3", 7),
            }),
            EntryPayload::Event(Event::TimerTriggered {
                timer: Timer {
                    meta: meta_span(ObjectKind::Timer, uid(7), "lifecycle_execution-3", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    purpose: TimerPurpose::TaskTimeout,
                    status: TimerStatus::Completed,
                    deadline: stamp(VIRTUAL_EPOCH_MILLIS + 30_000),
                },
            }),
            EntryPayload::Command(Command::FailTask(FailTask {
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 6),
                worker_id: "".to_string(),
                error: ExecutionError::Runtime(RuntimeError::TimedOut {
                    message: "task ran past its TimeoutSeconds deadline (1700000030000)".to_string(),
                }),
            })),
            EntryPayload::Event(Event::TaskFailed(TaskFailed {
                task: Task {
                    meta: meta_span(ObjectKind::Task, uid(6), "lifecycle_execution-2", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "late".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Failed,
                    deadline: Some(stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
                error: ExecutionError::Runtime(RuntimeError::TimedOut {
                    message: "task ran past its TimeoutSeconds deadline (1700000030000)".to_string(),
                }),
            })),
            // The caught failure completes the `Task` state as a *success* — the catcher's `Next` is
            // what the state routes on — so its output is the input the state was holding, not the
            // failure: the error is carried by the catcher's activation, nowhere else.
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(5), "lifecycle_execution-1", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
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
                next: pointer("/States/Ok"),
            })),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/Ok"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(8), "lifecycle_execution-4", stamp(VIRTUAL_EPOCH_MILLIS + 30_000), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Ok"),
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
                    meta: meta_span(ObjectKind::Activity, uid(8), "lifecycle_execution-4", stamp(VIRTUAL_EPOCH_MILLIS + 30_000), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Ok"),
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
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(8), "lifecycle_execution-4", stamp(VIRTUAL_EPOCH_MILLIS + 30_000), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Ok"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            // `Succeed`'s own `Output` is the state's *result*, distinct from the raw output it read
            // off its input: both are pinned here, so a state that echoed its input instead would show.
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(8), "lifecycle_execution-4", stamp(VIRTUAL_EPOCH_MILLIS + 30_000), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Ok"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"n": 1})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"caught": true})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!({"caught": true}),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(4), "lifecycle_execution-0", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"caught": true})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(4), "lifecycle_execution-0", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"caught": true})),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!({"caught": true}),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta_span(ObjectKind::Execution, uid(3), "lifecycle_execution", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"caught": true})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta_span(ObjectKind::Execution, uid(3), "lifecycle_execution", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"caught": true})),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![
            Act::Advance(Signal::TimerArmed, Duration::from_secs(29)),
            Act::Advance(Signal::None, Duration::from_secs(1)),
        ],
    };
    run_typed_case(&case).await;
}

// A retry's backoff is not a timer the run wakes on — it is an instant (`retry_state.next_available_at`)
// the *poll* compares the clock against — so this case moves the clock onto it and asks for work again,
// unlike `task_timeout_seconds_fails_the_run`, whose deadline the engine arms itself.
#[rustfmt::skip]
#[tokio::test]
async fn task_retry_re_arms_across_the_backoff() {
    let definition = r#"{
    "StartAt": "T",
    "States": {
        "T": {
            "Type": "Task",
            "Resource": "r",
            "Retry": [
                { "ErrorEquals": ["States.ALL"], "IntervalSeconds": 30, "BackoffRate": 1, "MaxAttempts": 2 }
            ],
            "Next": "P"
        },
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
                    checksum: 5272769823661151196,
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
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/T"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
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
                    state_path: path("/States/T"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateTask(ActivateTask {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 6),
                resource: "r".to_string(),
                arguments: json!({"n": 1}),
                retry_plan: vec![
                    RetryPolicy {
                        error_equals: vec!["States.ALL".to_string()],
                        interval_seconds: 30,
                        max_attempts: 2,
                        backoff_rate: 1.0,
                        max_delay_seconds: None,
                    },
                ],
                deadline: None,
            })),
            EntryPayload::Event(Event::TaskActivated {
                task: Task {
                    meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Pending,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![
                        RetryPolicy {
                            error_equals: vec!["States.ALL".to_string()],
                            interval_seconds: 30,
                            max_attempts: 2,
                            backoff_rate: 1.0,
                            max_delay_seconds: None,
                        },
                    ],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
            }),
            EntryPayload::Command(Command::ClaimTasks(ClaimTasks {
                request_id: request(2),
                worker_id: "w1".to_string(),
                resource: "r".to_string(),
                max_tasks: 10,
                lease_seconds: 60,
            })),
            EntryPayload::Event(Event::TasksClaimed(TasksClaimed {
                request_id: request(2),
                tasks: vec![
                    Task {
                        meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                            .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                        execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                        resource: "r".to_string(),
                        arguments: json!({"n": 1}),
                        status: TaskStatus::Running,
                        deadline: None,
                        worker_id: Some("w1".to_string()),
                        lease_expires_at: Some(stamp(VIRTUAL_EPOCH_MILLIS + 60_000)),
                        retry_plan: vec![
                            RetryPolicy {
                                error_equals: vec!["States.ALL".to_string()],
                                interval_seconds: 30,
                                max_attempts: 2,
                                backoff_rate: 1.0,
                                max_delay_seconds: None,
                            },
                        ],
                        retry_state: RetryState {
                            attempts: 0,
                            retrier_attempts: vec![],
                            next_available_at: None,
                        },
                    },
                ],
            })),
            EntryPayload::Command(Command::FailTask(FailTask {
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 0),
                worker_id: "w1".to_string(),
                error: ExecutionError::Runtime(RuntimeError::StateFailed {
                    state: "T".to_string(),
                    error: "boom".to_string(),
                    output: Box::new(json!(null)),
                }),
            })),
            EntryPayload::Event(Event::TaskFailed(TaskFailed {
                task: Task {
                    meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Pending,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![
                        RetryPolicy {
                            error_equals: vec!["States.ALL".to_string()],
                            interval_seconds: 30,
                            max_attempts: 2,
                            backoff_rate: 1.0,
                            max_delay_seconds: None,
                        },
                    ],
                    retry_state: RetryState {
                        attempts: 1,
                        retrier_attempts: vec![
                            RetrierAttemptState {
                                attempt_count: 1,
                                last_retry_at: Some(epoch()),
                            },
                        ],
                        next_available_at: Some(stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
                    },
                },
                error: ExecutionError::Runtime(RuntimeError::StateFailed {
                    state: "T".to_string(),
                    error: "boom".to_string(),
                    output: Box::new(json!(null)),
                }),
            })),
            EntryPayload::Command(Command::ClaimTasks(ClaimTasks {
                request_id: request(3),
                worker_id: "w1".to_string(),
                resource: "r".to_string(),
                max_tasks: 10,
                lease_seconds: 60,
            })),
            EntryPayload::Event(Event::TasksClaimed(TasksClaimed {
                request_id: request(3),
                tasks: vec![
                    Task {
                        meta: meta_span(ObjectKind::Task, uid(6), "lifecycle_execution-2", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                            .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                        execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                        resource: "r".to_string(),
                        arguments: json!({"n": 1}),
                        status: TaskStatus::Running,
                        deadline: None,
                        worker_id: Some("w1".to_string()),
                        lease_expires_at: Some(stamp(VIRTUAL_EPOCH_MILLIS + 90_000)),
                        retry_plan: vec![
                            RetryPolicy {
                                error_equals: vec!["States.ALL".to_string()],
                                interval_seconds: 30,
                                max_attempts: 2,
                                backoff_rate: 1.0,
                                max_delay_seconds: None,
                            },
                        ],
                        retry_state: RetryState {
                            attempts: 1,
                            retrier_attempts: vec![
                                RetrierAttemptState {
                                    attempt_count: 1,
                                    last_retry_at: Some(epoch()),
                                },
                            ],
                            next_available_at: None,
                        },
                    },
                ],
            })),
            EntryPayload::Command(Command::CompleteTask(CompleteTask {
                request_id: request(5),
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 0),
                worker_id: "w1".to_string(),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::TaskCompleted(TaskCompleted {
                request_id: request(5),
                task: Task {
                    meta: meta_span(ObjectKind::Task, uid(6), "lifecycle_execution-2", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Completed,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![
                        RetryPolicy {
                            error_equals: vec!["States.ALL".to_string()],
                            interval_seconds: 30,
                            max_attempts: 2,
                            backoff_rate: 1.0,
                            max_delay_seconds: None,
                        },
                    ],
                    retry_state: RetryState {
                        attempts: 1,
                        retrier_attempts: vec![
                            RetrierAttemptState {
                                attempt_count: 1,
                                last_retry_at: Some(epoch()),
                            },
                        ],
                        next_available_at: None,
                    },
                },
                output: json!({"ok": true}),
            })),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(5), "lifecycle_execution-1", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"ok": true})),
                    activity_state: None,
                    retry_state: Some(RetryState {
                        attempts: 1,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    }),
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(5), "lifecycle_execution-1", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"ok": true})),
                    activity_state: None,
                    retry_state: Some(RetryState {
                        attempts: 1,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    }),
                    output: Some(json!({"ok": true})),
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
                input: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(7), "lifecycle_execution-3", stamp(VIRTUAL_EPOCH_MILLIS + 30_000), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"ok": true}),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(7), "lifecycle_execution-3", stamp(VIRTUAL_EPOCH_MILLIS + 30_000), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"ok": true}),
                    input: Some(json!({"ok": true})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-3", 7),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(7), "lifecycle_execution-3", stamp(VIRTUAL_EPOCH_MILLIS + 30_000), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"ok": true}),
                    input: Some(json!({"ok": true})),
                    raw_output: Some(json!({"ok": true})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(7), "lifecycle_execution-3", stamp(VIRTUAL_EPOCH_MILLIS + 30_000), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"ok": true}),
                    input: Some(json!({"ok": true})),
                    raw_output: Some(json!({"ok": true})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"ok": true})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(4), "lifecycle_execution-0", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"ok": true})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(4), "lifecycle_execution-0", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"ok": true})),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta_span(ObjectKind::Execution, uid(3), "lifecycle_execution", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"ok": true})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta_span(ObjectKind::Execution, uid(3), "lifecycle_execution", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"ok": true})),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![
            Act::Drive(
                Signal::TaskArmed,
                Call::Poll {
                    worker_id: "w1".to_string(),
                    resource: "r".to_string(),
                    max_tasks: 10,
                    lease_seconds: 60,
                    expect: 1,
                },
            ),
            Act::Drive(
                Signal::TasksClaimed,
                Call::Fail {
                    worker_id: "w1".to_string(),
                    task: name("lifecycle_execution-2"),
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "T".to_string(),
                        error: "boom".to_string(),
                        output: Box::new(serde_json::Value::Null),
                    }),
                },
            ),
            Act::Drive(
                Signal::TaskRequeued,
                Call::Poll {
                    worker_id: "w1".to_string(),
                    resource: "r".to_string(),
                    max_tasks: 10,
                    lease_seconds: 60,
                    expect: 0,
                },
            ),
            Act::Advance(Signal::None, Duration::from_secs(30)),
            Act::Drive(
                Signal::None,
                Call::Poll {
                    worker_id: "w1".to_string(),
                    resource: "r".to_string(),
                    max_tasks: 10,
                    lease_seconds: 60,
                    expect: 1,
                },
            ),
            Act::Drive(
                Signal::TasksClaimed,
                Call::Complete {
                    worker_id: "w1".to_string(),
                    task: name("lifecycle_execution-2"),
                    request_id: request(5),
                    output: json!({ "ok": true }),
                },
            ),
        ],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn task_catch_routes_on_a_failed_task() {
    let definition = r#"{
    "StartAt": "T",
    "States": {
        "T": {
            "Type": "Task",
            "Resource": "r",
            "Catch": [ { "ErrorEquals": ["States.ALL"], "Next": "Recover" } ],
            "Next": "P"
        },
        "Recover": { "Type": "Pass", "End": true },
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
                    checksum: 2404642578733682253,
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
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/T"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
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
                    state_path: path("/States/T"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateTask(ActivateTask {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 6),
                resource: "r".to_string(),
                arguments: json!({"n": 1}),
                retry_plan: vec![],
                deadline: None,
            })),
            EntryPayload::Event(Event::TaskActivated {
                task: Task {
                    meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Pending,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
            }),
            EntryPayload::Command(Command::ClaimTasks(ClaimTasks {
                request_id: request(2),
                worker_id: "w1".to_string(),
                resource: "r".to_string(),
                max_tasks: 10,
                lease_seconds: 60,
            })),
            EntryPayload::Event(Event::TasksClaimed(TasksClaimed {
                request_id: request(2),
                tasks: vec![
                    Task {
                        meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                            .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                        execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                        resource: "r".to_string(),
                        arguments: json!({"n": 1}),
                        status: TaskStatus::Running,
                        deadline: None,
                        worker_id: Some("w1".to_string()),
                        lease_expires_at: Some(stamp(VIRTUAL_EPOCH_MILLIS + 60_000)),
                        retry_plan: vec![],
                        retry_state: RetryState {
                            attempts: 0,
                            retrier_attempts: vec![],
                            next_available_at: None,
                        },
                    },
                ],
            })),
            EntryPayload::Command(Command::FailTask(FailTask {
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 0),
                worker_id: "w1".to_string(),
                error: ExecutionError::Runtime(RuntimeError::StateFailed {
                    state: "T".to_string(),
                    error: "boom".to_string(),
                    output: Box::new(json!(null)),
                }),
            })),
            EntryPayload::Event(Event::TaskFailed(TaskFailed {
                task: Task {
                    meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Failed,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
                error: ExecutionError::Runtime(RuntimeError::StateFailed {
                    state: "T".to_string(),
                    error: "boom".to_string(),
                    output: Box::new(json!(null)),
                }),
            })),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
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
                next: pointer("/States/Recover"),
            })),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/Recover"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Recover"),
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
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Recover"),
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
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Recover"),
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
                    meta: meta(ObjectKind::Activity, uid(7), "lifecycle_execution-3")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/Recover"),
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
                    start_at: "T".to_string(),
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
                    start_at: "T".to_string(),
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
        acts: vec![
            Act::Drive(
                Signal::TaskArmed,
                Call::Poll {
                    worker_id: "w1".to_string(),
                    resource: "r".to_string(),
                    max_tasks: 10,
                    lease_seconds: 60,
                    expect: 1,
                },
            ),
            Act::Drive(
                Signal::TasksClaimed,
                Call::Fail {
                    worker_id: "w1".to_string(),
                    task: name("lifecycle_execution-2"),
                    error: ExecutionError::Runtime(RuntimeError::StateFailed {
                        state: "T".to_string(),
                        error: "boom".to_string(),
                        output: Box::new(serde_json::Value::Null),
                    }),
                },
            ),
        ],
    };
    run_typed_case(&case).await;
}

#[rustfmt::skip]
#[tokio::test]
async fn task_with_an_invalid_timeout_seconds_is_a_definition_error() {
    let definition = r#"{
    "StartAt": "T",
    "States": {
        "T": { "Type": "Task", "Resource": "r", "TimeoutSeconds": 0, "Next": "P" },
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
                    checksum: 10875901671326543404,
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
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/T"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
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
                    state_path: path("/States/T"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateTask(ActivateTask {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 6),
                resource: "r".to_string(),
                arguments: json!({"n": 1}),
                retry_plan: vec![],
                deadline: None,
            })),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Task TimeoutSeconds must be a positive integer".to_string())),
                },
            })),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Task TimeoutSeconds must be a positive integer".to_string())),
                },
            })),
            EntryPayload::Event(Event::TaskActivated {
                task: Task {
                    meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Pending,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
            }),
            EntryPayload::Event(Event::StateTerminating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
                    status: ActivityStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Task TimeoutSeconds must be a positive integer".to_string())),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CancelTask {
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 6),
            }),
            EntryPayload::Event(Event::ThreadTerminating {
                thread: Thread {
                    meta: meta(ObjectKind::Thread, uid(4), "lifecycle_execution-0")
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Task TimeoutSeconds must be a positive integer".to_string())),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateExecution(TerminateExecution {
                name: name("lifecycle_execution"),
                uid: Some(uid(3)),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Task TimeoutSeconds must be a positive integer".to_string())),
                },
            })),
            EntryPayload::Command(Command::TerminateState(TerminateState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Task TimeoutSeconds must be a positive integer".to_string())),
                },
            })),
            EntryPayload::Event(Event::TaskCancelled {
                task: Task {
                    meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Cancelled,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
            }),
            EntryPayload::Command(Command::ContinueTerminate {
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
            }),
            EntryPayload::Event(Event::ExecutionTerminating {
                execution: Execution {
                    deadline: None,
                    meta: meta(ObjectKind::Execution, uid(3), "lifecycle_execution"),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Terminating(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Task TimeoutSeconds must be a positive integer".to_string())),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::TerminateThread(TerminateThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Task TimeoutSeconds must be a positive integer".to_string())),
                },
            })),
            EntryPayload::Event(Event::StateTerminated {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
                    status: ActivityStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Task TimeoutSeconds must be a positive integer".to_string())),
                    }),
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
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
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Terminated(TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Task TimeoutSeconds must be a positive integer".to_string())),
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
                        error: ExecutionError::Runtime(RuntimeError::InvalidDefinition("Task TimeoutSeconds must be a positive integer".to_string())),
                    }),
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
        ],
        status: ExecutionStatus::Terminated(TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "Task TimeoutSeconds must be a positive integer".to_string(),
            )),
        }),
        acts: vec![],
    };
    run_typed_case(&case).await;
}

// A claim's delivery lease is a deadline the *queue* answers to rather than the state: past it the
// same entity — not failed, not re-minted, not even re-statused — is granted again, so another worker
// picks up where the first left off. Nothing is armed for the lapse, so this is the poll-side boundary
// asserted from both sides: the clock stops one second short of `lease_expires_at` and a poll must find
// nothing, then crosses it and the very next poll must hand the task over.
#[rustfmt::skip]
#[tokio::test]
async fn task_lease_lapse_is_reclaimed_by_the_next_poll() {
    let definition = r#"{
    "StartAt": "T",
    "States": {
        "T": { "Type": "Task", "Resource": "r", "Next": "P" },
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
                    checksum: 5298509650561049483,
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
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Running,
                    input: json!({"n": 1}),
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateState(ActivateState {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                state_path: path("/States/T"),
                input: json!({"n": 1}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta(ObjectKind::Activity, uid(5), "lifecycle_execution-1")
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
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
                    state_path: path("/States/T"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::ActivateTask(ActivateTask {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                owner: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 6),
                resource: "r".to_string(),
                arguments: json!({"n": 1}),
                retry_plan: vec![],
                deadline: None,
            })),
            EntryPayload::Event(Event::TaskActivated {
                task: Task {
                    meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Pending,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
            }),
            EntryPayload::Command(Command::ClaimTasks(ClaimTasks {
                request_id: request(2),
                worker_id: "w1".to_string(),
                resource: "r".to_string(),
                max_tasks: 10,
                lease_seconds: 30,
            })),
            EntryPayload::Event(Event::TasksClaimed(TasksClaimed {
                request_id: request(2),
                tasks: vec![
                    Task {
                        meta: meta(ObjectKind::Task, uid(6), "lifecycle_execution-2")
                            .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                        execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                        resource: "r".to_string(),
                        arguments: json!({"n": 1}),
                        status: TaskStatus::Running,
                        deadline: None,
                        worker_id: Some("w1".to_string()),
                        lease_expires_at: Some(stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
                        retry_plan: vec![],
                        retry_state: RetryState {
                            attempts: 0,
                            retrier_attempts: vec![],
                            next_available_at: None,
                        },
                    },
                ],
            })),
            // The clock has now crossed `lease_expires_at`, and that is the whole of the lapse: no record
            // marks it, so what the poll finds is a `Running` task whose lease has run out — claimable,
            // still naming `w1`, re-leased in place with a fresh window.
            EntryPayload::Command(Command::ClaimTasks(ClaimTasks {
                request_id: request(3),
                worker_id: "w2".to_string(),
                resource: "r".to_string(),
                max_tasks: 10,
                lease_seconds: 30,
            })),
            EntryPayload::Event(Event::TasksClaimed(TasksClaimed {
                request_id: request(3),
                tasks: vec![
                    Task {
                        meta: meta_span(ObjectKind::Task, uid(6), "lifecycle_execution-2", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                            .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                        execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                        resource: "r".to_string(),
                        arguments: json!({"n": 1}),
                        status: TaskStatus::Running,
                        deadline: None,
                        worker_id: Some("w2".to_string()),
                        lease_expires_at: Some(stamp(VIRTUAL_EPOCH_MILLIS + 60_000)),
                        retry_plan: vec![],
                        retry_state: RetryState {
                            attempts: 0,
                            retrier_attempts: vec![],
                            next_available_at: None,
                        },
                    },
                ],
            })),
            // The second worker settles the *same* task entity (uid 6, minted once by the activation):
            // a lapsed lease hands the entity over rather than minting a retry.
            EntryPayload::Command(Command::CompleteTask(CompleteTask {
                request_id: request(4),
                task: ref_to(ObjectKind::Task, "lifecycle_execution-2", 0),
                worker_id: "w2".to_string(),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::TaskCompleted(TaskCompleted {
                request_id: request(4),
                task: Task {
                    meta: meta_span(ObjectKind::Task, uid(6), "lifecycle_execution-2", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    resource: "r".to_string(),
                    arguments: json!({"n": 1}),
                    status: TaskStatus::Completed,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState {
                        attempts: 0,
                        retrier_attempts: vec![],
                        next_available_at: None,
                    },
                },
                output: json!({"ok": true}),
            })),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-1", 5),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(5), "lifecycle_execution-1", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"ok": true})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(5), "lifecycle_execution-1", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/T"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"n": 1}),
                    input: Some(json!({"n": 1})),
                    raw_output: Some(json!({"ok": true})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"ok": true})),
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
                input: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::StateActivating {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(7), "lifecycle_execution-3", stamp(VIRTUAL_EPOCH_MILLIS + 30_000), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"ok": true}),
                    input: None,
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateActivated {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(7), "lifecycle_execution-3", stamp(VIRTUAL_EPOCH_MILLIS + 30_000), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Running,
                    raw_input: json!({"ok": true}),
                    input: Some(json!({"ok": true})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Command(Command::CompleteState(CompleteState {
                activity: ref_to(ObjectKind::Activity, "lifecycle_execution-3", 7),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::StateCompleting {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(7), "lifecycle_execution-3", stamp(VIRTUAL_EPOCH_MILLIS + 30_000), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completing,
                    raw_input: json!({"ok": true}),
                    input: Some(json!({"ok": true})),
                    raw_output: Some(json!({"ok": true})),
                    activity_state: None,
                    retry_state: None,
                    output: None,
                },
            }),
            EntryPayload::Event(Event::StateCompleted {
                activity: Activity {
                    meta: meta_span(ObjectKind::Activity, uid(7), "lifecycle_execution-3", stamp(VIRTUAL_EPOCH_MILLIS + 30_000), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States/P"),
                    status: ActivityStatus::Completed,
                    raw_input: json!({"ok": true}),
                    input: Some(json!({"ok": true})),
                    raw_output: Some(json!({"ok": true})),
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!({"ok": true})),
                },
            }),
            EntryPayload::Command(Command::CompleteThread(CompleteThread {
                thread: ref_to(ObjectKind::Thread, "lifecycle_execution-0", 4),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::ThreadCompleting {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(4), "lifecycle_execution-0", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"ok": true})),
                },
            }),
            EntryPayload::Event(Event::ThreadCompleted {
                thread: Thread {
                    meta: meta_span(ObjectKind::Thread, uid(4), "lifecycle_execution-0", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000))
                        .with_owner(ref_to(ObjectKind::Execution, "lifecycle_execution", 3)),
                    execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                    state_path: path("/States"),
                    start_at: "T".to_string(),
                    index: 0,
                    status: ThreadStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"ok": true})),
                },
            }),
            EntryPayload::Command(Command::CompleteExecution(CompleteExecution {
                execution: ref_to(ObjectKind::Execution, "lifecycle_execution", 3),
                output: json!({"ok": true}),
            })),
            EntryPayload::Event(Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    meta: meta_span(ObjectKind::Execution, uid(3), "lifecycle_execution", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completing,
                    input: json!({"n": 1}),
                    output: Some(json!({"ok": true})),
                },
            }),
            EntryPayload::Event(Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    meta: meta_span(ObjectKind::Execution, uid(3), "lifecycle_execution", epoch(), stamp(VIRTUAL_EPOCH_MILLIS + 30_000)),
                    flow_version: ref_to(ObjectKind::FlowVersion, "lifecycle_flow-1", 2),
                    status: ExecutionStatus::Completed,
                    input: json!({"n": 1}),
                    output: Some(json!({"ok": true})),
                },
            }),
        ],
        status: ExecutionStatus::Completed,
        acts: vec![
            Act::Drive(
                Signal::TaskArmed,
                Call::Poll {
                    worker_id: "w1".to_string(),
                    resource: "r".to_string(),
                    max_tasks: 10,
                    lease_seconds: 30,
                    expect: 1,
                },
            ),
            // A poll while the lease stands: the task is neither handed to a second worker nor written
            // about, which the chain can only show by the `TasksClaimed` it does not contain.
            Act::Drive(
                Signal::TasksClaimed,
                Call::Poll {
                    worker_id: "w2".to_string(),
                    resource: "r".to_string(),
                    max_tasks: 10,
                    lease_seconds: 30,
                    expect: 0,
                },
            ),
            Act::Advance(Signal::None, Duration::from_secs(29)),
            Act::Drive(
                Signal::None,
                Call::Poll {
                    worker_id: "w2".to_string(),
                    resource: "r".to_string(),
                    max_tasks: 10,
                    lease_seconds: 30,
                    expect: 0,
                },
            ),
            Act::Advance(Signal::None, Duration::from_secs(1)),
            // Nothing new-gates this poll: the lapse is the clock's own business, so the act rests on
            // the instant the move just reached — and the grant is what proves the poll read it.
            Act::Drive(
                Signal::None,
                Call::Poll {
                    worker_id: "w2".to_string(),
                    resource: "r".to_string(),
                    max_tasks: 10,
                    lease_seconds: 30,
                    expect: 1,
                },
            ),
            Act::Drive(
                Signal::TasksClaimed,
                Call::Complete {
                    worker_id: "w2".to_string(),
                    task: name("lifecycle_execution-2"),
                    request_id: request(4),
                    output: json!({ "ok": true }),
                },
            ),
        ],
    };
    run_typed_case(&case).await;
}
