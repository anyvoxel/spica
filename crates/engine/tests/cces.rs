//! Unit tests for the CCES building blocks: the Storage projection, causality/atomicity of the
//! StreamProcessor's output, the ing/ed lifecycle split, the deferred-ed cascade on a Completing or
//! Terminating parent, and the cancel/timeout race guards.

mod common;

use serde_json::{Value, json};
use spica_asl::StateMachine;
use spica_engine::{
    ActivityId, ActivityState, ActivityStatus, ActivityValue, Command, Entry, EntryId,
    EntryPayload, Event, ExecutionError, ExecutionId, ExecutionStatus, ExecutionValue, Flow,
    FlowId, FlowName, FlowStatus, FlowVersion, FlowVersionId, InMemoryLogStream, LogStream, NodeId,
    RejectionType, RequestId, RetryState, Scheduler, Storage, StreamProcessor, TaskId, TaskStatus,
    TaskValue, TerminationReason, TimerId, TimerPurpose, TimerSink, TimerStatus, TimerValue,
    Timestamp, Variables,
};
use spica_scheduler::InMemoryScheduler;
use spica_storage::InMemoryStorage;
use tokio_stream::StreamExt;

fn parse_sm(definition: &str) -> StateMachine {
    serde_json::from_str(definition).expect("state machine should parse")
}

/// Pre-seed `sm` as a created flow version in `storage`, returning its [`FlowVersionId`].
/// Definition resolution happens from storage at dispatch time (the `CreateExecution` command
/// carries only the flow version id), so raw-seam drivers seed the definition directly rather than
/// driving a `CreateFlow` command. The definition is stored in its raw ASL string form. Mirrors what
/// the `FlowCreated` applier folds: a `Flow` row (by name, on first appearance) + a `FlowVersion` row
/// (keyed by `flow_version_id`, executions bind to it).
async fn seed_revision(storage: &mut InMemoryStorage, sm: StateMachine) -> FlowVersionId {
    let name = FlowName::new("test_flow").expect("static name is valid");
    let flow_id = FlowId::new();
    let flow_version_id = FlowVersionId::new();
    let created_at = Timestamp::from_millis(0);
    storage
        .put_flow(Flow {
            flow_id,
            name: name.clone(),
            created_at,
            updated_at: created_at,
            status: FlowStatus::Active,
            // Newest version pointer — the version seeded below is the only one, so it is the latest.
            latest_flow_version_id: flow_version_id,
        })
        .await
        .unwrap();
    storage
        .put_flow_version(FlowVersion {
            flow_version_id,
            flow_id,
            name,
            version: 1,
            definition: serde_json::to_string(&sm).expect("state machine serializes"),
            created_at,
        })
        .await
        .unwrap();
    flow_version_id
}

/// Applies events to storage through the real [`EventDispatcher`] + in-memory scheduler, mirroring
/// the event path of `StreamProcessor::run` — so timer events actually reach the scheduler. `EntryId`
/// in the applier context defaults to 1 (it only marks causality for scheduling, which projection
/// tests don't otherwise exercise).
struct Projector {
    dispatcher: spica_engine::EventDispatcher,
    scheduler: std::sync::Arc<InMemoryScheduler>,
}

impl Projector {
    fn new() -> Self {
        // No sink is attached here: projection tests only fold state and never wait on a fired
        // timer, so the scheduler's expiry callback goes unobserved. The service task exits once
        // the strong references drop below.
        Self {
            dispatcher: spica_engine::EventDispatcher::new(),
            scheduler: InMemoryScheduler::spawn(),
        }
    }

    async fn apply(&self, storage: &mut InMemoryStorage, event: &Event) {
        // Fixed deterministic timestamp for incidental applies; use `apply_at` to test time.
        self.apply_at(storage, event, spica_engine::Timestamp::from_millis(1))
            .await;
    }

    /// Apply one event with a caller-chosen entry timestamp (the tuple `(event, timestamp)` models a
    /// log entry). Lets tests observe that projection `created_at`/`updated_at` fold the entry's
    /// frozen timestamp deterministically.
    async fn apply_at(
        &self,
        storage: &mut InMemoryStorage,
        event: &Event,
        timestamp: spica_engine::Timestamp,
    ) {
        let mut txn = storage.begin_txn().unwrap();
        {
            let mut ctx = spica_engine::ApplierContext {
                storage: &mut *txn,
                scheduler: self.scheduler.as_ref(),
                cause_id: spica_engine::EntryId::new(1),
                timestamp,
            };
            self.dispatcher.apply(&mut ctx, event).await.unwrap();
        }
        txn.commit(None).unwrap();
    }
}

impl Default for Projector {
    fn default() -> Self {
        Self::new()
    }
}

/// A test [`TimerSink`] that appends the fired `TriggerTimer` to a shared `logstream` — the same
/// controlled write a real engine's `EngineTimerSink` performs, so raw-seam drivers exercise the
/// same push-through-engine path a `EngineBuilder::start()` would wire.
struct AppendingSink {
    log: std::sync::Arc<InMemoryLogStream<EntryPayload>>,
}

#[async_trait::async_trait]
impl TimerSink for AppendingSink {
    async fn trigger(&self, timer: TimerId, cause_id: EntryId) {
        // Envelope the fired command with placeholders; the log stamps the real position and stream.
        self.log
            .append(vec![Entry {
                stream_id: spica_engine::StreamId::nil(), // the log stamps the stream on append
                entry_id: spica_engine::EntryId::nil(),
                cause_id: Some(cause_id),
                timestamp: spica_engine::Timestamp::now(),
                payload: EntryPayload::Command(Command::TriggerTimer { timer }),
            }])
            .await
            .expect("raw-seam test log append cannot fail");
    }
}

/// Drives `sm` with `input` end-to-end through the raw CCES seam (no `Engine::start` async
/// wrapping), collecting every event applied to Storage. Stops when the execution reaches a
/// terminal status. Timer scheduling mirrors `StreamProcessor::run`: a real [`Scheduler`] owns a
/// `DelayQueue`, `TimerActivated`/`TimerCancelled` events are fed to it via the
/// [`EventDispatcher`], and this driver injects an [`AppendingSink`] (mirroring the engine's
/// `EngineTimerSink`) that the scheduler calls on expiry to append the fired `TriggerTimer` — the
/// push-through-engine path that keeps the log single-writer and strictly ordered.
async fn collect_events(sm: StateMachine, input: Value) -> Vec<Event> {
    // Boxed in `Arc` so the injected [`AppendingSink`] and this driver share the same underlying log.
    let logstream = std::sync::Arc::new(InMemoryLogStream::new());
    let mut storage = InMemoryStorage::new();
    // Create the definition into storage first: `CreateExecution` carries only the flow id,
    // and the handler resolves the machine from storage at dispatch time.
    let flow_id = seed_revision(&mut storage, sm).await;
    let _execution_id = common::submit_seed(flow_id, input, &*logstream)
        .await
        .unwrap();

    let mut processor = StreamProcessor::new();
    let dispatcher = spica_engine::EventDispatcher::new();
    // Self-contained in-memory scheduler + task service, wired the same way `StreamProcessor::run` wires
    // them. Fired timers are pushed back onto this log via the injected [`AppendingSink`]; settled
    // commands are pulled from the task service via `next_settled`.
    let scheduler = InMemoryScheduler::spawn();
    // The engine (and this driver) attach the controlled write entry before any timer can arm.
    scheduler.attach_sink(std::sync::Arc::new(AppendingSink {
        log: std::sync::Arc::clone(&logstream),
    }));

    let mut stream = logstream.stream_read(spica_engine::EntryId::new(1));
    let mut events = Vec::new();
    loop {
        tokio::select! {
            entry = stream.next() => {
                let Some(entry) = entry else { break; };
                match entry.payload {
                    EntryPayload::Command(command) => {
                        let out = processor
                            .dispatch(&command, &storage, entry.entry_id)
                            .await
                            .unwrap();
                        logstream.append(out).await.unwrap();
                    }
                    EntryPayload::Event(event) => {
                        let terminal = matches!(
                            &event,
                            Event::ExecutionCompleted { .. } | Event::ExecutionTerminated { .. }
                        );
                        let mut txn = storage.begin_txn().unwrap();
                        {
                            let mut ctx = spica_engine::ApplierContext {
                                storage: &mut *txn,
                                scheduler: scheduler.as_ref(),
                                cause_id: entry.entry_id,
                                timestamp: entry.timestamp,
                            };
                            dispatcher.apply(&mut ctx, &event).await.unwrap();
                        }
                        txn.commit(None).unwrap();
                        events.push(event);
                        if terminal {
                            break;
                        }
                    }
                    EntryPayload::Reject(_) => {
                        // A refused command — nothing to fold into Storage (a rejection has no
                        // projection) and these execution drivers never await a client command that
                        // rejects, so the record is ignored here beyond its durable presence on the
                        // log. Handled explicitly to keep the match exhaustive.
                    }
                }
            }
        }
    }
    drop(scheduler);
    events
}

/// Stable orderable prefix of each event's Debug form, for asserting on emission order.
fn kind_prefix(e: &Event) -> &'static str {
    match e {
        Event::FlowCreated { .. } => "FlowCreated",
        Event::FlowVersionCreated { .. } => "FlowVersionCreated",
        Event::ExecutionCreated { .. } => "ExecutionCreated",
        Event::ExecutionCompleting { .. } => "ExecutionCompleting",
        Event::ExecutionCompleted { .. } => "ExecutionCompleted",
        Event::ExecutionTerminating { .. } => "ExecutionTerminating",
        Event::ExecutionTerminated { .. } => "ExecutionTerminated",
        Event::StateActivating { .. } => "StateActivating",
        Event::StateActivated { .. } => "StateActivated",
        Event::StateCompleting { .. } => "StateCompleting",
        Event::StateCompleted { .. } => "StateCompleted",
        Event::StateTerminating { .. } => "StateTerminating",
        Event::StateTerminated { .. } => "StateTerminated",
        Event::TimerActivated { .. } => "TimerActivated",
        Event::TimerTriggered { .. } => "TimerTriggered",
        Event::TimerCancelled { .. } => "TimerCancelled",
        Event::TaskActivated { .. } => "TaskActivated",
        Event::TaskLeased { .. } => "TaskLeased",
        Event::TaskLeaseExpired { .. } => "TaskLeaseExpired",
        Event::TaskCompleted { .. } => "TaskCompleted",
        Event::TaskFailed { .. } => "TaskFailed",
        Event::RetryScheduled { .. } => "RetryScheduled",
        Event::TaskCancelled { .. } => "TaskCancelled",
        Event::VariablesAssigned { .. } => "VariablesAssigned",
        Event::StateTransitioned { .. } => "StateTransitioned",
        Event::ParallelBranchSpawned { .. } => "ParallelBranchSpawned",
    }
}

/// Position of the first event of a given kind in `v`, panicking if missing.
fn pos(v: &[Event], prefix: &str) -> usize {
    v.iter()
        .position(|e| kind_prefix(e) == prefix)
        .unwrap_or_else(|| {
            panic!(
                "missing {prefix} in {:?}",
                v.iter().map(kind_prefix).collect::<Vec<_>>()
            )
        })
}

// ── Storage projection ───────────────────────────────────────────────────────

#[tokio::test]
async fn storage_projects_execution_and_activity_state() {
    let exec = ExecutionId::nil();
    let activity = ActivityId::new();
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();

    projector
        .apply(
            &mut storage,
            &Event::ExecutionCreated {
                request_id: RequestId::nil(),
                execution: ExecutionValue {
                    id: exec,
                    flow_version_id: FlowVersionId::nil(),
                    root_execution: exec,
                    parent: None,
                    state_path: None,
                    status: ExecutionStatus::Running,
                    input: json!({ "x": 1 }),
                    output: None,
                },
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::StateActivating {
                activity: ActivityValue {
                    id: activity,
                    execution: exec,
                    root_execution: exec,
                    parent: NodeId::Execution(exec),
                    state_path: jsonptr::PointerBuf::parse("/states/S").unwrap(),
                    status: ActivityStatus::Running,
                    raw_input: json!({ "x": 1 }),
                    input: json!({ "x": 1 }),
                    raw_output: None,
                    activity_state: ActivityState::Leaf,
                    retry_state: RetryState::default(),
                    output: None,
                },
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::VariablesAssigned {
                execution: exec,
                variables: Variables::from([("g".to_string(), json!("hi"))]),
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::ExecutionCompleted {
                execution: ExecutionValue {
                    id: exec,
                    flow_version_id: FlowVersionId::nil(),
                    root_execution: exec,
                    parent: None,
                    state_path: None,
                    status: ExecutionStatus::Completed,
                    input: json!({ "x": 1 }),
                    output: Some(json!({ "done": true })),
                },
            },
        )
        .await;

    let e = storage.get_execution(exec).await.unwrap().unwrap();
    assert_eq!(e.status, ExecutionStatus::Completed);
    assert_eq!(e.variables.get("g"), Some(&json!("hi")));
    assert_eq!(e.output, Some(json!({ "done": true })));
    assert!(matches!(e.status, ExecutionStatus::Completed));
}

/// Projection timing facts fold the applied entry's frozen `timestamp`: `created_at` is stamped once
/// at birth (`== updated_at`), and `updated_at` advances on every later write while `created_at`
/// stays put. Covering Execution, Activity, Timer, and Task rows.
#[tokio::test]
async fn projection_records_create_and_update_timestamps() {
    let exec = ExecutionId::new();
    let activity = ActivityId::new();
    let timer = TimerId::new();
    let task = TaskId::new();
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();
    let t = |ms: u64| spica_engine::Timestamp::from_millis(ms);

    // Birth at t=100: created_at == updated_at == 100.
    projector
        .apply_at(
            &mut storage,
            &Event::ExecutionCreated {
                request_id: RequestId::nil(),
                execution: ExecutionValue {
                    id: exec,
                    flow_version_id: FlowVersionId::nil(),
                    root_execution: exec,
                    parent: None,
                    state_path: None,
                    status: ExecutionStatus::Running,
                    input: json!({}),
                    output: None,
                },
            },
            t(100),
        )
        .await;
    let e = storage.get_execution(exec).await.unwrap().unwrap();
    assert_eq!(e.created_at, t(100));
    assert_eq!(e.updated_at, t(100));

    // An update at t=200 advances updated_at, leaves created_at.
    projector
        .apply_at(
            &mut storage,
            &Event::VariablesAssigned {
                execution: exec,
                variables: Variables::from([("k".to_string(), json!(1))]),
            },
            t(200),
        )
        .await;
    let e = storage.get_execution(exec).await.unwrap().unwrap();
    assert_eq!(e.created_at, t(100), "created_at is immutable after birth");
    assert_eq!(e.updated_at, t(200));

    // Activity birth at t=200 (created == updated), then completion at t=300 (created stays, updated
    // advances to 300).
    projector
        .apply_at(
            &mut storage,
            &Event::StateActivating {
                activity: ActivityValue {
                    id: activity,
                    execution: exec,
                    root_execution: exec,
                    parent: NodeId::Execution(exec),
                    state_path: jsonptr::PointerBuf::parse("/states/S").unwrap(),
                    status: ActivityStatus::Running,
                    raw_input: json!({}),
                    input: json!({}),
                    raw_output: None,
                    activity_state: ActivityState::Leaf,
                    retry_state: RetryState::default(),
                    output: None,
                },
            },
            t(200),
        )
        .await;
    projector
        .apply_at(
            &mut storage,
            &Event::StateCompleted {
                activity: ActivityValue {
                    id: activity,
                    execution: exec,
                    root_execution: exec,
                    parent: NodeId::Execution(exec),
                    state_path: jsonptr::PointerBuf::parse("/states/S").unwrap(),
                    status: ActivityStatus::Completed,
                    raw_input: json!({}),
                    input: json!({}),
                    raw_output: None,
                    activity_state: ActivityState::Leaf,
                    retry_state: RetryState::default(),
                    output: Some(json!(42)),
                },
            },
            t(300),
        )
        .await;
    let a = storage.get_activity(activity).await.unwrap().unwrap();
    assert_eq!(a.created_at, t(200));
    assert_eq!(a.updated_at, t(300));

    // Timer birth at t=400 (created == updated), then completion at t=450 (created stays, updated
    // advances).
    projector
        .apply_at(
            &mut storage,
            &Event::TimerActivated {
                timer: TimerValue {
                    id: timer,
                    parent: NodeId::Execution(exec),
                    purpose: TimerPurpose::ExecutionTimeout,
                    status: TimerStatus::Active,
                    deadline: t(500),
                },
            },
            t(400),
        )
        .await;
    projector
        .apply_at(
            &mut storage,
            &Event::TimerTriggered {
                timer: TimerValue {
                    id: timer,
                    parent: NodeId::Execution(exec),
                    purpose: TimerPurpose::ExecutionTimeout,
                    status: TimerStatus::Completed,
                    deadline: t(500),
                },
            },
            t(450),
        )
        .await;
    let tm = storage.get_timer(timer).await.unwrap().unwrap();
    assert_eq!(tm.created_at, t(400));
    assert_eq!(tm.updated_at, t(450));

    // Task birth at t=600 (created == updated), then failure at t=650 (created stays, updated
    // advances).
    projector
        .apply_at(
            &mut storage,
            &Event::TaskActivated {
                task: TaskValue {
                    id: task,
                    parent: NodeId::Activity(activity),
                    resource: "urn:svc".to_string(),
                    arguments: json!({}),
                    status: TaskStatus::Pending,
                    deadline: None,
                    worker_id: None,
                    lease_until: None,
                },
            },
            t(600),
        )
        .await;
    projector
        .apply_at(
            &mut storage,
            &Event::TaskFailed {
                task: TaskValue {
                    id: task,
                    parent: NodeId::Activity(activity),
                    resource: "urn:svc".to_string(),
                    arguments: json!({}),
                    status: TaskStatus::Failed,
                    deadline: None,
                    worker_id: None,
                    lease_until: None,
                },
                error: ExecutionError::StateFailed {
                    state: "S".to_string(),
                    error: "boom".to_string(),
                    output: Value::Null,
                },
            },
            t(650),
        )
        .await;
    let tk = storage.get_task(task).await.unwrap().unwrap();
    assert_eq!(tk.created_at, t(600));
    assert_eq!(tk.updated_at, t(650));
}

// ── Causality / atomicity ────────────────────────────────────────────────────

#[tokio::test]
async fn every_non_root_entry_has_a_causal_parent() {
    let sm = parse_sm(r#"{ "StartAt": "A", "States": { "A": { "Type": "Succeed" } } }"#);
    let logstream = InMemoryLogStream::new();
    let mut storage = InMemoryStorage::new();
    // Create the definition into storage first (see `seed_revision`).
    let flow_id = seed_revision(&mut storage, sm).await;
    let _execution_id = common::submit_seed(flow_id, Value::Null, &logstream)
        .await
        .unwrap();

    let projector = Projector::new();
    let mut processor = StreamProcessor::new();
    let mut stream = logstream.stream_read(spica_engine::EntryId::new(1));
    while let Some(entry) = stream.next().await {
        match entry.payload {
            EntryPayload::Command(command) => {
                let entries = processor
                    .dispatch(&command, &storage, entry.entry_id)
                    .await
                    .unwrap();
                logstream.append(entries).await.unwrap();
            }
            EntryPayload::Event(event) => {
                let terminal = matches!(
                    &event,
                    Event::ExecutionCompleted { .. } | Event::ExecutionTerminated { .. }
                );
                projector.apply(&mut storage, &event).await;
                if terminal {
                    break;
                }
            }
            EntryPayload::Reject(_) => {
                // A refused command — nothing to fold, and this test never rejects (see the other
                // driver loop); keep the match exhaustive.
            }
        }
    }

    let entries = logstream.entries();
    assert!(!entries.is_empty());
    assert!(entries[0].cause_id.is_none(), "root entry has no cause");
    let mut known = std::collections::HashSet::new();
    known.insert(entries[0].entry_id);
    for entry in entries.iter().skip(1) {
        let parent = entry.cause_id.expect("non-root entry has a causal parent");
        assert!(
            known.contains(&parent),
            "causal parent {parent} not found among prior entries"
        );
        known.insert(entry.entry_id);
    }
}

// ── ing/ed split on the synchronous happy path ───────────────────────────────

#[tokio::test]
async fn pass_emits_ing_then_ed_in_order() {
    let sm = parse_sm(
        r#"{ "StartAt": "P", "States": { "P": { "Type": "Pass", "Output": 1, "End": true } } }"#,
    );
    let events = collect_events(sm, Value::Null).await;
    assert!(pos(&events, "ExecutionCreated") < pos(&events, "StateActivating"));
    assert!(pos(&events, "StateActivating") < pos(&events, "StateActivated"));
    assert!(pos(&events, "StateActivated") < pos(&events, "StateCompleting"));
    assert!(pos(&events, "StateCompleting") < pos(&events, "StateCompleted"));
    assert!(pos(&events, "StateCompleted") < pos(&events, "ExecutionCompleting"));
    assert!(pos(&events, "ExecutionCompleting") < pos(&events, "ExecutionCompleted"));
}

// ── Wait defers its `ed` until the armed timer fires ────────────────────────

#[tokio::test]
async fn wait_defers_state_completed_until_timer_fires() {
    let sm = parse_sm(
        r#"{
          "StartAt": "W",
          "States": {
            "W": { "Type": "Wait", "Seconds": 0, "Next": "P" },
            "P": { "Type": "Pass", "Output": { "done": true }, "End": true }
          }
        }"#,
    );
    let events = collect_events(sm, Value::Null).await;
    assert!(pos(&events, "StateActivating") < pos(&events, "TimerActivated"));
    assert!(pos(&events, "TimerActivated") < pos(&events, "TimerTriggered"));
    assert!(pos(&events, "TimerTriggered") < pos(&events, "StateCompleting"));
    assert!(pos(&events, "StateCompleting") < pos(&events, "StateCompleted"));
    assert!(pos(&events, "StateCompleted") < pos(&events, "ExecutionCompleted"));
}

// ── The cascade: TerminateExecution → sweep children → deferred ExecutionTerminated ──

#[tokio::test]
async fn terminate_execution_cancels_wait_and_drains() {
    // Set up the snapshot directly: a Running execution with a Running Wait activity that owns an
    // Active WaitResume timer. Injecting TerminateExecution must (a) emit ExecutionTerminating,
    // (b) sweep the activity and its timer, (c) drain the execution to ExecutionTerminated once
    // the children are terminal — and the cascade's emission order must be observable.
    let exec = ExecutionId::nil();
    let activity = ActivityId::new();
    let timer = TimerId::new();

    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();
    // Apply the full set-up via the real ing events so `active_children`/`parent` links are
    // projected by the same fold handlers running on the production path use.
    for ev in &[
        Event::ExecutionCreated {
            request_id: RequestId::nil(),
            execution: ExecutionValue {
                id: exec,
                flow_version_id: FlowVersionId::nil(),
                root_execution: exec,
                parent: None,
                state_path: None,
                status: ExecutionStatus::Running,
                input: Value::Null,
                output: None,
            },
        },
        Event::StateActivating {
            activity: ActivityValue {
                id: activity,
                execution: exec,
                root_execution: exec,
                parent: NodeId::Execution(exec),
                state_path: jsonptr::PointerBuf::parse("/states/W").unwrap(),
                status: ActivityStatus::Running,
                raw_input: Value::Null,
                input: Value::Null,
                raw_output: None,
                activity_state: ActivityState::Leaf,
                retry_state: RetryState::default(),
                output: None,
            },
        },
        Event::StateActivated {
            activity: ActivityValue {
                id: activity,
                execution: exec,
                root_execution: exec,
                parent: NodeId::Execution(exec),
                state_path: jsonptr::PointerBuf::parse("/states/W").unwrap(),
                status: ActivityStatus::Running,
                raw_input: Value::Null,
                input: Value::Null,
                raw_output: None,
                activity_state: ActivityState::Leaf,
                retry_state: RetryState::default(),
                output: None,
            },
        },
        Event::TimerActivated {
            timer: TimerValue {
                id: timer,
                parent: NodeId::Activity(activity),
                purpose: TimerPurpose::WaitResume,
                status: TimerStatus::Active,
                deadline: Timestamp::from_millis(1_000_000_000_000),
            },
        },
    ] {
        projector.apply(&mut storage, ev).await;
    }

    let logstream = InMemoryLogStream::new();
    let mut processor = StreamProcessor::new();

    // Inject TerminateExecution directly and drive it through the StreamProcessor.
    let cause = spica_engine::EntryId::new(1);
    let entries = processor
        .dispatch(
            &Command::TerminateExecution {
                id: exec,
                reason: TerminationReason::Cancelled,
            },
            &storage,
            cause,
        )
        .await
        .unwrap();
    logstream.append(entries).await.unwrap();

    let mut stream = logstream.stream_read(spica_engine::EntryId::new(1));
    let mut seen: Vec<Event> = Vec::new();
    while let Some(entry) = stream.next().await {
        match entry.payload {
            EntryPayload::Command(cmd) => {
                let entries = processor
                    .dispatch(&cmd, &storage, entry.entry_id)
                    .await
                    .unwrap();
                logstream.append(entries).await.unwrap();
            }
            EntryPayload::Event(ev) => {
                let terminal = matches!(
                    &ev,
                    Event::ExecutionTerminated { .. } | Event::ExecutionCompleted { .. }
                );
                projector.apply(&mut storage, &ev).await;
                seen.push(ev);
                if terminal {
                    break;
                }
            }
            EntryPayload::Reject(_) => {
                // A refused command — nothing to fold; this test never rejects, keep exhaustive.
            }
        }
    }

    // Emission order: the cancelling parent's ing arrives first, then each child's own terminal,
    // finally the parent's terminal ed once every child is drained.
    assert!(
        pos(&seen, "ExecutionTerminating") < pos(&seen, "StateTerminating"),
        "parent ing must precede child ing: {:?}",
        seen.iter().map(kind_prefix).collect::<Vec<_>>()
    );
    assert!(pos(&seen, "StateTerminating") < pos(&seen, "TimerCancelled"));
    assert!(pos(&seen, "TimerCancelled") < pos(&seen, "StateTerminated"));
    assert!(pos(&seen, "StateTerminated") < pos(&seen, "ExecutionTerminated"));
    let exec = storage.get_execution(exec).await.unwrap().unwrap();
    assert!(matches!(
        exec.status,
        ExecutionStatus::Terminated(TerminationReason::Cancelled)
    ));
    assert!(exec.active_children.is_empty(), "execution fully drained");
}

// ── Race guard: a late TriggerTimer after a cancel must be a no-op ──────────

#[tokio::test]
async fn late_trigger_timer_after_cancel_is_noop() {
    // An armed timer is cancelled in storage first; then a stale TriggerTimer arrives (a fire
    // that was already in flight). The handler must see the timer's terminal state and emit
    // nothing — no TimerTriggered, no TerminateExecution.
    let timer = TimerId::new();
    let exec = ExecutionId::nil();
    // The payload type isn't pinned by later use here (the log is only constructed then dropped),
    // so name it explicitly.
    let logstream = InMemoryLogStream::<EntryPayload>::new();
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();
    projector
        .apply(
            &mut storage,
            &Event::TimerActivated {
                timer: TimerValue {
                    id: timer,
                    parent: NodeId::Execution(exec),
                    purpose: TimerPurpose::ExecutionTimeout,
                    status: TimerStatus::Active,
                    deadline: Timestamp::from_millis(1_000_000_000_000),
                },
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::TimerCancelled {
                timer: TimerValue {
                    id: timer,
                    parent: NodeId::Execution(exec),
                    purpose: TimerPurpose::ExecutionTimeout,
                    status: TimerStatus::Cancelled,
                    deadline: Timestamp::from_millis(1_000_000_000_000),
                },
            },
        )
        .await;

    let mut processor = StreamProcessor::new();
    let out = processor
        .dispatch(
            &Command::TriggerTimer { timer },
            &storage,
            spica_engine::EntryId::new(500),
        )
        .await
        .unwrap();
    assert!(
        out.is_empty(),
        "a stale TriggerTimer must produce no entries: {out:?}"
    );
    drop(logstream);
}

// ── Timeout cascade: TimeoutSeconds fires past a blocking Wait ───────────────

#[tokio::test]
async fn execution_timeout_terminates_pending_execution() {
    let sm = parse_sm(
        r#"{
          "StartAt": "W",
          "TimeoutSeconds": 1,
          "States": {
            "W": { "Type": "Wait", "Seconds": 600, "Next": "P" },
            "P": { "Type": "Pass", "End": true }
          }
        }"#,
    );
    // The 1s execution timeout fires while the Wait is still blocked (600s). The cascade produces
    // ExecutionTerminated{Failed{TimedOut}} — not a hang on the 600s wait.
    let err = common::create_and_run(common::in_memory_builder(), sm, Value::Null)
        .await
        .expect_err("execution should time out");
    assert!(
        matches!(err, ExecutionError::TimedOut { .. }),
        "expected TimedOut, got {err:?}"
    );
    assert_eq!(err.error_name(), "States.Timeout");
}

// ── Engine::start smoke (kept for regression) ───────────────────────────────

#[tokio::test]
async fn engine_start_pass_with_assign_chain() {
    let sm = parse_sm(
        r#"{
          "StartAt": "Set",
          "States": {
            "Set": { "Type": "Pass", "Assign": { "g": "hi" }, "Next": "Read" },
            "Read": { "Type": "Pass", "Output": "{% $g %}", "End": true }
          }
        }"#,
    );
    let result = {
        common::create_and_run(common::in_memory_builder(), sm, Value::Null)
            .await
            .unwrap()
    };
    assert_eq!(result.output, json!("hi"));
}

#[tokio::test]
async fn engine_start_fail_produces_state_failed_error() {
    let sm = parse_sm(
        r#"{ "StartAt": "F", "States": { "F": { "Type": "Fail", "Error": "E1", "Cause": "boom" } } }"#,
    );
    let err = common::create_and_run(common::in_memory_builder(), sm, Value::Null)
        .await
        .unwrap_err();
    let err_name = err.error_name().to_string();
    match err {
        ExecutionError::StateFailed {
            ref error,
            ref output,
            ..
        } => {
            assert_eq!(error, "E1");
            assert_eq!(output, &json!({ "Error": "E1", "Cause": "boom" }));
        }
        other => panic!("expected StateFailed, got {other:?}"),
    }
    assert_eq!(err_name, "E1");
}

#[tokio::test]
async fn engine_create_flow_rejects_malformed_definition() {
    // A running engine is required to attempt `create_flow` (the typestate denies an unstarted
    // engine an operation), but the malformed-definition rejection is what we assert here.
    let engine = common::in_memory_builder().start().await.unwrap();
    // A definition that isn't even valid JSON, a JSON doc that isn't a StateMachine at all
    // (missing `StartAt`), and a `StartAt` of the wrong type are all rejected at the `create_flow`
    // boundary — a non-parseable definition must never enter the log nor Storage. Reusing one name
    // across the loop is fine because every call fails before appending anything.
    for bad in [
        "not json",
        "{}",
        r#"{ "States": { "S": { "Type": "Pass", "End": true } } }"#,
    ] {
        let err = engine
            .create_flow(FlowName::new("bad_flow").unwrap(), bad)
            .await
            .expect_err("malformed definition should be rejected before persisting");
        assert!(
            matches!(err, ExecutionError::InvalidDefinition(_)),
            "expected InvalidDefinition for {bad:?}, got {err:?}"
        );
    }
}

#[tokio::test]
async fn create_flow_handler_rejects_existing_name_as_reject_record() {
    let mut storage = InMemoryStorage::new();
    let mut processor = StreamProcessor::new();

    // Seed an existing flow named "test_flow" directly into storage — the same projection a prior
    // successful `CreateFlow` would have folded.
    let sm = parse_sm(r#"{ "StartAt": "S", "States": { "S": { "Type": "Pass", "End": true } } }"#);
    seed_revision(&mut storage, sm.clone()).await;

    // Forge a second `CreateFlow` for the same name straight into the engine, bypassing the `Engine`
    // boundary pre-check — exactly what a replayed or a racing-concurrent command can do. The handler
    // is the authoritative serialized point and must refuse it with an `AlreadyExists` `Reject`
    // record (the durable response entry for the refused command), never a silent empty emit.
    let entries = processor
        .dispatch(
            &Command::CreateFlow {
                request_id: RequestId::new(),
                name: FlowName::new("test_flow").unwrap(),
                definition: serde_json::to_string(&sm).unwrap(),
            },
            &storage,
            EntryId::new(2),
        )
        .await
        .unwrap();

    // Exactly one response entry, and it is a `Reject(AlreadyExists)` — never a duplicate
    // `FlowCreated`/`FlowVersionCreated`.
    assert_eq!(
        entries.len(),
        1,
        "a refused CreateFlow emits exactly one response entry, got {}: {entries:?}",
        entries.len()
    );
    match &entries[0].payload {
        EntryPayload::Reject(reject) => {
            assert_eq!(
                reject.rejection_type,
                RejectionType::AlreadyExists,
                "duplicate create must be classified AlreadyExists: {reject:?}"
            );
            assert!(
                reject.rejection_reason.contains("already exists"),
                "reason should name the conflict: {reject:?}"
            );
        }
        other => panic!("expected a Reject record, got {other:?}"),
    }
}

#[tokio::test]
async fn create_flow_handler_rejects_malformed_definition_as_reject_record() {
    let storage = InMemoryStorage::new();
    let mut processor = StreamProcessor::new();

    // A `CreateFlow` carrying a definition that doesn't parse as a `StateMachine`, forged straight
    // into the engine (bypassing the boundary validation). The handler must refuse it with an
    // `InvalidArgument` `Reject` (a response entry), previously this swallowed the error with a bare
    // `return` — emitting no entry at all.
    let entries = processor
        .dispatch(
            &Command::CreateFlow {
                request_id: RequestId::new(),
                name: FlowName::new("bad_flow").unwrap(),
                definition: "not a state machine".to_string(),
            },
            &storage,
            EntryId::new(1),
        )
        .await
        .unwrap();

    assert_eq!(
        entries.len(),
        1,
        "a refused CreateFlow emits exactly one response entry, got {}: {entries:?}",
        entries.len()
    );
    match &entries[0].payload {
        EntryPayload::Reject(reject) => {
            assert_eq!(
                reject.rejection_type,
                RejectionType::InvalidArgument,
                "a malformed definition must be classified InvalidArgument: {reject:?}"
            );
        }
        other => panic!("expected a Reject record, got {other:?}"),
    }
}

#[tokio::test]
async fn engine_explicit_lifecycle_runs_many_executions_on_one_processor() {
    let sm = parse_sm(
        r#"{
          "StartAt": "P",
          "States": {
            "P": { "Type": "Pass", "Output": "{% $states.input.x %}", "End": true }
          }
        }"#,
    );
    let engine = common::in_memory_builder().start().await.unwrap();
    // Create a definition (returns its never-reused flow_version_id), then run *two* executions
    // against that one created version — both driven by the same long-lived StreamProcessor, no session
    // per run.
    let definition = serde_json::to_string(&sm).unwrap();
    let flow_version_id = engine
        .create_flow(FlowName::new("my_flow").unwrap(), &definition)
        .await
        .unwrap();
    // `start_for_revision` returns the execution id as soon as the execution is born; the result
    // (success output or failure) is resolved by `wait_for_execution`, which polls the projection.
    let execution_id = engine
        .start_for_revision(flow_version_id, json!({ "x": 7 }))
        .await
        .expect("first execution should start");
    let result = engine
        .wait_for_execution(execution_id)
        .await
        .expect("first execution should succeed");
    assert_eq!(result.output, json!(7.0));
    let execution_id = engine
        .start_for_revision(flow_version_id, json!({ "x": 9 }))
        .await
        .expect("second execution against the same version should start");
    let result = engine
        .wait_for_execution(execution_id)
        .await
        .expect("second execution should succeed");
    assert_eq!(result.output, json!(9.0));
    // Clean shutdown: cancels the internal StreamProcessor and awaits it.
    engine.stop().await;
}

/// `start_for_revision` returns the execution id as soon as the execution is *born* — before it has
/// settled — and `wait_for_execution` then resolves the result from the durable projection. This
/// split is what lets a caller issue many executions and await each at its own pace: the id comes
/// back immediately, terminal state is an independent poll.
#[tokio::test]
async fn start_returns_id_before_terminal_and_wait_resolves_output() {
    let sm = parse_sm(
        r#"{
          "StartAt": "P",
          "States": { "P": { "Type": "Pass", "Output": "{% 21 * 2 %}", "End": true } }
        }"#,
    );
    let engine = common::in_memory_builder().start().await.unwrap();
    let definition = serde_json::to_string(&sm).unwrap();
    let flow_version_id = engine
        .create_flow(FlowName::new("my_flow").unwrap(), &definition)
        .await
        .unwrap();

    let execution_id = engine
        .start_for_revision(flow_version_id, Value::Null)
        .await
        .expect("start should return the execution id at birth");
    // The id is real (not a placeholder) and, crucially, `wait_for_execution` observes the same id.
    let result = engine
        .wait_for_execution(execution_id)
        .await
        .expect("a Pass execution completes");
    assert_eq!(result.output, json!(42.0));
    engine.stop().await;
}

/// `wait_for_execution` surfaces a *failure* as an [`ExecutionError`] — the same mapping the old
/// terminal-ack block returned — but via a poll of the projection rather than a live ack channel.
#[tokio::test]
async fn wait_for_execution_surfaces_failure_reason() {
    let sm = parse_sm(
        r#"{ "StartAt": "F", "States": { "F": { "Type": "Fail", "Error": "E1", "Cause": "boom" } } }"#,
    );
    let engine = common::in_memory_builder().start().await.unwrap();
    let definition = serde_json::to_string(&sm).unwrap();
    let flow_version_id = engine
        .create_flow(FlowName::new("my_flow").unwrap(), &definition)
        .await
        .unwrap();

    let execution_id = engine
        .start_for_revision(flow_version_id, Value::Null)
        .await
        .expect("start should return the id even for a failing flow");
    let err = engine
        .wait_for_execution(execution_id)
        .await
        .expect_err("a Fail state must surface as an execution error");
    match err {
        ExecutionError::StateFailed { ref error, .. } => assert_eq!(error, "E1"),
        other => panic!("expected StateFailed, got {other:?}"),
    }
    engine.stop().await;
}

/// Several concurrent `wait_for_execution` polls can observe the same running execution: each is an
/// independent read of the projection, so two callers awaiting one `start_for_revision` both resolve
/// to the same terminal result. (Any number of waiters is fine — there is no shared ack slot to
/// contend for, unlike the old single terminal ack per execution.)
#[tokio::test]
async fn many_waiters_resolve_the_same_execution() {
    // A Wait state keeps the execution in flight long enough that both waiters poll while it is
    // still running, proving they are independent and both observe the terminal result.
    let sm = parse_sm(
        r#"{
          "StartAt": "W",
          "States": { "W": { "Type": "Wait", "Seconds": 1, "End": true } }
        }"#,
    );
    let engine = common::in_memory_builder().start().await.unwrap();
    let definition = serde_json::to_string(&sm).unwrap();
    let flow_version_id = engine
        .create_flow(FlowName::new("my_flow").unwrap(), &definition)
        .await
        .unwrap();
    let execution_id = engine
        .start_for_revision(flow_version_id, Value::Null)
        .await
        .expect("start returns the id");

    // `wait_for_execution` takes `&self`, so `tokio::join!` polls two independent waiters concurrently
    // on the same running execution (still in its 1s Wait) without needing a `Clone`.
    let (r1, r2) = tokio::join!(
        engine.wait_for_execution(execution_id),
        engine.wait_for_execution(execution_id)
    );
    assert!(r1.expect("waiter 1 succeeds").output.is_null());
    assert!(r2.expect("waiter 2 succeeds").output.is_null());
    engine.stop().await;
}

/// The response registry (see `Engine::ack`) gives every acknowledgement-awaiter its **own** one-shot
/// channel, so any number of blocking operations can be in flight concurrently — the property the
/// earlier shared stream cursor lacked. This drives ten `create_flow` calls in parallel on one
/// Engine (each with a distinct name), and asserts every one returns a real, distinct `flow_id`.
#[tokio::test]
async fn engine_runs_many_create_flow_concurrently() {
    let sm = parse_sm(
        r#"{
          "StartAt": "P",
          "States": { "P": { "Type": "Pass", "End": true } }
        }"#,
    );
    let definition = serde_json::to_string(&sm).unwrap();
    let engine = common::in_memory_builder().start().await.unwrap();

    // `create_flow` takes `&self`, so ten independent tasks can register + append + await their own
    // ack concurrently instead of serializing through the Engine (or fighting over one stream cursor).
    let names: Vec<FlowName> = (0..10)
        .map(|i| FlowName::new(&format!("flow_{i}")).unwrap())
        .collect();
    let (a, b, c, d, e, f, g, h, i, j) = tokio::join!(
        engine.create_flow(names[0].clone(), &definition),
        engine.create_flow(names[1].clone(), &definition),
        engine.create_flow(names[2].clone(), &definition),
        engine.create_flow(names[3].clone(), &definition),
        engine.create_flow(names[4].clone(), &definition),
        engine.create_flow(names[5].clone(), &definition),
        engine.create_flow(names[6].clone(), &definition),
        engine.create_flow(names[7].clone(), &definition),
        engine.create_flow(names[8].clone(), &definition),
        engine.create_flow(names[9].clone(), &definition),
    );
    for (i, id) in [a, b, c, d, e, f, g, h, i, j].into_iter().enumerate() {
        let id = id.expect("concurrent create_flow should succeed");
        assert_ne!(
            id,
            FlowVersionId::nil(),
            "concurrent create_flow #{i} returned a real, distinct flow_version_id"
        );
    }
    engine.stop().await;
}

// ── Task lease lifecycle guards ───────────────────────────────────────────────
//
// These exercise the Zeebe-style job contract at the command/handler seam with full control over
// `worker_id` (the integration harness's in-memory worker hides it). Each test seeds a lone task row
// and dispatches a single command through the real handler. Only the task row is needed: every
// settlement guard (`is_activated` + `worker_id` match) returns *before* touching the owning
// activity, and the beyond-guard emissions (`CompleteState`, the timer sweep) are graceful no-ops on
// a storage with no activity — so a single row exercises the full guard.

/// Seed a `task` row with the given domain state, owning it under a throwaway activity.
async fn seed_task(
    storage: &mut InMemoryStorage,
    task_id: TaskId,
    status: TaskStatus,
    worker_id: Option<String>,
    lease_until: Option<Timestamp>,
) {
    storage
        .put_task(spica_engine::Task {
            value: TaskValue {
                id: task_id,
                parent: NodeId::Activity(ActivityId::new()),
                resource: "r".to_string(),
                arguments: Value::Null,
                status,
                deadline: None,
                worker_id,
                lease_until,
            },
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        })
        .await
        .unwrap();
}

/// Dispatch `command` through a fresh [`StreamProcessor`] (which resolves any needed definition from
/// `storage`) and return the emitted [`Entry`]s.
async fn dispatch_command(storage: &InMemoryStorage, command: Command) -> Vec<Entry> {
    let mut processor = StreamProcessor::new();
    processor
        .dispatch(&command, storage, EntryId::new(1))
        .await
        .unwrap()
}

#[tokio::test]
async fn assign_task_leases_available_task_to_worker() {
    let mut storage = InMemoryStorage::new();
    let task = TaskId::new();
    seed_task(&mut storage, task, TaskStatus::Pending, None, None).await;

    let entries = dispatch_command(
        &storage,
        Command::AssignTask {
            task,
            worker_id: "w1".into(),
            lease_seconds: 60,
        },
    )
    .await;
    let leased = entries
        .iter()
        .find_map(|e| match &e.payload {
            EntryPayload::Event(Event::TaskLeased { task }) => Some(task),
            _ => None,
        })
        .expect("AssignTask should emit TaskLeased");
    assert_eq!(leased.status, TaskStatus::Running);
    assert_eq!(leased.worker_id.as_deref(), Some("w1"));
    assert!(
        leased.lease_until.is_some(),
        "a claim must record a lease horizon"
    );
    // A lease-expiry timer is armed alongside the lease (Zeebe activation timeout).
    assert!(
        entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Command(Command::ActivateTimer {
                purpose: TimerPurpose::TaskLease,
                ..
            })
        )),
        "assign should arm a TaskLease timer"
    );
}

#[tokio::test]
async fn assign_task_ignores_already_leased_or_settled() {
    let mut storage = InMemoryStorage::new();
    // Already leased to w1: a racing pull by w2 must not steal it.
    let leased = TaskId::new();
    seed_task(
        &mut storage,
        leased,
        TaskStatus::Running,
        Some("w1".into()),
        Some(Timestamp::from_millis(1000)),
    )
    .await;
    let entries = dispatch_command(
        &storage,
        Command::AssignTask {
            task: leased,
            worker_id: "w2".into(),
            lease_seconds: 60,
        },
    )
    .await;
    assert!(
        entries.is_empty(),
        "a leased task must not be re-assigned: {entries:?}"
    );

    // Already settled (Completed): no reassignment either.
    let done = TaskId::new();
    seed_task(&mut storage, done, TaskStatus::Completed, None, None).await;
    let entries = dispatch_command(
        &storage,
        Command::AssignTask {
            task: done,
            worker_id: "w3".into(),
            lease_seconds: 60,
        },
    )
    .await;
    assert!(entries.is_empty(), "a settled task must not be reassigned");
}

#[tokio::test]
async fn pull_tasks_leases_only_available_tasks_of_resource() {
    // A bulk pull (the command behind `TaskApi::activate`) must lease exactly the `Pending` tasks of
    // its `resource` — never ones already leased/settled/cancelled, and never another resource's.
    let mut storage = InMemoryStorage::new();
    let pending1 = TaskId::new();
    let pending2 = TaskId::new();
    let running = TaskId::new();
    let done = TaskId::new();
    let cancelled = TaskId::new();
    let other_resource = TaskId::new();
    for id in [pending1, pending2] {
        seed_task(&mut storage, id, TaskStatus::Pending, None, None).await;
    }
    seed_task(
        &mut storage,
        running,
        TaskStatus::Running,
        Some("w1".into()),
        Some(Timestamp::from_millis(1000)),
    )
    .await;
    seed_task(&mut storage, done, TaskStatus::Completed, None, None).await;
    seed_task(&mut storage, cancelled, TaskStatus::Cancelled, None, None).await;
    // A `Pending` task of a *different* resource is not this pull's to grant.
    storage
        .put_task(spica_engine::Task {
            value: TaskValue {
                id: other_resource,
                parent: NodeId::Activity(ActivityId::new()),
                resource: "other".to_string(),
                arguments: Value::Null,
                status: TaskStatus::Pending,
                deadline: None,
                worker_id: None,
                lease_until: None,
            },
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        })
        .await
        .unwrap();

    let entries = dispatch_command(
        &storage,
        Command::PullTasks {
            request_id: RequestId::new(),
            worker_id: "w2".into(),
            resource: "r".into(),
            max_tasks: 10,
            lease_seconds: 60,
        },
    )
    .await;
    let leased: Vec<_> = entries
        .iter()
        .filter_map(|e| match &e.payload {
            EntryPayload::Event(Event::TaskLeased { task }) => {
                Some((task.id, &task.status, &task.worker_id))
            }
            _ => None,
        })
        .collect();
    // Exactly the two `Pending` tasks of `resource "r"` are leased to w2; the running / settled /
    // cancelled / foreign-resource tasks are untouched.
    let mut ids: Vec<_> = leased.iter().map(|(id, _, _)| *id).collect();
    ids.sort();
    let mut expect = vec![pending1, pending2];
    expect.sort();
    assert_eq!(
        ids, expect,
        "pull must grant only the resource's Pending tasks"
    );
    for (_, status, worker) in leased {
        assert_eq!(*status, TaskStatus::Running);
        assert_eq!(worker.as_deref(), Some("w2"));
    }
    // Each grant arms its TaskLease expiry timer.
    let timers = entries
        .iter()
        .filter(|e| {
            matches!(
                &e.payload,
                EntryPayload::Command(Command::ActivateTimer {
                    purpose: TimerPurpose::TaskLease,
                    ..
                })
            )
        })
        .count();
    assert_eq!(timers, 2, "each granted task arms a TaskLease timer");
}

#[tokio::test]
async fn pull_tasks_respects_max_tasks() {
    // `max_tasks` caps the grant: with 3 available, a pull of 2 grants exactly 2.
    let mut storage = InMemoryStorage::new();
    for _ in 0..3 {
        seed_task(&mut storage, TaskId::new(), TaskStatus::Pending, None, None).await;
    }
    let entries = dispatch_command(
        &storage,
        Command::PullTasks {
            request_id: RequestId::new(),
            worker_id: "w".into(),
            resource: "r".into(),
            max_tasks: 2,
            lease_seconds: 60,
        },
    )
    .await;
    let leased = entries
        .iter()
        .filter(|e| matches!(&e.payload, EntryPayload::Event(Event::TaskLeased { .. })))
        .count();
    assert_eq!(leased, 2, "max_tasks must cap the granted set");
}

#[tokio::test]
async fn stale_task_leased_does_not_override_owner_or_settlement() {
    // The conditional `TaskLeased` applier folds a lease only while the task is still `Pending`; a
    // stale/racing lease (already leased to someone else, or already settled/cancelled) is a no-op, so
    // the *state* advances exactly-once even though a racing pull may hand the *work* to two workers.
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();
    // Stale lease against an already-leased (Running) task: must not change who owns it.
    let running = TaskId::new();
    seed_task(
        &mut storage,
        running,
        TaskStatus::Running,
        Some("w1".into()),
        Some(Timestamp::from_millis(1000)),
    )
    .await;
    projector
        .apply(
            &mut storage,
            &Event::TaskLeased {
                task: TaskValue {
                    id: running,
                    parent: NodeId::Activity(ActivityId::new()),
                    resource: "r".to_string(),
                    arguments: Value::Null,
                    status: TaskStatus::Running,
                    deadline: None,
                    worker_id: Some("w2".into()),
                    lease_until: Some(Timestamp::from_millis(2000)),
                },
            },
        )
        .await;
    let t = storage.get_task(running).await.unwrap().unwrap();
    assert_eq!(
        t.status,
        TaskStatus::Running,
        "a stale lease must not steal a leased task"
    );
    assert_eq!(
        t.worker_id.as_deref(),
        Some("w1"),
        "lease ownership must be preserved"
    );

    // Stale lease against an already-settled (Completed) task: must not resurrect it.
    let done = TaskId::new();
    seed_task(&mut storage, done, TaskStatus::Completed, None, None).await;
    projector
        .apply(
            &mut storage,
            &Event::TaskLeased {
                task: TaskValue {
                    id: done,
                    parent: NodeId::Activity(ActivityId::new()),
                    resource: "r".to_string(),
                    arguments: Value::Null,
                    status: TaskStatus::Running,
                    deadline: None,
                    worker_id: Some("w3".into()),
                    lease_until: Some(Timestamp::from_millis(2000)),
                },
            },
        )
        .await;
    let t = storage.get_task(done).await.unwrap().unwrap();
    assert_eq!(
        t.status,
        TaskStatus::Completed,
        "a stale lease must not resurrect a settled task"
    );
}

#[tokio::test]
async fn complete_by_foreign_worker_is_rejected() {
    let mut storage = InMemoryStorage::new();
    let task = TaskId::new();
    seed_task(
        &mut storage,
        task,
        TaskStatus::Running,
        Some("w1".into()),
        Some(Timestamp::from_millis(1000)),
    )
    .await;
    let entries = dispatch_command(
        &storage,
        Command::CompleteTask {
            task,
            worker_id: "w2".into(),
            output: json!({ "ok": true }),
        },
    )
    .await;
    assert!(
        entries.is_empty(),
        "a foreign worker's complete must be a no-op: {entries:?}"
    );
}

#[tokio::test]
async fn leasing_worker_complete_settles_task() {
    let mut storage = InMemoryStorage::new();
    let task = TaskId::new();
    seed_task(
        &mut storage,
        task,
        TaskStatus::Running,
        Some("w1".into()),
        Some(Timestamp::from_millis(1000)),
    )
    .await;
    let entries = dispatch_command(
        &storage,
        Command::CompleteTask {
            task,
            worker_id: "w1".into(),
            output: json!({ "ok": true }),
        },
    )
    .await;
    let completed = entries
        .iter()
        .find_map(|e| match &e.payload {
            EntryPayload::Event(Event::TaskCompleted { task, .. }) => Some(task),
            _ => None,
        })
        .expect("the leasing worker's complete should emit TaskCompleted");
    assert_eq!(completed.status, TaskStatus::Completed);
    assert_eq!(completed.worker_id, None); // lease cleared
    assert_eq!(completed.lease_until, None);
    // The owning Task state resumes via CompleteState.
    assert!(
        entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Command(Command::CompleteState { .. })
        )),
        "a settled task should resume its state"
    );
}

#[tokio::test]
async fn late_complete_after_release_or_cancel_is_noop() {
    let mut storage = InMemoryStorage::new();
    // Re-queued (released → Active) after the lease lapsed: the stale worker's late settle drops.
    let requeued = TaskId::new();
    seed_task(&mut storage, requeued, TaskStatus::Pending, None, None).await;
    let entries = dispatch_command(
        &storage,
        Command::CompleteTask {
            task: requeued,
            worker_id: "w1".into(),
            output: json!(1),
        },
    )
    .await;
    assert!(
        entries.is_empty(),
        "a late complete on a re-queued task is a no-op: {entries:?}"
    );

    // Same for a cancelled task.
    let cancelled = TaskId::new();
    seed_task(&mut storage, cancelled, TaskStatus::Cancelled, None, None).await;
    let entries = dispatch_command(
        &storage,
        Command::CompleteTask {
            task: cancelled,
            worker_id: "w1".into(),
            output: json!(1),
        },
    )
    .await;
    assert!(
        entries.is_empty(),
        "a late complete on a cancelled task is a no-op: {entries:?}"
    );
}

#[tokio::test]
async fn release_requeues_task_for_a_fresh_claim() {
    let mut storage = InMemoryStorage::new();
    let task = TaskId::new();
    seed_task(
        &mut storage,
        task,
        TaskStatus::Running,
        Some("w1".into()),
        Some(Timestamp::from_millis(1000)),
    )
    .await;
    let entries = dispatch_command(&storage, Command::ReleaseTaskLease { task }).await;
    let expired = entries
        .iter()
        .find_map(|e| match &e.payload {
            EntryPayload::Event(Event::TaskLeaseExpired { task }) => Some(task.clone()),
            _ => None,
        })
        .expect("release should emit TaskLeaseExpired");
    assert_eq!(expired.status, TaskStatus::Pending);
    assert_eq!(expired.worker_id, None);

    // Re-queue makes the task claimable again: apply the expiry (fold it to storage), then a fresh
    // assign by another worker succeeds.
    let proj = Projector::new();
    proj.apply(&mut storage, &Event::TaskLeaseExpired { task: expired })
        .await;
    let entries = dispatch_command(
        &storage,
        Command::AssignTask {
            task,
            worker_id: "w2".into(),
            lease_seconds: 30,
        },
    )
    .await;
    assert!(
        entries
            .iter()
            .any(|e| matches!(&e.payload, EntryPayload::Event(Event::TaskLeased { .. }))),
        "a re-queued task can be claimed by a fresh worker: {entries:?}"
    );
}

#[tokio::test]
async fn fail_settlement_requires_lease_or_engine_authority() {
    let mut storage = InMemoryStorage::new();
    // A foreign worker cannot fail a task it does not lease.
    let foreign = TaskId::new();
    seed_task(
        &mut storage,
        foreign,
        TaskStatus::Running,
        Some("w1".into()),
        Some(Timestamp::from_millis(1000)),
    )
    .await;
    let entries = dispatch_command(
        &storage,
        Command::FailTask {
            task: foreign,
            worker_id: "w2".into(),
            error: ExecutionError::TimedOut {
                message: "x".into(),
            },
        },
    )
    .await;
    assert!(
        entries.is_empty(),
        "a foreign worker's fail must be a no-op: {entries:?}"
    );

    // The engine-authoritative backstop (empty worker_id, e.g. the TaskTimeout deadline) settles any
    // non-terminal task regardless of who holds the lease.
    let stalled = TaskId::new();
    seed_task(
        &mut storage,
        stalled,
        TaskStatus::Running,
        Some("w1".into()),
        Some(Timestamp::from_millis(1000)),
    )
    .await;
    let entries = dispatch_command(
        &storage,
        Command::FailTask {
            task: stalled,
            worker_id: String::new(),
            error: ExecutionError::TimedOut {
                message: "deadline".into(),
            },
        },
    )
    .await;
    assert!(
        entries
            .iter()
            .any(|e| matches!(&e.payload, EntryPayload::Event(Event::TaskFailed { .. }))),
        "the engine backstop fail should settle the task: {entries:?}"
    );
}
