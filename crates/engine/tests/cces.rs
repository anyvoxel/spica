//! Unit tests for the CCES building blocks: the Storage projection, causality/atomicity of the
//! StreamProcessor's output, the ing/ed lifecycle split, the deferred-ed cascade on a Completing or
//! Terminating parent, and the cancel/timeout race guards.

mod common;

use serde_json::{Value, json};
use spica_asl::StateMachine;
use spica_engine::{
    Activity, ActivityId, ActivityStatus, Command, Entry, EntryId, EntryPayload, Event, Execution,
    ExecutionError, ExecutionId, ExecutionStatus, Flow, FlowName, FlowStatus, FlowVersion,
    InMemoryLogStream, LogStream, ObjectReference, RejectionType, RequestId, RetryPolicy,
    RetryState, RuntimeError, Storage, StreamProcessor, Task, TaskStatus, TerminationReason,
    Thread, ThreadStatus, Timer, TimerId, TimerPurpose, TimerStatus, Timestamp, Variables,
};
use spica_scheduler::{InMemoryScheduler, Scheduler, TimerSink};
use spica_storage::InMemoryStorage;
use tokio_stream::StreamExt;

fn parse_sm(definition: &str) -> StateMachine {
    serde_json::from_str(definition).expect("state machine should parse")
}

/// Build a distinct execution [`ObjectReference`] shaped exactly like `Execution::reference()`
/// (the generated `obj-<uid>` name + uid), so an in-memory storage round-trips by reference.
fn exec_ref() -> spica_engine::ObjectReference {
    let uid: ulid::Ulid = ExecutionId::new().into();
    spica_engine::ObjectReference::new(
        spica_engine::ObjectKind::Execution,
        spica_engine::PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(uid.0 as u64),
        uid,
    )
}

/// Build a distinct activity [`ObjectReference`] shaped exactly like `Activity::reference()`
/// (the generated `obj-<uid>` name + uid), so an in-memory storage round-trips by reference.
fn act_ref() -> spica_engine::ObjectReference {
    let uid: ulid::Ulid = ActivityId::new().into();
    spica_engine::ObjectReference::new(
        spica_engine::ObjectKind::Activity,
        spica_engine::PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(uid.0 as u64),
        uid,
    )
}

/// Build the task [`ObjectReference`] for a task's raw id, shaped exactly like `Task::reference()`
/// (the generated `obj-<uid>` name + uid), so an in-memory storage round-trips by reference.
fn task_ref(task: ulid::Ulid) -> spica_engine::ObjectReference {
    let uid: ulid::Ulid = task;
    spica_engine::ObjectReference::new(
        spica_engine::ObjectKind::Task,
        spica_engine::PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(uid.0 as u64),
        uid,
    )
}

/// Build the timer [`ObjectReference`] for a timer's raw id, shaped exactly like
/// `Timer::reference()` (the generated `obj-<uid>` name + uid), so an in-memory storage round-trips
/// by reference.
fn timer_ref(timer: TimerId) -> spica_engine::ObjectReference {
    let uid: ulid::Ulid = timer.into();
    spica_engine::ObjectReference::new(
        spica_engine::ObjectKind::Timer,
        spica_engine::PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(uid.0 as u64),
        uid,
    )
}

/// Pre-seed `sm` as a created flow version in `storage`, returning its [`ObjectReference`].
/// Definition resolution happens from storage at dispatch time (the `CreateExecution` command
/// carries only the flow version reference), so raw-seam drivers seed the definition directly rather
/// than driving a `CreateFlow` command. The definition is stored in its raw ASL string form. Mirrors
/// what the `FlowCreated` applier folds: a `Flow` row (by name, on first appearance) + a `FlowVersion`
/// row (keyed by its `{flow_name}-{version}` name, executions bind to the reference).
async fn seed_revision(storage: &mut InMemoryStorage, sm: StateMachine) -> ObjectReference {
    let version = 1u32;
    let flow_name = FlowName::new("test_flow").expect("static name is valid");
    let version_name = FlowVersion::version_name(&flow_name, version);
    let flow_version_uid = ulid::Ulid::new();
    let created_at = Timestamp::from_millis(0);
    // The owning Flow's reference, attached to the version below — same scope, uid nil (the flow's
    // name is its sole identity).
    let owner = spica_engine::OwnerReference::new(
        spica_engine::ObjectKind::Flow,
        spica_engine::ObjectName::plain("test_flow").expect("static name is valid"),
        ulid::Ulid::nil(),
    );
    storage
        .put_flow(Flow {
            meta: spica_engine::ObjectMeta::builder(
                spica_engine::ObjectKind::Flow,
                // Name is the flow's sole identity — no generation id, so uid is nil.
                ulid::Ulid::nil(),
            )
            .name(spica_engine::ObjectName::plain("test_flow").expect("static name is valid"))
            .at(created_at)
            .build(),
            status: FlowStatus::Active,
            // Newest-version counter — the version seeded below is the only one, so it is the latest.
            latest_version: version,
        })
        .await
        .unwrap();
    storage
        .put_flow_version(FlowVersion {
            meta: spica_engine::ObjectMeta::builder(
                spica_engine::ObjectKind::FlowVersion,
                flow_version_uid,
            )
            .name(version_name.clone())
            .at(created_at)
            .build()
            .with_owner(owner),
            version,
            definition: serde_json::to_string(&sm).expect("state machine serializes"),
            checksum: FlowVersion::definition_checksum(
                &serde_json::to_string(&sm).expect("state machine serializes"),
            ),
        })
        .await
        .unwrap();
    ObjectReference::new(
        spica_engine::ObjectKind::FlowVersion,
        version_name,
        flow_version_uid,
    )
}

/// Applies events to storage through the real [`EventDispatcher`], mirroring the event path of
/// `StreamProcessor::run`. Projection tests only fold state and never wait on a fired timer, so any
/// applier-declared timer effects are discarded here (unlike `collect_events`, which routes them).
struct Projector {
    dispatcher: spica_engine::EventDispatcher,
}

impl Projector {
    fn new() -> Self {
        Self {
            dispatcher: spica_engine::EventDispatcher::new(),
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
            // A test projector folds Events into a scratch txn; any applier-declared timer effects are
            // discarded (no real scheduler is attached to these raw-seam drivers).
            let mut ctx = spica_engine::ApplierContext {
                storage: &mut *txn,
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
    async fn trigger(&self, timer: &ObjectReference) {
        // Envelope the fired command with placeholders; the log stamps the real position and stream.
        // The fired `TriggerTimer` carries no cause — provenance is derived from the entry that armed it.
        self.log
            .append(vec![Entry {
                stream_id: spica_engine::StreamId::nil(), // the log stamps the stream on append
                entry_id: spica_engine::EntryId::nil(),
                cause_id: None,
                timestamp: spica_engine::Timestamp::now(),
                payload: EntryPayload::Command(Command::TriggerTimer {
                    timer: timer.clone(),
                }),
            }])
            .await
            .expect("raw-seam test log append cannot fail");
    }
}

/// Re-derive the physical schedule from a durable timer event — the consumer-owned analogue of the
/// engine's former post-commit effect replay (see [`collect_events`]).
fn apply_event_to_scheduler(scheduler: &std::sync::Arc<InMemoryScheduler>, event: &Event) {
    match event {
        Event::TimerActivated { timer } => {
            scheduler.schedule(&timer.reference(), timer.deadline);
        }
        Event::TimerCancelled { timer } => scheduler.cancel(&timer.reference()),
        _ => {}
    }
}

/// Drives `sm` with `input` end-to-end through the raw CCES seam (no `Engine::start` async
/// wrapping), collecting every event applied to Storage. Stops when the execution reaches a
/// terminal status. Timer scheduling mirrors the consumer-owned model: a real [`Scheduler`] owns a
/// `DelayQueue`, and the physical arm/cancel is re-derived here from the durable
/// `TimerActivated`/`TimerCancelled` event (the engine itself declares no effects). This driver
/// injects an [`AppendingSink`] that the scheduler calls on expiry to append the fired
/// `TriggerTimer` — the push-into-log path that keeps the log single-writer and strictly ordered.
async fn collect_events(sm: StateMachine, input: Value) -> Vec<Event> {
    // Boxed in `Arc` so the injected [`AppendingSink`] and this driver share the same underlying log.
    let logstream = std::sync::Arc::new(InMemoryLogStream::new());
    let mut storage = InMemoryStorage::new();
    // Create the definition into storage first: `CreateExecution` carries only the flow id,
    // and the handler resolves the machine from storage at dispatch time.
    let flow_version = seed_revision(&mut storage, sm).await;
    common::submit_seed(flow_version, input, &*logstream)
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
                        // Fold the event into a scratch txn; the fold is a pure projection.
                        let mut txn = storage.begin_txn().unwrap();
                        {
                            let mut ctx = spica_engine::ApplierContext {
                                storage: &mut *txn,
                                timestamp: entry.timestamp,
                            };
                            dispatcher.apply(&mut ctx, &event).await.unwrap();
                        }
                        txn.commit(None).unwrap();
                        // Re-derive any physical schedule from the durable event (no effects in band).
                        apply_event_to_scheduler(&scheduler, &event);
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
                    EntryPayload::Noop => {
                        // This hand-rolled driver appends dispatched batches directly (via
                        // `dispatch` + `append`) and does not add a batch-terminating Noop, so none
                        // are ever read here. Skipped to keep the match exhaustive — a real processor
                        // applies eagerly at production and would just round off the batch here.
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
        Event::ThreadCreated { .. } => "ThreadCreated",
        Event::ThreadCompleting { .. } => "ThreadCompleting",
        Event::ThreadCompleted { .. } => "ThreadCompleted",
        Event::ThreadTerminating { .. } => "ThreadTerminating",
        Event::ThreadTerminated { .. } => "ThreadTerminated",
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
        Event::TasksClaimed { .. } => "TasksClaimed",
        Event::TaskLeaseExpired { .. } => "TaskLeaseExpired",
        Event::TaskCompleted { .. } => "TaskCompleted",
        Event::TaskFailed { .. } => "TaskFailed",
        Event::TaskCancelled { .. } => "TaskCancelled",
        Event::VariablesAssigned { .. } => "VariablesAssigned",
        Event::StateTransitioned { .. } => "StateTransitioned",
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
    let exec = exec_ref();
    let activity = act_ref();
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();

    projector
        .apply(
            &mut storage,
            &Event::ExecutionCreated {
                request_id: RequestId::nil(),
                execution: Execution {
                    flow_version: ObjectReference::nil(),
                    status: ExecutionStatus::Running,
                    input: json!({ "x": 1 }),
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Execution,
                        exec.uid,
                    )
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .build(),
                },
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::StateActivating {
                activity: Activity {
                    execution: exec.clone(),
                    state_path: jsonptr::PointerBuf::parse("/states/S").unwrap(),
                    status: ActivityStatus::Running,
                    raw_input: json!({ "x": 1 }),
                    input: Some(json!({ "x": 1 })),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Activity,
                        activity.uid,
                    )
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .build()
                    .with_owner(exec.clone()),
                },
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::VariablesAssigned {
                scope: exec.clone(),
                variables: Variables::from([("g".to_string(), json!("hi"))]),
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::ExecutionCompleted {
                execution: Execution {
                    flow_version: ObjectReference::nil(),
                    status: ExecutionStatus::Completed,
                    input: json!({ "x": 1 }),
                    output: Some(json!({ "done": true })),
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Execution,
                        exec.uid,
                    )
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .build(),
                },
            },
        )
        .await;

    let e = storage.get_execution(&exec).await.unwrap().unwrap();
    assert_eq!(e.status, ExecutionStatus::Completed);
    assert_eq!(e.variables.get("g"), Some(&json!("hi")));
    assert_eq!(e.output, Some(json!({ "done": true })));
    assert!(matches!(e.status, ExecutionStatus::Completed));
}

/// The event-carried `Execution.created_at`/`updated_at` (stamped at event construction, not taken
/// from log `Entry` metadata) survive the projection round-trip — including the de-flattened
/// execution row in Storage — and a completion advances `updated_at` while `created_at` stays put.
#[tokio::test]
async fn execution_domain_timestamps_follow_the_lifecycle() {
    let exec = exec_ref();
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();

    // Birth: created == updated.
    projector
        .apply(
            &mut storage,
            &Event::ExecutionCreated {
                request_id: RequestId::nil(),
                execution: Execution {
                    flow_version: ObjectReference::nil(),
                    status: ExecutionStatus::Running,
                    input: json!({}),
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Execution,
                        exec.uid,
                    )
                    .timestamps(
                        spica_engine::Timestamp::from_millis(100),
                        spica_engine::Timestamp::from_millis(100),
                    )
                    .build(),
                },
            },
        )
        .await;
    let e = storage.get_execution(&exec).await.unwrap().unwrap();
    // `e.value` is the domain `Execution`; `e.created_at` would be the record's entry-derived field.
    assert_eq!(
        e.value.meta.created_at,
        spica_engine::Timestamp::from_millis(100),
        "birth created_at survives projection"
    );
    assert_eq!(
        e.value.meta.updated_at,
        spica_engine::Timestamp::from_millis(100),
        "birth updated_at equals created_at"
    );

    // Completion: updated_at advances, created_at immutably carried forward.
    projector
        .apply(
            &mut storage,
            &Event::ExecutionCompleted {
                execution: Execution {
                    flow_version: ObjectReference::nil(),
                    status: ExecutionStatus::Completed,
                    input: json!({}),
                    output: Some(json!(true)),
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Execution,
                        exec.uid,
                    )
                    .timestamps(
                        spica_engine::Timestamp::from_millis(100),
                        spica_engine::Timestamp::from_millis(300),
                    )
                    .build(),
                },
            },
        )
        .await;
    let e = storage.get_execution(&exec).await.unwrap().unwrap();
    assert_eq!(
        e.value.meta.created_at,
        spica_engine::Timestamp::from_millis(100),
        "created_at is immutable across the lifecycle"
    );
    assert_eq!(
        e.value.meta.updated_at,
        spica_engine::Timestamp::from_millis(300),
        "updated_at advances to the completing event's stamp"
    );
}

/// The event-carried `Activity`/`Timer`/`Task` `created_at`/`updated_at` (stamped at event
/// construction, mirroring `Execution`) survive the projection round-trip — including the
/// de-flattened rows in Storage — and each transition advances the domain value's `updated_at`
/// while its `created_at` stays put. This exercises the mutation-applier value-sync (`t.value
/// .updated_at = <event>.updated_at`) across Activity, Timer, and Task lifecycle transitions.
#[tokio::test]
async fn leaf_domain_timestamps_follow_the_lifecycle() {
    let exec = exec_ref();
    let activity = act_ref();
    let timer = TimerId::new();
    let task = ulid::Ulid::new();
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();
    let ts = spica_engine::Timestamp::from_millis;

    let act_birth = |at: u64| Activity {
        execution: exec.clone(),
        state_path: jsonptr::PointerBuf::parse("/states/S").unwrap(),
        status: ActivityStatus::Running,
        raw_input: json!({}),
        input: Some(json!({})),
        raw_output: None,
        activity_state: None,
        retry_state: None,
        output: None,
        meta: spica_engine::ObjectMeta::builder(spica_engine::ObjectKind::Activity, activity.uid)
            .timestamps(ts(at), ts(at))
            .build()
            .with_owner(exec.clone()),
    };
    // Activity birth (created == updated), then a lifecycle transition advances `updated_at`.
    projector
        .apply(
            &mut storage,
            &Event::StateActivating {
                activity: act_birth(100),
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::StateActivated {
                activity: Activity {
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Activity,
                        activity.uid,
                    )
                    .timestamps(ts(100), ts(200))
                    .build(),
                    ..act_birth(100)
                },
            },
        )
        .await;
    let a = storage.get_activity(&activity).await.unwrap().unwrap();
    assert_eq!(
        a.value.meta.created_at,
        ts(100),
        "activity created_at immutable"
    );
    assert_eq!(
        a.value.meta.updated_at,
        ts(200),
        "activity updated_at advances"
    );

    // Timer birth, then completion advances `updated_at`.
    let timer_birth = Timer {
        execution: exec.clone(),
        purpose: TimerPurpose::ExecutionTimeout,
        status: TimerStatus::Active,
        deadline: ts(500),
        meta: spica_engine::ObjectMeta::builder(spica_engine::ObjectKind::Timer, timer.0)
            .timestamps(ts(100), ts(100))
            .build()
            .with_owner(exec.clone()),
    };
    projector
        .apply(
            &mut storage,
            &Event::TimerActivated {
                timer: timer_birth.clone(),
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::TimerTriggered {
                timer: Timer {
                    status: TimerStatus::Completed,
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Timer,
                        timer.0,
                    )
                    .timestamps(ts(100), ts(150))
                    .build(),
                    ..timer_birth
                },
            },
        )
        .await;
    let tm = storage.get_timer(&timer_ref(timer)).await.unwrap().unwrap();
    assert_eq!(
        tm.value.meta.created_at,
        ts(100),
        "timer created_at immutable"
    );
    assert_eq!(
        tm.value.meta.updated_at,
        ts(150),
        "timer updated_at advances"
    );

    // Task birth, then completion advances `updated_at`.
    let task_birth = Task {
        execution: spica_engine::ObjectReference::nil(),
        resource: "urn:svc".to_string(),
        arguments: json!({}),
        status: TaskStatus::Pending,
        deadline: None,
        worker_id: None,
        lease_until: None,
        retry_plan: vec![],
        retry_state: RetryState::default(),
        meta: spica_engine::ObjectMeta::builder(spica_engine::ObjectKind::Task, task)
            .timestamps(ts(100), ts(100))
            .build()
            .with_owner(activity.clone()),
    };
    projector
        .apply(
            &mut storage,
            &Event::TaskActivated {
                task: task_birth.clone(),
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::TaskCompleted {
                request_id: spica_engine::RequestId::nil(),
                task: Task {
                    status: TaskStatus::Completed,
                    worker_id: None,
                    lease_until: None,
                    meta: spica_engine::ObjectMeta::builder(spica_engine::ObjectKind::Task, task)
                        .timestamps(ts(100), ts(180))
                        .build(),
                    ..task_birth
                },
                output: Value::Null,
            },
        )
        .await;
    let tk = storage.get_task(&task_ref(task)).await.unwrap().unwrap();
    assert_eq!(
        tk.value.meta.created_at,
        ts(100),
        "task created_at immutable"
    );
    assert_eq!(
        tk.value.meta.updated_at,
        ts(180),
        "task updated_at advances"
    );
}

/// Projection timing facts fold the applied entry's frozen `timestamp`: `created_at` is stamped once
/// at birth (`== updated_at`), and `updated_at` advances on every later write while `created_at`
/// stays put. Covering Execution, Activity, Timer, and Task rows.
#[tokio::test]
async fn projection_records_create_and_update_timestamps() {
    let exec = exec_ref();
    let activity = act_ref();
    let timer = TimerId::new();
    let task = ulid::Ulid::new();
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();
    let t = |ms: u64| spica_engine::Timestamp::from_millis(ms);

    // Birth at t=100: created_at == updated_at == 100.
    projector
        .apply_at(
            &mut storage,
            &Event::ExecutionCreated {
                request_id: RequestId::nil(),
                execution: Execution {
                    flow_version: ObjectReference::nil(),
                    status: ExecutionStatus::Running,
                    input: json!({}),
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Execution,
                        exec.uid,
                    )
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .build(),
                },
            },
            t(100),
        )
        .await;
    let e = storage.get_execution(&exec).await.unwrap().unwrap();
    assert_eq!(e.created_at, t(100));
    assert_eq!(e.updated_at, t(100));

    // An update at t=200 advances updated_at, leaves created_at.
    projector
        .apply_at(
            &mut storage,
            &Event::VariablesAssigned {
                scope: exec.clone(),
                variables: Variables::from([("k".to_string(), json!(1))]),
            },
            t(200),
        )
        .await;
    let e = storage.get_execution(&exec).await.unwrap().unwrap();
    assert_eq!(e.created_at, t(100), "created_at is immutable after birth");
    assert_eq!(e.updated_at, t(200));

    // Activity birth at t=200 (created == updated), then completion at t=300 (created stays, updated
    // advances to 300).
    projector
        .apply_at(
            &mut storage,
            &Event::StateActivating {
                activity: Activity {
                    execution: exec.clone(),
                    state_path: jsonptr::PointerBuf::parse("/states/S").unwrap(),
                    status: ActivityStatus::Running,
                    raw_input: json!({}),
                    input: Some(json!({})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Activity,
                        activity.uid,
                    )
                    .timestamps(t(200), t(200))
                    .build()
                    .with_owner(exec.clone()),
                },
            },
            t(200),
        )
        .await;
    projector
        .apply_at(
            &mut storage,
            &Event::StateCompleted {
                activity: Activity {
                    execution: exec.clone(),
                    state_path: jsonptr::PointerBuf::parse("/states/S").unwrap(),
                    status: ActivityStatus::Completed,
                    raw_input: json!({}),
                    input: Some(json!({})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!(42)),
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Activity,
                        activity.uid,
                    )
                    .timestamps(t(200), t(300))
                    .build()
                    .with_owner(exec.clone()),
                },
            },
            t(300),
        )
        .await;
    let a = storage.get_activity(&activity).await.unwrap().unwrap();
    assert_eq!(a.created_at, t(200));
    assert_eq!(a.updated_at, t(300));

    // Timer birth at t=400 (created == updated), then completion at t=450 (created stays, updated
    // advances).
    projector
        .apply_at(
            &mut storage,
            &Event::TimerActivated {
                timer: Timer {
                    execution: exec.clone(),
                    purpose: TimerPurpose::ExecutionTimeout,
                    status: TimerStatus::Active,
                    deadline: t(500),
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Timer,
                        timer.0,
                    )
                    .timestamps(t(400), t(400))
                    .build()
                    .with_owner(exec.clone()),
                },
            },
            t(400),
        )
        .await;
    projector
        .apply_at(
            &mut storage,
            &Event::TimerTriggered {
                timer: Timer {
                    execution: exec.clone(),
                    purpose: TimerPurpose::ExecutionTimeout,
                    status: TimerStatus::Completed,
                    deadline: t(500),
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Timer,
                        timer.0,
                    )
                    .timestamps(t(400), t(450))
                    .build()
                    .with_owner(exec.clone()),
                },
            },
            t(450),
        )
        .await;
    let tm = storage.get_timer(&timer_ref(timer)).await.unwrap().unwrap();
    assert_eq!(tm.created_at, t(400));
    assert_eq!(tm.updated_at, t(450));

    // Task birth at t=600 (created == updated), then failure at t=650 (created stays, updated
    // advances).
    projector
        .apply_at(
            &mut storage,
            &Event::TaskActivated {
                task: Task {
                    execution: spica_engine::ObjectReference::nil(),
                    resource: "urn:svc".to_string(),
                    arguments: json!({}),
                    status: TaskStatus::Pending,
                    deadline: None,
                    worker_id: None,
                    lease_until: None,
                    retry_plan: vec![],
                    retry_state: RetryState::default(),
                    meta: spica_engine::ObjectMeta::builder(spica_engine::ObjectKind::Task, task)
                        .timestamps(t(600), t(600))
                        .build()
                        .with_owner(activity.clone()),
                },
            },
            t(600),
        )
        .await;
    projector
        .apply_at(
            &mut storage,
            &Event::TaskFailed {
                task: Task {
                    execution: spica_engine::ObjectReference::nil(),
                    resource: "urn:svc".to_string(),
                    arguments: json!({}),
                    status: TaskStatus::Failed,
                    deadline: None,
                    worker_id: None,
                    lease_until: None,
                    retry_plan: vec![],
                    retry_state: RetryState::default(),
                    meta: spica_engine::ObjectMeta::builder(spica_engine::ObjectKind::Task, task)
                        .timestamps(t(600), t(650))
                        .build()
                        .with_owner(activity.clone()),
                },
                error: ExecutionError::Runtime(RuntimeError::StateFailed {
                    state: "S".to_string(),
                    error: "boom".to_string(),
                    output: Box::new(Value::Null),
                }),
            },
            t(650),
        )
        .await;
    let tk = storage.get_task(&task_ref(task)).await.unwrap().unwrap();
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
    let flow_version = seed_revision(&mut storage, sm).await;
    common::submit_seed(flow_version, Value::Null, &logstream)
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
            EntryPayload::Noop => {
                // This hand-rolled driver appends via `dispatch` + `append` without a terminating
                // Noop, so none are read; skipped to keep the match exhaustive.
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

// ── complete-phase events carry `raw_output` (not null) ──────────────────────

#[tokio::test]
async fn pass_complete_events_populate_raw_output() {
    // A Pass with no `Output` defaults its result to the input; both complete-phase events must
    // carry that raw result in `raw_output` (the design-review gap #7 — previously left `null`),
    // and `StateCompleted` carries the projected `output` alongside it.
    let input = serde_json::json!({ "v": 1 });
    let sm = parse_sm(r#"{ "StartAt": "P", "States": { "P": { "Type": "Pass", "End": true } } }"#);
    let events = collect_events(sm, input.clone()).await;

    let completing = events
        .iter()
        .find_map(|e| match e {
            Event::StateCompleting { activity } => Some(&activity.raw_output),
            _ => None,
        })
        .expect("a StateCompleting is emitted");
    assert_eq!(
        completing.as_ref(),
        Some(&input),
        "StateCompleting carries the raw result"
    );

    let completed = events
        .iter()
        .find_map(|e| match e {
            Event::StateCompleted { activity } => Some(activity),
            _ => None,
        })
        .expect("a StateCompleted is emitted");
    assert_eq!(
        completed.raw_output.as_ref(),
        Some(&input),
        "StateCompleted carries the raw result"
    );
    assert_eq!(
        completed.output.as_ref(),
        Some(&input),
        "StateCompleted carries the projected output"
    );
}

// ── branch Assign targets a Thread scope and inherits parent variables ───────

#[tokio::test]
async fn thread_scope_receives_assign_and_inherits_parent_variables() {
    let exec = exec_ref();
    let activity = act_ref();
    let thread = Thread {
        execution: exec.clone(),
        state_path: jsonptr::PointerBuf::parse("/states/P/branches/0/states").unwrap(),
        index: 0,
        status: ThreadStatus::Running,
        input: json!({}),
        output: None,
        meta: spica_engine::ObjectMeta::builder(
            spica_engine::ObjectKind::Thread,
            ulid::Ulid::new(),
        )
        .at(spica_engine::Timestamp::from_millis(0))
        .build()
        .with_owner(activity.clone()),
    };
    let thread_ref = thread.reference();
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();

    // A running Execution with `g` already assigned.
    projector
        .apply(
            &mut storage,
            &Event::ExecutionCreated {
                request_id: RequestId::nil(),
                execution: Execution {
                    flow_version: ObjectReference::nil(),
                    status: ExecutionStatus::Running,
                    input: json!({}),
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Execution,
                        exec.uid,
                    )
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .build(),
                },
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::VariablesAssigned {
                scope: exec.clone(),
                variables: Variables::from([("g".to_string(), json!("hi"))]),
            },
        )
        .await;
    // The spawning container Activity owned by that Execution.
    projector
        .apply(
            &mut storage,
            &Event::StateActivating {
                activity: Activity {
                    execution: exec.clone(),
                    state_path: jsonptr::PointerBuf::parse("/states/P").unwrap(),
                    status: ActivityStatus::Running,
                    raw_input: json!({}),
                    input: Some(json!({})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Activity,
                        activity.uid,
                    )
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .build()
                    .with_owner(exec.clone()),
                },
            },
        )
        .await;

    // Spawn the thread: the `ThreadCreated` applier must seed its variables from the enclosing
    // scope (the Execution, via the container Activity's owner) so the branch sees `$g`.
    projector
        .apply(
            &mut storage,
            &Event::ThreadCreated {
                thread: thread.clone(),
            },
        )
        .await;
    let row = storage.get_thread(&thread_ref).await.unwrap().unwrap();
    assert_eq!(row.variables.get("g"), Some(&json!("hi")));

    // A branch `Assign` targets the Thread scope: the applier must write into the thread's own
    // variable snapshot rather than dropping it (the old Execution-only path missed it).
    projector
        .apply(
            &mut storage,
            &Event::VariablesAssigned {
                scope: thread_ref.clone(),
                variables: Variables::from([("x".to_string(), json!(1))]),
            },
        )
        .await;
    let row = storage.get_thread(&thread_ref).await.unwrap().unwrap();
    assert_eq!(row.variables.get("x"), Some(&json!(1)));
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
    let exec = exec_ref();
    let activity = act_ref();
    let timer = TimerId::new();

    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();
    // Apply the full set-up via the real ing events so `active_children`/`parent` links are
    // projected by the same fold handlers running on the production path use.
    for ev in &[
        Event::ExecutionCreated {
            request_id: RequestId::nil(),
            execution: Execution {
                flow_version: ObjectReference::nil(),
                status: ExecutionStatus::Running,
                input: Value::Null,
                output: None,
                meta: spica_engine::ObjectMeta::builder(
                    spica_engine::ObjectKind::Execution,
                    exec.uid,
                )
                .timestamps(
                    spica_engine::Timestamp::from_millis(0),
                    spica_engine::Timestamp::from_millis(0),
                )
                .build(),
            },
        },
        Event::StateActivating {
            activity: Activity {
                execution: exec.clone(),
                state_path: jsonptr::PointerBuf::parse("/states/W").unwrap(),
                status: ActivityStatus::Running,
                raw_input: Value::Null,
                input: Some(Value::Null),
                raw_output: None,
                activity_state: None,
                retry_state: None,
                output: None,
                meta: spica_engine::ObjectMeta::builder(
                    spica_engine::ObjectKind::Activity,
                    activity.uid,
                )
                .timestamps(
                    spica_engine::Timestamp::from_millis(0),
                    spica_engine::Timestamp::from_millis(0),
                )
                .build()
                .with_owner(exec.clone()),
            },
        },
        Event::StateActivated {
            activity: Activity {
                execution: exec.clone(),
                state_path: jsonptr::PointerBuf::parse("/states/W").unwrap(),
                status: ActivityStatus::Running,
                raw_input: Value::Null,
                input: Some(Value::Null),
                raw_output: None,
                activity_state: None,
                retry_state: None,
                output: None,
                meta: spica_engine::ObjectMeta::builder(
                    spica_engine::ObjectKind::Activity,
                    activity.uid,
                )
                .timestamps(
                    spica_engine::Timestamp::from_millis(0),
                    spica_engine::Timestamp::from_millis(0),
                )
                .build()
                .with_owner(exec.clone()),
            },
        },
        Event::TimerActivated {
            timer: Timer {
                execution: exec.clone(),
                purpose: TimerPurpose::WaitResume,
                status: TimerStatus::Active,
                deadline: Timestamp::from_millis(1_000_000_000_000),
                meta: spica_engine::ObjectMeta::builder(spica_engine::ObjectKind::Timer, timer.0)
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .build()
                    .with_owner(activity.clone()),
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
                name: exec.name.clone(),
                uid: Some(exec.uid),
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
            EntryPayload::Noop => {
                // This hand-rolled driver appends via `dispatch` + `append` without a terminating
                // Noop, so none are read; skipped to keep the match exhaustive.
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
    let exec = storage.get_execution(&exec).await.unwrap().unwrap();
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
    let exec = exec_ref();
    // The payload type isn't pinned by later use here (the log is only constructed then dropped),
    // so name it explicitly.
    let logstream = InMemoryLogStream::<EntryPayload>::new();
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();
    projector
        .apply(
            &mut storage,
            &Event::TimerActivated {
                timer: Timer {
                    execution: exec.clone(),
                    purpose: TimerPurpose::ExecutionTimeout,
                    status: TimerStatus::Active,
                    deadline: Timestamp::from_millis(1_000_000_000_000),
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Timer,
                        timer.0,
                    )
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .build()
                    .with_owner(exec.clone()),
                },
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::TimerCancelled {
                timer: Timer {
                    execution: exec.clone(),
                    purpose: TimerPurpose::ExecutionTimeout,
                    status: TimerStatus::Cancelled,
                    deadline: Timestamp::from_millis(1_000_000_000_000),
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Timer,
                        timer.0,
                    )
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .build()
                    .with_owner(exec.clone()),
                },
            },
        )
        .await;

    let mut processor = StreamProcessor::new();
    let out = processor
        .dispatch(
            &Command::TriggerTimer {
                timer: timer_ref(timer),
            },
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
        matches!(err, ExecutionError::Runtime(RuntimeError::TimedOut { .. })),
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
        ExecutionError::Runtime(RuntimeError::StateFailed {
            ref error,
            ref output,
            ..
        }) => {
            assert_eq!(error, "E1");
            assert_eq!(output.as_ref(), &json!({ "Error": "E1", "Cause": "boom" }));
        }
        other => panic!("expected StateFailed, got {other:?}"),
    }
    assert_eq!(err_name, "E1");
}

#[tokio::test]
async fn engine_create_flow_rejects_malformed_definition() {
    // A running engine is required to attempt `create_flow` (the typestate denies an unstarted
    // engine an operation), but the malformed-definition rejection is what we assert here.
    let engine = common::LocalClient::start(common::in_memory_builder())
        .await
        .unwrap();
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
            matches!(
                err,
                ExecutionError::Runtime(RuntimeError::InvalidDefinition(_))
            ),
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
async fn create_execution_handler_rejects_existing_name_as_reject_record() {
    let mut storage = InMemoryStorage::new();
    let mut processor = StreamProcessor::new();

    // Seed an existing execution named "dup_run" directly into storage — the same projection a prior
    // successful `CreateExecution` would have folded (the name is now the execution's primary key).
    let uid: ulid::Ulid = ExecutionId::new().into();
    let name = spica_engine::ObjectName::plain("dup_run").unwrap();
    let _id = ObjectReference::new(spica_engine::ObjectKind::Execution, name.clone(), uid);
    storage
        .put_execution(spica_engine::ExecutionRecord {
            value: Execution {
                flow_version: ObjectReference::nil(),
                status: ExecutionStatus::Running,
                input: Value::Null,
                output: None,
                meta: spica_engine::ObjectMeta::builder(spica_engine::ObjectKind::Execution, uid)
                    .name(name.clone())
                    .at(Timestamp::from_millis(0))
                    .build(),
            },
            variables: Variables::new(),
            current_activity: None,
            active_children: std::collections::HashSet::new(),
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        })
        .await
        .unwrap();

    // Forge a second `CreateExecution` for the same name straight into the engine, bypassing the
    // `Engine` boundary pre-check — exactly what a replayed or racing-concurrent command can do. The
    // handler is the authoritative serialized point and must refuse it with an `AlreadyExists`
    // `Reject` record — never a duplicate `ExecutionCreated` that would clobber the first row.
    let entries = processor
        .dispatch(
            &Command::CreateExecution {
                request_id: RequestId::new(),
                name,
                flow_version: ObjectReference::nil(),
                input: Value::Null,
            },
            &storage,
            EntryId::new(2),
        )
        .await
        .unwrap();

    // Exactly one response entry, and it is a `Reject(AlreadyExists)` — never a duplicate
    // `ExecutionCreated`.
    assert_eq!(
        entries.len(),
        1,
        "a refused CreateExecution emits exactly one response entry, got {}: {entries:?}",
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

/// Seed a single user-named execution row directly — the projection a prior successful
/// `StartExecution` would have folded — to exercise the `TerminateExecution` handler's load + guard
/// paths without driving a full run.
async fn seed_named_execution(
    storage: &mut InMemoryStorage,
    name: spica_engine::ObjectName,
    uid: ulid::Ulid,
    status: ExecutionStatus,
) {
    let _id = ObjectReference::new(spica_engine::ObjectKind::Execution, name.clone(), uid);
    storage
        .put_execution(spica_engine::ExecutionRecord {
            value: Execution {
                flow_version: ObjectReference::nil(),
                status,
                input: Value::Null,
                output: None,
                meta: spica_engine::ObjectMeta::builder(spica_engine::ObjectKind::Execution, uid)
                    .name(name)
                    .at(Timestamp::from_millis(0))
                    .build(),
            },
            variables: Variables::new(),
            current_activity: None,
            active_children: std::collections::HashSet::new(),
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn terminate_execution_guards_and_rejects_problems() {
    use spica_engine::ObjectName as ON;
    let mut storage = InMemoryStorage::new();
    let mut processor = StreamProcessor::new();
    let uid: ulid::Ulid = ExecutionId::new().into();
    let other: ulid::Ulid = ExecutionId::new().into();

    // Seed one running execution "guard_run" (known incarnation uid).
    seed_named_execution(
        &mut storage,
        ON::plain("guard_run").unwrap(),
        uid,
        ExecutionStatus::Running,
    )
    .await;

    // (a) uid mismatch → a single StateConflict Reject, never a termination.
    let entries = processor
        .dispatch(
            &Command::TerminateExecution {
                name: ON::plain("guard_run").unwrap(),
                uid: Some(other),
                reason: TerminationReason::Cancelled,
            },
            &storage,
            EntryId::new(2),
        )
        .await
        .unwrap();
    assert_eq!(
        entries.len(),
        1,
        "a uid-mismatched terminate emits exactly one entry: {entries:?}"
    );
    match &entries[0].payload {
        EntryPayload::Reject(r) => {
            assert_eq!(
                r.rejection_type,
                RejectionType::StateConflict,
                "incarnation guard must be StateConflict: {r:?}"
            );
        }
        other => panic!("expected Reject(StateConflict), got {other:?}"),
    }

    // (b) unknown name → a single NotFound Reject.
    let entries = processor
        .dispatch(
            &Command::TerminateExecution {
                name: ON::plain("ghost").unwrap(),
                uid: None,
                reason: TerminationReason::Cancelled,
            },
            &storage,
            EntryId::new(3),
        )
        .await
        .unwrap();
    assert_eq!(
        entries.len(),
        1,
        "a not-found terminate emits exactly one entry: {entries:?}"
    );
    match &entries[0].payload {
        EntryPayload::Reject(r) => {
            assert_eq!(r.rejection_type, RejectionType::NotFound, "{r:?}");
        }
        other => panic!("expected Reject(NotFound), got {other:?}"),
    }

    // (c) matching uid → proceeds: emits ExecutionTerminating (then the terminal ed), no Reject.
    let entries = processor
        .dispatch(
            &Command::TerminateExecution {
                name: ON::plain("guard_run").unwrap(),
                uid: Some(uid),
                reason: TerminationReason::Cancelled,
            },
            &storage,
            EntryId::new(4),
        )
        .await
        .unwrap();
    assert!(
        entries
            .iter()
            .all(|e| matches!(e.payload, EntryPayload::Event(_))),
        "a guarded-and-matching terminate must not Reject, got {entries:?}"
    );
    assert!(
        matches!(
            &entries[0].payload,
            EntryPayload::Event(Event::ExecutionTerminating { .. })
        ),
        "termination begins with ExecutionTerminating: {entries:?}"
    );
}

#[tokio::test]
async fn terminate_execution_rejects_already_terminal() {
    use spica_engine::ObjectName as ON;
    let mut storage = InMemoryStorage::new();
    let mut processor = StreamProcessor::new();
    let uid: ulid::Ulid = ExecutionId::new().into();

    // Seed an already-terminated execution: a later Terminate cannot run — refuse with InvalidState.
    seed_named_execution(
        &mut storage,
        ON::plain("done_run").unwrap(),
        uid,
        ExecutionStatus::Terminated(TerminationReason::Cancelled),
    )
    .await;
    let entries = processor
        .dispatch(
            &Command::TerminateExecution {
                name: ON::plain("done_run").unwrap(),
                uid: Some(uid),
                reason: TerminationReason::Cancelled,
            },
            &storage,
            EntryId::new(2),
        )
        .await
        .unwrap();
    assert_eq!(
        entries.len(),
        1,
        "a non-running terminate emits exactly one entry: {entries:?}"
    );
    match &entries[0].payload {
        EntryPayload::Reject(r) => {
            assert_eq!(r.rejection_type, RejectionType::InvalidState, "{r:?}");
        }
        other => panic!("expected Reject(InvalidState), got {other:?}"),
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
    let engine = common::LocalClient::start(common::in_memory_builder())
        .await
        .unwrap();
    // Create a definition (returns its never-reused version reference), then run *two* executions
    // against that one created version — both driven by the same long-lived StreamProcessor, no session
    // per run.
    let definition = serde_json::to_string(&sm).unwrap();
    let flow_version = engine
        .create_flow(FlowName::new("my_flow").unwrap(), &definition)
        .await
        .unwrap();
    // `start_for_revision` returns the execution id as soon as the execution is born; the result
    // (success output or failure) is resolved by `wait_for_execution`, which polls the projection.
    let execution_id = engine
        .start_for_revision(
            common::execution_name(),
            flow_version.clone(),
            json!({ "x": 7 }),
        )
        .await
        .expect("first execution should start");
    let result = engine
        .wait_for_execution(&execution_id)
        .await
        .expect("first execution should succeed");
    assert_eq!(result.output, json!(7.0));
    let execution_id = engine
        .start_for_revision(common::execution_name(), flow_version, json!({ "x": 9 }))
        .await
        .expect("second execution against the same version should start");
    let result = engine
        .wait_for_execution(&execution_id)
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
    let engine = common::LocalClient::start(common::in_memory_builder())
        .await
        .unwrap();
    let definition = serde_json::to_string(&sm).unwrap();
    let flow_version = engine
        .create_flow(FlowName::new("my_flow").unwrap(), &definition)
        .await
        .unwrap();

    let execution_id = engine
        .start_for_revision(common::execution_name(), flow_version, Value::Null)
        .await
        .expect("start should return the execution id at birth");
    // The id is real (not a placeholder) and, crucially, `wait_for_execution` observes the same id.
    let result = engine
        .wait_for_execution(&execution_id)
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
    let engine = common::LocalClient::start(common::in_memory_builder())
        .await
        .unwrap();
    let definition = serde_json::to_string(&sm).unwrap();
    let flow_version = engine
        .create_flow(FlowName::new("my_flow").unwrap(), &definition)
        .await
        .unwrap();

    let execution_id = engine
        .start_for_revision(common::execution_name(), flow_version, Value::Null)
        .await
        .expect("start should return the id even for a failing flow");
    let err = engine
        .wait_for_execution(&execution_id)
        .await
        .expect_err("a Fail state must surface as an execution error");
    match err {
        ExecutionError::Runtime(RuntimeError::StateFailed { ref error, .. }) => {
            assert_eq!(error, "E1")
        }
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
    let engine = common::LocalClient::start(common::in_memory_builder())
        .await
        .unwrap();
    let definition = serde_json::to_string(&sm).unwrap();
    let flow_version = engine
        .create_flow(FlowName::new("my_flow").unwrap(), &definition)
        .await
        .unwrap();
    let execution_id = engine
        .start_for_revision(common::execution_name(), flow_version, Value::Null)
        .await
        .expect("start returns the id");

    // `wait_for_execution` takes `&self`, so `tokio::join!` polls two independent waiters concurrently
    // on the same running execution (still in its 1s Wait) without needing a `Clone`.
    let (r1, r2) = tokio::join!(
        engine.wait_for_execution(&execution_id),
        engine.wait_for_execution(&execution_id)
    );
    assert!(r1.expect("waiter 1 succeeds").output.is_null());
    assert!(r2.expect("waiter 2 succeeds").output.is_null());
    engine.stop().await;
}

/// The response registry (see `Engine::ack`) gives every acknowledgement-awaiter its **own** one-shot
/// channel, so any number of blocking operations can be in flight concurrently — the property the
/// earlier shared stream cursor lacked. This drives ten `create_flow` calls in parallel on one
/// Engine (each with a distinct name), and asserts every one returns a real, distinct
/// version reference.
#[tokio::test]
async fn engine_runs_many_create_flow_concurrently() {
    let sm = parse_sm(
        r#"{
          "StartAt": "P",
          "States": { "P": { "Type": "Pass", "End": true } }
        }"#,
    );
    let definition = serde_json::to_string(&sm).unwrap();
    let engine = common::LocalClient::start(common::in_memory_builder())
        .await
        .unwrap();

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
            ObjectReference::nil(),
            "concurrent create_flow #{i} returned a real, distinct flow_version"
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
    task_id: ulid::Ulid,
    status: TaskStatus,
    worker_id: Option<String>,
    lease_until: Option<Timestamp>,
) {
    storage
        .put_task(spica_engine::TaskRecord {
            value: Task {
                execution: spica_engine::ObjectReference::nil(),
                resource: "r".to_string(),
                arguments: Value::Null,
                status,
                deadline: None,
                worker_id,
                lease_until,
                retry_plan: vec![],
                retry_state: RetryState::default(),
                meta: spica_engine::ObjectMeta::builder(spica_engine::ObjectKind::Task, task_id)
                    .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                    .build()
                    .with_owner(act_ref()),
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
async fn poll_tasks_leases_only_available_tasks_of_resource() {
    // A bulk pull (the command behind `TaskApi::poll_tasks`) must lease exactly the `Pending` tasks of
    // its `resource` — never ones already leased/settled/cancelled, and never another resource's.
    let mut storage = InMemoryStorage::new();
    let pending1 = ulid::Ulid::new();
    let pending2 = ulid::Ulid::new();
    let running = ulid::Ulid::new();
    let done = ulid::Ulid::new();
    let cancelled = ulid::Ulid::new();
    let other_resource = ulid::Ulid::new();
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
        .put_task(spica_engine::TaskRecord {
            value: Task {
                execution: spica_engine::ObjectReference::nil(),
                resource: "other".to_string(),
                arguments: Value::Null,
                status: TaskStatus::Pending,
                deadline: None,
                worker_id: None,
                lease_until: None,
                retry_plan: vec![],
                retry_state: RetryState::default(),
                meta: spica_engine::ObjectMeta::builder(
                    spica_engine::ObjectKind::Task,
                    other_resource,
                )
                .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                .build()
                .with_owner(act_ref()),
            },
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        })
        .await
        .unwrap();

    let entries = dispatch_command(
        &storage,
        Command::ClaimTasks {
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
            EntryPayload::Event(Event::TasksClaimed { tasks, .. }) => Some(tasks.iter()),
            _ => None,
        })
        .flatten()
        .map(|t| (t.reference().uid, &t.status, &t.worker_id))
        .collect();
    // Exactly the two `Pending` tasks of `resource "r"` are leased to w2; the running / settled /
    // cancelled / foreign-resource tasks are untouched.
    let mut ids: Vec<_> = leased.iter().map(|(id, _, _)| *id).collect();
    ids.sort();
    let mut expect: Vec<ulid::Ulid> = vec![pending1, pending2];
    expect.sort();
    assert_eq!(
        ids, expect,
        "pull must grant only the resource's Pending tasks"
    );
    for (_, status, worker) in leased {
        assert_eq!(*status, TaskStatus::Running);
        assert_eq!(worker.as_deref(), Some("w2"));
    }
    // Each grant arms its DeliveryLease expiry timer (emitted inline with the lease).
    let timers = entries
        .iter()
        .filter(|e| {
            matches!(
                &e.payload,
                EntryPayload::Event(Event::TimerActivated { timer })
                    if timer.purpose == TimerPurpose::DeliveryLease
            )
        })
        .count();
    assert_eq!(timers, 2, "each granted task arms a DeliveryLease timer");
}

#[tokio::test]
async fn poll_tasks_respects_max_tasks() {
    // `max_tasks` caps the grant: with 3 available, a pull of 2 grants exactly 2.
    let mut storage = InMemoryStorage::new();
    for _ in 0..3 {
        seed_task(
            &mut storage,
            ulid::Ulid::new(),
            TaskStatus::Pending,
            None,
            None,
        )
        .await;
    }
    let entries = dispatch_command(
        &storage,
        Command::ClaimTasks {
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
        .filter_map(|e| match &e.payload {
            EntryPayload::Event(Event::TasksClaimed { tasks, .. }) => Some(tasks.len()),
            _ => None,
        })
        .sum::<usize>();
    assert_eq!(leased, 2, "max_tasks must cap the granted set");
}

#[tokio::test]
async fn stale_task_leased_does_not_override_owner_or_settlement() {
    // The conditional `TasksClaimed` applier folds each lease only while the task is still `Pending`; a
    // stale/racing lease (already leased to someone else, or already settled/cancelled) is a no-op, so
    // the *state* advances exactly-once even though a racing pull may hand the *work* to two workers.
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();
    // Stale lease against an already-leased (Running) task: must not change who owns it.
    let running = ulid::Ulid::new();
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
            &Event::TasksClaimed {
                request_id: spica_engine::RequestId::nil(),
                tasks: vec![Task {
                    execution: spica_engine::ObjectReference::nil(),
                    resource: "r".to_string(),
                    arguments: Value::Null,
                    status: TaskStatus::Running,
                    deadline: None,
                    worker_id: Some("w2".into()),
                    lease_until: Some(Timestamp::from_millis(2000)),
                    retry_plan: vec![],
                    retry_state: RetryState::default(),
                    meta: spica_engine::ObjectMeta::builder(
                        spica_engine::ObjectKind::Task,
                        running,
                    )
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .build()
                    .with_owner(act_ref()),
                }],
            },
        )
        .await;
    let t = storage.get_task(&task_ref(running)).await.unwrap().unwrap();
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
    let done = ulid::Ulid::new();
    seed_task(&mut storage, done, TaskStatus::Completed, None, None).await;
    projector
        .apply(
            &mut storage,
            &Event::TasksClaimed {
                request_id: spica_engine::RequestId::nil(),
                tasks: vec![Task {
                    execution: spica_engine::ObjectReference::nil(),
                    resource: "r".to_string(),
                    arguments: Value::Null,
                    status: TaskStatus::Running,
                    deadline: None,
                    worker_id: Some("w3".into()),
                    lease_until: Some(Timestamp::from_millis(2000)),
                    retry_plan: vec![],
                    retry_state: RetryState::default(),
                    meta: spica_engine::ObjectMeta::builder(spica_engine::ObjectKind::Task, done)
                        .timestamps(
                            spica_engine::Timestamp::from_millis(0),
                            spica_engine::Timestamp::from_millis(0),
                        )
                        .build()
                        .with_owner(act_ref()),
                }],
            },
        )
        .await;
    let t = storage.get_task(&task_ref(done)).await.unwrap().unwrap();
    assert_eq!(
        t.status,
        TaskStatus::Completed,
        "a stale lease must not resurrect a settled task"
    );
}

#[tokio::test]
async fn complete_by_foreign_worker_is_rejected() {
    let mut storage = InMemoryStorage::new();
    let task = ulid::Ulid::new();
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
            task: task_ref(task),
            worker_id: "w2".into(),
            output: json!({ "ok": true }),
            request_id: spica_engine::RequestId::nil(),
        },
    )
    .await;
    // A foreign worker's complete is a request/response refusal: the hander emits a `Reject`
    // (StateConflict — the task is leased to another worker) so the awaiting worker learns why, rather
    // than a silent no-op leaving it to hang on an unmatchable ack.
    let rejects: Vec<_> = entries
        .iter()
        .filter_map(|e| match &e.payload {
            EntryPayload::Reject(rej) => Some(rej),
            _ => None,
        })
        .collect();
    assert_eq!(
        rejects.len(),
        1,
        "a foreign settle must produce exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        spica_engine::RejectionType::StateConflict
    );
}

#[tokio::test]
async fn leasing_worker_complete_settles_task() {
    let mut storage = InMemoryStorage::new();
    let task = ulid::Ulid::new();
    seed_task(
        &mut storage,
        task,
        TaskStatus::Running,
        Some("w1".into()),
        Some(Timestamp::from_millis(1000)),
    )
    .await;
    let request_id = spica_engine::RequestId::new();
    let entries = dispatch_command(
        &storage,
        Command::CompleteTask {
            task: task_ref(task),
            worker_id: "w1".into(),
            output: json!({ "ok": true }),
            request_id,
        },
    )
    .await;
    let completed = entries
        .iter()
        .find_map(|e| match &e.payload {
            EntryPayload::Event(Event::TaskCompleted {
                request_id: echoed,
                task,
                ..
            }) => {
                // The success event echoes the worker's own request id back — the correlation key the
                // request/response ack routes on.
                assert_eq!(*echoed, request_id);
                Some(task)
            }
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
async fn late_complete_after_release_or_cancel_is_refused() {
    let mut storage = InMemoryStorage::new();
    // A helper asserting that a settle on a task not currently Running to the reporting worker is
    // refused with a single `InvalidState` `Reject` (the request/response dlivery), never a silent no-op.
    async fn assert_refused(storage: &InMemoryStorage, task: ulid::Ulid) {
        let entries = dispatch_command(
            storage,
            Command::CompleteTask {
                task: task_ref(task),
                worker_id: "w1".into(),
                output: json!(1),
                request_id: spica_engine::RequestId::nil(),
            },
        )
        .await;
        let rejects: Vec<_> = entries
            .iter()
            .filter_map(|e| match &e.payload {
                EntryPayload::Reject(rej) => Some(rej),
                _ => None,
            })
            .collect();
        assert_eq!(
            rejects.len(),
            1,
            "a settle on a non-Running task must produce exactly one Reject: {entries:?}"
        );
        assert_eq!(
            rejects[0].rejection_type,
            spica_engine::RejectionType::InvalidState
        );
    }

    // Re-queued (released → Pending) after the lease lapsed: the stale worker's late settle is refused.
    let requeued = ulid::Ulid::new();
    seed_task(&mut storage, requeued, TaskStatus::Pending, None, None).await;
    assert_refused(&storage, requeued).await;

    // Same for a cancelled task.
    let cancelled = ulid::Ulid::new();
    seed_task(&mut storage, cancelled, TaskStatus::Cancelled, None, None).await;
    assert_refused(&storage, cancelled).await;
}

#[tokio::test]
async fn release_requeues_task_for_a_fresh_claim() {
    let mut storage = InMemoryStorage::new();
    let task = ulid::Ulid::new();
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
        Command::ReleaseTaskLease {
            task: task_ref(task),
        },
    )
    .await;
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
    // poll by another worker succeeds.
    let proj = Projector::new();
    proj.apply(&mut storage, &Event::TaskLeaseExpired { task: expired })
        .await;
    let entries = dispatch_command(
        &storage,
        Command::ClaimTasks {
            request_id: RequestId::new(),
            worker_id: "w2".into(),
            resource: "r".into(),
            max_tasks: 10,
            lease_seconds: 30,
        },
    )
    .await;
    assert!(
        entries
            .iter()
            .any(|e| matches!(&e.payload, EntryPayload::Event(Event::TasksClaimed { tasks, .. }) if !tasks.is_empty())),
        "a re-queued task can be claimed by a fresh worker: {entries:?}"
    );
}

#[tokio::test]
async fn fail_settlement_requires_lease_or_engine_authority() {
    let mut storage = InMemoryStorage::new();
    // A foreign worker cannot fail a task it does not lease.
    let foreign = ulid::Ulid::new();
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
            task: task_ref(foreign),
            worker_id: "w2".into(),
            error: ExecutionError::Runtime(RuntimeError::TimedOut {
                message: "x".into(),
            }),
        },
    )
    .await;
    assert!(
        entries.is_empty(),
        "a foreign worker's fail must be a no-op: {entries:?}"
    );

    // The engine-authoritative backstop (empty worker_id, e.g. the TaskTimeout deadline) settles any
    // non-terminal task regardless of who holds the lease.
    let stalled = ulid::Ulid::new();
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
            task: task_ref(stalled),
            worker_id: String::new(),
            error: ExecutionError::Runtime(RuntimeError::TimedOut {
                message: "deadline".into(),
            }),
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

#[tokio::test]
async fn task_fail_requeues_same_entity_with_backoff_gate() {
    // The task self-decides its retry from its frozen `retry_plan`: on a matching retrier with
    // budget remaining, the SAME task entity re-queues to `Pending` gated by `next_available_at` —
    // no separate RetryScheduled event, no retry timer armed, worker/lease cleared.
    let mut storage = InMemoryStorage::new();
    let task = ulid::Ulid::new();
    let parent = act_ref();
    storage
        .put_task(spica_engine::TaskRecord {
            value: Task {
                execution: spica_engine::ObjectReference::nil(),
                resource: "r".to_string(),
                arguments: Value::Null,
                status: TaskStatus::Running,
                deadline: None,
                worker_id: Some("w1".into()),
                lease_until: Some(Timestamp::from_millis(1000)),
                retry_plan: vec![RetryPolicy {
                    error_equals: vec!["States.ALL".into()],
                    interval_seconds: 1,
                    max_attempts: 3,
                    backoff_rate: 1.0,
                    max_delay_seconds: None,
                }],
                retry_state: RetryState::default(),
                meta: spica_engine::ObjectMeta::builder(spica_engine::ObjectKind::Task, task)
                    .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                    .build()
                    .with_owner(parent.clone()),
            },
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        })
        .await
        .unwrap();

    let entries = dispatch_command(
        &storage,
        Command::FailTask {
            task: task_ref(task),
            worker_id: "w1".into(),
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "S".to_string(),
                error: "boom".to_string(),
                output: Box::new(Value::Null),
            }),
        },
    )
    .await;
    let failed = entries
        .iter()
        .find_map(|e| match &e.payload {
            EntryPayload::Event(Event::TaskFailed { task, .. }) => Some(task),
            _ => None,
        })
        .expect("a matching retrier should emit TaskFailed (retry scheduled)");
    // Same task entity reused — no fresh task id, no separate RetryScheduled event.
    assert_eq!(failed.reference(), task_ref(task));
    assert_eq!(
        failed.status,
        TaskStatus::Pending,
        "retry re-queues to Pending"
    );
    assert_eq!(failed.worker_id, None, "lease cleared on re-queue");
    assert_eq!(failed.lease_until, None);
    assert_eq!(failed.retry_state.attempts, 1);
    assert!(
        failed.retry_state.next_available_at.is_some(),
        "backoff gate set"
    );
    assert_eq!(failed.retry_state.retrier_attempts.len(), 1);
    assert_eq!(failed.retry_state.retrier_attempts[0].attempt_count, 1);
    // No retry timer: the task is gated by `next_available_at`, and the only child swept is the
    // prior lease — exactly one TaskFailed, no TimerActivated for a retry.
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::TimerActivated { .. })
        )),
        "a retry must not arm a timer; it gated by next_available_at: {entries:?}"
    );
}

#[tokio::test]
async fn retrying_task_is_not_claimable_until_gate_lapses() {
    // A task re-queued by a retry carries a `next_available_at` backoff gate: it stays `Pending`
    // but is not claimable (polled) until that instant has passed.
    let mut storage = InMemoryStorage::new();
    let task = ulid::Ulid::new();
    storage
        .put_task(spica_engine::TaskRecord {
            value: Task {
                execution: spica_engine::ObjectReference::nil(),
                resource: "r".to_string(),
                arguments: Value::Null,
                status: TaskStatus::Pending,
                deadline: None,
                worker_id: None,
                lease_until: None,
                retry_plan: vec![],
                retry_state: RetryState {
                    attempts: 1,
                    retrier_attempts: vec![],
                    next_available_at: Some(Timestamp::from_millis(4_000_000_000_000)),
                },
                meta: spica_engine::ObjectMeta::builder(spica_engine::ObjectKind::Task, task)
                    .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                    .build()
                    .with_owner(act_ref()),
            },
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        })
        .await
        .unwrap();

    let entries = dispatch_command(
        &storage,
        Command::ClaimTasks {
            request_id: RequestId::new(),
            worker_id: "w1".into(),
            resource: "r".into(),
            max_tasks: 10,
            lease_seconds: 60,
        },
    )
    .await;
    assert!(
        !entries
            .iter()
            .any(|e| matches!(&e.payload, EntryPayload::Event(Event::TasksClaimed { tasks, .. }) if !tasks.is_empty())),
        "a retrying task whose backoff gate has not lapsed must not be claimable: {entries:?}"
    );
}
