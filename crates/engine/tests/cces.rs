//! Unit tests for the CCES building blocks: the Storage projection, causality/atomicity of the
//! StreamProcessor's output, the ing/ed lifecycle split, the deferred-ed cascade on a Completing or
//! Terminating parent, and the cancel/timeout race guards.

mod common;

use serde_json::{Value, json};
use spica_asl::StateMachine;
use spica_engine::{
    ActivateState, ActivateTask, Activity, ActivityKind, ActivityStatus, ClaimTasks, Command,
    CompleteExecution, CompleteState, CompleteTask, CompleteThread, CreateExecution, CreateFlow,
    Entry, EntryId, EntryPayload, Event, Execution, ExecutionCreated, ExecutionError,
    ExecutionKind, ExecutionStatus, FailTask, Flow, FlowCreated, FlowName, FlowStatus, FlowVersion,
    FlowVersionCreated, FlowVersionKind, HasRawObjectRef, InMemoryLogStream, LogStream, ObjectRef,
    RawObjectRef, RejectionType, RequestId, RetryPolicy, RetryState, RuntimeError, SpawnThread,
    StateTransitioned, Storage, StreamProcessor, Task, TaskCompleted, TaskFailed, TaskKind,
    TaskStatus, TasksClaimed, TerminateExecution, TerminateState, TerminateThread,
    TerminationReason, Thread, ThreadKind, ThreadOwner, ThreadStatus, Timer, TimerKind, TimerOwner,
    TimerStatus, Timestamp, Variables, VariablesAssigned,
};
use spica_scheduler::{InMemoryScheduler, Scheduler, TimerSink};
use spica_storage::InMemoryStorage;
use tokio_stream::StreamExt;

fn parse_sm(definition: &str) -> StateMachine {
    serde_json::from_str(definition).expect("state machine should parse")
}

/// Build a distinct execution reference shaped exactly like `meta.raw_object_ref()` (the generated
/// `obj-<uid>` name + uid), so an in-memory storage round-trips by reference. The type carries the
/// kind, so a fixture handed to a flat-address read says `.as_raw_object_ref()` itself.
fn exec_ref() -> spica_engine::ObjectRef<ExecutionKind> {
    let uid: ulid::Ulid = ulid::Ulid::new();
    spica_engine::ObjectRef::new(
        spica_engine::PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(uid.0 as u64),
        uid,
    )
}

/// Build a distinct activity reference shaped exactly like `meta.raw_object_ref()` (the generated
/// `obj-<uid>` name + uid), so an in-memory storage round-trips by reference.
fn act_ref() -> spica_engine::ObjectRef<ActivityKind> {
    let uid: ulid::Ulid = ulid::Ulid::new();
    spica_engine::ObjectRef::new(
        spica_engine::PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(uid.0 as u64),
        uid,
    )
}

/// Build the task reference for a task's raw id, shaped exactly like `meta.raw_object_ref()` (the
/// generated `obj-<uid>` name + uid), so an in-memory storage round-trips by reference.
fn task_ref(task: ulid::Ulid) -> spica_engine::ObjectRef<TaskKind> {
    let uid: ulid::Ulid = task;
    spica_engine::ObjectRef::new(
        spica_engine::PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(uid.0 as u64),
        uid,
    )
}

/// Build the timer reference for a timer's raw id, shaped exactly like `meta.raw_object_ref()` (the
/// generated `obj-<uid>` name + uid), so an in-memory storage round-trips by reference.
fn timer_ref(timer: ulid::Ulid) -> spica_engine::ObjectRef<TimerKind> {
    spica_engine::ObjectRef::new(
        spica_engine::PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(timer.0 as u64),
        timer,
    )
}

/// Build the thread reference for a thread's raw id, shaped exactly like `meta.raw_object_ref()` (the
/// generated `obj-<uid>` name + uid), so an in-memory storage round-trips by reference.
fn thread_ref(thread: ulid::Ulid) -> spica_engine::ObjectRef<ThreadKind> {
    spica_engine::ObjectRef::new(
        spica_engine::PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(thread.0 as u64),
        thread,
    )
}

/// The owner slot of a fixture activity the folds below only ever read as a *parent edge* — the run's
/// derived root thread, which a top-level state hangs off. No thread row has to exist for such an
/// edge to be projected (`add_child`/`remove_child` tolerate a missing parent), so a fixture whose
/// subject is not the drain itself can name a stable stand-in instead of seeding a whole root thread.
fn activity_root_thread_owner() -> ObjectRef<ThreadKind> {
    common::thread_owner("child", 4)
}

/// Pre-seed `sm` as a created flow version in `storage`, returning its [`RawObjectRef`].
/// Definition resolution happens from storage at dispatch time (the `CreateExecution` command
/// carries only the flow version reference), so raw-seam drivers seed the definition directly rather
/// than driving a `CreateFlow` command. The definition is stored in its raw ASL string form. Mirrors
/// what the `FlowCreated` applier folds: a `Flow` row (by name, on first appearance) + a `FlowVersion`
/// row (keyed by its `{flow_name}-{version}` name, executions bind to the reference).
async fn seed_revision(
    storage: &mut InMemoryStorage,
    sm: StateMachine,
) -> spica_engine::ObjectRef<FlowVersionKind> {
    let version = 1u32;
    let flow_name = FlowName::new("test_flow").expect("static name is valid");
    let version_name = FlowVersion::version_name(&flow_name, version);
    let flow_version_uid = ulid::Ulid::new();
    let created_at = Timestamp::from_millis(0);
    // The owning Flow's reference, attached to the version below — same scope, uid nil (the flow's
    // name is its sole identity). The slot's type names the kind, so only name and uid are passed.
    let owner = spica_engine::ObjectRef::<spica_engine::FlowKind>::new(
        spica_engine::ObjectName::plain("test_flow").expect("static name is valid"),
        ulid::Ulid::nil(),
    );
    storage
        .put_flow(Flow {
            meta: spica_engine::ObjectMeta::builder(
                // Name is the flow's sole identity — no generation id, so uid is nil.
                ulid::Ulid::nil(),
            )
            .name(spica_engine::ObjectName::plain("test_flow").expect("static name is valid"))
            .at(created_at)
            .with_owner(spica_engine::NoOwner::new()),
            status: FlowStatus::Active,
            // Newest-version counter — the version seeded below is the only one, so it is the latest.
            latest_version: version,
        })
        .await
        .unwrap();
    storage
        .put_flow_version(FlowVersion {
            meta: spica_engine::ObjectMeta::builder(flow_version_uid)
                .name(version_name.clone())
                .at(created_at)
                .with_owner(owner),
            version,
            definition: serde_json::to_string(&sm).expect("state machine serializes"),
            checksum: FlowVersion::definition_checksum(
                &serde_json::to_string(&sm).expect("state machine serializes"),
            ),
        })
        .await
        .unwrap();
    spica_engine::ObjectRef::new(version_name, flow_version_uid)
}

/// Applies events to storage through [`dispatch_event`](spica_engine::dispatch_event), mirroring the event path of
/// `StreamProcessor::run`. Projection tests only fold state and never wait on a fired timer, so any
/// applier-declared timer effects are discarded here (unlike `collect_events`, which routes them).
struct Projector;

impl Projector {
    fn new() -> Self {
        Self
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
            spica_engine::dispatch_event(&mut ctx, event).await.unwrap();
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
    async fn trigger(&self, timer: &ObjectRef<TimerKind>) {
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
            scheduler.schedule(&timer.meta.object_ref(), timer.deadline);
        }
        Event::TimerCancelled { timer } => scheduler.cancel(&timer.meta.object_ref()),
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
                            spica_engine::dispatch_event(&mut ctx, &event).await.unwrap();
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
        Event::FlowCreated(FlowCreated { .. }) => "FlowCreated",
        Event::FlowVersionCreated(FlowVersionCreated { .. }) => "FlowVersionCreated",
        Event::ExecutionCreated(ExecutionCreated { .. }) => "ExecutionCreated",
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
        Event::TasksClaimed(TasksClaimed { .. }) => "TasksClaimed",
        Event::TaskCompleted(TaskCompleted { .. }) => "TaskCompleted",
        Event::TaskFailed(TaskFailed { .. }) => "TaskFailed",
        Event::TaskCancelled { .. } => "TaskCancelled",
        Event::VariablesAssigned(VariablesAssigned { .. }) => "VariablesAssigned",
        Event::StateTransitioned(StateTransitioned { .. }) => "StateTransitioned",
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
            &Event::ExecutionCreated(ExecutionCreated {
                request_id: RequestId::nil(),
                execution: Execution {
                    deadline: None,
                    flow_version: ObjectRef::<FlowVersionKind>::nil(),
                    status: ExecutionStatus::Running,
                    input: json!({ "x": 1 }),
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(exec.uid())
                        .timestamps(
                            spica_engine::Timestamp::from_millis(0),
                            spica_engine::Timestamp::from_millis(0),
                        )
                        .with_owner(spica_engine::NoOwner::new()),
                },
            }),
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::StateActivating {
                activity: Activity {
                    execution: exec.clone(),
                    state_path: jsonptr::PointerBuf::parse("/States/S").unwrap().into(),
                    status: ActivityStatus::Running,
                    raw_input: json!({ "x": 1 }),
                    input: Some(json!({ "x": 1 })),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(activity.uid())
                        .timestamps(
                            spica_engine::Timestamp::from_millis(0),
                            spica_engine::Timestamp::from_millis(0),
                        )
                        .with_owner(activity_root_thread_owner()),
                },
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::ExecutionCompleted {
                execution: Execution {
                    deadline: None,
                    flow_version: ObjectRef::<FlowVersionKind>::nil(),
                    status: ExecutionStatus::Completed,
                    input: json!({ "x": 1 }),
                    output: Some(json!({ "done": true })),
                    meta: spica_engine::ObjectMeta::builder(exec.uid())
                        .timestamps(
                            spica_engine::Timestamp::from_millis(0),
                            spica_engine::Timestamp::from_millis(0),
                        )
                        .with_owner(spica_engine::NoOwner::new()),
                },
            },
        )
        .await;

    let e = storage.get_execution(&exec).await.unwrap().unwrap();
    assert_eq!(e.status, ExecutionStatus::Completed);
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
            &Event::ExecutionCreated(ExecutionCreated {
                request_id: RequestId::nil(),
                execution: Execution {
                    deadline: None,
                    flow_version: ObjectRef::<FlowVersionKind>::nil(),
                    status: ExecutionStatus::Running,
                    input: json!({}),
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(exec.uid())
                        .timestamps(
                            spica_engine::Timestamp::from_millis(100),
                            spica_engine::Timestamp::from_millis(100),
                        )
                        .with_owner(spica_engine::NoOwner::new()),
                },
            }),
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
                    deadline: None,
                    flow_version: ObjectRef::<FlowVersionKind>::nil(),
                    status: ExecutionStatus::Completed,
                    input: json!({}),
                    output: Some(json!(true)),
                    meta: spica_engine::ObjectMeta::builder(exec.uid())
                        .timestamps(
                            spica_engine::Timestamp::from_millis(100),
                            spica_engine::Timestamp::from_millis(300),
                        )
                        .with_owner(spica_engine::NoOwner::new()),
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
    let timer = ulid::Ulid::new();
    let task = ulid::Ulid::new();
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();
    let ts = spica_engine::Timestamp::from_millis;

    let act_birth = |at: u64| Activity {
        execution: exec.clone(),
        state_path: jsonptr::PointerBuf::parse("/States/S").unwrap().into(),
        status: ActivityStatus::Running,
        raw_input: json!({}),
        input: Some(json!({})),
        raw_output: None,
        activity_state: None,
        retry_state: None,
        output: None,
        meta: spica_engine::ObjectMeta::builder(activity.uid())
            .timestamps(ts(at), ts(at))
            .with_owner(activity_root_thread_owner()),
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
                    meta: spica_engine::ObjectMeta::builder(activity.uid())
                        .timestamps(ts(100), ts(200))
                        .with_owner(activity_root_thread_owner()),
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
        status: TimerStatus::Active,
        deadline: ts(500),
        meta: spica_engine::ObjectMeta::builder(timer)
            .timestamps(ts(100), ts(100))
            .with_owner(TimerOwner::Execution(exec.clone())),
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
                    meta: spica_engine::ObjectMeta::builder(timer)
                        .timestamps(ts(100), ts(150))
                        .with_owner(TimerOwner::Execution(exec.clone())),
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
        execution: spica_engine::ObjectRef::<ExecutionKind>::nil(),
        resource: "urn:svc".to_string(),
        arguments: json!({}),
        status: TaskStatus::Pending,
        deadline: None,
        worker_id: None,
        lease_expires_at: None,
        retry_plan: vec![],
        retry_state: RetryState::default(),
        meta: spica_engine::ObjectMeta::builder(task)
            .timestamps(ts(100), ts(100))
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
            &Event::TaskCompleted(TaskCompleted {
                request_id: spica_engine::RequestId::nil(),
                task: Task {
                    status: TaskStatus::Completed,
                    worker_id: None,
                    lease_expires_at: None,
                    meta: spica_engine::ObjectMeta::builder(task)
                        .timestamps(ts(100), ts(180))
                        .with_owner(activity.clone()),
                    ..task_birth
                },
                output: Value::Null,
            }),
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
    let timer = ulid::Ulid::new();
    let task = ulid::Ulid::new();
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();
    let t = |ms: u64| spica_engine::Timestamp::from_millis(ms);

    // Birth at t=100: created_at == updated_at == 100.
    projector
        .apply_at(
            &mut storage,
            &Event::ExecutionCreated(ExecutionCreated {
                request_id: RequestId::nil(),
                execution: Execution {
                    deadline: None,
                    flow_version: ObjectRef::<FlowVersionKind>::nil(),
                    status: ExecutionStatus::Running,
                    input: json!({}),
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(exec.uid())
                        .timestamps(
                            spica_engine::Timestamp::from_millis(0),
                            spica_engine::Timestamp::from_millis(0),
                        )
                        .with_owner(spica_engine::NoOwner::new()),
                },
            }),
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
            &Event::ExecutionCompleting {
                execution: Execution {
                    deadline: None,
                    flow_version: ObjectRef::<FlowVersionKind>::nil(),
                    status: ExecutionStatus::Completing,
                    input: json!({}),
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(exec.uid())
                        .timestamps(t(100), t(200))
                        .with_owner(spica_engine::NoOwner::new()),
                },
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
                    state_path: jsonptr::PointerBuf::parse("/States/S").unwrap().into(),
                    status: ActivityStatus::Running,
                    raw_input: json!({}),
                    input: Some(json!({})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(activity.uid())
                        .timestamps(t(200), t(200))
                        .with_owner(activity_root_thread_owner()),
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
                    state_path: jsonptr::PointerBuf::parse("/States/S").unwrap().into(),
                    status: ActivityStatus::Completed,
                    raw_input: json!({}),
                    input: Some(json!({})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: Some(json!(42)),
                    meta: spica_engine::ObjectMeta::builder(activity.uid())
                        .timestamps(t(200), t(300))
                        .with_owner(activity_root_thread_owner()),
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
                    status: TimerStatus::Active,
                    deadline: t(500),
                    meta: spica_engine::ObjectMeta::builder(timer)
                        .timestamps(t(400), t(400))
                        .with_owner(TimerOwner::Execution(exec.clone())),
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
                    status: TimerStatus::Completed,
                    deadline: t(500),
                    meta: spica_engine::ObjectMeta::builder(timer)
                        .timestamps(t(400), t(450))
                        .with_owner(TimerOwner::Execution(exec.clone())),
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
                    execution: spica_engine::ObjectRef::<ExecutionKind>::nil(),
                    resource: "urn:svc".to_string(),
                    arguments: json!({}),
                    status: TaskStatus::Pending,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState::default(),
                    meta: spica_engine::ObjectMeta::builder(task)
                        .timestamps(t(600), t(600))
                        .with_owner(activity.clone()),
                },
            },
            t(600),
        )
        .await;
    projector
        .apply_at(
            &mut storage,
            &Event::TaskFailed(TaskFailed {
                task: Task {
                    execution: spica_engine::ObjectRef::<ExecutionKind>::nil(),
                    resource: "urn:svc".to_string(),
                    arguments: json!({}),
                    status: TaskStatus::Failed,
                    deadline: None,
                    worker_id: None,
                    lease_expires_at: None,
                    retry_plan: vec![],
                    retry_state: RetryState::default(),
                    meta: spica_engine::ObjectMeta::builder(task)
                        .timestamps(t(600), t(650))
                        .with_owner(activity.clone()),
                },
                error: ExecutionError::Runtime(RuntimeError::StateFailed {
                    state: "S".to_string(),
                    error: "boom".to_string(),
                    output: Box::new(Value::Null),
                }),
            }),
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
    // The run's derived root Thread: the scope a *top-level* state runs in, so where a top-level
    // `Assign` lands — and the enclosing scope a fan-out Thread inherits its variables from.
    let root_thread = Thread {
        execution: exec.clone(),
        state_path: jsonptr::PointerBuf::parse("/States").unwrap().into(),
        start_at: "P".to_string(),
        index: 0,
        status: ThreadStatus::Running,
        input: json!({}),
        output: None,
        meta: spica_engine::ObjectMeta::builder(ulid::Ulid::new())
            .timestamps(
                spica_engine::Timestamp::from_millis(0),
                spica_engine::Timestamp::from_millis(0),
            )
            .with_owner(ThreadOwner::Execution(exec.clone())),
    };
    let root_thread_ref = root_thread.meta.raw_object_ref();
    let thread = Thread {
        execution: exec.clone(),
        state_path: jsonptr::PointerBuf::parse("/States/P/Branches/0/States")
            .unwrap()
            .into(),
        start_at: "A".to_string(),
        index: 0,
        status: ThreadStatus::Running,
        input: json!({}),
        output: None,
        meta: spica_engine::ObjectMeta::builder(ulid::Ulid::new())
            .at(spica_engine::Timestamp::from_millis(0))
            .with_owner(ThreadOwner::Activity(activity.clone())),
    };
    let thread_ref = thread.meta.raw_object_ref();
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();

    // A running Execution carrying the run's input.
    projector
        .apply(
            &mut storage,
            &Event::ExecutionCreated(ExecutionCreated {
                request_id: RequestId::nil(),
                execution: Execution {
                    deadline: None,
                    flow_version: ObjectRef::<FlowVersionKind>::nil(),
                    status: ExecutionStatus::Running,
                    input: json!({}),
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(exec.uid())
                        .timestamps(
                            spica_engine::Timestamp::from_millis(0),
                            spica_engine::Timestamp::from_millis(0),
                        )
                        .with_owner(spica_engine::NoOwner::new()),
                },
            }),
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::ThreadCreated {
                thread: root_thread.clone(),
            },
        )
        .await;
    // A top-level `Assign` targets the root Thread — the scope the top-level states run in.
    projector
        .apply(
            &mut storage,
            &Event::VariablesAssigned(VariablesAssigned {
                scope: common::thread_owner_of(root_thread_ref.clone()),
                variables: Variables::from([("g".to_string(), json!("hi"))]),
            }),
        )
        .await;
    // The spawning container Activity, owned by that root Thread.
    projector
        .apply(
            &mut storage,
            &Event::StateActivating {
                activity: Activity {
                    execution: exec.clone(),
                    state_path: jsonptr::PointerBuf::parse("/States/P").unwrap().into(),
                    status: ActivityStatus::Running,
                    raw_input: json!({}),
                    input: Some(json!({})),
                    raw_output: None,
                    activity_state: None,
                    retry_state: None,
                    output: None,
                    meta: spica_engine::ObjectMeta::builder(activity.uid())
                        .timestamps(
                            spica_engine::Timestamp::from_millis(0),
                            spica_engine::Timestamp::from_millis(0),
                        )
                        .with_owner(common::thread_owner_of(root_thread_ref.clone())),
                },
            },
        )
        .await;

    // Spawn the thread: the `ThreadCreated` applier must seed its variables from the enclosing scope
    // (the root Thread, reached through the container Activity's owner) so the branch sees `$g`.
    projector
        .apply(
            &mut storage,
            &Event::ThreadCreated {
                thread: thread.clone(),
            },
        )
        .await;
    let row = storage
        .get_thread(&thread_ref.clone().typed::<ThreadKind>())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.variables.get("g"), Some(&json!("hi")));

    // A branch `Assign` targets the branch Thread: the applier writes into the thread's own
    // variable snapshot, so the branch sees its own `$x` alongside the inherited `$g`.
    projector
        .apply(
            &mut storage,
            &Event::VariablesAssigned(VariablesAssigned {
                scope: common::thread_owner_of(thread_ref.clone()),
                variables: Variables::from([("x".to_string(), json!(1))]),
            }),
        )
        .await;
    let row = storage
        .get_thread(&thread_ref.clone().typed::<ThreadKind>())
        .await
        .unwrap()
        .unwrap();
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
    // Active `Seconds` timer. Injecting TerminateExecution must (a) emit ExecutionTerminating,
    // (b) sweep the activity and its timer, (c) drain the execution to ExecutionTerminated once
    // the children are terminal — and the cascade's emission order must be observable.
    let exec = exec_ref();
    let activity = act_ref();
    let timer = ulid::Ulid::new();
    // The run's derived root Thread: every state hangs off it, so the execution drains *through* it
    // — the two-level chain whose emission order the assertions below pin.
    let root_thread = Thread {
        execution: exec.clone(),
        state_path: jsonptr::PointerBuf::parse("/States").unwrap().into(),
        start_at: "W".to_string(),
        index: 0,
        status: ThreadStatus::Running,
        input: Value::Null,
        output: None,
        meta: spica_engine::ObjectMeta::builder(ulid::Ulid::new())
            .timestamps(
                spica_engine::Timestamp::from_millis(0),
                spica_engine::Timestamp::from_millis(0),
            )
            .with_owner(ThreadOwner::Execution(exec.clone())),
    };
    let root_thread_ref = root_thread.meta.raw_object_ref();

    let mut storage = InMemoryStorage::new();
    // Each sweep resolves the machine its owner binds to before it can terminate a real state, so the
    // `Wait` definition at `/States/W` must be resolvable from storage.
    let revision = seed_revision(
        &mut storage,
        parse_sm(
            r#"{ "StartAt": "W", "States": { "W": { "Type": "Wait", "Seconds": 1, "End": true } } }"#,
        ),
    )
    .await;
    let projector = Projector::new();
    // Apply the full set-up via the real ing events so `active_children`/`parent` links are
    // projected by the same fold handlers running on the production path use.
    for ev in &[
        Event::ExecutionCreated(ExecutionCreated {
            request_id: RequestId::nil(),
            execution: Execution {
                deadline: None,
                flow_version: revision.clone(),
                status: ExecutionStatus::Running,
                input: Value::Null,
                output: None,
                meta: spica_engine::ObjectMeta::builder(exec.uid())
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .with_owner(spica_engine::NoOwner::new()),
            },
        }),
        // After `ExecutionCreated`: the root thread is attached to the execution's `active_children`
        // by this fold, which is what makes the execution wait on it below.
        Event::ThreadCreated {
            thread: root_thread.clone(),
        },
        Event::StateActivating {
            activity: Activity {
                execution: exec.clone(),
                state_path: jsonptr::PointerBuf::parse("/States/W").unwrap().into(),
                status: ActivityStatus::Running,
                raw_input: Value::Null,
                input: Some(Value::Null),
                raw_output: None,
                activity_state: None,
                retry_state: None,
                output: None,
                meta: spica_engine::ObjectMeta::builder(activity.uid())
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .with_owner(common::thread_owner_of(root_thread_ref.clone())),
            },
        },
        Event::StateActivated {
            activity: Activity {
                execution: exec.clone(),
                state_path: jsonptr::PointerBuf::parse("/States/W").unwrap().into(),
                status: ActivityStatus::Running,
                raw_input: Value::Null,
                input: Some(Value::Null),
                raw_output: None,
                activity_state: None,
                retry_state: None,
                output: None,
                meta: spica_engine::ObjectMeta::builder(activity.uid())
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .with_owner(common::thread_owner_of(root_thread_ref.clone())),
            },
        },
        Event::TimerActivated {
            timer: Timer {
                execution: exec.clone(),
                status: TimerStatus::Active,
                deadline: Timestamp::from_millis(1_000_000_000_000),
                meta: spica_engine::ObjectMeta::builder(timer)
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .with_owner(TimerOwner::Activity(activity.clone())),
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
            &Command::TerminateExecution(TerminateExecution {
                name: exec.name().clone(),
                uid: Some(exec.uid()),
                reason: TerminationReason::Cancelled,
            }),
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

// ── Race guard: a late TriggerTimer after a cancel must be refused ──────────

#[tokio::test]
async fn late_trigger_timer_after_cancel_is_refused() {
    // An armed timer is cancelled in storage first; then a stale TriggerTimer arrives (a fire
    // that was already in flight). The handler must see the timer's terminal state and act on
    // nothing — no TimerTriggered, no TerminateExecution — recording the refusal instead, so the
    // durable log explains why the deadline went unenforced.
    let timer = ulid::Ulid::new();
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
                    status: TimerStatus::Active,
                    deadline: Timestamp::from_millis(1_000_000_000_000),
                    meta: spica_engine::ObjectMeta::builder(timer)
                        .timestamps(
                            spica_engine::Timestamp::from_millis(0),
                            spica_engine::Timestamp::from_millis(0),
                        )
                        .with_owner(TimerOwner::Execution(exec.clone())),
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
                    status: TimerStatus::Cancelled,
                    deadline: Timestamp::from_millis(1_000_000_000_000),
                    meta: spica_engine::ObjectMeta::builder(timer)
                        .timestamps(
                            spica_engine::Timestamp::from_millis(0),
                            spica_engine::Timestamp::from_millis(0),
                        )
                        .with_owner(TimerOwner::Execution(exec.clone())),
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
    let rejects: Vec<_> = out
        .iter()
        .filter_map(|e| match &e.payload {
            EntryPayload::Reject(rej) => Some(rej),
            _ => None,
        })
        .collect();
    assert_eq!(
        rejects.len(),
        1,
        "a stale TriggerTimer owes exactly one Reject and nothing else: {out:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::InvalidState,
        "a timer already past Active is the wrong-state case, not a missing row: {out:?}"
    );
    assert!(
        !out.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::TimerTriggered { .. })
                | EntryPayload::Command(Command::TerminateExecution(_))
        )),
        "a refused fire must neither fire the timer nor terminate its run: {out:?}"
    );
    drop(logstream);
}

/// A `TriggerTimer` naming a timer that does not exist is refused rather than dropped silently. The row
/// is written by the batch that arms the timer and the scheduler only fires it once that batch is
/// durable, so a miss is the log and the projection disagreeing — the command's own precondition, and
/// the one entry it owes either way.
#[tokio::test]
async fn trigger_timer_without_a_row_is_refused_not_dropped() {
    let storage = InMemoryStorage::new();
    let entries = dispatch_command(
        &storage,
        Command::TriggerTimer {
            timer: timer_ref(ulid::Ulid::new()),
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
        "a fire for a timer that does not exist still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::NotFound,
        "a missing timer row is the command's own precondition, not an engine fault: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::TimerTriggered { .. })
                | EntryPayload::Command(Command::TerminateExecution(_))
        )),
        "a refused fire must neither fire the timer nor terminate its run: {entries:?}"
    );
}

/// A `TerminateThread` naming a thread that does not exist is refused rather than dropped silently: a
/// thread row is written by the batch that creates it, before anything could name it in a sweep, and
/// nothing ever removes a row — so a miss is the log and the projection disagreeing, not a teardown that
/// arrived after its thread was gone.
#[tokio::test]
async fn terminate_thread_without_a_row_is_refused_not_dropped() {
    let storage = InMemoryStorage::new();
    let entries = dispatch_command(
        &storage,
        Command::TerminateThread(TerminateThread {
            thread: thread_ref(ulid::Ulid::new()),
            reason: TerminationReason::Cancelled,
        }),
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
        "a sweep for a thread that does not exist still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::NotFound,
        "a missing thread row is the command's own precondition, not an engine fault: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::ThreadTerminating { .. })
                | EntryPayload::Event(Event::ThreadTerminated { .. })
        )),
        "a refused sweep must not open or close a termination for the row that is gone: {entries:?}"
    );
}

/// A `CompleteThread` naming a thread that does not exist is refused rather than dropped silently: a
/// thread row is written by the batch that creates it, before anything could complete it, and nothing
/// ever removes a row — so a miss is the log and the projection disagreeing, not a finish that arrived
/// after its thread was gone.
#[tokio::test]
async fn complete_thread_without_a_row_is_refused_not_dropped() {
    let storage = InMemoryStorage::new();
    let entries = dispatch_command(
        &storage,
        Command::CompleteThread(CompleteThread {
            thread: thread_ref(ulid::Ulid::new()),
            output: json!({ "ok": true }),
        }),
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
        "a finish for a thread that does not exist still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::NotFound,
        "a missing thread row is the command's own precondition, not an engine fault: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::ThreadCompleting { .. })
                | EntryPayload::Event(Event::ThreadCompleted { .. })
        )),
        "a refused finish must not open or close a completion for the row that is gone: {entries:?}"
    );
}

/// A thread that has already left `Running` — here one torn down by its container's sweep — is refused
/// rather than dropped: the success finish's intent is already satisfied, but the durable log should
/// say the second arrival was a duplicate instead of leaving it indistinguishable from one that
/// applied.
#[tokio::test]
async fn complete_thread_for_a_thread_already_past_running_is_refused() {
    let mut storage = InMemoryStorage::new();
    let thread = thread_ref(ulid::Ulid::new());
    seed_thread_owned_by_execution_at(
        &mut storage,
        thread.clone(),
        exec_ref(),
        ThreadStatus::Terminating(TerminationReason::Cancelled),
    )
    .await;
    let entries = dispatch_command(
        &storage,
        Command::CompleteThread(CompleteThread {
            thread,
            output: json!({ "ok": true }),
        }),
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
        "a duplicate finish still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::InvalidState,
        "a thread past Running is the wrong state, not a missing one: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::ThreadCompleting { .. })
                | EntryPayload::Event(Event::ThreadCompleted { .. })
        )),
        "a refused duplicate must not re-open a completion for a thread already finishing: {entries:?}"
    );
}

/// A `TerminateState` naming an activity that does not exist is refused rather than dropped silently: an
/// activity row is written by the batch that activates it, before any sweep could name it, and nothing
/// ever removes a row — so a miss is the log and the projection disagreeing, not a teardown that arrived
/// after its activity was gone.
#[tokio::test]
async fn terminate_state_without_a_row_is_refused_not_dropped() {
    let storage = InMemoryStorage::new();
    let entries = dispatch_command(
        &storage,
        Command::TerminateState(TerminateState {
            activity: act_ref(),
            reason: TerminationReason::Cancelled,
        }),
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
        "a teardown for an activity that does not exist still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::NotFound,
        "a missing activity row is the command's own precondition, not an engine fault: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::StateTerminating { .. })
                | EntryPayload::Event(Event::StateTerminated { .. })
        )),
        "a refused teardown must not open or close a termination for the row that is gone: {entries:?}"
    );
}

// ── Race guard: a cancel racing a Wait's timer fire must still drain ─────────

#[tokio::test]
async fn terminating_wait_drains_when_its_timer_fires_first() {
    // A cancel parks the Wait activity in `Terminating` while its still-Active resume timer keeps the
    // sweep deferred. The timer then fires *before* `CancelTimer` is dispatched, so the fired timer's
    // child edge is gone and `CancelTimer` would no-op on it: the fired timer's own settle relay is
    // the only thing left that can drain the stranded activity. The late `CompleteState` the fire also
    // issues is refused rather than acted on — safe only because that relay exists.
    let exec = exec_ref();
    let activity = act_ref();
    let timer = ulid::Ulid::new();
    // Every state's owner is a `Thread` — the derived root thread for a top-level run — so the
    // activity below is owned by one, matching the shape `CreateExecution` emits.
    let thread = Thread {
        execution: exec.clone(),
        state_path: jsonptr::PointerBuf::parse("/States").unwrap().into(),
        start_at: "W".to_string(),
        index: 0,
        status: ThreadStatus::Running,
        input: Value::Null,
        output: None,
        meta: spica_engine::ObjectMeta::builder(ulid::Ulid::new())
            .timestamps(
                spica_engine::Timestamp::from_millis(0),
                spica_engine::Timestamp::from_millis(0),
            )
            .with_owner(ThreadOwner::Execution(exec.clone())),
    };
    let thread_ref = thread.meta.raw_object_ref();

    // The refused `CompleteState` still resolves the owning scope's machine before it can reject, so
    // the definition the activity's `state_path` points into must be resolvable from storage.
    let mut storage = InMemoryStorage::new();
    let revision = seed_revision(
        &mut storage,
        parse_sm(
            r#"{ "StartAt": "W", "States": { "W": { "Type": "Wait", "Seconds": 1, "End": true } } }"#,
        ),
    )
    .await;

    let wait_activity = || Activity {
        execution: exec.clone(),
        state_path: jsonptr::PointerBuf::parse("/States/W").unwrap().into(),
        status: ActivityStatus::Running,
        raw_input: Value::Null,
        input: Some(Value::Null),
        raw_output: None,
        activity_state: None,
        retry_state: None,
        output: None,
        meta: spica_engine::ObjectMeta::builder(activity.uid())
            .timestamps(
                spica_engine::Timestamp::from_millis(0),
                spica_engine::Timestamp::from_millis(0),
            )
            .with_owner(common::thread_owner_of(thread_ref.clone())),
    };

    let projector = Projector::new();
    for ev in &[
        Event::ThreadCreated {
            thread: thread.clone(),
        },
        Event::ExecutionCreated(ExecutionCreated {
            request_id: RequestId::nil(),
            execution: Execution {
                deadline: None,
                flow_version: revision.clone(),
                status: ExecutionStatus::Running,
                input: Value::Null,
                output: None,
                meta: spica_engine::ObjectMeta::builder(exec.uid())
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .with_owner(spica_engine::NoOwner::new()),
            },
        }),
        Event::StateActivating {
            activity: wait_activity(),
        },
        Event::StateActivated {
            activity: wait_activity(),
        },
        Event::TimerActivated {
            timer: Timer {
                execution: exec.clone(),
                status: TimerStatus::Active,
                deadline: Timestamp::from_millis(1_000_000_000_000),
                meta: spica_engine::ObjectMeta::builder(timer)
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .with_owner(TimerOwner::Activity(activity.clone())),
            },
        },
    ] {
        projector.apply(&mut storage, ev).await;
    }

    // The cancel arrives first, while the timer is still live: the sweep defers on that child.
    let entries = dispatch_command(
        &storage,
        Command::TerminateState(TerminateState {
            activity: activity.clone(),
            reason: TerminationReason::Cancelled,
        }),
    )
    .await;
    assert!(
        entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::StateTerminating { .. })
        )),
        "the cancel opens with StateTerminating: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::StateTerminated { .. })
        )),
        "the terminal ed must be deferred on the live timer child: {entries:?}"
    );
    for e in &entries {
        if let EntryPayload::Event(ev) = &e.payload {
            projector.apply(&mut storage, ev).await;
        }
    }

    // The timer fires before the sweep reaches it: its edge is removed, and the fired timer must relay
    // its own settle so the stranded `Terminating` activity drains.
    let entries = dispatch_command(
        &storage,
        Command::TriggerTimer {
            timer: timer_ref(timer),
        },
    )
    .await;
    assert!(
        entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::TimerTriggered { .. })
        )),
        "a live timer fires: {entries:?}"
    );
    assert!(
        entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Command(Command::ContinueTerminate { owner }) if *owner == activity.clone().into_raw_object_ref()
        )),
        "the fired timer must relay its settle so the stranded activity drains: {entries:?}"
    );
    for e in &entries {
        if let EntryPayload::Event(ev) = &e.payload {
            projector.apply(&mut storage, ev).await;
        }
    }

    // The `CompleteState` the fire issued arrives at a non-Running activity: refused durably, never a
    // silent no-op.
    let entries = dispatch_command(
        &storage,
        Command::CompleteState(CompleteState {
            activity: activity.clone(),
            output: Value::Null,
        }),
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
        "a late CompleteState must produce exactly one Reject: {entries:?}"
    );
    assert_eq!(rejects[0].rejection_type, RejectionType::InvalidState);

    // The relay's Continue drives the drain that the refused command no longer performs.
    let entries = dispatch_command(
        &storage,
        Command::ContinueTerminate {
            owner: activity.clone().into_raw_object_ref(),
        },
    )
    .await;
    assert!(
        entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::StateTerminated { .. })
        )),
        "the relayed drain must terminate the stranded activity: {entries:?}"
    );
}

// ── Race guard: a Task's timeout timer must relay its settle too ────────────

#[tokio::test]
async fn terminating_task_drains_when_its_deadline_timer_fires() {
    // A `TimeoutSeconds` hangs off the Task activity, so a cancel that races it strands the activity
    // exactly like the Wait case. The relay must run *before* the in-flight task lookup: "no in-flight
    // task" is precisely the state a cancel leaves behind (it swept the task already), and returning
    // early there would leave the activity parked in `Terminating` forever.
    let exec = exec_ref();
    let activity = act_ref();
    let timer = ulid::Ulid::new();
    // Every state's owner is a `Thread` — the derived root thread for a top-level run — so the
    // activity below is owned by one, matching the shape `CreateExecution` emits.
    let thread = Thread {
        execution: exec.clone(),
        state_path: jsonptr::PointerBuf::parse("/States").unwrap().into(),
        start_at: "T".to_string(),
        index: 0,
        status: ThreadStatus::Running,
        input: Value::Null,
        output: None,
        meta: spica_engine::ObjectMeta::builder(ulid::Ulid::new())
            .timestamps(
                spica_engine::Timestamp::from_millis(0),
                spica_engine::Timestamp::from_millis(0),
            )
            .with_owner(ThreadOwner::Execution(exec.clone())),
    };
    let thread_ref = thread.meta.raw_object_ref();

    let mut storage = InMemoryStorage::new();
    // The terminate resolves the machine its owning thread binds to before it can sweep, so the
    // definition the activity's `state_path` points into must be resolvable from storage — same as
    // the Wait analogue but for a `Task`, whose `TimeoutSeconds` deadline hangs off this activity.
    let revision = seed_revision(
        &mut storage,
        parse_sm(
            r#"{ "StartAt": "T", "States": { "T": { "Type": "Task", "Resource": "arn:aws:lambda:::f", "End": true } } }"#,
        ),
    )
    .await;

    let task_activity = || Activity {
        execution: exec.clone(),
        state_path: jsonptr::PointerBuf::parse("/States/T").unwrap().into(),
        status: ActivityStatus::Running,
        raw_input: Value::Null,
        input: Some(Value::Null),
        raw_output: None,
        activity_state: None,
        retry_state: None,
        output: None,
        meta: spica_engine::ObjectMeta::builder(activity.uid())
            .timestamps(
                spica_engine::Timestamp::from_millis(0),
                spica_engine::Timestamp::from_millis(0),
            )
            .with_owner(common::thread_owner_of(thread_ref.clone())),
    };

    let projector = Projector::new();
    for ev in &[
        Event::ThreadCreated {
            thread: thread.clone(),
        },
        Event::ExecutionCreated(ExecutionCreated {
            request_id: RequestId::nil(),
            execution: Execution {
                deadline: None,
                flow_version: revision.clone(),
                status: ExecutionStatus::Running,
                input: Value::Null,
                output: None,
                meta: spica_engine::ObjectMeta::builder(exec.uid())
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .with_owner(spica_engine::NoOwner::new()),
            },
        }),
        Event::StateActivating {
            activity: task_activity(),
        },
        Event::StateActivated {
            activity: task_activity(),
        },
        Event::TimerActivated {
            timer: Timer {
                execution: exec.clone(),
                status: TimerStatus::Active,
                deadline: Timestamp::from_millis(1_000_000_000_000),
                meta: spica_engine::ObjectMeta::builder(timer)
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .with_owner(TimerOwner::Activity(activity.clone())),
            },
        },
    ] {
        projector.apply(&mut storage, ev).await;
    }

    // The cancel parks the activity in `Terminating`, deferring on the live timer.
    let entries = dispatch_command(
        &storage,
        Command::TerminateState(TerminateState {
            activity: activity.clone(),
            reason: TerminationReason::Cancelled,
        }),
    )
    .await;
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::StateTerminated { .. })
        )),
        "the terminal ed must be deferred on the live timer child: {entries:?}"
    );
    for e in &entries {
        if let EntryPayload::Event(ev) = &e.payload {
            projector.apply(&mut storage, ev).await;
        }
    }

    // The deadline elapses: with no in-flight task left, only the relay can drain the activity.
    let entries = dispatch_command(
        &storage,
        Command::TriggerTimer {
            timer: timer_ref(timer),
        },
    )
    .await;
    assert!(
        entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Command(Command::ContinueTerminate { owner }) if *owner == activity.clone().into_raw_object_ref()
        )),
        "a fired deadline must relay its settle even with no in-flight task: {entries:?}"
    );
}

// ── A supervisory timer must not block the success finish ────────────────────

#[tokio::test]
async fn complete_state_sweeps_a_live_supervisory_timer_before_finishing() {
    // A `Task`'s `TimeoutSeconds` child only *bounds* the state; once the task settles it is moot, so the
    // base complete step sweeps it rather than waiting on it (the deadline may be minutes out). Pinned
    // here: a `CompleteState` arriving with one still attached must still finish — if the sweep ever
    // moves out of the base, this step silently defers on that child forever, since a `Running`
    // activity has no child-settle path back into `complete`.
    let exec = exec_ref();
    let activity = act_ref();
    let timer = ulid::Ulid::new();
    // Every state's owner is a `Thread` — the derived root thread for a top-level run — so the
    // activity below is owned by one, matching the shape `CreateExecution` emits.
    let thread = Thread {
        execution: exec.clone(),
        state_path: jsonptr::PointerBuf::parse("/States").unwrap().into(),
        start_at: "T".to_string(),
        index: 0,
        status: ThreadStatus::Running,
        input: Value::Null,
        output: None,
        meta: spica_engine::ObjectMeta::builder(ulid::Ulid::new())
            .timestamps(
                spica_engine::Timestamp::from_millis(0),
                spica_engine::Timestamp::from_millis(0),
            )
            .with_owner(ThreadOwner::Execution(exec.clone())),
    };
    let thread_ref = thread.meta.raw_object_ref();

    let mut storage = InMemoryStorage::new();
    let revision = seed_revision(
        &mut storage,
        parse_sm(
            r#"{ "StartAt": "T", "States": { "T": { "Type": "Task", "Resource": "arn:aws:lambda:::f", "End": true } } }"#,
        ),
    )
    .await;

    let task_activity = || Activity {
        execution: exec.clone(),
        state_path: jsonptr::PointerBuf::parse("/States/T").unwrap().into(),
        status: ActivityStatus::Running,
        raw_input: Value::Null,
        input: Some(Value::Null),
        raw_output: None,
        activity_state: None,
        retry_state: None,
        output: None,
        meta: spica_engine::ObjectMeta::builder(activity.uid())
            .timestamps(
                spica_engine::Timestamp::from_millis(0),
                spica_engine::Timestamp::from_millis(0),
            )
            .with_owner(common::thread_owner_of(thread_ref.clone())),
    };

    let projector = Projector::new();
    for ev in &[
        Event::ThreadCreated {
            thread: thread.clone(),
        },
        Event::ExecutionCreated(ExecutionCreated {
            request_id: RequestId::nil(),
            execution: Execution {
                deadline: None,
                flow_version: revision.clone(),
                status: ExecutionStatus::Running,
                input: Value::Null,
                output: None,
                meta: spica_engine::ObjectMeta::builder(exec.uid())
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .with_owner(spica_engine::NoOwner::new()),
            },
        }),
        Event::StateActivating {
            activity: task_activity(),
        },
        Event::StateActivated {
            activity: task_activity(),
        },
        Event::TimerActivated {
            timer: Timer {
                execution: exec.clone(),
                status: TimerStatus::Active,
                deadline: Timestamp::from_millis(1_000_000_000_000),
                meta: spica_engine::ObjectMeta::builder(timer)
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .with_owner(TimerOwner::Activity(activity.clone())),
            },
        },
    ] {
        projector.apply(&mut storage, ev).await;
    }

    // The settled task resumes the state while the timeout timer is still live.
    let entries = dispatch_command(
        &storage,
        Command::CompleteState(CompleteState {
            activity: activity.clone(),
            output: Value::Null,
        }),
    )
    .await;
    assert!(
        entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::TimerCancelled { timer: t }) if t.meta.uid == timer
        )),
        "the base complete step must sweep the live supervisory timer: {entries:?}"
    );
    assert!(
        entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::StateCompleted { .. })
        )),
        "the swept activity must reach its success finish: {entries:?}"
    );

    // The finishing decision is durable from the moment the command is accepted, so `StateCompleting`
    // precedes everything the step does afterwards — including the supervisory sweep it opens with.
    let events: Vec<Event> = entries
        .iter()
        .filter_map(|e| match &e.payload {
            EntryPayload::Event(ev) => Some(ev.clone()),
            _ => None,
        })
        .collect();
    assert!(pos(&events, "StateCompleting") < pos(&events, "TimerCancelled"));
    assert!(pos(&events, "TimerCancelled") < pos(&events, "StateCompleted"));
}

// ── A Wait's complete step is never deferred ─────────────────────────────────

#[tokio::test]
async fn a_premature_complete_routes_immediately_and_a_later_timer_fire_is_a_noop() {
    // A `Wait` owns a resume timer, but it reaches the complete step only through that timer's settle —
    // which drains the timer's child edge in the same batch — so `after_completing` is always childless
    // and projects in one go. A `CompleteState` that nonetheless lands while the timer is still live
    // (the engine never issues one early; the timer is its sole producer) is therefore answered the
    // same way: the terminal and the route to the successor land in this same batch, nothing is left
    // to drain later, and a subsequent timer fire finds an already-terminal activity and settles as a
    // no-op.
    let exec = exec_ref();
    let activity = act_ref();
    let timer = ulid::Ulid::new();
    // Every state's owner is a `Thread` — the derived root thread for a top-level run — so the
    // activity below is owned by one, matching the shape `CreateExecution` emits.
    let thread = Thread {
        execution: exec.clone(),
        state_path: jsonptr::PointerBuf::parse("/States").unwrap().into(),
        start_at: "W".to_string(),
        index: 0,
        status: ThreadStatus::Running,
        input: Value::Null,
        output: None,
        meta: spica_engine::ObjectMeta::builder(ulid::Ulid::new())
            .timestamps(
                spica_engine::Timestamp::from_millis(0),
                spica_engine::Timestamp::from_millis(0),
            )
            .with_owner(ThreadOwner::Execution(exec.clone())),
    };
    let thread_ref = thread.meta.raw_object_ref();

    let mut storage = InMemoryStorage::new();
    let revision = seed_revision(
        &mut storage,
        parse_sm(
            r#"{
              "StartAt": "W",
              "States": {
                "W": { "Type": "Wait", "Seconds": 1, "Next": "P" },
                "P": { "Type": "Pass", "End": true }
              }
            }"#,
        ),
    )
    .await;

    let wait_activity = || Activity {
        execution: exec.clone(),
        state_path: jsonptr::PointerBuf::parse("/States/W").unwrap().into(),
        status: ActivityStatus::Running,
        raw_input: Value::Null,
        input: Some(Value::Null),
        raw_output: None,
        activity_state: None,
        retry_state: None,
        output: None,
        meta: spica_engine::ObjectMeta::builder(activity.uid())
            .timestamps(
                spica_engine::Timestamp::from_millis(0),
                spica_engine::Timestamp::from_millis(0),
            )
            .with_owner(common::thread_owner_of(thread_ref.clone())),
    };

    let projector = Projector::new();
    for ev in &[
        Event::ThreadCreated {
            thread: thread.clone(),
        },
        Event::ExecutionCreated(ExecutionCreated {
            request_id: RequestId::nil(),
            execution: Execution {
                deadline: None,
                flow_version: revision.clone(),
                status: ExecutionStatus::Running,
                input: Value::Null,
                output: None,
                meta: spica_engine::ObjectMeta::builder(exec.uid())
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .with_owner(spica_engine::NoOwner::new()),
            },
        }),
        Event::StateActivating {
            activity: wait_activity(),
        },
        Event::StateActivated {
            activity: wait_activity(),
        },
        Event::TimerActivated {
            timer: Timer {
                execution: exec.clone(),
                status: TimerStatus::Active,
                deadline: Timestamp::from_millis(1_000_000_000_000),
                meta: spica_engine::ObjectMeta::builder(timer)
                    .timestamps(
                        spica_engine::Timestamp::from_millis(0),
                        spica_engine::Timestamp::from_millis(0),
                    )
                    .with_owner(TimerOwner::Activity(activity.clone())),
            },
        },
    ] {
        projector.apply(&mut storage, ev).await;
    }

    // (1) The premature complete: accepted (the activity is running), opened, and — childless by the
    // invariant — finished and routed in the same batch, with no `StateCompleted` deferred.
    let raw_result = json!({ "n": 7 });
    let entries = dispatch_command(
        &storage,
        Command::CompleteState(CompleteState {
            activity: activity.clone(),
            output: raw_result.clone(),
        }),
    )
    .await;
    assert!(
        entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::StateCompleting { .. })
        )),
        "the accepted complete opens the finish: {entries:?}"
    );
    let completed = entries
        .iter()
        .find_map(|e| match &e.payload {
            EntryPayload::Event(Event::StateCompleted { activity }) => Some(activity),
            _ => None,
        })
        .expect("the premature complete must reach its terminal without deferring");
    assert_eq!(
        completed.output.as_ref(),
        Some(&raw_result),
        "the raw result carried on the command is the projection's result"
    );
    assert!(
        entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::StateTransitioned(t)) if t.next.as_str() == "/States/P"
        )),
        "the premature complete must route through the state's finish: {entries:?}"
    );
    for e in &entries {
        if let EntryPayload::Event(ev) = &e.payload {
            projector.apply(&mut storage, ev).await;
        }
    }

    // (2) The timer fires later: its owner already reached its terminal in (1), so the settle drains
    // nothing and no Continue hop is issued.
    let entries = dispatch_command(
        &storage,
        Command::TriggerTimer {
            timer: timer_ref(timer),
        },
    )
    .await;
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Command(Command::ContinueComplete { .. })
        )),
        "a terminal activity has no finish left to drain: {entries:?}"
    );
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
    // ExecutionTerminated{Failed{TimedOut}} — not a hang on the 600s wait. The wait resolves to the
    // terminal snapshot; the timeout is read off its `status`, not the call's error.
    let exec = common::create_and_run(common::in_memory_builder(), sm, Value::Null)
        .await
        .expect("a timed-out execution still returns its terminal snapshot");
    let err = match &exec.status {
        ExecutionStatus::Terminated(TerminationReason::Failed { error }) => error,
        other => panic!("expected Terminated(Failed), got {other:?}"),
    };
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
    assert_eq!(result.output, Some(json!("hi")));
}

#[tokio::test]
async fn engine_start_fail_produces_state_failed_error() {
    let sm = parse_sm(
        r#"{ "StartAt": "F", "States": { "F": { "Type": "Fail", "Error": "E1", "Cause": "boom" } } }"#,
    );
    let exec = common::create_and_run(common::in_memory_builder(), sm, Value::Null)
        .await
        .expect("a failed run returns its terminal snapshot");
    let err = match &exec.status {
        ExecutionStatus::Terminated(TerminationReason::Failed { error }) => error,
        other => panic!("expected Terminated(Failed), got {other:?}"),
    };
    let err_name = err.error_name().to_string();
    match err {
        ExecutionError::Runtime(RuntimeError::StateFailed { error, output, .. }) => {
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
            &Command::CreateFlow(CreateFlow {
                request_id: RequestId::new(),
                name: FlowName::new("test_flow").unwrap(),
                definition: serde_json::to_string(&sm).unwrap(),
            }),
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
    let uid: ulid::Ulid = ulid::Ulid::new();
    let name = spica_engine::ObjectName::plain("dup_run").unwrap();
    let _id = RawObjectRef::new(spica_engine::ObjectKind::Execution, name.clone(), uid);
    storage
        .put_execution(spica_engine::ExecutionRecord {
            value: Execution {
                deadline: None,
                flow_version: ObjectRef::<FlowVersionKind>::nil(),
                status: ExecutionStatus::Running,
                input: Value::Null,
                output: None,
                meta: spica_engine::ObjectMeta::builder(uid)
                    .name(name.clone())
                    .at(Timestamp::from_millis(0))
                    .with_owner(spica_engine::NoOwner::new()),
            },
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
            &Command::CreateExecution(CreateExecution {
                request_id: RequestId::new(),
                name,
                flow_version: ObjectRef::<FlowVersionKind>::nil(),
                input: Value::Null,
            }),
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
    let _id = RawObjectRef::new(spica_engine::ObjectKind::Execution, name.clone(), uid);
    storage
        .put_execution(spica_engine::ExecutionRecord {
            value: Execution {
                deadline: None,
                flow_version: ObjectRef::<FlowVersionKind>::nil(),
                status,
                input: Value::Null,
                output: None,
                meta: spica_engine::ObjectMeta::builder(uid)
                    .name(name)
                    .at(Timestamp::from_millis(0))
                    .with_owner(spica_engine::NoOwner::new()),
            },
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
    let uid: ulid::Ulid = ulid::Ulid::new();
    let other: ulid::Ulid = ulid::Ulid::new();

    // Seed one running execution "guard_run" (known incarnation uid).
    seed_named_execution(
        &mut storage,
        ON::plain("guard_run").unwrap(),
        uid,
        ExecutionStatus::Running,
    )
    .await;

    // (a) uid mismatch → a single InvalidState Reject, never a termination.
    let entries = processor
        .dispatch(
            &Command::TerminateExecution(TerminateExecution {
                name: ON::plain("guard_run").unwrap(),
                uid: Some(other),
                reason: TerminationReason::Cancelled,
            }),
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
                RejectionType::InvalidState,
                "incarnation guard must be InvalidState: {r:?}"
            );
        }
        other => panic!("expected Reject(InvalidState), got {other:?}"),
    }

    // (b) unknown name → a single NotFound Reject.
    let entries = processor
        .dispatch(
            &Command::TerminateExecution(TerminateExecution {
                name: ON::plain("ghost").unwrap(),
                uid: None,
                reason: TerminationReason::Cancelled,
            }),
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
            &Command::TerminateExecution(TerminateExecution {
                name: ON::plain("guard_run").unwrap(),
                uid: Some(uid),
                reason: TerminationReason::Cancelled,
            }),
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
    let uid: ulid::Ulid = ulid::Ulid::new();

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
            &Command::TerminateExecution(TerminateExecution {
                name: ON::plain("done_run").unwrap(),
                uid: Some(uid),
                reason: TerminationReason::Cancelled,
            }),
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
            &Command::CreateFlow(CreateFlow {
                request_id: RequestId::new(),
                name: FlowName::new("bad_flow").unwrap(),
                definition: "not a state machine".to_string(),
            }),
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

/// A command payload carries its owner in a *typed* slot, so a wrong kind cannot be built as a
/// value at all. The one place it can still arrive is the wire, where the payload is decoded — and
/// the slot's own `Deserialize` runs the checked conversion there, so a payload naming a thread where
/// an activity belongs is refused on the way in rather than repaired into a row nothing could read.
#[test]
fn owner_slots_refuse_a_foreign_owner_kind_at_decode() {
    let command = Command::ActivateTask(ActivateTask {
        execution: exec_ref(),
        owner: act_ref(),
        task: task_ref(ulid::Ulid::from(9u128)),
        resource: "service-a".to_string(),
        arguments: json!({}),
        retry_plan: Vec::new(),
        deadline: None,
    });
    let wire = serde_json::to_value(&command).expect("a command is serializable");
    // The same payload with only the owner's kind swapped — what a stale or buggy writer could still
    // put on the wire.
    let mut foreign = wire.clone();
    foreign["ActivateTask"]["owner"] = serde_json::to_value(spica_engine::RawObjectRef::new(
        spica_engine::ObjectKind::Thread,
        common::name("branch"),
        ulid::Ulid::new(),
    ))
    .expect("a reference is serializable");

    let error = serde_json::from_value::<Command>(foreign)
        .expect_err("a foreign owner kind must not decode into a typed slot");
    assert!(
        error.to_string().contains("Thread"),
        "the refusal names the kind it saw: {error}"
    );
    // The same payload with its own owner decodes: what was refused is the kind, not the shape.
    assert_eq!(
        serde_json::from_value::<Command>(wire).expect("its own shape decodes"),
        command
    );
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
    assert_eq!(result.output, Some(json!(7.0)));
    let execution_id = engine
        .start_for_revision(common::execution_name(), flow_version, json!({ "x": 9 }))
        .await
        .expect("second execution against the same version should start");
    let result = engine
        .wait_for_execution(&execution_id)
        .await
        .expect("second execution should succeed");
    assert_eq!(result.output, Some(json!(9.0)));
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
    assert_eq!(result.output, Some(json!(42.0)));
    engine.stop().await;
}

/// `wait_for_execution` returns the **terminal [`Execution`] snapshot even for a failed run** — it
/// does not convert failure into an error. The caller reads the failure off `status`:
/// `Terminated(reason)` for a `Fail`-terminated execution, in contrast to the old contract that
/// surfaced the failure as the call's `Err`.
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
    let exec = engine
        .wait_for_execution(&execution_id)
        .await
        .expect("a failed run still returns its terminal Execution to inspect");
    match &exec.status {
        ExecutionStatus::Terminated(TerminationReason::Failed { error }) => {
            assert_eq!(error.error_name(), "E1")
        }
        other => panic!("expected Terminated(Failed), got {other:?}"),
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
    assert!(
        r1.expect("waiter 1 succeeds")
            .output
            .as_ref()
            .unwrap()
            .is_null()
    );
    assert!(
        r2.expect("waiter 2 succeeds")
            .output
            .as_ref()
            .unwrap()
            .is_null()
    );
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
            ObjectRef::<FlowVersionKind>::nil(),
            "concurrent create_flow #{i} returned a real, distinct flow_version"
        );
    }
    engine.stop().await;
}

// ── Task lease lifecycle guards ───────────────────────────────────────────────
//
// These exercise the Zeebe-style job contract at the command/handler seam with full control over
// `worker_id` (the integration harness's in-memory worker hides it). Each test seeds a lone task row
// and dispatches a single command through the real handler. Only the task row is needed for the
// guards: every settlement guard (`is_activated` + `worker_id` match) returns *before* touching the
// owning activity. A test that asserts the *beyond-guard* emission must also seed that activity (see
// [`seed_owning_activity`]), since the task's settle is handed to the owning activity's container,
// which reads the activity to decide what the settle means.

/// Seed a `task` row with the given domain state, owning it under a throwaway activity, and return
/// that owning activity's reference — the row [`seed_owning_activity`] can seed when a test needs the
/// settle to reach past the guards.
async fn seed_task(
    storage: &mut InMemoryStorage,
    task_id: ulid::Ulid,
    status: TaskStatus,
    worker_id: Option<String>,
    lease_expires_at: Option<Timestamp>,
) -> spica_engine::ObjectRef<ActivityKind> {
    let owner = act_ref();
    storage
        .put_task(spica_engine::TaskRecord {
            value: Task {
                execution: spica_engine::ObjectRef::<ExecutionKind>::nil(),
                resource: "r".to_string(),
                arguments: Value::Null,
                status,
                deadline: None,
                worker_id,
                lease_expires_at,
                retry_plan: vec![],
                retry_state: RetryState::default(),
                meta: spica_engine::ObjectMeta::builder(task_id)
                    .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                    .with_owner(owner.clone()),
            },
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        })
        .await
        .unwrap();
    owner
}

/// Seed the `Running` activity row that owns a [`seed_task`] task — the row the owning activity's
/// container reads to decide a settle. Its own owner is a throwaway thread: nothing on this path reads
/// the scope above it.
async fn seed_owning_activity(
    storage: &mut InMemoryStorage,
    activity: spica_engine::ObjectRef<ActivityKind>,
) {
    let uid: ulid::Ulid = ulid::Ulid::new();
    let owner = RawObjectRef::new(
        spica_engine::ObjectKind::Thread,
        spica_engine::PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(uid.0 as u64),
        uid,
    );
    seed_activity_owned_by(storage, activity, common::thread_owner_of(owner)).await;
}

/// Seed a **live scope** above the activity owning a [`seed_task`] task: a flow version whose machine
/// defines the activity's `state_path`, the run bound to it, and the owning thread. This is the chain
/// the container's Running arm walks to reach the activity's state — its `StateHandler::child_completed`
/// is picked from the machine the owning thread binds to, so a settle whose owner is only an
/// unwritten stand-in (see [`seed_owning_activity`]) resolves no state and resumes nothing.
async fn seed_owning_scope(
    storage: &mut InMemoryStorage,
    activity: spica_engine::ObjectRef<ActivityKind>,
) {
    let sm = parse_sm(
        r#"{ "StartAt": "S", "States": { "S": { "Type": "Task", "Resource": "arn:aws:lambda:::f", "End": true } } }"#,
    );
    let flow_version = seed_revision(storage, sm).await;
    let execution = exec_ref();
    storage
        .put_execution(spica_engine::ExecutionRecord {
            value: Execution {
                deadline: None,
                flow_version,
                status: ExecutionStatus::Running,
                input: Value::Null,
                output: None,
                meta: spica_engine::ObjectMeta::builder(execution.uid())
                    .at(Timestamp::from_millis(0))
                    .with_owner(spica_engine::NoOwner::new()),
            },
            active_children: std::collections::HashSet::new(),
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        })
        .await
        .unwrap();
    let thread = thread_ref(ulid::Ulid::new());
    seed_thread_owned_by_execution(storage, thread.clone(), execution).await;
    seed_activity_owned_by(storage, activity, thread).await;
}

/// Seed the `Running` activity row named by `activity`, owned by `owner` — the row a settle or a
/// complete reads to find the scope above it.
async fn seed_activity_owned_by(
    storage: &mut InMemoryStorage,
    activity: spica_engine::ObjectRef<ActivityKind>,
    owner: ObjectRef<ThreadKind>,
) {
    seed_activity_owned_by_at(storage, activity, owner, ActivityStatus::Running).await;
}

/// The same row with an explicit status, for the cases about an owner that has already left `Running`.
async fn seed_activity_owned_by_at(
    storage: &mut InMemoryStorage,
    activity: spica_engine::ObjectRef<ActivityKind>,
    owner: ObjectRef<ThreadKind>,
    status: ActivityStatus,
) {
    storage
        .put_activity(spica_engine::ActivityRecord {
            value: Activity {
                execution: spica_engine::ObjectRef::<ExecutionKind>::nil(),
                state_path: jsonptr::PointerBuf::parse("/States/S").unwrap().into(),
                status,
                raw_input: json!({ "x": 1 }),
                input: Some(json!({ "x": 1 })),
                raw_output: None,
                activity_state: None,
                retry_state: None,
                output: None,
                meta: spica_engine::ObjectMeta::builder(activity.uid())
                    .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                    .with_owner(owner),
            },
            active_children: std::collections::HashSet::new(),
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        })
        .await
        .unwrap();
}

/// Seed the `Running` thread row named by `thread`, bound to `execution` — the row an
/// `ActivateState` resolves its machine from. The row's meta carries no explicit name, so it keys
/// under the same generated `child-<uid>` a [`thread_ref`] builds.
async fn seed_thread_owned_by_execution(
    storage: &mut InMemoryStorage,
    thread: ObjectRef<ThreadKind>,
    execution: ObjectRef<ExecutionKind>,
) {
    seed_thread_owned_by_execution_at(storage, thread, execution, ThreadStatus::Running).await;
}

/// The same row with an explicit status, for the cases about a thread that has already left `Running`.
async fn seed_thread_owned_by_execution_at(
    storage: &mut InMemoryStorage,
    thread: ObjectRef<ThreadKind>,
    execution: ObjectRef<ExecutionKind>,
    status: ThreadStatus,
) {
    storage
        .put_thread(spica_engine::ThreadRecord::from_value(
            Thread {
                meta: spica_engine::ObjectMeta::builder(thread.uid())
                    .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                    .with_owner(ThreadOwner::Execution(execution.clone())),
                execution,
                state_path: jsonptr::PointerBuf::parse("/States").unwrap().into(),
                start_at: "S".into(),
                index: 0,
                status,
                input: json!({}),
                output: None,
            },
            std::collections::HashSet::new(),
        ))
        .await
        .unwrap();
}

/// A lease that has not lapsed yet, for the cases about a task a worker *holds* (as opposed to one
/// whose lease ran out and is therefore up for grabs again).
fn live_lease() -> Timestamp {
    Timestamp::now()
        .checked_add(std::time::Duration::from_secs(3_600))
        .expect("an hour from now fits in a timestamp")
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
    // A bulk pull (the command behind `TaskApi::poll_tasks`) must lease exactly the claimable tasks of
    // its `resource` — the `Pending` ones — and never one a worker still holds under a live lease, nor
    // a settled/cancelled/foreign-resource one.
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
        Some(live_lease()),
    )
    .await;
    seed_task(&mut storage, done, TaskStatus::Completed, None, None).await;
    seed_task(&mut storage, cancelled, TaskStatus::Cancelled, None, None).await;
    // A `Pending` task of a *different* resource is not this pull's to grant.
    storage
        .put_task(spica_engine::TaskRecord {
            value: Task {
                execution: spica_engine::ObjectRef::<ExecutionKind>::nil(),
                resource: "other".to_string(),
                arguments: Value::Null,
                status: TaskStatus::Pending,
                deadline: None,
                worker_id: None,
                lease_expires_at: None,
                retry_plan: vec![],
                retry_state: RetryState::default(),
                meta: spica_engine::ObjectMeta::builder(other_resource)
                    .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                    .with_owner(act_ref()),
            },
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        })
        .await
        .unwrap();

    let entries = dispatch_command(
        &storage,
        Command::ClaimTasks(ClaimTasks {
            request_id: RequestId::new(),
            worker_id: "w2".into(),
            resource: "r".into(),
            max_tasks: 10,
            lease_seconds: 60,
        }),
    )
    .await;
    let leased: Vec<_> = entries
        .iter()
        .filter_map(|e| match &e.payload {
            EntryPayload::Event(Event::TasksClaimed(TasksClaimed { tasks, .. })) => {
                Some(tasks.iter())
            }
            _ => None,
        })
        .flatten()
        .map(|t| (t.meta.raw_object_ref().uid, &t.status, &t.worker_id))
        .collect();
    // Exactly the two `Pending` tasks of `resource "r"` are leased to w2; the live lease, the settled,
    // the cancelled and the foreign-resource tasks are untouched.
    let mut ids: Vec<_> = leased.iter().map(|(id, _, _)| *id).collect();
    ids.sort();
    let mut expect: Vec<ulid::Ulid> = vec![pending1, pending2];
    expect.sort();
    assert_eq!(
        ids, expect,
        "pull must grant only the resource's claimable tasks"
    );
    for (_, status, worker) in leased {
        assert_eq!(*status, TaskStatus::Running);
        assert_eq!(worker.as_deref(), Some("w2"));
    }
    // A grant writes no side-effect child at all: the lease window lives only as `lease_expires_at` on the
    // leased task itself, so a poll can never be gated behind a timer's lifecycle.
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::TimerActivated { .. })
        )),
        "a claim arms no timer: {entries:?}"
    );
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
        Command::ClaimTasks(ClaimTasks {
            request_id: RequestId::new(),
            worker_id: "w".into(),
            resource: "r".into(),
            max_tasks: 2,
            lease_seconds: 60,
        }),
    )
    .await;
    let leased = entries
        .iter()
        .filter_map(|e| match &e.payload {
            EntryPayload::Event(Event::TasksClaimed(TasksClaimed { tasks, .. })) => {
                Some(tasks.len())
            }
            _ => None,
        })
        .sum::<usize>();
    assert_eq!(leased, 2, "max_tasks must cap the granted set");
}

#[tokio::test]
async fn stale_task_leased_does_not_override_owner_or_settlement() {
    // The conditional `TasksClaimed` applier folds each lease only while the task is still *claimable
    // at the entry's own timestamp*; a stale/racing lease (one a live lease still covers, or one
    // already settled/cancelled) is a no-op, so the *state* advances exactly-once even though a racing
    // pull may hand the *work* to two workers.
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();
    // Stale lease against an already-leased (Running) task: must not change who owns it. `Projector`
    // folds at its fixed 1 ms entry stamp, so this task's 1000 ms lease is live *at that instant* —
    // which is exactly the condition the fold re-decides (a lapsed lease would be a steal instead).
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
            &Event::TasksClaimed(TasksClaimed {
                request_id: spica_engine::RequestId::nil(),
                tasks: vec![Task {
                    execution: spica_engine::ObjectRef::<ExecutionKind>::nil(),
                    resource: "r".to_string(),
                    arguments: Value::Null,
                    status: TaskStatus::Running,
                    deadline: None,
                    worker_id: Some("w2".into()),
                    lease_expires_at: Some(Timestamp::from_millis(2000)),
                    retry_plan: vec![],
                    retry_state: RetryState::default(),
                    meta: spica_engine::ObjectMeta::builder(running)
                        .timestamps(
                            spica_engine::Timestamp::from_millis(0),
                            spica_engine::Timestamp::from_millis(0),
                        )
                        .with_owner(act_ref()),
                }],
            }),
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
            &Event::TasksClaimed(TasksClaimed {
                request_id: spica_engine::RequestId::nil(),
                tasks: vec![Task {
                    execution: spica_engine::ObjectRef::<ExecutionKind>::nil(),
                    resource: "r".to_string(),
                    arguments: Value::Null,
                    status: TaskStatus::Running,
                    deadline: None,
                    worker_id: Some("w3".into()),
                    lease_expires_at: Some(Timestamp::from_millis(2000)),
                    retry_plan: vec![],
                    retry_state: RetryState::default(),
                    meta: spica_engine::ObjectMeta::builder(done)
                        .timestamps(
                            spica_engine::Timestamp::from_millis(0),
                            spica_engine::Timestamp::from_millis(0),
                        )
                        .with_owner(act_ref()),
                }],
            }),
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
        Command::CompleteTask(CompleteTask {
            task: task_ref(task),
            worker_id: "w2".into(),
            output: json!({ "ok": true }),
            request_id: spica_engine::RequestId::nil(),
        }),
    )
    .await;
    // A foreign worker's complete is a request/response refusal: the hander emits a `Reject`
    // (InvalidState — the task is leased to another worker, so the reporter's handle on it is stale)
    // so the awaiting worker learns why, rather than a silent no-op leaving it to hang on an
    // unmatchable ack.
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
        spica_engine::RejectionType::InvalidState
    );
}

#[tokio::test]
async fn leasing_worker_complete_settles_task() {
    let mut storage = InMemoryStorage::new();
    let task = ulid::Ulid::new();
    let owner = seed_task(
        &mut storage,
        task,
        TaskStatus::Running,
        Some("w1".into()),
        Some(Timestamp::from_millis(1000)),
    )
    .await;
    // The beyond-guard path runs: the settle reaches the owning activity's container, which reads that
    // row (a `Running` one) and its scope to pick the state whose `child_completed` decides the settle
    // means "resume my state" — so the whole chain above the activity has to be live.
    seed_owning_scope(&mut storage, owner).await;
    let request_id = spica_engine::RequestId::new();
    let entries = dispatch_command(
        &storage,
        Command::CompleteTask(CompleteTask {
            task: task_ref(task),
            worker_id: "w1".into(),
            output: json!({ "ok": true }),
            request_id,
        }),
    )
    .await;
    let completed = entries
        .iter()
        .find_map(|e| match &e.payload {
            EntryPayload::Event(Event::TaskCompleted(TaskCompleted {
                request_id: echoed,
                task,
                ..
            })) => {
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
    assert_eq!(completed.lease_expires_at, None);
    // The owning Task state resumes via CompleteState.
    assert!(
        entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Command(Command::CompleteState(CompleteState { .. }))
        )),
        "a settled task should resume its state"
    );
}

#[tokio::test]
async fn complete_without_a_live_owning_activity_is_refused() {
    let mut storage = InMemoryStorage::new();
    let task = ulid::Ulid::new();
    // The task is seeded owned by an activity that was never written — the owning row is gone. The
    // settle has no container to hand itself to, so the handler must answer that *before* it logs a
    // `TaskCompleted` nothing could resume.
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
        Command::CompleteTask(CompleteTask {
            task: task_ref(task),
            worker_id: "w1".into(),
            output: json!({ "ok": true }),
            request_id: spica_engine::RequestId::nil(),
        }),
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
        "an ownerless settle must produce exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::InvalidState,
        "an ownerless settle is the stale-incarnation case the sibling guards refuse, not an engine fault"
    );
    assert!(
        !entries
            .iter()
            .any(|e| matches!(&e.payload, EntryPayload::Event(Event::TaskCompleted(_)))),
        "the terminal event must not be written when nothing can resume it: {entries:?}"
    );
    let t = storage.get_task(&task_ref(task)).await.unwrap().unwrap();
    assert_eq!(
        t.status,
        TaskStatus::Running,
        "a refused settle must leave the task leased for its worker"
    );
}

/// A `CompleteExecution` naming a row that does not exist is refused, and refused *as* the command's
/// own precondition: the command is the root thread's relay (`complete_thread`), so it only ever
/// names a run that exists. Answering it with a termination cascade would fan out a
/// `TerminateExecution` for the very row that is gone, which that handler refuses in turn — so the
/// single `Reject` is the whole outcome, and the one followup entry every command owes.
#[tokio::test]
async fn complete_execution_without_a_row_is_refused_not_terminated() {
    let storage = InMemoryStorage::new();
    let entries = dispatch_command(
        &storage,
        Command::CompleteExecution(CompleteExecution {
            execution: exec_ref(),
            output: json!({ "ok": true }),
        }),
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
        "a command whose row is gone still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::NotFound,
        "a gone row is the command's own precondition, not an engine fault: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Command(Command::TerminateExecution(_))
        )),
        "the refusal must not fan out a termination for the row that is gone: {entries:?}"
    );
}

/// A `CompleteExecution` arriving after the run has already decided its outcome is refused, not
/// swallowed: this relay lost a race to the termination cascade (a cancel, a timeout), so the
/// command's precondition — a running run to complete — no longer holds. The single `Reject` is the
/// one followup entry the command owes, and the log's only explanation for why nothing was applied.
#[tokio::test]
async fn complete_execution_on_a_finishing_run_is_refused_not_swallowed() {
    let mut storage = InMemoryStorage::new();
    let name = spica_engine::ObjectName::plain("swept_run").expect("a valid user name");
    let uid: ulid::Ulid = ulid::Ulid::new();
    seed_named_execution(
        &mut storage,
        name.clone(),
        uid,
        ExecutionStatus::Terminating(TerminationReason::Cancelled),
    )
    .await;
    let entries = dispatch_command(
        &storage,
        Command::CompleteExecution(CompleteExecution {
            execution: ObjectRef::<ExecutionKind>::new(name, uid),
            output: json!({ "ok": true }),
        }),
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
        "a relay onto a finishing run still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::InvalidState,
        "a run that already decided its outcome is a wrong-state refusal: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::ExecutionCompleting { .. })
        )),
        "a refused relay must not re-open a run that is already finishing: {entries:?}"
    );
}

/// A `CompleteState` naming an activity that does not exist is refused, and refused *as* the command's
/// own precondition: the row is born in the same batch as its activation and nothing removes it, so a
/// miss means the command was forged or the projection is corrupt. Terminating a missing row is not an
/// answer — `TerminateState` no-ops on one, and no scope is resolvable to fail either — so the single
/// `Reject` is the whole outcome, and the one followup entry every command owes.
#[tokio::test]
async fn complete_state_without_a_row_is_refused_not_terminated() {
    let storage = InMemoryStorage::new();
    let entries = dispatch_command(
        &storage,
        Command::CompleteState(CompleteState {
            activity: act_ref(),
            output: json!({ "ok": true }),
        }),
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
        "a command whose row is gone still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::NotFound,
        "a gone row is the command's own precondition, not an engine fault: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Command(Command::TerminateState(_))
        )),
        "the refusal must not fan out a termination for the row that is gone: {entries:?}"
    );
}

/// The same judgement one hop out: the activity row exists but the thread its owner slot names does
/// not. Rows are never removed, so the pair is a corrupt projection or a forged command — refused as
/// the command's own precondition. Answering it with silence would leave the command with no entry at
/// all *and* no state handler to pick from, since resolving the machine is what needs the thread.
#[tokio::test]
async fn complete_state_without_its_owning_thread_is_refused_not_swallowed() {
    let mut storage = InMemoryStorage::new();
    // The seeded activity's owner is a throwaway thread that is never written, which is exactly the
    // shape under test. One ref, bound once: `act_ref()` mints a fresh uid per call, so seeding with
    // one and dispatching with another would address two different rows and pin the miss one hop too
    // early.
    let activity = act_ref();
    seed_owning_activity(&mut storage, activity.clone()).await;
    let entries = dispatch_command(
        &storage,
        Command::CompleteState(CompleteState {
            activity,
            output: json!({ "ok": true }),
        }),
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
        "a command whose owning scope is gone still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::NotFound,
        "a missing owning thread is the command's own precondition: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::StateCompleting { .. })
        )),
        "no finish may open for an activity whose owner cannot be resolved: {entries:?}"
    );
}

/// A `SpawnThread` whose owning container activity does not exist is refused rather than dropped
/// silently: the owner row is written in the same batch as the fan-out and nothing ever removes a
/// row, so a miss means the command was forged or the projection is corrupt — the command's own
/// precondition, and the one entry it owes either way.
#[tokio::test]
async fn spawn_thread_without_its_owner_activity_is_refused_not_dropped() {
    let storage = InMemoryStorage::new();
    let entries = dispatch_command(
        &storage,
        Command::SpawnThread(SpawnThread {
            owner: act_ref(),
            execution: exec_ref(),
            state_path: Some(
                jsonptr::PointerBuf::parse("/States/P/Branches/0/States")
                    .unwrap()
                    .into(),
            ),
            index: 0,
            start_at: "S".into(),
            input: json!({}),
        }),
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
        "a fan-out with no container to fan under still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::NotFound,
        "a gone owner is the command's own precondition, not an engine fault: {entries:?}"
    );
    assert!(
        !entries
            .iter()
            .any(|e| matches!(&e.payload, EntryPayload::Event(Event::ThreadCreated { .. }))),
        "no child may be created under an owner that does not exist: {entries:?}"
    );
}

/// One hop past the miss: the owner row is there, but it has already left `Running` (here: a container
/// drained by an outer cancel). A fan-out is only ever emitted while its container is `Running` and
/// commands are dispatched in append order, so this pair is the log and the projection disagreeing —
/// refused, not dropped silently the way a late branch would be.
#[tokio::test]
async fn spawn_thread_whose_owner_is_no_longer_running_is_refused_not_dropped() {
    let mut storage = InMemoryStorage::new();
    // The owning thread is written too, so the status is the only thing that can account for the
    // refusal — the guard under test precedes every use of the thread below.
    let thread_uid: ulid::Ulid = ulid::Ulid::new();
    let thread_ref = ObjectRef::<ThreadKind>::new(
        spica_engine::PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(thread_uid.0 as u64),
        thread_uid,
    );
    storage
        .put_thread(spica_engine::ThreadRecord::from_value(
            Thread {
                meta: spica_engine::ObjectMeta::builder(thread_uid)
                    .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                    .with_owner(ThreadOwner::Execution(exec_ref())),
                execution: exec_ref(),
                state_path: jsonptr::PointerBuf::parse("/States").unwrap().into(),
                start_at: "S".into(),
                index: 0,
                status: ThreadStatus::Running,
                input: json!({}),
                output: None,
            },
            std::collections::HashSet::new(),
        ))
        .await
        .unwrap();
    // One ref, bound once: `act_ref()` mints a fresh uid per call, and the row seeded below is what the
    // command must name for the guard under test to be the thing that refuses.
    let activity = act_ref();
    seed_activity_owned_by_at(
        &mut storage,
        activity.clone(),
        thread_ref,
        ActivityStatus::Terminated(TerminationReason::Cancelled),
    )
    .await;
    let entries = dispatch_command(
        &storage,
        Command::SpawnThread(SpawnThread {
            owner: activity,
            execution: exec_ref(),
            state_path: Some(
                jsonptr::PointerBuf::parse("/States/P/Branches/0/States")
                    .unwrap()
                    .into(),
            ),
            index: 0,
            start_at: "S".into(),
            input: json!({}),
        }),
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
        "a fan-out into a container that is gone still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::InvalidState,
        "a container that left Running is the wrong-state case, not a missing row: {entries:?}"
    );
    assert!(
        !entries
            .iter()
            .any(|e| matches!(&e.payload, EntryPayload::Event(Event::ThreadCreated { .. }))),
        "no child may be created under a container that is no longer Running: {entries:?}"
    );
}

/// One hop past that: the owner row is there and still `Running`, but the thread its owner slot names is
/// not. Nothing removes a row, and that slot is written in the batch that births the activity, so the
/// pair is the log and the projection disagreeing — refused, not dropped silently.
#[tokio::test]
async fn spawn_thread_without_its_owning_thread_is_refused_not_dropped() {
    let mut storage = InMemoryStorage::new();
    // The activity's owner is a thread that is never written, which is exactly the shape under test.
    // One ref, bound once: `act_ref()` mints a fresh uid per call, so seeding with one and dispatching
    // with another would pin the miss one hop too early.
    let activity = act_ref();
    let thread_uid: ulid::Ulid = ulid::Ulid::new();
    let thread_ref = ObjectRef::<ThreadKind>::new(
        spica_engine::PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(thread_uid.0 as u64),
        thread_uid,
    );
    seed_activity_owned_by(&mut storage, activity.clone(), thread_ref).await;
    let entries = dispatch_command(
        &storage,
        Command::SpawnThread(SpawnThread {
            owner: activity,
            execution: exec_ref(),
            state_path: Some(
                jsonptr::PointerBuf::parse("/States/P/Branches/0/States")
                    .unwrap()
                    .into(),
            ),
            index: 0,
            start_at: "S".into(),
            input: json!({}),
        }),
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
        "a fan-out whose tree cannot be resolved still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::NotFound,
        "an activity naming a thread that does not exist is the command's own precondition: {entries:?}"
    );
    assert!(
        !entries
            .iter()
            .any(|e| matches!(&e.payload, EntryPayload::Event(Event::ThreadCreated { .. }))),
        "no child may be created under an owner whose tree cannot be resolved: {entries:?}"
    );
}

/// One hop past the misses, the machine itself fails to resolve: the owning thread exists, but the run
/// it belongs to never does. The command may still be perfectly valid, so the run is refused rather
/// than terminated — terminating would kill it for a context failure it did not cause, and the single
/// `Reject` is the entry the command owes either way.
#[tokio::test]
async fn complete_state_whose_machine_cannot_resolve_is_refused_not_terminated() {
    let mut storage = InMemoryStorage::new();
    // One ref, bound once: `act_ref()` mints a fresh uid per call, so seeding with one and dispatching
    // with another would address two different rows and make the activity miss, not the machine, the
    // failure under test.
    let activity = act_ref();
    let root: ulid::Ulid = ulid::Ulid::new();
    let exec = ObjectRef::<ExecutionKind>::new(
        spica_engine::PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(root.0 as u64),
        root,
    );
    let thread_uid: ulid::Ulid = ulid::Ulid::new();
    let thread_ref = ObjectRef::<ThreadKind>::new(
        spica_engine::PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(thread_uid.0 as u64),
        thread_uid,
    );
    // The thread row is written but the execution it names is not: resolving the machine reads that
    // execution for the tree's shared flow version, so this is where the failure surfaces.
    storage
        .put_thread(spica_engine::ThreadRecord::from_value(
            Thread {
                meta: spica_engine::ObjectMeta::builder(thread_uid)
                    .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                    .with_owner(ThreadOwner::Execution(exec.clone())),
                execution: exec,
                state_path: jsonptr::PointerBuf::parse("/States").unwrap().into(),
                start_at: "S".into(),
                index: 0,
                status: ThreadStatus::Running,
                input: json!({}),
                output: None,
            },
            std::collections::HashSet::new(),
        ))
        .await
        .unwrap();
    seed_activity_owned_by(&mut storage, activity.clone(), thread_ref).await;
    let entries = dispatch_command(
        &storage,
        Command::CompleteState(CompleteState {
            activity,
            output: json!({ "ok": true }),
        }),
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
        "a command whose machine cannot be resolved still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::NotFound,
        "an unresolvable machine is the command's own precondition, not an engine fault: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Command(Command::TerminateState(_))
                | EntryPayload::Command(Command::TerminateExecution(_))
        )),
        "a context failure must not terminate a run that did nothing wrong: {entries:?}"
    );
}

/// An `ActivateState` whose owning thread does not exist is refused, not answered by terminating the
/// whole run: the command may be perfectly valid, and nothing has been persisted to attach a
/// state-level terminate to. Rows are never removed and a thread row is written by the batch that
/// creates it — before anything could emit an activation into it — so the miss is the log and the
/// projection disagreeing: the command's own precondition.
#[tokio::test]
async fn activate_state_without_its_owning_thread_is_refused_not_terminated() {
    let storage = InMemoryStorage::new();
    let entries = dispatch_command(
        &storage,
        Command::ActivateState(ActivateState {
            execution: exec_ref(),
            owner: thread_ref(ulid::Ulid::new()),
            state_path: jsonptr::PointerBuf::parse("/States/S").unwrap().into(),
            input: json!({}),
        }),
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
        "an activation with no owning thread still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::NotFound,
        "a missing owning thread is the command's own precondition: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Command(Command::TerminateState(_))
                | EntryPayload::Command(Command::TerminateExecution(_))
        )),
        "a context failure must not terminate a run that did nothing wrong: {entries:?}"
    );
}

/// A thread that exists but has already left `Running` cannot take a new activation. Nothing is in
/// flight that the command would duplicate, so it is not a duplicate to swallow: it names the wrong
/// incarnation — the thread already had its outcome opened — and is refused on the same footing as the
/// sibling handlers' non-Running guards.
#[tokio::test]
async fn activate_state_on_a_thread_past_running_is_refused() {
    let mut storage = InMemoryStorage::new();
    let execution = exec_ref();
    let thread = thread_ref(ulid::Ulid::new());
    seed_thread_owned_by_execution_at(
        &mut storage,
        thread.clone(),
        execution.clone(),
        ThreadStatus::Completed,
    )
    .await;
    let entries = dispatch_command(
        &storage,
        Command::ActivateState(ActivateState {
            execution,
            owner: thread,
            state_path: jsonptr::PointerBuf::parse("/States/S").unwrap().into(),
            input: json!({}),
        }),
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
        "an activation on a settled thread owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::InvalidState,
        "a thread past Running names the wrong incarnation: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Command(Command::TerminateState(_))
                | EntryPayload::Command(Command::TerminateExecution(_))
        )),
        "a command on a thread that already settled must not terminate the run: {entries:?}"
    );
}

/// The same judgement one hop out: the thread row exists but the run it belongs to never does, so the
/// machine cannot resolve. Still the command's precondition, still refused rather than terminated.
#[tokio::test]
async fn activate_state_whose_machine_cannot_resolve_is_refused_not_terminated() {
    let mut storage = InMemoryStorage::new();
    // The thread row names an execution that is never written; resolving the machine reads that
    // execution for the tree's shared flow version, so this is where the failure surfaces.
    let thread = thread_ref(ulid::Ulid::new());
    seed_thread_owned_by_execution(&mut storage, thread.clone(), exec_ref()).await;
    let entries = dispatch_command(
        &storage,
        Command::ActivateState(ActivateState {
            execution: exec_ref(),
            owner: thread,
            state_path: jsonptr::PointerBuf::parse("/States/S").unwrap().into(),
            input: json!({}),
        }),
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
        "an unresolvable machine still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::NotFound,
        "an unresolvable machine is the command's own precondition, not an engine fault: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Command(Command::TerminateState(_))
                | EntryPayload::Command(Command::TerminateExecution(_))
        )),
        "a context failure must not terminate a run that did nothing wrong: {entries:?}"
    );
}

/// A resolvable machine that does not define the state being entered — what an unchecked successor
/// path (`Next`/`StartAt` naming no reachable state) looks like from here. Refused on the same
/// footing as the two misses above, so the durable log names the bad path instead of an activation
/// that could not proceed.
#[tokio::test]
async fn activate_state_naming_a_state_its_machine_does_not_define_is_refused() {
    let mut storage = InMemoryStorage::new();
    let sm = parse_sm(r#"{ "StartAt": "S", "States": { "S": { "Type": "Succeed" } } }"#);
    let flow_version = seed_revision(&mut storage, sm).await;
    let execution = exec_ref();
    storage
        .put_execution(spica_engine::ExecutionRecord {
            value: Execution {
                deadline: None,
                flow_version,
                status: ExecutionStatus::Running,
                input: Value::Null,
                output: None,
                meta: spica_engine::ObjectMeta::builder(execution.uid())
                    .at(Timestamp::from_millis(0))
                    .with_owner(spica_engine::NoOwner::new()),
            },
            active_children: std::collections::HashSet::new(),
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        })
        .await
        .unwrap();
    let thread = thread_ref(ulid::Ulid::new());
    seed_thread_owned_by_execution(&mut storage, thread.clone(), execution.clone()).await;
    let entries = dispatch_command(
        &storage,
        Command::ActivateState(ActivateState {
            execution,
            owner: thread,
            state_path: jsonptr::PointerBuf::parse("/States/NoSuchState")
                .unwrap()
                .into(),
            input: json!({}),
        }),
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
        "a state the machine does not define still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::NotFound,
        "a successor path naming no state is the command's own precondition: {entries:?}"
    );
    assert!(
        !entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::StateActivating { .. })
        )),
        "no activity may be minted for a state that does not exist: {entries:?}"
    );
}

#[tokio::test]
async fn late_complete_after_steal_or_cancel_is_refused() {
    let mut storage = InMemoryStorage::new();
    // A helper asserting that a settle on a task this worker does not hold is refused with a single
    // `Reject` (the request/response delivery), never a silent no-op — the reporting worker is told
    // why. Every case here shares the one `InvalidState` classification, so the *reason* is what has
    // to tell them apart: asserting only the type would still pass if the handler collapsed the
    // ownership check into the status check.
    async fn assert_refused(storage: &InMemoryStorage, task: ulid::Ulid, reason_fragment: &str) {
        let entries = dispatch_command(
            storage,
            Command::CompleteTask(CompleteTask {
                task: task_ref(task),
                worker_id: "w1".into(),
                output: json!(1),
                request_id: spica_engine::RequestId::nil(),
            }),
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
            "a settle on a task this worker does not hold must produce exactly one Reject: {entries:?}"
        );
        assert_eq!(
            rejects[0].rejection_type,
            spica_engine::RejectionType::InvalidState
        );
        assert!(
            rejects[0].rejection_reason.contains(reason_fragment),
            "the refusal must say why: expected {reason_fragment:?} in {:?}",
            rejects[0].rejection_reason
        );
    }

    // Stolen by a second worker once the first one's lease lapsed: the task runs for `w2` now, so the
    // stale `w1`'s late settle is refused on ownership — the state advances once for whoever holds it.
    let stolen = ulid::Ulid::new();
    seed_task(
        &mut storage,
        stolen,
        TaskStatus::Running,
        Some("w2".into()),
        Some(live_lease()),
    )
    .await;
    assert_refused(&storage, stolen, "is leased to").await;

    // A task no worker ever leased has no settle to take: the status refuses it before the lease is
    // ever looked at.
    let unclaimed = ulid::Ulid::new();
    seed_task(&mut storage, unclaimed, TaskStatus::Pending, None, None).await;
    assert_refused(&storage, unclaimed, "but it is Pending").await;

    // Same for a cancelled task.
    let cancelled = ulid::Ulid::new();
    seed_task(&mut storage, cancelled, TaskStatus::Cancelled, None, None).await;
    assert_refused(&storage, cancelled, "but it is Cancelled").await;
}

#[tokio::test]
async fn late_complete_after_a_lapsed_lease_is_accepted() {
    // The deliberate other half of a steal: a lease that lapsed with nobody re-claiming it leaves the
    // task `Running` for `w1`, so `w1`'s late settle is accepted — the work is done, and re-queueing it
    // for a second worker would run it twice. Only a task *taken over* refuses the old worker.
    let mut storage = InMemoryStorage::new();
    let task = ulid::Ulid::new();
    let owner = seed_task(
        &mut storage,
        task,
        TaskStatus::Running,
        Some("w1".into()),
        Some(Timestamp::from_millis(1000)),
    )
    .await;
    // The settle is handed to the owning activity's container, so that row has to exist for the
    // settle to be accepted at all — this test is about the lease, not about a missing owner.
    seed_owning_activity(&mut storage, owner).await;
    let entries = dispatch_command(
        &storage,
        Command::CompleteTask(CompleteTask {
            task: task_ref(task),
            worker_id: "w1".into(),
            output: json!({ "ok": true }),
            request_id: RequestId::new(),
        }),
    )
    .await;
    let completed = entries
        .iter()
        .find_map(|e| match &e.payload {
            EntryPayload::Event(Event::TaskCompleted(TaskCompleted { task, .. })) => Some(task),
            _ => None,
        })
        .expect("the lapsed-but-unstolen worker's settle is accepted");
    assert_eq!(completed.status, TaskStatus::Completed);
    assert_eq!(completed.worker_id, None);
}

#[tokio::test]
async fn lapsed_lease_is_reclaimed_by_a_fresh_poll() {
    // A lease that lapsed with nobody watching is handed back *lazily*: the task is left `Running`
    // where it stands (still naming `w1`), and the next poll's discovery grants the same entity to
    // `w2`. No expiry event, no requeue step, and no timer is involved anywhere in the transition.
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
        Command::ClaimTasks(ClaimTasks {
            request_id: RequestId::new(),
            worker_id: "w2".into(),
            resource: "r".into(),
            max_tasks: 10,
            lease_seconds: 30,
        }),
    )
    .await;
    let granted = entries
        .iter()
        .find_map(|e| match &e.payload {
            EntryPayload::Event(Event::TasksClaimed(TasksClaimed { tasks, .. })) => {
                tasks.first().cloned()
            }
            _ => None,
        })
        .expect("a task whose lease lapsed must be grantable again");
    assert_eq!(granted.meta.raw_object_ref().uid, task);
    assert_eq!(granted.worker_id.as_deref(), Some("w2"));
    assert!(
        granted.lease_expires_at > Some(Timestamp::from_millis(1000)),
        "the second lease must be a fresh window, not the lapsed one"
    );
}

/// A `FailTask` naming a task that does not exist is refused rather than dropped silently: a task row
/// is written by the batch that activates it and nothing ever removes a row, so a miss is the report
/// forged into the log or a corrupt projection — the command's own precondition. The report is
/// fire-and-forget, so the `Reject` is the only account of why it settled nothing.
#[tokio::test]
async fn fail_task_without_a_row_is_refused_not_dropped() {
    let storage = InMemoryStorage::new();
    let entries = dispatch_command(
        &storage,
        Command::FailTask(FailTask {
            task: task_ref(ulid::Ulid::new()),
            worker_id: "w1".into(),
            error: ExecutionError::Runtime(RuntimeError::TimedOut {
                message: "x".into(),
            }),
        }),
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
        "a report for a task that does not exist still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::NotFound,
        "a missing task row is the command's own precondition, not an engine fault: {entries:?}"
    );
    assert!(
        !entries
            .iter()
            .any(|e| matches!(&e.payload, EntryPayload::Event(Event::TaskFailed(_)))),
        "a refused report must not settle the row that is gone: {entries:?}"
    );
}

/// The second half of the settle-once contract: a task that already settled is refused, so a duplicate
/// report — or one racing the engine's own `TimeoutSeconds` backstop — cannot advance the state twice.
#[tokio::test]
async fn fail_task_for_an_already_settled_task_is_refused() {
    let mut storage = InMemoryStorage::new();
    // Settled by a *different* worker's earlier report: what the duplicate is told apart by is the
    // terminal status, not the lease, so the reporter here still holds a (stale) lease.
    let task = ulid::Ulid::new();
    seed_task(
        &mut storage,
        task,
        TaskStatus::Failed,
        Some("w1".into()),
        Some(Timestamp::from_millis(1000)),
    )
    .await;
    let entries = dispatch_command(
        &storage,
        Command::FailTask(FailTask {
            task: task_ref(task),
            worker_id: "w1".into(),
            error: ExecutionError::Runtime(RuntimeError::TimedOut {
                message: "x".into(),
            }),
        }),
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
        "a duplicate report still owes exactly one Reject: {entries:?}"
    );
    assert_eq!(
        rejects[0].rejection_type,
        RejectionType::InvalidState,
        "a settled task is the wrong state, not a missing one: {entries:?}"
    );
    assert!(
        !entries
            .iter()
            .any(|e| matches!(&e.payload, EntryPayload::Event(Event::TaskFailed(_)))),
        "a refused duplicate must not settle the task a second time: {entries:?}"
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
        Command::FailTask(FailTask {
            task: task_ref(foreign),
            worker_id: "w2".into(),
            error: ExecutionError::Runtime(RuntimeError::TimedOut {
                message: "x".into(),
            }),
        }),
    )
    .await;
    // Refused, not silently dropped: the report is fire-and-forget, so the `Reject` is the only
    // account of why a failure reported by a worker that does not hold the lease settled nothing.
    assert_eq!(
        entries.len(),
        1,
        "a foreign worker's fail owes exactly one response entry: {entries:?}"
    );
    match &entries[0].payload {
        EntryPayload::Reject(reject) => assert_eq!(
            reject.rejection_type,
            RejectionType::InvalidState,
            "a foreign lease is the wrong state, not a missing row: {reject:?}"
        ),
        other => panic!("expected a Reject record, got {other:?}"),
    }
    assert!(
        !entries
            .iter()
            .any(|e| matches!(&e.payload, EntryPayload::Event(Event::TaskFailed(_)))),
        "a refused report must not settle the task: {entries:?}"
    );

    // The engine-authoritative backstop (empty worker_id, e.g. the TimeoutSeconds deadline) settles any
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
        Command::FailTask(FailTask {
            task: task_ref(stalled),
            worker_id: String::new(),
            error: ExecutionError::Runtime(RuntimeError::TimedOut {
                message: "deadline".into(),
            }),
        }),
    )
    .await;
    assert!(
        entries.iter().any(|e| matches!(
            &e.payload,
            EntryPayload::Event(Event::TaskFailed(TaskFailed { .. }))
        )),
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
                execution: spica_engine::ObjectRef::<ExecutionKind>::nil(),
                resource: "r".to_string(),
                arguments: Value::Null,
                status: TaskStatus::Running,
                deadline: None,
                worker_id: Some("w1".into()),
                lease_expires_at: Some(Timestamp::from_millis(1000)),
                retry_plan: vec![RetryPolicy {
                    error_equals: vec!["States.ALL".into()],
                    interval_seconds: 1,
                    max_attempts: 3,
                    backoff_rate: 1.0,
                    max_delay_seconds: None,
                }],
                retry_state: RetryState::default(),
                meta: spica_engine::ObjectMeta::builder(task)
                    .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                    .with_owner(parent.clone()),
            },
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        })
        .await
        .unwrap();

    let entries = dispatch_command(
        &storage,
        Command::FailTask(FailTask {
            task: task_ref(task),
            worker_id: "w1".into(),
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "S".to_string(),
                error: "boom".to_string(),
                output: Box::new(Value::Null),
            }),
        }),
    )
    .await;
    let failed = entries
        .iter()
        .find_map(|e| match &e.payload {
            EntryPayload::Event(Event::TaskFailed(TaskFailed { task, .. })) => Some(task),
            _ => None,
        })
        .expect("a matching retrier should emit TaskFailed (retry scheduled)");
    // Same task entity reused — no fresh task id, no separate RetryScheduled event.
    assert_eq!(
        failed.meta.raw_object_ref(),
        task_ref(task).into_raw_object_ref()
    );
    assert_eq!(
        failed.status,
        TaskStatus::Pending,
        "retry re-queues to Pending"
    );
    assert_eq!(failed.worker_id, None, "lease cleared on re-queue");
    assert_eq!(failed.lease_expires_at, None);
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
                execution: spica_engine::ObjectRef::<ExecutionKind>::nil(),
                resource: "r".to_string(),
                arguments: Value::Null,
                status: TaskStatus::Pending,
                deadline: None,
                worker_id: None,
                lease_expires_at: None,
                retry_plan: vec![],
                retry_state: RetryState {
                    attempts: 1,
                    retrier_attempts: vec![],
                    next_available_at: Some(Timestamp::from_millis(4_000_000_000_000)),
                },
                meta: spica_engine::ObjectMeta::builder(task)
                    .timestamps(Timestamp::from_millis(0), Timestamp::from_millis(0))
                    .with_owner(act_ref()),
            },
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        })
        .await
        .unwrap();

    let entries = dispatch_command(
        &storage,
        Command::ClaimTasks(ClaimTasks {
            request_id: RequestId::new(),
            worker_id: "w1".into(),
            resource: "r".into(),
            max_tasks: 10,
            lease_seconds: 60,
        }),
    )
    .await;
    assert!(
        !entries
            .iter()
            .any(|e| matches!(&e.payload, EntryPayload::Event(Event::TasksClaimed(TasksClaimed { tasks, .. })) if !tasks.is_empty())),
        "a retrying task whose backoff gate has not lapsed must not be claimable: {entries:?}"
    );
}
