//! Unit tests for the CCES building blocks: the Storage projection, causality/atomicity of the
//! Processor's output, the ing/ed lifecycle split, the deferred-ed cascade on a Completing or
//! Terminating parent, and the cancel/timeout race guards.

use serde_json::{Value, json};
use spica_asl::StateMachine;
use spica_engine::{
    ActivityId, Command, Engine, Entry, EntryPayload, Event, ExecutionError, ExecutionId, IdSource,
    InMemoryLogStream, InMemoryStorage, LogStream, NodeId, Processor, Status, Storage,
    TerminationReason, TimerId, TimerPurpose, Timestamp,
};
use tokio_stream::StreamExt;

fn parse_sm(definition: &str) -> StateMachine {
    serde_json::from_str(definition).expect("state machine should parse")
}

/// Applies events to storage through the real [`EventDispatcher`] + [`SchedulerHandle`], mirroring
/// the event path of `Processor::run` — so timer events actually reach the scheduler. `StreamId`/`
/// `EntryId` in the applier context default to 1 (they only matter for scheduling, which projection
/// tests don't otherwise exercise).
struct Projector {
    dispatcher: spica_engine::EventDispatcher,
    scheduler: spica_engine::SchedulerHandle,
}

impl Projector {
    fn new() -> Self {
        // Fire channel is unread here: projection tests only fold state, they never wait on a
        // fired timer. The scheduler's loop exits once the handle drops below.
        let (fire_tx, _fire_rx) = tokio::sync::mpsc::unbounded_channel::<(
            spica_engine::StreamId,
            spica_engine::EntryId,
            Command,
        )>();
        let (scheduler, _handle) = spica_engine::SchedulerHandle::spawn(fire_tx);
        Self {
            dispatcher: spica_engine::EventDispatcher::new(),
            scheduler,
        }
    }

    async fn apply(&self, storage: &mut InMemoryStorage, event: &Event) {
        let mut ctx = spica_engine::ApplierContext {
            storage,
            scheduler: &self.scheduler,
            stream_id: spica_engine::StreamId::new(1),
            cause_id: spica_engine::EntryId::new(1),
        };
        self.dispatcher.apply(&mut ctx, event).await.unwrap();
    }
}

impl Default for Projector {
    fn default() -> Self {
        Self::new()
    }
}

/// Drives `sm` with `input` end-to-end through the raw CCES seam (no `Engine::start` async
/// wrapping), collecting every event applied to Storage. Stops when the execution reaches a
/// terminal status. Timer scheduling mirrors `Processor::run`: a real [`SchedulerHandle`] owns a
/// `DelayQueue`, `TimerActivated`/`TimerCancelled` events are fed to it via the
/// [`EventDispatcher`], and a fired `CompleteTimer` is routed back here over a channel and appended
/// by this driver — the single writer to the log, so the stream stays strictly ordered.
async fn collect_events(sm: StateMachine, input: Value) -> Vec<Event> {
    let logstream = InMemoryLogStream::new();
    let mut ids = IdSource::new();
    let (execution_id, _stream_id) = Engine::submit(input, &logstream, &mut ids).await.unwrap();

    let mut storage = InMemoryStorage::new();
    let mut processor = Processor::new(sm, execution_id);
    let dispatcher = spica_engine::EventDispatcher::new();
    // Scheduler + its fire channel, wired the same way `Processor::run` wires them.
    let (fire_tx, mut fire_rx) = tokio::sync::mpsc::unbounded_channel::<(
        spica_engine::StreamId,
        spica_engine::EntryId,
        Command,
    )>();
    let (scheduler, _handle) = spica_engine::SchedulerHandle::spawn(fire_tx);
    // Handle dropped at scope end is fine: the loop exits once `scheduler` is dropped below.

    let mut stream = logstream.stream_read(spica_engine::EntryId::new(1));
    let mut events = Vec::new();
    loop {
        tokio::select! {
            entry = stream.next() => {
                let Some(entry) = entry else { break; };
                match entry.payload {
                    EntryPayload::Command(command) => {
                        let out = processor
                            .dispatch(&command, &storage, entry.entry_id, entry.stream_id)
                            .await
                            .unwrap();
                        logstream.append(out).await.unwrap();
                    }
                    EntryPayload::Event(event) => {
                        let terminal = matches!(
                            &event,
                            Event::ExecutionCompleted { .. } | Event::ExecutionTerminated { .. }
                        );
                        let mut ctx = spica_engine::ApplierContext {
                            storage: &mut storage,
                            scheduler: &scheduler,
                            stream_id: entry.stream_id,
                            cause_id: entry.entry_id,
                        };
                        dispatcher.apply(&mut ctx, &event).await.unwrap();
                        events.push(event);
                        if terminal {
                            break;
                        }
                    }
                }
            }
            Some((stream_id, cause_id, command)) = fire_rx.recv() => {
                // A timer's deadline elapsed: the scheduler routed CompleteTimer here. Envelope with
                // a placeholder entry_id — the log assigns the real position on append.
                let entry = Entry {
                    stream_id,
                    entry_id: spica_engine::EntryId::nil(),
                    cause_id: Some(cause_id),
                    timestamp: spica_engine::Timestamp::now(),
                    payload: EntryPayload::Command(command),
                };
                logstream.append(vec![entry]).await.unwrap();
            }
        }
    }
    drop(scheduler);
    events
}

/// Stable orderable prefix of each event's Debug form, for asserting on emission order.
fn kind_prefix(e: &Event) -> &'static str {
    match e {
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
        Event::TimerCompleted { .. } => "TimerCompleted",
        Event::TimerCancelled { .. } => "TimerCancelled",
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
    let exec = ExecutionId::nil();
    let activity = ActivityId::new();
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();

    projector
        .apply(
            &mut storage,
            &Event::ExecutionCreated {
                id: exec,
                input: json!({ "x": 1 }),
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::StateActivating {
                execution: exec,
                activity,
                state: "S".to_string(),
                input: json!({ "x": 1 }),
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::VariablesAssigned {
                execution: exec,
                assignments: serde_json::Map::from_iter([("g".to_string(), json!("hi"))]),
            },
        )
        .await;
    projector
        .apply(
            &mut storage,
            &Event::ExecutionCompleted {
                id: exec,
                output: json!({ "done": true }),
            },
        )
        .await;

    let e = storage.get_execution(exec).await.unwrap().unwrap();
    assert_eq!(e.status, Status::Completed);
    assert_eq!(e.scope.get("g"), Some(&json!("hi")));
    assert_eq!(e.output, Some(json!({ "done": true })));
    assert!(matches!(e.status, Status::Completed));
}

// ── Causality / atomicity ────────────────────────────────────────────────────

#[tokio::test]
async fn every_non_root_entry_has_a_causal_parent() {
    let sm = parse_sm(r#"{ "StartAt": "A", "States": { "A": { "Type": "Succeed" } } }"#);
    let logstream = InMemoryLogStream::new();
    let mut ids = IdSource::new();
    let (execution_id, _stream_id) = Engine::submit(Value::Null, &logstream, &mut ids)
        .await
        .unwrap();

    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();
    let mut processor = Processor::new(sm, execution_id);
    let mut stream = logstream.stream_read(spica_engine::EntryId::new(1));
    while let Some(entry) = stream.next().await {
        match entry.payload {
            EntryPayload::Command(command) => {
                let entries = processor
                    .dispatch(&command, &storage, entry.entry_id, entry.stream_id)
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
    assert!(pos(&events, "TimerActivated") < pos(&events, "TimerCompleted"));
    assert!(pos(&events, "TimerCompleted") < pos(&events, "StateCompleting"));
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
            id: exec,
            input: Value::Null,
        },
        Event::StateActivating {
            execution: exec,
            activity,
            state: "W".to_string(),
            input: Value::Null,
        },
        Event::StateActivated { activity },
        Event::TimerActivated {
            parent: NodeId::Activity(activity),
            timer,
            purpose: TimerPurpose::WaitResume,
            deadline: Timestamp::from_millis(1_000_000_000_000),
        },
    ] {
        projector.apply(&mut storage, ev).await;
    }

    let logstream = InMemoryLogStream::new();
    let mut processor = Processor::new(
        parse_sm(
            r#"{
              "StartAt": "W",
              "States": {
                "W": { "Type": "Wait", "Seconds": 60, "Next": "P" },
                "P": { "Type": "Pass", "End": true }
              }
            }"#,
        ),
        exec,
    );

    // Inject TerminateExecution directly and drive it through the Processor.
    let cause = spica_engine::EntryId::new(1);
    let stream_id = spica_engine::StreamId::new(1);
    let entries = processor
        .dispatch(
            &Command::TerminateExecution {
                id: exec,
                reason: TerminationReason::Cancelled,
            },
            &storage,
            cause,
            stream_id,
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
                    .dispatch(&cmd, &storage, entry.entry_id, entry.stream_id)
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
        Status::Terminated(TerminationReason::Cancelled)
    ));
    assert!(exec.active_children.is_empty(), "execution fully drained");
}

// ── Race guard: a late CompleteTimer after a cancel must be a no-op ──────────

#[tokio::test]
async fn late_complete_timer_after_cancel_is_noop() {
    // An armed timer is cancelled in storage first; then a stale CompleteTimer arrives (a fire
    // that was already in flight). The handler must see the timer's terminal state and emit
    // nothing — no TimerCompleted, no TerminateExecution.
    let timer = TimerId::new();
    let exec = ExecutionId::nil();
    let logstream = InMemoryLogStream::new();
    let mut storage = InMemoryStorage::new();
    let projector = Projector::new();
    projector
        .apply(
            &mut storage,
            &Event::TimerActivated {
                parent: NodeId::Execution(exec),
                timer,
                purpose: TimerPurpose::ExecutionTimeout,
                deadline: Timestamp::from_millis(1_000_000_000_000),
            },
        )
        .await;
    projector
        .apply(&mut storage, &Event::TimerCancelled { timer })
        .await;

    let mut processor = Processor::new(
        parse_sm(r#"{ "StartAt": "A", "States": { "A": { "Type": "Succeed" } } }"#),
        exec,
    );
    let out = processor
        .dispatch(
            &Command::CompleteTimer { timer },
            &storage,
            spica_engine::EntryId::new(500),
            spica_engine::StreamId::new(1),
        )
        .await
        .unwrap();
    assert!(
        out.is_empty(),
        "a stale CompleteTimer must produce no entries: {out:?}"
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
    let err = Engine::start(sm, Value::Null)
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
    let result = Engine::start(sm, Value::Null).await.unwrap();
    assert_eq!(result.output, json!("hi"));
}

#[tokio::test]
async fn engine_start_fail_produces_state_failed_error() {
    let sm = parse_sm(
        r#"{ "StartAt": "F", "States": { "F": { "Type": "Fail", "Error": "E1", "Cause": "boom" } } }"#,
    );
    let err = Engine::start(sm, Value::Null).await.unwrap_err();
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
