//! Shared helpers for the engine integration test suites.
//!
//! The one-shot `Engine::run` / `Engine::run_with_task_handlers` conveniences were removed together
//! with the in-process CLI (they had no production callers once `spica` became a pure-remote client
//! and `spica-server` drove the explicit lifecycle). Tests that still want a one-shot "create an
//! anonymous flow and run it" therefore reproduce that sequence here, built only on the retained
//! public API — `Engine::start` → `create_flow` → `start_for_revision` → `wait_for_execution` — so
//! the helpers exercise the same canonical path a server would.

#![allow(dead_code)] // a given suite uses only some helpers; that is expected of a shared module

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use jsonptr::PointerBuf;
use serde_json::Value;
use spica_asl::StateMachine;
use spica_client::worker::{
    ClaimedTask, InMemoryTaskService, TaskApi as WorkerTaskApi, TaskApiError, TaskFailure,
    TaskHandler, TaskService,
};
use spica_engine::{
    ActivatedTask, ClaimTasks, Command, CompleteTask, CreateExecution, CreateFlow, Engine,
    EngineBuilder, Entry, EntryId, EntryPayload, Event, Execution, ExecutionCreated,
    ExecutionError, ExecutionStatus, FailTask, FlowName, FlowVersionCreated, Hook, LogStream,
    ObjectKind, ObjectMeta, ObjectName, ObjectReference, PlainName, Reject, RequestId,
    RuntimeError, StatePath, StreamId, Task, TaskApi, TaskCompleted, TasksClaimed, Timestamp,
    Variables,
};
use spica_machinery::{
    Clock, CountingIdGenerator, IdGenerator, ManualClock, SystemClock, SystemIdGenerator,
};
use spica_scheduler::{InMemoryScheduler, Scheduler, TimerSink};
use spica_storage::InMemoryStorage;
use tokio::sync::{Mutex, oneshot};
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;

/// An in-process adapter presenting the engine's inbound [`spica_engine::TaskApi`] as the
/// worker-facing [`spica_client::worker::TaskApi`], so an [`InMemoryTaskService`] can be driven against
/// a running engine in tests. This is the engine-side half of the boundary, owning the
/// `ClaimedTask ↔ ActivatedTask` and `TaskFailure ↔ ExecutionError` mappings — the mirror image of
/// `GrpcTaskApi`, which owns the wire half. It lives in test code (not a crate) because it is the only
/// place the engine-linked worker contract and the engine's own inbound trait meet in-process.
pub(crate) struct EngineTaskApi {
    inner: Arc<dyn spica_engine::TaskApi>,
}

impl EngineTaskApi {
    pub(crate) fn new(inner: Arc<dyn spica_engine::TaskApi>) -> Self {
        Self { inner }
    }

    /// Reconstruct an engine [`spica_engine::ObjectName`] from the worker's scalar String task name
    /// (its canonical name, as returned by `poll_tasks`).
    fn task_name(s: &str, op: &str) -> Result<spica_engine::ObjectName, TaskApiError> {
        spica_engine::ObjectName::from_parsed(s)
            .map_err(|_| TaskApiError(format!("{op}: invalid task name: {s:?}")))
    }

    /// Reconstruct an engine [`spica_engine::RequestId`] from the worker-supplied String (its ULID).
    fn request_id(s: &str, op: &str) -> Result<spica_engine::RequestId, TaskApiError> {
        s.parse::<ulid::Ulid>()
            .map(spica_engine::RequestId::from)
            .map_err(|_| TaskApiError(format!("{op}: invalid request_id ULID: {s:?}")))
    }
}

#[async_trait::async_trait]
impl WorkerTaskApi for EngineTaskApi {
    async fn poll_tasks(
        &self,
        worker_id: &str,
        resource: &str,
        max_tasks: usize,
        lease_seconds: u64,
    ) -> Result<Vec<ClaimedTask>, TaskApiError> {
        let tasks = self
            .inner
            .poll_tasks(worker_id, resource, max_tasks, lease_seconds)
            .await
            .map_err(|e| TaskApiError(e.to_string()))?;
        Ok(tasks
            .into_iter()
            .map(|t| ClaimedTask {
                task_name: t.task.as_str().to_string(),
                resource: t.resource,
                arguments: t.arguments,
            })
            .collect())
    }

    async fn complete(
        &self,
        worker_id: &str,
        task_name: &str,
        request_id: &str,
        output: Value,
    ) -> Result<(), TaskApiError> {
        let name = Self::task_name(task_name, "CompleteTask")?;
        let request_id = Self::request_id(request_id, "CompleteTask")?;
        self.inner
            .complete(worker_id, name, request_id, output)
            .await
            .map_err(|e| TaskApiError(e.to_string()))
    }

    async fn fail(
        &self,
        worker_id: &str,
        task_name: &str,
        error: TaskFailure,
    ) -> Result<(), TaskApiError> {
        let name = Self::task_name(task_name, "FailTask")?;
        let TaskFailure { error_name, output } = error;
        // The worker reports ASL error *semantics*; map them onto the engine's `StateFailed`. The
        // `state` field is Display-only and the worker can't know it, so it stays empty — Retry/Catch
        // match on `error_name`/`output`, never on `state`.
        let exec_err = ExecutionError::Runtime(RuntimeError::StateFailed {
            state: String::new(),
            error: error_name,
            output: Box::new(output),
        });
        self.inner
            .fail(worker_id, name, exec_err)
            .await
            .map_err(|e| TaskApiError(e.to_string()))
    }
}

/// A builder wired to in-memory log + storage backends — the M1 test default. `EngineBuilder` itself
/// no longer fabricates backends (it takes caller-supplied trait objects; see
/// [`EngineBuilder::with_backends`](spica_engine::EngineBuilder::with_backends)), so the in-memory
/// log/store pair is assembled here from the seam crates and injected. A scheduler is **not** part of
/// `EngineBuilder` anymore — the consumer owns timer scheduling (see [`LocalClient::start`], which
/// builds the scheduler + sink around the booted engine).
pub fn in_memory_builder() -> EngineBuilder {
    EngineBuilder::with_backends(
        Box::new(spica_engine::InMemoryLogStream::<EntryPayload>::new()),
        Box::new(InMemoryStorage::new()),
    )
}

/// [`in_memory_builder`] plus a readable handle on the log behind it — the builder to use when a test
/// asserts on the run's whole **entry chain** and not only on its outcome.
pub fn recording_builder() -> (
    EngineBuilder,
    Arc<spica_engine::InMemoryLogStream<EntryPayload>>,
) {
    let log = Arc::new(spica_engine::InMemoryLogStream::<EntryPayload>::new());
    (
        EngineBuilder::with_backends(
            Box::new(RecordingLog(log.clone())),
            Box::new(InMemoryStorage::new()),
        ),
        log,
    )
}

/// A [`LogStream`] that delegates to an [`InMemoryLogStream`] while keeping the same log readable by
/// the caller. `EngineBuilder` consumes its backends by value and `InMemoryLogStream` is not `Clone`,
/// so a suite that needs to read what the run wrote hands the engine this shared handle and reads
/// `entries()` off the underlying log once the run has quiesced.
#[derive(Clone)]
pub struct RecordingLog(Arc<spica_engine::InMemoryLogStream<EntryPayload>>);

#[async_trait]
impl LogStream<EntryPayload> for RecordingLog {
    fn stream_id(&self) -> StreamId {
        self.0.stream_id()
    }

    async fn append(&self, entries: Vec<Entry>) -> Result<EntryId, spica_logstream::LogError> {
        self.0.append(entries).await
    }

    async fn read(&self, entry_id: EntryId) -> Result<Option<Entry>, spica_logstream::LogError> {
        self.0.read(entry_id).await
    }

    fn stream_read(&self, from: EntryId) -> Pin<Box<dyn Stream<Item = Entry> + Send + 'static>> {
        self.0.stream_read(from)
    }
}

/// The run's whole causal chain: every record the engine wrote, in append order, minus the `Noop`
/// batch terminators, in its **typed** form — the payloads themselves. Asserting this pins the order,
/// the kind **and** the values of what a run produces; a terminal-status assertion cannot state what a
/// run must *not* have written.
///
/// The typed form pins identities literally (`00000000000000000000000001`) instead of masking them
/// away, so it only says something about a run whose stamps and identities are supplied by the test
/// (see [`run_typed_case`]); what it buys in return is that a renamed variant or a changed field fails
/// with the parser naming it, rather than as a diff between two long serialized blobs.
pub fn typed_entry_chain(log: &spica_engine::InMemoryLogStream<EntryPayload>) -> Vec<EntryPayload> {
    learned_chain(log)
}

/// Read a run's chain, checking what holds for *any* reading of it on the way through: the envelope
/// around each payload ([`assert_envelope`]) and the timing invariants ([`Stamps`]). Both are
/// properties of the stream rather than of any one assertion, so they are made here — once, on the way
/// to the payloads — instead of being left to whichever case happens to pin the values they govern.
///
/// The `Noop` terminators are dropped rather than asserted on: they are the batch markers the envelope
/// rules are about, and a chain asserts *records*.
fn learned_chain(log: &spica_engine::InMemoryLogStream<EntryPayload>) -> Vec<EntryPayload> {
    let written = log.entries();
    let entries: Vec<&Entry> = written.iter().collect();
    assert_envelope(&entries);

    let mut payloads = Vec::new();
    let mut stamps = Stamps::default();
    for entry in &entries {
        if matches!(entry.payload, EntryPayload::Noop) {
            continue;
        }
        stamps.learn_value(&payload_values(&entry.payload));
        payloads.push(entry.payload.clone());
    }
    stamps.assert_times_are_sane();

    payloads
}

/// The envelope contract, checked over the whole stream rather than pinned per record: what holds for
/// *every* append is a rule, so a violation fails once with its own message instead of showing up as a
/// wholesale diff of a per-record table.
///
/// - **positions** — one stream, and a dense 1-based position run: a gap means a lost append, an
///   un-stamped `nil` position means an entry reached the log un-materialized, and a follower's
///   watermark arithmetic (`W + 1`) depends on both;
/// - **stamps** — the append stamps never go backwards (see [`assert_append_times`]);
/// - **causes** — the batch/commit graph a follower replays by (see `assert_causal_chain`).
pub fn assert_envelope(entries: &[&Entry]) {
    let first = entries.first().expect("a run appends at least one entry");
    for (position, entry) in entries.iter().enumerate() {
        assert_eq!(
            entry.stream_id, first.stream_id,
            "the entry at index {position} carries a different stream id than the rest"
        );
        assert_eq!(
            entry.entry_id.get(),
            (position + 1) as i64,
            "the entry at index {position} does not hold its own position; the log stamps a dense \
             run starting at 1"
        );
    }
    assert_append_times(entries);
    assert_causal_chain(entries);
}

/// The log's own append stamps, in append order. The log *is* the order (by entry id), but a stamp
/// that went backwards would make a time-sorted reading of the same history disagree with the causal
/// one — the audit trail's value is that both agree.
fn assert_append_times(entries: &[&Entry]) {
    let mut previous: Option<u64> = None;
    for (position, entry) in entries.iter().enumerate() {
        let at = entry.timestamp.as_millis();
        if let Some(previous) = previous {
            assert!(
                at >= previous,
                "the append stamp went backwards at the entry at index {position}: {at} < {previous} ms"
            );
        }
        previous = Some(at);
    }
}

/// The causal graph. Every record a handler appends is stamped with the position of the command it
/// was reacting to, so the stream is a sequence of **batches** — and a follower's commit watermark
/// means exactly "the last batch's last record". The rules below state that shape:
///
/// 1. a cause points **backwards at a `Command`** — handlers append only in reaction to one, so a
///    cause naming an event, or a later entry, is a broken link;
/// 2. a command's records form **one** run, and a `Noop` carrying that same cause closes it. A run
///    reopened after closing, a run left unclosed, or a `Noop` displaced from the run's end all break
///    "one batch, committed once" — the invariant both replay and the commit protocol rest on;
/// 3. a record with **no** cause is a worker-initiated append: it is a `Command`/`Reject`, the log's
///    terminator `Noop` follows it, and it can never sit inside a batch.
///
/// The `Noop` terminators are deliberately absent from the chain table (they carry no payload worth
/// reading), so this is the only thing standing between a lost commit marker and a green suite.
fn assert_causal_chain(entries: &[&Entry]) {
    // The batch in progress: its cause, and `None` once its commit marker has closed it.
    let mut open: Option<EntryId> = None;
    let mut committed: HashSet<EntryId> = HashSet::new();
    for (position, entry) in entries.iter().enumerate() {
        let Some(cause) = entry.cause_id else {
            assert!(
                open.is_none(),
                "the entry at index {position} has no cause but interrupts the batch of {}",
                open.expect("checked by the assertion above")
            );
            // A worker-initiated append is closed by the log's own terminator, so the record after it
            // is that `Noop` — never another record of any kind.
            if !matches!(entry.payload, EntryPayload::Noop) {
                let next = entries.get(position + 1).unwrap_or_else(|| {
                    panic!("the entry at index {position} was never terminated by a Noop")
                });
                assert!(
                    next.cause_id.is_none() && matches!(next.payload, EntryPayload::Noop),
                    "the entry at index {position} is un-terminated: the log closes a \
                     worker-initiated append with a cause-less Noop, found {} instead",
                    payload_kind_of(next)
                );
            }
            continue;
        };

        if open != Some(cause) {
            assert!(
                open.is_none(),
                "batch {} was never committed before batch {cause} started at the entry at index \
                 {position}",
                open.expect("checked by the assertion above")
            );
            assert!(
                committed.insert(cause),
                "batch {cause} was committed and then reopened at the entry at index {position}"
            );
            assert_cause_points_at_a_command(entries, cause, position);
            open = Some(cause);
        }
        if matches!(entry.payload, EntryPayload::Noop) {
            open = None; // This is the batch's commit marker: the run is closed.
        }
    }
    assert!(
        open.is_none(),
        "the stream ends with batch {} uncommitted",
        open.expect("checked by the assertion above")
    );
}

/// Rule 1 of [`assert_causal_chain`]: the cause names an entry that is both **earlier** and a
/// `Command`. `EntryId::nil()` (an un-stamped id) is negative, so it can never pass.
fn assert_cause_points_at_a_command(entries: &[&Entry], cause: EntryId, position: usize) {
    let index = usize::try_from(cause.get() - 1)
        .ok()
        .filter(|index| *index < position);
    assert!(
        matches!(
            index
                .and_then(|index| entries.get(index))
                .map(|e| &e.payload),
            Some(EntryPayload::Command(_))
        ),
        "the entry at index {position} is caused by {cause}, which is not an earlier Command"
    );
}

/// `CreateFlow(CreateFlow { .. })` / `StateCompleting { activity: .. }` → the variant name. Read off
/// the payload's derived `Debug` rendering rather than a hand-written match, so the helper does not
/// have to be extended for every new variant.
fn payload_kind(payload: &EntryPayload) -> String {
    let rendered = match payload {
        EntryPayload::Command(command) => format!("{command:?}"),
        EntryPayload::Event(event) => format!("{event:?}"),
        EntryPayload::Reject(reject) => format!("{reject:?}"),
        EntryPayload::Noop => unreachable!("filtered out by learned_chain"),
    };
    rendered
        .split(['(', ' ', '{'])
        .next()
        .unwrap_or_default()
        .to_string()
}

/// [`payload_kind`] for an entry that may be a `Noop`: the envelope checks report on entries a chain
/// drops, so they need a kind for the one payload [`payload_kind`] refuses to name.
fn payload_kind_of(entry: &Entry) -> String {
    match &entry.payload {
        EntryPayload::Noop => "Noop".to_string(),
        other => payload_kind(other),
    }
}

/// The values a payload carries, as a follower reading the log would parse them — minus the
/// externally-tagged wrapper, which only repeats the kind. The payload is serialized (rather than its
/// fields listed by hand) so that a field added to a command or event is *seen* by the timing walk
/// below instead of slipping past it.
fn payload_values(payload: &EntryPayload) -> Value {
    let value = match payload {
        EntryPayload::Command(command) => serde_json::to_value(command),
        EntryPayload::Event(event) => serde_json::to_value(event),
        EntryPayload::Reject(reject) => serde_json::to_value(reject),
        EntryPayload::Noop => return Value::Null,
    }
    .expect("an engine payload is serializable");
    match value {
        Value::Object(map) if map.len() == 1 => {
            map.into_iter().next().expect("the length was checked").1
        }
        other => other,
    }
}

/// The timing invariants a stream must satisfy, checked while its chain is read: an object's birth is
/// immutable and its update stamp never goes backwards — per object, and across the chain, which is
/// what an appended stream must satisfy to be replayable in order. A typed chain pins those stamps
/// literally, so it can say what they *are*; the invariants they carry hold for every run, so they are
/// checked for every run rather than only for the cases that happen to state them.
#[derive(Default)]
struct Stamps {
    born: HashMap<String, u64>,
    last_update: HashMap<String, u64>,
    last_update_overall: Option<u64>,
    /// Collected rather than panicked on the spot, so one run reports every broken object instead of
    /// only the first.
    violations: Vec<String>,
}

impl Stamps {
    fn learn_value(&mut self, value: &Value) {
        match value {
            Value::Object(map) => {
                let uid = map.get("uid").and_then(Value::as_str);
                let created = map.get("created_at").and_then(Value::as_u64);
                let updated = map.get("updated_at").and_then(Value::as_u64);
                if let (Some(uid), Some(created), Some(updated)) = (uid, created, updated) {
                    self.check_times(uid, created, updated);
                }
                for child in map.values() {
                    self.learn_value(child);
                }
            }
            Value::Array(items) => items.iter().for_each(|item| self.learn_value(item)),
            _ => {}
        }
    }

    fn check_times(&mut self, uid: &str, created: u64, updated: u64) {
        match self.born.get(uid) {
            Some(born) if *born != created => self.violations.push(format!(
                "{uid}: born at {born} ms but re-born at {created} ms"
            )),
            Some(_) => {}
            None => {
                self.born.insert(uid.to_string(), created);
            }
        }
        if updated < created {
            self.violations.push(format!(
                "{uid}: updated at {updated} ms before its birth at {created} ms"
            ));
        }
        let previous = self.last_update.insert(uid.to_string(), updated);
        if let Some(previous) = previous
            && updated < previous
        {
            self.violations.push(format!(
                "{uid}: updated at {updated} ms after {previous} ms — went backwards"
            ));
        }
        if let Some(previous) = self.last_update_overall
            && updated < previous
        {
            self.violations.push(format!(
                "the append order and the update stamps disagree: {updated} ms after {previous} ms"
            ));
        }
        self.last_update_overall = Some(updated);
    }

    fn assert_times_are_sane(&self) {
        assert!(
            self.violations.is_empty(),
            "timing invariants broken:\n  {}",
            self.violations.join("\n  ")
        );
    }
}

/// Compare a typed chain record by record, naming the diverging record's position and its kind in the
/// report — so a chain that took a different branch says *which* record and *what* it wrote, rather
/// than handing the reader two twenty-record payload dumps to diff by eye.
pub fn assert_typed_chain(actual: &[EntryPayload], expected: &[EntryPayload]) {
    for (position, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        assert_eq!(
            actual,
            expected,
            "record {position} of the chain ({})",
            payload_kind(actual)
        );
    }
    assert_eq!(
        actual.len(),
        expected.len(),
        "chain length; the actual chain as the log holds it:\n{}",
        typed_chain_literals(actual)
    );
}

/// Render a typed chain as the log's own JSON — one record per line, in append order — so a length
/// mismatch shows what the run actually wrote beside the expectation built in code.
pub fn typed_chain_literals(chain: &[EntryPayload]) -> String {
    chain
        .iter()
        .map(|payload| {
            let json = serde_json::to_string(payload).expect("an engine payload is serializable");
            format!("            r#\"{json}\"#,")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The flow and execution names every lifecycle case runs under. Fixed rather than the randomized
/// [`anonymous_name`]/[`execution_name`]: each test boots its own storage, so a literal name cannot
/// collide, and a literal name is *itself* assertable — the chain then checks that the `FlowCreated`
/// and `ExecutionCreated` acks echo the name that was submitted, which a random name cannot.
const LIFECYCLE_FLOW: &str = "lifecycle_flow";
const LIFECYCLE_EXECUTION: &str = "lifecycle_execution";

/// The instant every stamp of a timer-free typed case carries — the [`VirtualClient`]'s own clock
/// reading, which is what makes a timestamp assertable at all.
pub fn epoch() -> Timestamp {
    Timestamp::from_millis(VIRTUAL_EPOCH_MILLIS)
}

/// A stamp at `millis`, for the cases whose clock the test has advanced.
pub fn stamp(millis: u64) -> Timestamp {
    Timestamp::from_millis(millis)
}

/// The `n`-th identity the injected [`CountingIdGenerator`] mints (it starts at `1`), so a chain
/// says which object a reference names by *how* it was minted rather than by a random ulid's text.
pub fn uid(n: u64) -> ulid::Ulid {
    ulid::Ulid::from(u128::from(n))
}

/// The `n`-th request id [`LocalClient::request_id`] mints — the same counter a run's acks are
/// correlated by, offset to [`FIRST_REQUEST_ID`] so it never renders as a `uid` does.
pub fn request(n: u64) -> RequestId {
    RequestId::from(ulid::Ulid::from(u128::from(FIRST_REQUEST_ID + n)))
}

/// The object name a static literal spells, in either flavor — a plain user name (`lifecycle_flow`) or
/// the generated `{base}-{n}` form of the engine's own addresses (`lifecycle_execution-0`), whose `-`
/// a plain name may never carry.
pub fn name(name: &str) -> ObjectName {
    ObjectName::from_parsed(name).expect("a static literal is a valid object name")
}

/// A plain flow name, e.g. `lifecycle_flow` — the [`FlowName`] a `CreateFlow` addresses by.
pub fn flow_name(name: &str) -> FlowName {
    FlowName::new(name).expect("a static literal is a valid flow name")
}

/// The metadata a timer-free run mints for the object `object_name` of kind `kind`, identified by the
/// `n`-th uid: every object's own name plus the injected identity, stamped at the clock's single
/// instant ([`epoch`]). A case whose clock has been advanced builds its `ObjectMeta` through
/// [`ObjectMeta::builder`] instead, since these stamps would be wrong for it.
pub fn meta(kind: ObjectKind, uid: ulid::Ulid, object_name: &str) -> ObjectMeta {
    ObjectMeta::builder(kind, uid)
        .name(name(object_name))
        .at(epoch())
        .build()
}

/// [`meta`] with both stamps spelled out — for a case whose clock has been advanced (see
/// [`TypedCase::acts`]), where an object minted before the move and updated by it carries two
/// different instants.
pub fn meta_span(
    kind: ObjectKind,
    uid: ulid::Ulid,
    object_name: &str,
    created: Timestamp,
    updated: Timestamp,
) -> ObjectMeta {
    ObjectMeta::builder(kind, uid)
        .name(name(object_name))
        .timestamps(created, updated)
        .build()
}

/// A variable scope's delta as an `Assign` writes it — a map, so the literal lists its pairs and the
/// order they are listed in does not matter.
pub fn vars(pairs: &[(&str, Value)]) -> Variables {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

/// A container's slot map — a `Map`'s `children` or a `Parallel`'s `branches`, both index → child, the
/// reverse lookup a settle is identified by. Like [`vars`] the literal lists its pairs, because a
/// `HashMap`'s iteration order is seeded per process: a chain that pinned the order it happened to
/// serialize in would only be reproducible within one run of the suite.
pub fn indexed_refs(pairs: &[(usize, ObjectReference)]) -> HashMap<usize, ObjectReference> {
    pairs.iter().map(|(i, r)| (*i, r.clone())).collect()
}

/// A reference to the object `name` of kind `kind`, whose identity is the `n`-th uid minted.
pub fn ref_to(kind: ObjectKind, object: &str, n: u64) -> ObjectReference {
    ObjectReference::new(kind, name(object), uid(n))
}

/// The JSON Pointer `/segments` (`""` being the document root) — the form a `StatePath` wraps.
pub fn path(segments: &str) -> StatePath {
    StatePath::from(pointer(segments))
}

/// [`path`] before it is wrapped: `StateTransitioned` carries the raw pointer.
pub fn pointer(segments: &str) -> PointerBuf {
    let mut buf = PointerBuf::new();
    for segment in segments.split('/').filter(|s| !s.is_empty()) {
        buf.push_back(segment);
    }
    buf
}

/// A point in a run's log that an [`Act`] must wait for.
///
/// A signal is evaluated against a **window** of the log, not the whole of it: the window opens just
/// past the record that met the previous act's own signal. The record a signal is met on is therefore
/// never met again, so the same signal reads as "ready *again*" — which is what a run that arms a
/// second deadline (a retried task's backoff) needs in order to express its second step — while the
/// records the previous act *itself* caused stay inside the window, so an act can gate on what the one
/// before it wrote. A case that acts once is unaffected: its window opens at the run's first entry.
#[derive(Clone, Copy, Debug)]
pub enum Signal {
    /// A `TimerActivated` in the window: the run has armed a deadline against the reading it held at
    /// the time. Moving the clock before the arm lands would make the deadline read the moved
    /// reading, so the case would pin an expiry the engine never decided.
    TimerArmed,
    /// A `TaskActivated` in the window: the `Task` state has minted the task an external worker is
    /// meant to settle, so a call naming it addresses something that exists.
    TaskArmed,
    /// A `TasksClaimed` in the window: a task has been leased to the worker about to settle it, which
    /// is the only state from which a settle is accepted.
    TasksClaimed,
    /// A `TaskFailed` in the window that put the task **back in the queue** — one whose retrier matched,
    /// leaving a `next_available_at` gate. That gate is an instant, not a deadline recorded as a timer
    /// the run would wake on, so the case has to move the clock onto it itself: this signal is what says
    /// the instant exists.
    TaskRequeued,
    /// **No record** gates this act. Nothing is awaited: the act rests on what the previous one left
    /// behind, which is the run's own state rather than a new entry — a clock instant the run has
    /// already computed (a retried task's backoff), or the *absence* of records being the point (a
    /// poll that must find nothing).
    None,
}

impl Signal {
    /// Whether this entry is the point being waited for.
    fn holds(&self, payload: &EntryPayload) -> bool {
        match self {
            Self::None => true,
            Self::TimerArmed => {
                matches!(payload, EntryPayload::Event(Event::TimerActivated { .. }))
            }
            Self::TaskArmed => matches!(payload, EntryPayload::Event(Event::TaskActivated { .. })),
            Self::TasksClaimed => matches!(payload, EntryPayload::Event(Event::TasksClaimed(_))),
            Self::TaskRequeued => matches!(
                payload,
                EntryPayload::Event(Event::TaskFailed(failed))
                    if failed.task.retry_state.next_available_at.is_some()
            ),
        }
    }

    /// What to report when the run never reaches it (see [`await_log`]).
    fn describe(&self) -> &'static str {
        match self {
            Self::None => "nothing (the act is gated on the run's own state)",
            Self::TimerArmed => "a timer to be armed",
            Self::TaskArmed => "a task to be minted",
            Self::TasksClaimed => "a task to be leased",
            Self::TaskRequeued => "a failed attempt to be re-queued",
        }
    }
}

/// One thing the case's own world must do to a run, once the run has reached the `signal` that says it
/// is ready for it — see [`TypedCase::acts`]. The clock and the external worker are the two things a
/// run cannot supply itself, and *when* each acts is as much a part of what a chain asserts as what
/// the act makes the run write: a deadline fired early, or a settle accepted before its lease, both
/// write records the case's chain has no room for.
#[derive(Clone, Debug)]
pub enum Act {
    /// Move the clock by the duration.
    Advance(Signal, std::time::Duration),
    /// Make one external worker call.
    Drive(Signal, Call),
}

impl Act {
    fn signal(&self) -> Signal {
        match self {
            Self::Advance(signal, _) | Self::Drive(signal, _) => *signal,
        }
    }
}

/// A call the external worker makes into a running execution. Addressed the way the worker's own API
/// is — by the task's **name**, the scalar `poll_tasks` hands back — not by the engine's
/// `ObjectReference`; the worker never learns the uid of what it settles.
#[derive(Clone, Debug)]
pub enum Call {
    /// Claim up to `max_tasks` of `resource`, leasing each for `lease_seconds`. The request id is the
    /// client's own next — the case's chain spells the value the counter yields. `expect` is how many
    /// the case says are claimable: a poll that must find **nothing** (`0`) pins a gate that leaves no
    /// record of its own — a task inside its retry backoff, or one already leased — which the chain
    /// can then only show as the `TasksClaimed` it does *not* contain.
    Poll {
        worker_id: String,
        resource: String,
        max_tasks: usize,
        lease_seconds: u64,
        expect: usize,
    },
    /// Report `task` completed with `output`. `request_id` is the *case's* — a worker-supplied
    /// correlation key the chain can name, so the settle is pinned rather than masked.
    Complete {
        worker_id: String,
        task: ObjectName,
        request_id: RequestId,
        output: Value,
    },
    /// Report `task` failed with `error` (the engine accepts this only from the lease holder).
    Fail {
        worker_id: String,
        task: ObjectName,
        error: ExecutionError,
    },
    /// A call the engine must **refuse**: the runner asserts it errors, and the chain proves the run
    /// wrote nothing for it. What a guard *rejects* is otherwise unobservable — an accepted-but-wrong
    /// settle and a refused one differ only in the records the case says are absent.
    Refused(Box<Call>),
}

impl Call {
    /// Make the call against the run, asserting the outcome the case declares.
    async fn perform(&self, client: &VirtualClient) {
        match self {
            Self::Poll {
                worker_id,
                resource,
                max_tasks,
                lease_seconds,
                expect,
            } => {
                let claimed = client
                    .poll_tasks(worker_id, resource, *max_tasks, *lease_seconds)
                    .await
                    .expect("a poll of claimable work is answered");
                assert_eq!(
                    claimed.len(),
                    *expect,
                    "a poll the case scheduled must find the work it waits for, and only that"
                );
            }
            Self::Complete {
                worker_id,
                task,
                request_id,
                output,
            } => {
                client
                    .complete(worker_id, task.clone(), *request_id, output.clone())
                    .await
                    .expect("a settle the case scheduled is accepted");
            }
            Self::Fail {
                worker_id,
                task,
                error,
            } => {
                client
                    .fail(worker_id, task.clone(), error.clone())
                    .await
                    .expect("a failure the case scheduled is accepted");
            }
            Self::Refused(inner) => {
                let refusal = match inner.as_ref() {
                    Self::Complete {
                        worker_id,
                        task,
                        request_id,
                        output,
                    } => {
                        client
                            .complete(worker_id, task.clone(), *request_id, output.clone())
                            .await
                    }
                    Self::Fail {
                        worker_id,
                        task,
                        error,
                    } => client.fail(worker_id, task.clone(), error.clone()).await,
                    Self::Poll { .. } => panic!(
                        "a refused call is a settle: a poll is answered with no work, not refused"
                    ),
                    Self::Refused(_) => panic!("a refused call cannot itself be refused"),
                };
                assert!(
                    refusal.is_err(),
                    "a settle the case declares refused must be rejected by the engine"
                );
            }
        }
    }
}

/// A lifecycle case whose chain is asserted in its **typed** form — see [`typed_entry_chain`]: the
/// payloads the run wrote, each built as the engine's own value, plus what the run has to supply for
/// that form to be pinnable at all (see [`run_typed_case`]).
///
/// Definition, input, chain and status stay together because the chain is what the definition *means*:
/// read apart from its machine it says nothing. A suite writes each case with `#[rustfmt::skip]` — the
/// chain is a data table, one record per line, and rustfmt's argument-per-line split would bury the
/// run's shape.
pub struct TypedCase {
    /// The definition under test, exactly as submitted.
    pub definition: &'static str,
    /// The execution's input, as JSON.
    pub input: &'static str,
    /// Every record the run must write, in order, each built as the engine's own value. Built in
    /// code rather than parsed from text: a field the payload no longer carries, or a name it
    /// changed, is then a compile error at the expectation — not a parse or a diff at run time —
    /// and the case's own definition can be echoed by name rather than repeated.
    pub chain: Vec<EntryPayload>,
    /// The terminal status the client sees — the engine's own value rather than its JSON, so a status
    /// the client no longer has (or never reaches) is a compile error here too.
    pub status: ExecutionStatus,
    /// What the case's own world must do to the run before it can finish, in order: each act first
    /// waits for the run to reach its [`Act::signal`] *within the window since the act before it*, then
    /// moves the clock or makes one external worker call. Empty for a run that arms no deadline and
    /// calls nothing external, which therefore ends on its own. A timed or externally-driven state
    /// cannot reach its end without its act, so the schedule belongs to the case rather than to the
    /// caller — and *when* each act happens is part of what the chain asserts: an engine that fired a
    /// deadline early, or accepted a settle before its lease, would write a record the chain has no
    /// room for.
    pub acts: Vec<Act>,
}

/// Run `case` on a run whose clock and identities the test owns, and assert its whole entry chain
/// typed plus the terminal status the client sees.
///
/// The virtual clock is what makes the typed form assertable: `created_at`, `updated_at` and every
/// deadline are read off the injected clock, so under the wall clock they are unknowable and a literal
/// could only mask them. Fixed at [`VIRTUAL_EPOCH_MILLIS`], every stamp of a case that arms no timer is
/// that one instant, and the identities are the injected counter's in mint order; a case that does arm
/// one carries the move that fires it in [`TypedCase::acts`], after which its stamps are that
/// instant plus the move. A deadline in the chain is then an absolute instant rather than "some time
/// later", which is what pins the arithmetic rather than merely that something fired.
pub async fn run_typed_case(case: &TypedCase) {
    let (outcome, chain) = run_virtual_and_record(case.definition, case.input, &case.acts).await;

    assert_typed_chain(&chain, &case.chain);

    let execution =
        outcome.expect("a case's run reaches a terminal state; it never errors the client");
    assert_eq!(execution.status, case.status, "terminal status");
}

/// [`run_and_record`] under the test's own clock and identities: the run the typed chain is asserted
/// on, returning both its outcome and its typed chain. `acts` is [`TypedCase::acts`] — the moves and
/// external calls a run needs before it can finish.
async fn run_virtual_and_record(
    definition: &str,
    input: &str,
    acts: &[Act],
) -> (Result<Execution, ExecutionError>, Vec<EntryPayload>) {
    let (client, log, execution) = virtual_run_from(definition, input, 1).await;
    // Where each act's window opens: just past the record that met the previous act's signal, so a
    // signal asks about what has happened since the run was last observed at a point the case named
    // (see [`Signal`]) rather than matching the first occurrence of itself still standing.
    let mut acted_at = 0;
    for act in acts {
        let from = acted_at.min(log.entries().len());
        let signal = act.signal();
        // A signal that waits on no record consumes none, which leaves the window where it was: the
        // act's own appends are then in the next act's window, which is what an act gated on the run's
        // state (rather than on a new entry) has to leave behind for the one after it.
        if !matches!(signal, Signal::None) {
            acted_at = await_log_where(&log, signal.describe(), |entries| {
                entries[from..]
                    .iter()
                    .position(|entry| signal.holds(&entry.payload))
                    .map(|at| from + at + 1)
            })
            .await;
        }
        match act {
            Act::Advance(_, by) => {
                // The arm must be durable *and* handed to the scheduler before the clock moves past
                // it. A late arm would still fire (the deadline it carries is already reached), but
                // the ordering the case asserts would no longer be the run's own — so give the
                // dispatch that feeds the scheduler the same room a real-time case would have.
                tokio::time::sleep(NOT_ENOUGH_REAL_TIME).await;
                client.advance(*by);
            }
            // A call needs no such room: it returns only once its own effect is durable (the poll
            // awaits its grant, the settle its `TaskCompleted`). A refused call returns at once — and
            // what it must not have written is exactly what the chain checks.
            Act::Drive(_, call) => call.perform(&client).await,
        }
        // The act's own appends land before the next window opens, so the next signal is about what
        // this act caused rather than a straggler of the previous one.
        tokio::time::sleep(NOT_ENOUGH_REAL_TIME).await;
    }
    // Bounded, always: a case that declares acts has to reach its terminal *through* them, and an
    // act that failed to wake the scheduler would otherwise hang the suite instead of failing it.
    // The bound is generous next to the ms a case that needs no act takes.
    let result = tokio::time::timeout(TERMINAL_WITHIN, client.wait_for_execution(&execution))
        .await
        .expect(
            "the run reaches its terminal within the bound, without real waiting on a deadline",
        );
    // As in `run_and_record`: `wait_for_execution` resolves on the terminal ack, which the closing
    // sweep of the same cascade follows — let those appends land before snapshotting the log.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let chain = typed_entry_chain(&log);
    client.stop().await;

    (result, chain)
}

/// How long any case's run may take to reach its terminal, in real time. The bound is a failure
/// detector, not a delay: every case here ends as soon as its own cascade does, and one that only ends
/// after this has a run that is stuck — most often a deadline the case's acts never reached, which
/// would otherwise hang the suite rather than fail it.
pub(crate) const TERMINAL_WITHIN: std::time::Duration = std::time::Duration::from_secs(5);

/// Run `definition` with `input` to its terminal state and return the run's **raw** entries — envelope
/// and `Noop` batch terminators included — for a suite that asserts on the envelope a chain is read
/// *through* (see [`assert_envelope`]), and mutates those entries to prove the assertions it makes
/// about them are not vacuous.
pub async fn run_raw_entries(definition: &str, input: &str) -> Vec<Entry> {
    let (_, log, engine) = run_and_record(definition, input).await;
    let entries = log.entries();
    engine.stop().await;
    entries
}

/// The first identity the harness mints for a **request** — deliberately far from `1`, where the
/// engine's own identity counter begins. Both counters count, so a request id and a `uid` of the same
/// ordinal would otherwise render as the same text: a typed chain literal pasted into the wrong field
/// would then match, and a reader could not tell a correlation id from an incarnation id by eye.
/// `1 << 32` sits in the band whose ULID rendering is all decimal (`…04000000`, incrementing) — as
/// readable as the engine's `…00000001` and never mistakable for it.
pub(crate) const FIRST_REQUEST_ID: u64 = 1 << 32;

/// The instant a [`VirtualClient`]'s clock starts at. Any fixed value works — nothing compares it to
/// the wall clock — but a round one keeps a failure's numbers readable, and starting far from the epoch
/// means a deadline computed from it can never be mistaken for an un-set (`0`) timestamp.
pub(crate) const VIRTUAL_EPOCH_MILLIS: u64 = 1_700_000_000_000;

/// A window in which a scheduler keyed on *real* elapsed time would still be waiting. A virtual advance
/// below skips minutes, so this real pause cannot let a real-time implementation fire — it only gives
/// the run room to have behaved wrongly. A wrong run fails here; a right one is silent by construction.
pub(crate) const NOT_ENOUGH_REAL_TIME: std::time::Duration = std::time::Duration::from_millis(20);

/// Wait (bounded) for `what` to become true of the run's log — how a suite observes a step of a run no
/// caller is awaiting, e.g. that a fired timer's resumption has been appended. The bound is a failure
/// detector, not a delay: the wait ends the moment the step lands, and a step that never lands reports
/// `what` rather than timing out silently. Real time is the right budget here because what it waits on
/// is the engine's own dispatch making progress, not the run's clock.
pub(crate) async fn await_log(
    log: &spica_engine::InMemoryLogStream<EntryPayload>,
    what: &str,
    predicate: impl Fn(&[Entry]) -> bool,
) {
    await_log_where(log, what, |entries| predicate(entries).then_some(())).await;
}

/// [`await_log`] for a wait that must report *what it matched*: `locate` is handed the log and asked
/// for a value derived from the first entry satisfying it, which is what the wait yields. A typed case
/// uses it to learn where an [`Act`]'s [`Signal`] was met, so the next act's window can open past it.
pub(crate) async fn await_log_where<T>(
    log: &spica_engine::InMemoryLogStream<EntryPayload>,
    what: &str,
    locate: impl Fn(&[Entry]) -> Option<T>,
) -> T {
    let reached = tokio::time::timeout(TERMINAL_WITHIN, async {
        loop {
            if let Some(found) = locate(&log.entries()) {
                return found;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await;
    match reached {
        Ok(found) => found,
        Err(elapsed) => panic!("the run never reached: {what} (waited {elapsed:?})"),
    }
}

/// Whether the log holds a record of `kind` — the [`payload_kind`] name, e.g. `"TimerTriggered"`.
pub(crate) fn has_record(log: &spica_engine::InMemoryLogStream<EntryPayload>, kind: &str) -> bool {
    log.entries().iter().any(|entry| {
        !matches!(entry.payload, EntryPayload::Noop) && payload_kind(&entry.payload) == kind
    })
}

/// Boot a [`VirtualClient`] on the recording backends and create the flow + execution a virtual-time
/// suite asserts against, returning the still-running client, its readable log and the execution.
///
/// It uses the same fixed [`LIFECYCLE_FLOW`]/[`LIFECYCLE_EXECUTION`] names the other suites' chain
/// tables carry, so a virtual run's shared prefix reads exactly as a real-time one's does — the clock
/// changes when a run's records happen, never what they say.
///
/// Creation returns as soon as the `ExecutionCreated` ack lands; the activation cascade it triggers
/// runs on, so a caller observes the run reaching its first blocking point with [`await_log`].
pub(crate) async fn virtual_run(
    definition: &str,
    input: &str,
) -> (
    VirtualClient,
    Arc<spica_engine::InMemoryLogStream<EntryPayload>>,
    ObjectReference,
) {
    virtual_run_from(definition, input, 1).await
}

/// [`virtual_run`] with the run's identities starting at `first_id` — see
/// [`VirtualClient::start_from`] for why a suite moves that number.
pub(crate) async fn virtual_run_from(
    definition: &str,
    input: &str,
    first_id: u64,
) -> (
    VirtualClient,
    Arc<spica_engine::InMemoryLogStream<EntryPayload>>,
    ObjectReference,
) {
    let input: Value = serde_json::from_str(input).expect("an input literal is valid JSON");
    let (builder, log) = recording_builder();
    let client = VirtualClient::start_from(builder, first_id)
        .await
        .expect("the engine boots under the test's clock");
    let flow_version = client
        .create_flow(
            FlowName::new(LIFECYCLE_FLOW).expect("a static literal is a valid name"),
            definition,
        )
        .await
        .expect("the flow is created");
    let execution = client
        .start_for_revision(
            ObjectName::plain(LIFECYCLE_EXECUTION).expect("a static literal is a valid name"),
            flow_version,
            input,
        )
        .await
        .expect("the execution starts");
    (client, log, execution)
}

/// Run `definition` with `input` to its terminal state, returning the outcome, the readable log and the
/// still-running client: the two callers above differ only in what they take from the same run.
async fn run_and_record(
    definition: &str,
    input: &str,
) -> (
    Result<Execution, ExecutionError>,
    Arc<spica_engine::InMemoryLogStream<EntryPayload>>,
    LocalClient,
) {
    let input: Value = serde_json::from_str(input).expect("an input literal is valid JSON");
    let (builder, log) = recording_builder();
    let engine = LocalClient::start(builder).await.expect("the engine boots");

    let flow_version = engine
        .create_flow(
            FlowName::new(LIFECYCLE_FLOW).expect("a static literal is a valid name"),
            definition,
        )
        .await
        .expect("the flow is created");
    let id = engine
        .start_for_revision(
            ObjectName::plain(LIFECYCLE_EXECUTION).expect("a static literal is a valid name"),
            flow_version,
            input,
        )
        .await
        .expect("the execution starts");

    let result = engine.wait_for_execution(&id).await;
    // `wait_for_execution` resolves on the terminal ack, which the closing sweep of the same cascade
    // follows — let those appends land before snapshotting the log, so a missing trailing record reads
    // as absent rather than as a race.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    (result, log, engine)
}

/// Seed a `CreateExecution` command directly onto a caller-supplied log — the raw CCES seam that
/// used to be `Engine::submit`. Raw-seam drivers (which build their own log + StreamProcessor and never
/// call [`EngineBuilder::start`](spica_engine::EngineBuilder::start)) still need to mint a birth
/// command, so the removed free function's body lives here, built only on the public log/entry
/// types. The seeded execution uses a generated name (the raw seam has no user name to supply); the
/// handler mints the durable `uid` at dispatch, so the seed is fire-and-forget — callers read the
/// resulting rows by stream position, never by a pre-known reference. (There is no per-execution
/// stream — a LogStream is one stream, so stream identity lives on the log, not the caller.)
pub async fn submit_seed(
    flow_version: ObjectReference,
    input: Value,
    logstream: &(impl LogStream<EntryPayload> + ?Sized),
) -> Result<(), ExecutionError> {
    // A static base + u64 suffix is never invalid.
    let name = PlainName::new("seed")
        .expect("static literal is a valid segment")
        .generated_from_key(ulid::Ulid::new().0 as u64);
    logstream
        .append(vec![Entry {
            stream_id: StreamId::nil(), // placeholder — the log stamps the stream on append.
            entry_id: EntryId::nil(),   // placeholder — the log assigns the position on append.
            cause_id: None,
            timestamp: Timestamp::now(),
            payload: EntryPayload::Command(Command::CreateExecution(CreateExecution {
                // Seed is fire-and-forget: nothing awaits this execution's terminal ack, so we mint
                // a throwaway request id (no registry entry routes to it).
                request_id: RequestId::new(),
                name,
                flow_version,
                input,
            })),
        }])
        .await?;
    Ok(())
}

/// Create `sm` under an anonymous name and run one execution with `input` against `builder`'s own
/// backends — the explicit equivalent of the removed `Engine::run`. Consumes `builder` (starting it
/// boots the one long-lived StreamProcessor) and, on completion, drops the running `Engine`, so the
/// result is the single execution's output. **This does not call `Engine::stop`** — the StreamProcessor
/// task is left to be torn down when the engine drops, which suits a one-shot test run.
pub async fn create_and_run(
    builder: EngineBuilder,
    sm: StateMachine,
    input: Value,
) -> Result<Execution, ExecutionError> {
    create_and_run_with_handlers(builder, sm, input, HashMap::new()).await
}

/// Like [`create_and_run`], but boots the engine with `task_handlers` (the handlers are fixed for
/// the engine's lifetime) — the explicit equivalent of the removed `Engine::run_with_task_handlers`.
pub async fn create_and_run_with_handlers(
    builder: EngineBuilder,
    sm: StateMachine,
    input: Value,
    task_handlers: HashMap<String, Arc<dyn TaskHandler>>,
) -> Result<Execution, ExecutionError> {
    // `LocalClient::start` injects a local `AckHook` (so the blocking calls below can await their
    // outcomes) and boots the single long-lived StreamProcessor.
    let client = LocalClient::start(builder).await?;

    // The worker is a separate role (no longer spawned by the engine): boot it against the client's
    // inbound TaskApi (adapted to the worker-facing trait), and own its lifecycle here — cancel it
    // before returning so the strong `Arc` (a reference to the engine's inner state) it holds is
    // released as the engine drops.
    let cancel = CancellationToken::new();
    let worker = {
        let api = Arc::new(EngineTaskApi::new(Arc::new(client.clone())));
        let cancel = cancel.clone();
        let service = InMemoryTaskService::spawn(task_handlers);
        tokio::spawn(async move { service.run(api, cancel).await })
    };

    // Persist the definition the way a string-supplying caller would — the durable record is the raw
    // string, never the transient struct.
    let definition = serde_json::to_string(&sm).map_err(|e| {
        ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
            "serialize state machine: {e}"
        )))
    })?;
    let flow_version = client.create_flow(anonymous_name(), &definition).await?;
    let execution_id = client
        .start_for_revision(execution_name(), flow_version, input)
        .await?;
    let result = client.wait_for_execution(&execution_id).await;

    // Shut the worker down before the engine drops (worker-precedes-engine, the `task_api` contract).
    cancel.cancel();
    let _ = worker.await;
    result
}

/// A throwaway [`FlowName`] so each one-shot run never collides with a user-created flow; the
/// name's charset (`[A-Za-z0-9_]`) admits the `anon_` + ULID form.
pub fn anonymous_name() -> FlowName {
    FlowName::new(&format!("anon_{}", ulid::Ulid::new()))
        .expect("a ULID-suffixed anonymous name always satisfies FlowName's charset")
}

/// A throwaway, collision-free execution name for tests that don't care about the (now required)
/// user-supplied execution name. Uses the plain (**user**) form — a `CreateExecution` execution is
/// user-named by contract, and a generated child (e.g. the ExecutionTimeout timer) derives its own
/// name from this *plain* base, so it must not itself be generated. The random `_<ulid>` tail keeps
/// it collision-free without `-`.
pub fn execution_name() -> ObjectName {
    ObjectName::plain(&format!("run_{}", ulid::Ulid::new()))
        .expect("a ULID-suffixed user name is always valid")
}

// ── local blocking client ───────────────────────────────────────────────────────
//
// The engine is append + observe only; the blocking request/response API (`create_flow`, start,
// task poll/settle) now lives in the consumer (spica-server). Integration tests reimplement that
// consumer here as a `LocalClient`: an `AckHook` observer injected at boot correlates an awaited
// command's outcome to its caller via the echoed `request_id`, and `LocalClient` exposes the same
// shapes the engine's blocking API used to. `LocalClient` derefs to the `Engine` so the retained
// read/lifecycle methods (`wait_for_execution`, `stop`, `get_object`, …) resolve unchanged.

/// The awaiting outcome-event variant a pending entry expects. Gating on the variant matters because
/// a `CreateFlow` emits two request-id-bearing events (`FlowCreated`, then `FlowVersionCreated`)
/// sharing one `request_id`; only the awaited variant may wake its caller.
#[derive(Clone, Copy, PartialEq, Eq)]
enum AckTarget {
    FlowVersionCreated,
    ExecutionCreated,
    TaskCompleted,
    Grant,
}

/// The payload an acknowledgement delivers: the applied [`Event`], a [`Reject`], or a granted task set.
enum AckOutcome {
    Applied(Box<Event>),
    Rejected(Reject),
    Granted(Vec<ActivatedTask>),
}

/// Failure modes of awaiting an acknowledgement.
enum AckFailure {
    Dropped,
    Rejected(Reject),
}

/// The [`Hook`] this client injects into the engine: correlates applied events / rejections / grants
/// to awaiting callers by the echoed `request_id`. `register` must precede the command's append.
struct AckHook {
    pending: Mutex<HashMap<RequestId, (AckTarget, oneshot::Sender<AckOutcome>)>>,
}

impl AckHook {
    fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    async fn register(&self, key: RequestId, target: AckTarget) -> oneshot::Receiver<AckOutcome> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(key, (target, tx));
        rx
    }

    /// The `request_id` an event echoes and its awaited [`AckTarget`], when the variant is an awaited
    /// outcome. `FlowCreated` (same `CreateFlow` request_id) is deliberately not an awaited target.
    fn resolves(event: &Event) -> Option<(RequestId, AckTarget)> {
        match event {
            Event::FlowVersionCreated(FlowVersionCreated { request_id, .. }) => {
                Some((*request_id, AckTarget::FlowVersionCreated))
            }
            Event::ExecutionCreated(ExecutionCreated { request_id, .. }) => {
                Some((*request_id, AckTarget::ExecutionCreated))
            }
            Event::TaskCompleted(TaskCompleted { request_id, .. }) => {
                Some((*request_id, AckTarget::TaskCompleted))
            }
            Event::TasksClaimed(TasksClaimed { request_id, .. }) => {
                Some((*request_id, AckTarget::Grant))
            }
            _ => None,
        }
    }

    /// Rebuild the worker-facing grant from a durable `TasksClaimed` (see the server's `AckHook`).
    fn granted_from(tasks: &[Task]) -> Vec<ActivatedTask> {
        tasks
            .iter()
            .map(|t| ActivatedTask {
                task: t.meta.name.clone(),
                resource: t.resource.clone(),
                arguments: t.arguments.clone(),
            })
            .collect()
    }
}

#[async_trait]
impl Hook for AckHook {
    async fn on_event_applied(&self, event: &Event) {
        let Some((request_id, target)) = Self::resolves(event) else {
            return;
        };
        let mut pending = self.pending.lock().await;
        let Some((t, tx)) = pending.remove(&request_id) else {
            return;
        };
        if t != target {
            return;
        }
        // A Grant is answered by the durable `TasksClaimed` itself; every other awaited outcome is
        // Applied. A Grant target only ever aligns with a `TasksClaimed`.
        let outcome = match target {
            AckTarget::Grant => match event {
                Event::TasksClaimed(TasksClaimed { tasks, .. }) => {
                    AckOutcome::Granted(Self::granted_from(tasks))
                }
                _ => return,
            },
            _ => AckOutcome::Applied(Box::new(event.clone())),
        };
        let _ = tx.send(outcome);
    }

    async fn on_command_rejected(&self, request_id: RequestId, reject: &Reject) {
        let mut pending = self.pending.lock().await;
        if let Some((_, tx)) = pending.remove(&request_id) {
            let _ = tx.send(AckOutcome::Rejected(reject.clone()));
        }
    }
}

/// The composite [`Hook`] a [`LocalClient`] injects: routes the acknowledgement facts to its
/// [`AckHook`], and re-derives physical timer arms/cancels from the durable
/// `TimerActivated`/`TimerCancelled` events — the consumer-owned side of timer scheduling (see
/// [`LocalClient::start`]).
struct CompositeHook {
    ack: Arc<AckHook>,
    scheduler: Arc<dyn Scheduler>,
}

#[async_trait]
impl Hook for CompositeHook {
    async fn on_event_applied(&self, event: &Event) {
        match event {
            Event::TimerActivated { timer } => {
                // The durable event carries the timer's absolute deadline; re-arm the physical
                // schedule from that persisted moment.
                self.scheduler.schedule(&timer.reference(), timer.deadline);
            }
            Event::TimerCancelled { timer } => self.scheduler.cancel(&timer.reference()),
            _ => {}
        }
        self.ack.on_event_applied(event).await;
    }

    async fn on_command_rejected(&self, request_id: RequestId, reject: &Reject) {
        self.ack.on_command_rejected(request_id, reject).await;
    }
}

/// The [`TimerSink`] a [`LocalClient`] attaches to its scheduler: routes an expired timer's
/// `TriggerTimer` back into the engine's append path. Holds the engine behind a **weak** slot filled
/// at boot (see [`LocalClient::start`]): the slot is owned by the observer chain the engine itself
/// holds (hook → scheduler → sink), so a strong reference there would keep the engine alive forever
/// and defeat `LocalClient::stop`'s `Arc::try_unwrap`. A fire happens only while the engine is alive
/// (post-start), so the weak upgrade succeeds right up to teardown.
struct EngineTimerSink {
    engine: Arc<Mutex<Option<std::sync::Weak<Engine>>>>,
}

#[async_trait]
impl TimerSink for EngineTimerSink {
    async fn trigger(&self, timer: &ObjectReference) {
        let Some(engine) = self
            .engine
            .lock()
            .await
            .clone()
            .and_then(|weak| weak.upgrade())
        else {
            return; // engine not booted or already dropped; nothing to resume.
        };
        // Fire-and-forget: a dropped append (engine shutting down) is not this consumer's fault.
        let _ = engine
            .append_command(Command::TriggerTimer {
                timer: timer.clone(),
            })
            .await;
    }
}

/// The blocking convenience client over a running [`Engine`] — the engine's removed blocking API,
/// reimplemented as a consumer for integration tests. Derefs to the `Engine` so retained methods
/// resolve unchanged; `create_flow`/`start_for_revision` are inherent, and the task claim/settle API
/// is implemented via [`spica_engine::TaskApi`].
#[derive(Clone)]
pub(crate) struct LocalClient {
    /// The running engine this client drives and reads from.
    pub(crate) engine: Arc<Engine>,
    /// The clock this client's engine was assembled with — the same one injected into the scheduler.
    /// A client reads it wherever it needs "now" (the retry gate below), so a run under a manual clock
    /// sees one time on both sides of the seam.
    clock: Arc<dyn Clock>,
    /// The scheduler booted alongside the engine, kept so a caller that moves the clock itself can
    /// re-evaluate the armed timers against it (see [`VirtualClient`]).
    scheduler: Arc<dyn Scheduler>,
    /// The caller-side id source behind [`Self::request_id`] — ack correlation is the caller's to
    /// name, so reproducibility is only complete if this client mints predictably too.
    requests: Arc<CountingIdGenerator>,
    ack: Arc<AckHook>,
}

impl std::ops::Deref for LocalClient {
    type Target = Engine;

    fn deref(&self) -> &Engine {
        &self.engine
    }
}

impl LocalClient {
    /// Inject the composite observer and boot the engine, then wrap it.
    ///
    /// Timer scheduling is **consumer-owned** (the engine no longer holds a scheduler): this boots
    /// an [`InMemoryScheduler`], injects a comosite [`Hook`] that (a) routes the `AckHook` facts and
    /// (b) re-derives physical timer arms from durable `TimerActivated`/`TimerCancelled` events, and
    /// attaches a [`TimerSink`] that routes an expired timer's `TriggerTimer` back into the engine's
    /// append path.
    pub(crate) async fn start(builder: EngineBuilder) -> Result<Self, ExecutionError> {
        Self::start_with_seams(builder, Arc::new(SystemClock), Arc::new(SystemIdGenerator)).await
    }

    /// [`Self::start`] with the run's time and identities supplied by the injected [`Clock`] and
    /// [`IdGenerator`] instead of the wall clock and fresh random ULIDs: every stamp the engine writes
    /// and every deadline it decides on reads `clock`, so does the scheduler booted here — the one clock
    /// both sides of the seam must share (see
    /// [`EngineBuilder::with_clock`](spica_engine::EngineBuilder::with_clock)) — and every `uid` a
    /// dispatch mints reads `ids`. Pair it with [`VirtualClient`] to drive a run's deadlines without
    /// waiting for them and to name its objects predictably.
    pub(crate) async fn start_with_seams(
        builder: EngineBuilder,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn IdGenerator>,
    ) -> Result<Self, ExecutionError> {
        let ack = Arc::new(AckHook::new());
        let scheduler: Arc<dyn Scheduler> = InMemoryScheduler::spawn_with_clock(Arc::clone(&clock));
        // The sink can only append once the engine exists, but the engine needs the hook at boot; a
        // fire happens strictly post-start, so a slot filled here after boot closes the gap. Held as
        // a `Weak` to keep the observer chain (hook → scheduler → sink) from keeping the engine alive
        // (see `EngineTimerSink`).
        let engine_slot: Arc<Mutex<Option<std::sync::Weak<Engine>>>> = Arc::new(Mutex::new(None));
        scheduler.attach_sink(Arc::new(EngineTimerSink {
            engine: engine_slot.clone(),
        }));
        let hook = CompositeHook {
            ack: ack.clone(),
            scheduler: scheduler.clone(),
        };
        let engine = Arc::new(
            builder
                .with_clock(Arc::clone(&clock))
                .with_id_generator(Arc::clone(&ids))
                .with_hook(Arc::new(hook))
                .start()
                .await?,
        );
        *engine_slot.lock().await = Some(Arc::downgrade(&engine));
        Ok(Self {
            engine,
            clock,
            scheduler,
            requests: Arc::new(CountingIdGenerator::starting_at(FIRST_REQUEST_ID)),
            ack,
        })
    }

    /// The next `request_id` for an operation this client appends — the id the engine's ack must echo
    /// back. The engine takes request ids from its caller (they correlate an ack; they are not entity
    /// identity, and never enter the handler's identity path), so a wall-clock/random id minted here
    /// would leave every run's acks differing even under an injected clock and id generator. Counting
    /// keeps them distinct within a run, which is all correlation requires, and reproducible across
    /// runs, which is what lets a whole log repeat.
    pub(crate) fn request_id(&self) -> RequestId {
        RequestId::from(self.requests.next_ulid())
    }

    /// Re-evaluate the booted scheduler's armed timers against the clock's current reading — a no-op
    /// under the wall clock, and the second half of a manual advance (see [`VirtualClient::advance`]).
    pub(crate) fn tick(&self) {
        self.scheduler.tick();
    }

    /// Controlled shutdown, forwarding to [`Engine::stop`] when this client is the engine's sole
    /// remaining `Arc` holder (a worker that cloned the engine must be dropped first — the engine's
    /// own `stop` refuses to consume it otherwise).
    pub(crate) async fn stop(self) {
        if Arc::strong_count(&self.engine) != 1 {
            // A sibling `Arc<Engine>` still lives (e.g. a worker cloned it); we cannot consume it.
            // Dropping `self` releases this reference; a cleaner shutdown requires dropping the
            // sibling first.
            return;
        }
        if let Ok(engine) = Arc::try_unwrap(self.engine) {
            engine.stop().await;
        }
    }

    async fn await_event(rx: oneshot::Receiver<AckOutcome>) -> Result<Box<Event>, AckFailure> {
        match rx.await {
            Ok(AckOutcome::Applied(ev)) => Ok(ev),
            Ok(AckOutcome::Rejected(reject)) => Err(AckFailure::Rejected(reject)),
            Ok(AckOutcome::Granted(_)) => {
                unreachable!("a task grant is awaited via await_tasks, not await_event")
            }
            Err(_) => Err(AckFailure::Dropped),
        }
    }

    async fn await_tasks(
        rx: oneshot::Receiver<AckOutcome>,
    ) -> Result<Vec<ActivatedTask>, AckFailure> {
        match rx.await {
            Ok(AckOutcome::Granted(tasks)) => Ok(tasks),
            Ok(AckOutcome::Rejected(reject)) => Err(AckFailure::Rejected(reject)),
            Ok(AckOutcome::Applied(_)) => {
                unreachable!("a ClaimTasks ack delivers Granted, never an applied Event")
            }
            Err(_) => Err(AckFailure::Dropped),
        }
    }

    fn ack_failure_error(failure: AckFailure) -> ExecutionError {
        match failure {
            AckFailure::Dropped => ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "no acknowledgement received (engine not started, or its StreamProcessor never applied \
                 the awaited outcome)"
                    .to_string(),
            )),
            AckFailure::Rejected(reject) => ExecutionError::Rejected(reject),
        }
    }

    /// Create a new flow version and return its created version's [`ObjectReference`].
    pub(crate) async fn create_flow(
        &self,
        name: FlowName,
        definition: &str,
    ) -> Result<ObjectReference, ExecutionError> {
        // Fail fast: an unparseable definition can never enter the log or Storage.
        if serde_json::from_str::<spica_asl::StateMachine>(definition).is_err() {
            return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "malformed flow definition: does not parse as a StateMachine".to_string(),
            )));
        }
        if self
            .engine
            .get_object(ObjectKind::Flow, &ObjectName::Plain(name.clone()))
            .await?
            .is_some()
        {
            return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                format!("flow {name} already exists"),
            )));
        }
        let request_id = self.request_id();
        let rx = self
            .ack
            .register(request_id, AckTarget::FlowVersionCreated)
            .await;
        self.engine
            .append_command(Command::CreateFlow(CreateFlow {
                request_id,
                name: name.clone(),
                definition: definition.to_owned(),
            }))
            .await?;
        let event = match Self::await_event(rx).await {
            Ok(ev) => *ev,
            Err(failure) => return Err(Self::ack_failure_error(failure)),
        };
        let Event::FlowVersionCreated(FlowVersionCreated { flow_version, .. }) = event else {
            unreachable!(
                "AckHook routes CreateFlow's ack only to a FlowVersionCreated event; got {event:?}"
            );
        };
        Ok(flow_version.reference())
    }

    /// Start an execution against `flow_version`, returning the execution's id at birth.
    pub(crate) async fn start_for_revision(
        &self,
        name: ObjectName,
        flow_version: ObjectReference,
        input: Value,
    ) -> Result<ObjectReference, ExecutionError> {
        // Boundary pre-check: the name is the execution's storage primary key (per-scope unique).
        if self
            .engine
            .get_object(ObjectKind::Execution, &name)
            .await?
            .is_some()
        {
            return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                format!("execution {name} already exists"),
            )));
        }
        let request_id = self.request_id();
        let rx = self
            .ack
            .register(request_id, AckTarget::ExecutionCreated)
            .await;
        self.engine
            .append_command(Command::CreateExecution(CreateExecution {
                request_id,
                name,
                flow_version,
                input,
            }))
            .await?;
        let event = match Self::await_event(rx).await {
            Ok(ev) => *ev,
            Err(failure) => return Err(Self::ack_failure_error(failure)),
        };
        match event {
            Event::ExecutionCreated(ExecutionCreated { execution, .. }) => {
                Ok(execution.reference())
            }
            _ => unreachable!("AckHook only delivers ExecutionCreated to this ack"),
        }
    }
}

/// A [`LocalClient`] whose time belongs to the test: one [`ManualClock`] is injected into the engine
/// (every stamp it writes, every deadline it decides) *and* into the scheduler its timers expire on, so
/// moving it moves the whole run — a `Wait` of 60 s, a task `TimeoutSeconds`, a lease lapse and a retry
/// backoff all reach their boundary in no real time and with no slack.
///
/// The clock's reading starts wherever the test sets it and never moves on its own, which is what makes
/// a run's records reproducible: the same definition produces the same chain twice.
///
/// Advancing is deliberately **one** call: the clock and the scheduler's re-evaluation go together (see
/// [`Scheduler::tick`](spica_scheduler::Scheduler::tick)), so a test cannot move time and quietly leave
/// the loop that watches it behind.
pub(crate) struct VirtualClient {
    client: LocalClient,
    clock: Arc<ManualClock>,
}

impl std::ops::Deref for VirtualClient {
    type Target = LocalClient;

    fn deref(&self) -> &LocalClient {
        &self.client
    }
}

impl VirtualClient {
    /// Boot an engine whose time the returned client owns, on `builder`'s backends. Requires a Tokio
    /// runtime (the engine and the scheduler both spawn tasks).
    ///
    /// Both seams are substituted, not just the clock: [`ManualClock`] fixes *when* everything happens
    /// and [`CountingIdGenerator`] fixes *what everything is called*, so a run of the same definition
    /// repeats itself exactly — the log is then reproducible rather than merely re-ordered the same
    /// way, which is what lets a test pin a chain's identities instead of masking them out.
    ///
    /// `first_id` is where the run's identities begin — the seam a suite moves to show that the ids it
    /// compares are really the injected ones (two runs that differ only in this number must produce
    /// different logs).
    pub(crate) async fn start_from(
        builder: EngineBuilder,
        first_id: u64,
    ) -> Result<Self, ExecutionError> {
        let clock = Arc::new(ManualClock::new(Timestamp::from_millis(
            VIRTUAL_EPOCH_MILLIS,
        )));
        let client = LocalClient::start_with_seams(
            builder,
            Arc::clone(&clock) as Arc<dyn Clock>,
            Arc::new(CountingIdGenerator::starting_at(first_id)),
        )
        .await?;
        Ok(Self { client, clock })
    }

    /// Move the run's clock forward by `by` and re-evaluate the scheduler against the new reading —
    /// every deadline the move passed fires (in order, through the engine's own append path) and the
    /// ones it did not stay armed. Callers await the resulting cascade through the log (see
    /// [`await_log`]) or through the execution's terminal ack (`wait_for_execution`).
    pub(crate) fn advance(&self, by: std::time::Duration) {
        self.clock.advance(by);
        self.client.scheduler.tick();
    }

    /// Controlled shutdown, as [`LocalClient::stop`] (`VirtualClient` itself holds no engine clone).
    pub(crate) async fn stop(self) {
        self.client.stop().await;
    }
}

#[async_trait]
impl spica_engine::TaskApi for LocalClient {
    async fn poll_tasks(
        &self,
        worker_id: &str,
        resource: &str,
        max_tasks: usize,
        lease_seconds: u64,
    ) -> Result<Vec<ActivatedTask>, ExecutionError> {
        // Read-first gate (an idle poll stays a pure query and appends nothing). Read from the run's
        // own clock, not the wall clock: claimability is decided against the gates and leases the
        // engine stamped from that clock, so a manual clock must be the one that decides here too —
        // and the discovery already applies exactly the predicate the dispatch will (`is_claimable_at`).
        let claimable_now = {
            let now = self.clock.now();
            match self
                .engine
                .activatable_tasks(resource, now, max_tasks)
                .await
            {
                Ok(ts) => !ts.is_empty(),
                Err(_) => false,
            }
        };
        if !claimable_now {
            return Ok(Vec::new());
        }
        let request_id = self.request_id();
        let rx = self.ack.register(request_id, AckTarget::Grant).await;
        self.engine
            .append_command(Command::ClaimTasks(ClaimTasks {
                request_id,
                worker_id: worker_id.to_string(),
                resource: resource.to_string(),
                max_tasks,
                lease_seconds,
            }))
            .await?;
        match Self::await_tasks(rx).await {
            Ok(tasks) => Ok(tasks),
            Err(failure) => Err(Self::ack_failure_error(failure)),
        }
    }

    async fn complete(
        &self,
        worker_id: &str,
        task: ObjectName,
        request_id: RequestId,
        output: Value,
    ) -> Result<(), ExecutionError> {
        let rx = self
            .ack
            .register(request_id, AckTarget::TaskCompleted)
            .await;
        let task_ref = ObjectReference::new(ObjectKind::Task, task, ulid::Ulid::nil());
        self.engine
            .append_command(Command::CompleteTask(CompleteTask {
                request_id,
                task: task_ref,
                worker_id: worker_id.to_string(),
                output,
            }))
            .await?;
        match Self::await_event(rx).await {
            Ok(_) => Ok(()),
            Err(failure) => Err(Self::ack_failure_error(failure)),
        }
    }

    async fn fail(
        &self,
        worker_id: &str,
        task: ObjectName,
        error: ExecutionError,
    ) -> Result<(), ExecutionError> {
        let task_ref = ObjectReference::new(ObjectKind::Task, task, ulid::Ulid::nil());
        self.engine
            .append_command(Command::FailTask(FailTask {
                task: task_ref,
                worker_id: worker_id.to_string(),
                error,
            }))
            .await
    }
}
