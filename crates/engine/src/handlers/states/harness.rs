//! Shared fixtures and drivers for the state handlers' `activate` / `complete` unit tests.
//!
//! A dispatch is driven the way the leader drives one: the emitted Events are folded eagerly into a
//! **working overlay** over the store, and the batch is committed at the end — so an assertion reads
//! the fold back off the store's committed face, not merely off the entry chain. Two drivers exist,
//! one per lifecycle step:
//!
//! - [`activate`] opens an `ActivateState` on a live scope;
//! - [`complete`] opens a `CompleteState`, taking the store a preceding `activate` left behind (or a
//!   hand-seeded one from [`complete_store`]). Continuing on that store is what lets a `Wait`/`Task`
//!   test reach its `complete` with the timer child its own activation armed still attached — the
//!   child state the two states' `on_completing` are actually written to handle.
//!
//! Both drivers dispatch through [`build_state_handlers`]'s registry rather than a named factory, so
//! every test also covers the `State` → handler selection.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde_json::Value;
use spica_asl::State;
use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
use spica_storage::InMemoryStorage;

use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext, OverlaySink};
use crate::handlers::dispatch::build_state_handlers;
use crate::storage::{ActivityRecord, Storage, ThreadRecord};
use crate::types::command::{ActivateState, CompleteState};
use crate::types::id::EntryId;
use crate::types::meta::{ObjectKind, ObjectMeta, ObjectName, ObjectReference};
use crate::types::state_path::StatePath;
use crate::working::WorkingState;
use crate::{
    Activity, ActivityState, ActivityStatus, Entry, EntryPayload, Thread, ThreadStatus, Timestamp,
};

// ── deterministic inputs ────────────────────────────────────────────────────

/// The instant every stamp reads: one `ManualClock` reading serves an activity's meta, each
/// envelope's timestamp and the deadlines a state decides, so `at()` pins them all at once.
pub fn at() -> Timestamp {
    Timestamp::from_millis(1_000)
}

/// The `n`-th identity a [`CountingIdGenerator`] mints (it starts at `1`). The seeded references use
/// ids far above that range, so a *freshly minted* object reads as `uid(1)`, `uid(2)`, … rather than
/// blending into the objects the test wrote down itself.
pub fn uid(n: u64) -> ulid::Ulid {
    ulid::Ulid::from(u128::from(n))
}

pub fn obj_name(name: &str) -> ObjectName {
    ObjectName::from_parsed(name).expect("a static literal is a valid object name")
}

pub fn pointer(document: &str) -> jsonptr::PointerBuf {
    let mut buf = jsonptr::PointerBuf::new();
    for segment in document.split('/').filter(|s| !s.is_empty()) {
        buf.push_back(segment);
    }
    buf
}

pub fn path(document: &str) -> StatePath {
    StatePath::from(pointer(document))
}

/// The input a seeded activity carries — the same value [`seeded_scope`] seeds on the thread, so the
/// scope's variables and the activity's input agree.
pub fn seeded_input() -> Value {
    serde_json::json!({ "n": 1 })
}

// ── seeded references ───────────────────────────────────────────────────────

pub fn execution_ref() -> ObjectReference {
    ObjectReference::new(ObjectKind::Execution, obj_name("execution"), uid(90))
}

/// The scope every activity under test is owned by: a fan-out thread (the derived root thread for a
/// top-level run, per the engine's unified-owner model) at `/States`.
pub fn thread_ref() -> ObjectReference {
    ObjectReference::new(ObjectKind::Thread, obj_name("execution-0"), uid(91))
}

/// The reference `activate` mints on a clean store: the injected generator's first id, named from the
/// partition counter's first free suffix over the execution's plain base name.
pub fn minted_activity_ref() -> ObjectReference {
    ObjectReference::new(ObjectKind::Activity, obj_name("execution-0"), uid(1))
}

/// The meta `activate` mints for that activity: `created_at == updated_at == at()` (the birth
/// moment), owned by the scope the command named.
pub fn minted_activity_meta() -> ObjectMeta {
    ObjectMeta::builder(ObjectKind::Activity, uid(1))
        .name(obj_name("execution-0"))
        .at(at())
        .build()
        .with_owner(thread_ref())
}

/// The activity value `activate` mints on a clean store: [`minted_activity_ref`] with the given
/// `state_path`, `Running`, carrying `input` as its raw input and nothing else yet. Every state's
/// `StateActivating` is this value plus whatever its own `initialize` seeds.
pub fn minted_activity(state_path: StatePath, input: Value) -> Activity {
    Activity {
        meta: minted_activity_meta(),
        execution: execution_ref(),
        state_path,
        status: ActivityStatus::Running,
        raw_input: input,
        input: None,
        raw_output: None,
        activity_state: None,
        retry_state: None,
        output: None,
    }
}

/// The `ActivateState` a test drives: the state named by `state_path`, owned by [`thread_ref`].
pub fn activate_cmd(state_path: StatePath, input: Value) -> ActivateState {
    ActivateState {
        execution: execution_ref(),
        owner: thread_ref(),
        state_path,
        input,
    }
}

/// The `CompleteState` naming [`minted_activity_ref`] — the activity [`activate`] mints and
/// [`seeded_activity`] seeds — carrying the state's raw result `output`.
pub fn complete_cmd(output: Value) -> CompleteState {
    CompleteState {
        activity: minted_activity_ref(),
        output,
    }
}

// ── seeded rows ─────────────────────────────────────────────────────────────

/// The scope a state's activity is owned by — the thread `activate` resolves and screens for
/// liveness before it does anything else, and the owner `complete` loads the scope variables from.
pub fn seeded_scope(status: ThreadStatus) -> ThreadRecord {
    let thread = Thread {
        meta: ObjectMeta::builder(ObjectKind::Thread, thread_ref().uid)
            .name(thread_ref().name)
            .at(at())
            .build()
            .with_owner(execution_ref()),
        execution: execution_ref(),
        state_path: path("/States"),
        start_at: "P".to_string(),
        index: 0,
        status,
        input: seeded_input(),
        output: None,
    };
    let mut row = ThreadRecord::from_value(thread, HashSet::new());
    row.born(at());
    row
}

/// A `Running` activity row at `/States/P`, owned by [`thread_ref`] and carrying `input` as both its
/// raw and processed input — the row a `CompleteState` names. `children` are the child refs still
/// attached (a `Wait`'s resume timer, a `Task`'s deadline); the state's own `on_completing` is what
/// disposes of them.
pub fn seeded_activity(
    input: Value,
    children: impl IntoIterator<Item = ObjectReference>,
) -> ActivityRecord {
    let mut activity = minted_activity(path("/States/P"), input.clone());
    activity.input = Some(input);
    let mut row =
        ActivityRecord::from_value(activity, children.into_iter().collect::<HashSet<_>>());
    row.born(at());
    row
}

/// The container variant of [`seeded_activity`]: a row the caller shapes fully, so a `Parallel`/`Map`
/// activity can carry the fan-out plan its `child_completed` reads, and an already-settled `status`
/// with the child refs still attached. Those two are what decide whether the next settle is a
/// convergence point or just a replenish.
pub fn seeded_activity_with(
    state_path: StatePath,
    input: Value,
    activity_state: ActivityState,
    status: ActivityStatus,
    children: impl IntoIterator<Item = ObjectReference>,
) -> ActivityRecord {
    let mut activity = minted_activity(state_path, input.clone());
    activity.input = Some(input);
    activity.activity_state = Some(activity_state);
    activity.status = status;
    let mut row =
        ActivityRecord::from_value(activity, children.into_iter().collect::<HashSet<_>>());
    row.born(at());
    row
}

/// The reference of the `index`-th fan-out child (a `Parallel` branch / `Map` item). The ids sit far
/// above the injected generator's range, so a freshly minted object never collides with a seeded one.
pub fn child_ref(index: usize) -> ObjectReference {
    ObjectReference::new(
        ObjectKind::Thread,
        obj_name(&format!("child-{index}")),
        uid(80 + index as u64),
    )
}

/// A settled fan-out child row (a `Parallel` branch / `Map` item) — the per-child outcome the
/// container either aggregates or fails on. `state_path` is the child's own sub-machine table; the
/// container's convergence reads only `status` and `output` off this row, never the path.
pub fn seeded_child_thread(
    state_path: StatePath,
    index: usize,
    output: Value,
    status: ThreadStatus,
) -> ThreadRecord {
    let reference = child_ref(index);
    let thread = Thread {
        meta: ObjectMeta::builder(ObjectKind::Thread, reference.uid)
            .name(reference.name)
            .at(at())
            .build()
            .with_owner(minted_activity_ref()),
        execution: execution_ref(),
        state_path,
        start_at: "I0".to_string(),
        index,
        status,
        input: seeded_input(),
        output: Some(output),
    };
    let mut row = ThreadRecord::from_value(thread, HashSet::new());
    row.born(at());
    row
}

/// A store holding just the live scope and a `Running` activity row (see [`seeded_activity`]) — where
/// a `complete` test starts when it is not continuing a preceding `activate`.
pub async fn complete_store(
    input: Value,
    children: impl IntoIterator<Item = ObjectReference>,
) -> InMemoryStorage {
    let mut store = InMemoryStorage::new();
    store
        .put_thread(seeded_scope(ThreadStatus::Running))
        .await
        .expect("the in-memory store seeds a thread row");
    store
        .put_activity(seeded_activity(input, children))
        .await
        .expect("the in-memory store seeds an activity row");
    store
}

/// Seed `store` with the run a `child_completed` test starts from: the live owning scope (from
/// [`seeded_scope`]), the container activity row, and every fan-out child row. All of them are
/// *already settled or in flight as written down* — that state is the input to the settle being
/// driven, not something the driver produces.
pub async fn seed_container(
    store: &mut InMemoryStorage,
    activity: ActivityRecord,
    children: impl IntoIterator<Item = ThreadRecord>,
) {
    store
        .put_thread(seeded_scope(ThreadStatus::Running))
        .await
        .expect("the in-memory store seeds a thread row");
    store
        .put_activity(activity)
        .await
        .expect("the in-memory store seeds an activity row");
    for child in children {
        store
            .put_thread(child)
            .await
            .expect("the in-memory store seeds a child thread row");
    }
}

// ── drivers ─────────────────────────────────────────────────────────────────

/// One dispatch's observable result: the entries the collector enveloped, and the store the emitted
/// Events were folded into.
pub struct Dispatch {
    pub entries: Vec<Entry>,
    pub store: InMemoryStorage,
}

impl Dispatch {
    /// The payloads in emission order — what a dispatch's entry chain is asserted on, since the
    /// collector leaves the position/stream ids to the log.
    pub fn chain(&self) -> Vec<EntryPayload> {
        self.entries.iter().map(|e| e.payload.clone()).collect()
    }

    /// The committed activity row at `reference`, or `None` where nothing was folded.
    pub async fn activity(&self, reference: &ObjectReference) -> Option<ActivityRecord> {
        self.store
            .get_activity(reference)
            .await
            .expect("the in-memory store reads")
    }

    /// The committed refs still attached to `reference` as its children.
    pub async fn children(&self, reference: &ObjectReference) -> HashSet<ObjectReference> {
        self.store
            .get_children(reference.clone())
            .await
            .expect("the in-memory store reads")
    }
}

/// Drive `state`'s inherited [`StateHandler::activate`](crate::handlers::state_handler::StateHandler)
/// once over a working overlay — the leader's shape, so the emitted `StateActivating`/`StateActivated`
/// (and any timer a state arms) fold into the transaction and a later read sees them. `scope` seeds
/// the store beforehand, and the batch is committed afterwards as the leader's driver does, so the
/// fold lands on the store's committed face — which is what the assertions read.
pub async fn activate(state: &State, cmd: &ActivateState, scope: Option<ThreadRecord>) -> Dispatch {
    let mut store = InMemoryStorage::new();
    if let Some(thread) = scope {
        store
            .put_thread(thread)
            .await
            .expect("the in-memory store seeds a thread row");
    }

    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(at()));
    let ids: Arc<dyn IdGenerator> = Arc::new(CountingIdGenerator::new());
    // The overlay owns the batch's single transaction, exactly as on the leader (see `leader.rs`).
    let work = WorkingState::new(store.begin_txn().expect("the in-memory store begins a txn"));
    let mut out = Collector::new(
        EntryId::new(1),
        Some(OverlaySink::new(&work)),
        clock.clone(),
        ids.clone(),
    );
    let mut env = EvalEnv::new();
    let mut definitions = HashMap::new();
    let state_handlers = build_state_handlers();
    // Scoped so the context's borrow of the overlay ends before the batch is committed.
    {
        let mut ctx = HandlerContext {
            env: &mut env,
            storage: &work,
            clock,
            ids,
            definitions: &mut definitions,
            state_handlers: &state_handlers,
        };
        state_handlers
            .create(state)
            .expect("every State variant has a registered handler")
            .activate(&mut ctx, &mut out, cmd)
            .await;
    }
    let entries = out.into_entries();
    work.commit(None).await.expect("the batch commits");

    Dispatch { entries, store }
}

/// Drive `state`'s inherited [`StateHandler::complete`](crate::handlers::state_handler::StateHandler)
/// once over `store` — the mirror of [`activate`], seeded either from a preceding activation's store or
/// from [`complete_store`].
pub async fn complete(state: &State, store: InMemoryStorage, cmd: &CompleteState) -> Dispatch {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(at()));
    let ids: Arc<dyn IdGenerator> = Arc::new(CountingIdGenerator::new());
    let work = WorkingState::new(store.begin_txn().expect("the in-memory store begins a txn"));
    let mut out = Collector::new(
        EntryId::new(1),
        Some(OverlaySink::new(&work)),
        clock.clone(),
        ids.clone(),
    );
    let mut env = EvalEnv::new();
    let mut definitions = HashMap::new();
    let state_handlers = build_state_handlers();
    {
        let mut ctx = HandlerContext {
            env: &mut env,
            storage: &work,
            clock,
            ids,
            definitions: &mut definitions,
            state_handlers: &state_handlers,
        };
        state_handlers
            .create(state)
            .expect("every State variant has a registered handler")
            .complete(&mut ctx, &mut out, cmd)
            .await;
    }
    let entries = out.into_entries();
    work.commit(None).await.expect("the batch commits");

    Dispatch { entries, store }
}

/// Drive `state`'s [`StateHandler::child_completed`](crate::handlers::state_handler::StateHandler)
/// once over `store` — the container half of the lifecycle, reached in production through
/// [`child_settled`](crate::handlers::child_completed::child_settled)'s `Running`-activity arm.
///
/// The activity value and the scope variables are read back out of `store` exactly as
/// `dispatch_child_completed` reads them, so a test seeds the world (see [`seed_container`]) and this
/// driver only supplies the state definition the production path resolves from the machine document.
/// That definition lookup (`machine_for_thread` → `resolve_state_for`) is the one seam not exercised
/// here: it maps a `state_path` into a machine, not into a state's decision.
pub async fn child_completed(
    state: &State,
    store: InMemoryStorage,
    activity: ObjectReference,
    child: ObjectReference,
) -> Dispatch {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(at()));
    let ids: Arc<dyn IdGenerator> = Arc::new(CountingIdGenerator::new());
    let work = WorkingState::new(store.begin_txn().expect("the in-memory store begins a txn"));
    let mut out = Collector::new(
        EntryId::new(1),
        Some(OverlaySink::new(&work)),
        clock.clone(),
        ids.clone(),
    );
    let mut env = EvalEnv::new();
    let mut definitions = HashMap::new();
    let state_handlers = build_state_handlers();
    {
        let mut ctx = HandlerContext {
            env: &mut env,
            storage: &work,
            clock,
            ids,
            definitions: &mut definitions,
            state_handlers: &state_handlers,
        };
        let activity_value = ctx
            .storage
            .get_activity(&activity)
            .await
            .expect("the in-memory store reads")
            .expect("the container activity row is seeded");
        let scope_ref = activity_value
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        // The owner is always a `Thread` — read it directly, as `dispatch_child_completed` does; the
        // driver needs only the scope variables it evaluates against.
        let thread = ctx
            .storage
            .get_thread(&scope_ref)
            .await
            .expect("the in-memory store reads")
            .expect("the owning thread is seeded");
        let variables = thread.variables.clone();
        state_handlers
            .create(state)
            .expect("every State variant has a registered handler")
            .child_completed(
                &mut ctx,
                &mut out,
                activity,
                &activity_value.value(),
                &variables,
                child,
            )
            .await;
    }
    let entries = out.into_entries();
    work.commit(None).await.expect("the batch commits");

    Dispatch { entries, store }
}
