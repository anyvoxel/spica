//! Fixtures for building **engine-types values by hand** — references, metadata, owners, state paths.
//!
//! These are what a test needs to *say* an expected value: a chain's records name their objects by a
//! deterministic uid rather than a random ulid, so an assertion can pin what a run produced. Nothing
//! here reaches the engine: every item is built out of `spica-engine-types` alone, which is what lets
//! both the engine's unit tests and its integration suites — a separate crate, blind to `#[cfg(test)]`
//! items — share one set instead of restating it.

use std::collections::HashMap;

use jsonptr::PointerBuf;
use serde_json::Value;
use spica_engine_types::{
    ActivityKind, FlowKind, FlowName, NoOwner, ObjectKind, ObjectKindMarker, ObjectMeta,
    ObjectMetaBuilder, ObjectName, ObjectRef, RawObjectRef, RequestId, StatePath, ThreadKind,
    ThreadOwner, TimerOwner, Timestamp, Variables,
};

/// The first identity the harness mints for a **request** — deliberately far from `1`, where the
/// engine's own identity counter begins. Both counters count, so a request id and a `uid` of the same
/// ordinal would otherwise render as the same text: a typed chain literal pasted into the wrong field
/// would then match, and a reader could not tell a correlation id from an incarnation id by eye.
/// `1 << 32` sits in the band whose ULID rendering is all decimal (`…04000000`, incrementing) — as
/// readable as the engine's `…00000001` and never mistakable for it.
pub const FIRST_REQUEST_ID: u64 = 1 << 32;

/// The instant a virtual client's clock starts at. Any fixed value works — nothing compares it to
/// the wall clock — but a round one keeps a failure's numbers readable, and starting far from the epoch
/// means a deadline computed from it can never be mistaken for an un-set (`0`) timestamp.
pub const VIRTUAL_EPOCH_MILLIS: u64 = 1_700_000_000_000;
/// The instant every stamp of a timer-free typed case carries — the virtual client's own clock
/// reading, which is what makes a timestamp assertable at all.
pub fn epoch() -> Timestamp {
    Timestamp::from_millis(VIRTUAL_EPOCH_MILLIS)
}

/// A stamp at `millis`, for the cases whose clock the test has advanced.
pub fn stamp(millis: u64) -> Timestamp {
    Timestamp::from_millis(millis)
}

/// The `n`-th identity the injected counting id generator mints (it starts at `1`), so a chain
/// says which object a reference names by *how* it was minted rather than by a random ulid's text.
pub fn uid(n: u64) -> ulid::Ulid {
    ulid::Ulid::from(u128::from(n))
}

/// The `n`-th request id the local client's `request_id` mints — the same counter a run's acks are
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

/// The metadata a timer-free run mints for the object `object_name`, identified by the `n`-th uid:
/// every object's own name plus the injected identity, stamped at the clock's single instant
/// ([`epoch`]). The kind is the caller's business — it comes from `K`, resolved at the record the
/// meta is written into, and so is the owner: a case with an owner finishes the builder with
/// [`ObjectMetaBuilder::with_owner`], a root object uses [`meta_root`]. A case whose clock has been
/// advanced builds its `ObjectMeta` through [`ObjectMeta::builder`] instead, since these stamps would
/// be wrong for it.
pub fn meta<K: ObjectKindMarker>(uid: ulid::Ulid, object_name: &str) -> ObjectMetaBuilder<K> {
    ObjectMeta::builder(uid).name(name(object_name)).at(epoch())
}

/// [`meta`] with both stamps spelled out — for a case whose clock has been advanced (see
/// `TypedCase::acts`), where an object minted before the move and updated by it carries two
/// different instants.
pub fn meta_span<K: ObjectKindMarker>(
    uid: ulid::Ulid,
    object_name: &str,
    created: Timestamp,
    updated: Timestamp,
) -> ObjectMetaBuilder<K> {
    ObjectMeta::builder(uid)
        .name(name(object_name))
        .timestamps(created, updated)
}

/// The metadata of a **root** object — a `Flow` or a top-level `Execution`, whose owner slot is
/// [`NoOwner`] by type. The bound is that fact spelled at the call: no generic helper can hand out an
/// ownerless meta for an object the type says is owned.
pub fn meta_root<K: ObjectKindMarker<OwnedBy = NoOwner>>(
    uid: ulid::Ulid,
    object_name: &str,
) -> ObjectMeta<K> {
    meta(uid, object_name).with_owner(NoOwner::new())
}

/// [`meta_root`] with both stamps spelled out — see [`meta_span`].
pub fn meta_span_root<K: ObjectKindMarker<OwnedBy = NoOwner>>(
    uid: ulid::Ulid,
    object_name: &str,
    created: Timestamp,
    updated: Timestamp,
) -> ObjectMeta<K> {
    meta_span(uid, object_name, created, updated).with_owner(NoOwner::new())
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
pub fn indexed_refs<K: ObjectKindMarker>(
    pairs: &[(usize, ObjectRef<K>)],
) -> HashMap<usize, ObjectRef<K>> {
    pairs.iter().map(|(i, r)| (*i, r.clone())).collect()
}

/// A reference to the object `object` of the kind the *slot* demands, whose identity is the `n`-th uid
/// minted. The kind is carried by the type, so a fixture cannot build a value the field could not
/// hold — a wrong-kind literal is a compile error, not a value the engine has to reject.
pub fn ref_to<K: ObjectKindMarker>(object: &str, n: u64) -> ObjectRef<K> {
    ObjectRef::new(name(object), uid(n))
}

/// A reference to the object `object` of kind `kind` as its *flat* address — the erased
/// `RawObjectRef` a storage lookup or a heterogeneous collection takes.
pub fn flat_ref_to(kind: ObjectKind, object: &str, n: u64) -> RawObjectRef {
    RawObjectRef::new(kind, name(object), uid(n))
}

/// The owner slot of a **task**: the activity named `object`/`n`, in the slot's own type. A fixture
/// cannot ask for a kind the slot does not admit — the type carries the kind, not a runtime check.
pub fn activity_owner(object: &str, n: u64) -> ObjectRef<ActivityKind> {
    ObjectRef::new(name(object), uid(n))
}

/// [`activity_owner`] for a fixture already holding the activity's *flat* reference (an
/// `RawObjectRef` is what a storage lookup takes, so a fixture may legitimately carry the untyped
/// address too). The slot's own checked conversion runs here, so a fixture naming the wrong kind
/// fails at its own construction rather than building a record the engine cannot represent.
pub fn activity_owner_of(reference: RawObjectRef) -> ObjectRef<ActivityKind> {
    reference
        .try_into()
        .expect("fixture: a task's owner is an activity")
}

/// The owner slot of an **activity**: the thread named `object`/`n`, in the slot's own type — a
/// top-level run's derived root thread, or a fan-out branch's thread (see `ActivityKind::OwnedBy`).
/// A fixture cannot ask for a kind the slot does not admit — the type carries the kind, not a runtime
/// check.
pub fn thread_owner(object: &str, n: u64) -> ObjectRef<ThreadKind> {
    ObjectRef::new(name(object), uid(n))
}

/// [`thread_owner`] for a fixture already holding the thread's *flat* reference — the form a storage
/// lookup takes, so a fixture seeding a thread row carries the untyped address too.
pub fn thread_owner_of(reference: RawObjectRef) -> ObjectRef<ThreadKind> {
    reference
        .try_into()
        .expect("fixture: an activity's owner is a thread")
}

/// The owner slot of a **flow version**: the flow named `object`/`n`, in the slot's own type (see
/// `FlowVersionKind::OwnedBy`). A fixture cannot ask for a kind the slot does not admit — the type
/// carries the kind, not a runtime check.
pub fn flow_owner(object: &str, n: u64) -> ObjectRef<FlowKind> {
    ObjectRef::new(name(object), uid(n))
}

/// The owner slot of a **thread** whose scope is the top-level run — a root thread, owned by the
/// `Execution` it stands in for (see `ThreadOwner`).
pub fn root_thread_owner(object: &str, n: u64) -> ThreadOwner {
    ThreadOwner::Execution(ObjectRef::new(name(object), uid(n)))
}

/// [`root_thread_owner`] for a fixture already holding the execution's *flat* reference (the form a
/// storage lookup takes, so a fixture may legitimately carry the untyped address too).
pub fn root_thread_owner_of(execution: RawObjectRef) -> ThreadOwner {
    ThreadOwner::Execution(
        execution
            .try_into()
            .expect("fixture: a root thread is owned by its execution"),
    )
}

/// The owner slot of a **thread** fanned out by a container: `object`/`n` is the owning `Parallel`/
/// `Map` activity (see `ThreadOwner`).
pub fn fanout_thread_owner(object: &str, n: u64) -> ThreadOwner {
    ThreadOwner::Activity(ObjectRef::new(name(object), uid(n)))
}

/// [`fanout_thread_owner`] for a fixture already holding the container activity's *flat* reference.
pub fn fanout_thread_owner_of(activity: RawObjectRef) -> ThreadOwner {
    ThreadOwner::Activity(
        activity
            .try_into()
            .expect("fixture: a fan-out thread is owned by its container activity"),
    )
}

/// The owner slot of a **timer** armed by the run itself (a run's `TimeoutSeconds`), in the union's
/// own type (see `TimerOwner`), for a fixture already holding the execution's *flat* reference.
pub fn execution_timer_owner_of(execution: RawObjectRef) -> TimerOwner {
    TimerOwner::Execution(
        execution
            .try_into()
            .expect("fixture: an execution-timeout timer is owned by its run"),
    )
}

/// The owner slot of a **timer** armed by the waiting activity (a `Wait`'s `Seconds` or a state's
/// `TimeoutSeconds`): `object`/`n` is the owning activity.
pub fn activity_timer_owner(object: &str, n: u64) -> TimerOwner {
    TimerOwner::Activity(ObjectRef::new(name(object), uid(n)))
}

/// [`activity_timer_owner`] for a fixture already holding the waiting activity's *flat* reference.
pub fn activity_timer_owner_of(activity: RawObjectRef) -> TimerOwner {
    TimerOwner::Activity(
        activity
            .try_into()
            .expect("fixture: a state timer is owned by its waiting activity"),
    )
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
