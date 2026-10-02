//! Deterministic inputs the handler tests share: the single clock reading every stamp uses, and the
//! helpers that name a seeded object by hand.
//!
//! A unit test drives a handler directly, so nothing mints its inputs for it — every reference and
//! timestamp a seeded row carries is written down instead. Pinning both is what makes the expected
//! records literal: a stamp is [`at`], and a seeded object reads by *how* it was named rather than by
//! a random ulid's text. The seeds therefore stay the same across a suite that runs each handler in
//! its own test process.
//!
//! Sibling to `states::harness`, which owns the same idea for the state handlers' deeper seed graph.

use crate::StatePath;
use crate::Timestamp;
use crate::types::meta::{ObjectKindMarker, ObjectName, ObjectRef};

/// The instant every stamp reads: one `ManualClock` reading serves the seeded rows' meta and the
/// collector's envelopes alike, so `at()` pins them all at once.
pub(crate) fn at() -> Timestamp {
    Timestamp::from_millis(1_000)
}

/// The `n`-th identity a counting id generator mints (it starts at `1`). Seeded references use ids
/// far above that range, so a *freshly minted* object reads as `uid(1)`, `uid(2)`, … rather than
/// blending into the objects the test wrote down itself.
pub(crate) fn uid(n: u64) -> ulid::Ulid {
    ulid::Ulid::from(u128::from(n))
}

pub(crate) fn obj_name(name: &str) -> ObjectName {
    ObjectName::from_parsed(name).expect("a static literal is a valid object name")
}

/// A seeded address of the kind the caller names in the type — so a fixture reaches a typed slot
/// without a conversion, and cannot name a kind that slot does not admit.
pub(crate) fn object_ref<K: ObjectKindMarker>(name: &str, uid: u64) -> ObjectRef<K> {
    ObjectRef::new(obj_name(name), ulid::Ulid::from(u128::from(uid)))
}

pub(crate) fn pointer(segments: &str) -> jsonptr::PointerBuf {
    let mut buf = jsonptr::PointerBuf::new();
    for segment in segments.split('/').filter(|s| !s.is_empty()) {
        buf.push_back(segment);
    }
    buf
}

pub(crate) fn path(segments: &str) -> StatePath {
    StatePath::from(pointer(segments))
}
