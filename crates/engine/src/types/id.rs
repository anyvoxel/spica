use derive_more::{From, Into};
use serde::{Deserialize, Serialize};

use crate::types::meta::PlainName;

// Entry positions (`EntryId`) and stream identity (`StreamId`) are intrinsic to the append-only
// log, so they live in the payload-agnostic `spica-logstream` crate and are re-exported here
// unchanged — every engine caller keeps the same identifiers, and the two are guaranteed to be the
// exact types the log produces/consumes.
pub use spica_logstream::{EntryId, StreamId};

/// Defines a ULID-backed identifier newtype (e.g. [`ExecutionId`], [`ActivityId`], [`TimerId`]) with
/// the conventional derives and constructors, collapsing the per-type boilerplate into one macro.
///
/// The `#[doc = $doc]` attribute carries the type's own documentation. `From`/`Into` (derive_more)
/// give the standard newtype conversions — `From<Ulid>` wraps a raw ULID, `Into<Ulid>` unwraps back
/// to it — and `new()`/`nil()`/`Default`/`Display` are the uniform id surface every such newtype
/// shares. Only the type name and doc differ, so both are macro parameters (the `i64`-backed
/// position ids `EntryId`/`StreamId` live in the `spica-logstream` crate instead).
macro_rules! ulid_id_type {
    ($name:ident, $doc:expr) => {
        #[doc = $doc]
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            Serialize,
            Deserialize,
            From,
            Into,
        )]
        pub struct $name(pub ulid::Ulid);

        impl $name {
            /// Mints a fresh, globally-unique id.
            pub fn new() -> Self {
                Self(ulid::Ulid::new())
            }

            /// The all-zero id: a `Discriminant` placeholder when registering handler/applier tables
            /// (only the variant discriminant matters, never the payload) or an "unknown / not yet
            /// materialized" sentinel. Never persisted as a real id.
            pub fn nil() -> Self {
                Self(ulid::Ulid::nil())
            }
        }

        impl Default for $name {
            /// The default id is the `nil()` sentinel (all-zero / "unknown"), not a freshly-minted
            /// one — matching how an id defaults in storage/registration contexts where it means
            /// "not yet materialized".
            fn default() -> Self {
                Self::nil()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }
    };
}

ulid_id_type!(
    ExecutionId,
    "Identifies one execution of a state machine within a stream. Uses a ULID rather than a \
     monotonic counter so an id can be minted anywhere; the durable `Command::CreateExecution` owns \
     it and the projected `Event::ExecutionCreated` / storage row inherit it. Serialization uses \
     ULID's canonical 26-char string form via the `serde` feature on the `ulid` crate."
);

ulid_id_type!(
    ActivityId,
    "Identifies the execution of a single state within the execution tree, entered deep inside \
     state handlers and the transition cascade. The durable ordering of activities (which state \
     follows which) is the log's causal `entry_id`, not a numeric activity counter, so a ULID \
     minted in place via `Collector::next_activity` is unambiguous."
);

ulid_id_type!(
    ThreadId,
    "Identifies one scoped sub-run of the shared machine — a `Thread` spawned for a `Parallel` \
     branch or `Map` item. Threads are ULIDs minted in place via `Collector::next_thread` (like \
     executions/activities); the durable causal (`entry_id`) ordering and the owning-edge topology \
     come from the log, not a numeric thread counter."
);

ulid_id_type!(
    TimerId,
    "Identifies a timer scheduled by the Engine (an execution `TimeoutSeconds` or a Wait's \
     `Seconds`). Timers are ULIDs minted in place via `Collector::next_timer`; the durable clock for \
     *which* deadline and causal (`entry_id`) ordering comes from the log itself, not a numeric timer \
     counter."
);

// NOTE: there is deliberately **no** `TaskId`. A Task's internal `uid` is a plain `ulid::Ulid`
// minted via `Collector::next_task` (see `ActivatedTask` / the worker bridge), and the worker
// addresses a task by its **canonical name** (`ObjectName`) — cards, claims and settlements carry
// the name, not a uid — so no separate external id type is needed (finding #13 follow-up).

/// A user-supplied, **immutable** name identifying a logical flow — the identity under which
/// [`Flow`](crate::Flow)s are addressed (creating the same name again appends a new
/// [`FlowVersion`](crate::FlowVersion) to the same flow). This is the human-readable alias external
/// actors use to address a flow; internal execution references never use it directly — they bind to
/// a version's `ObjectReference` instead, so a name that is later deleted + re-created (a fresh flow
/// incarnation) can never alias an in-flight execution onto the wrong definition.
///
/// # Validation
///
/// A name is accepted iff:
/// - it is **non-empty** and at most **64** characters;
/// - the first character is an ASCII letter or digit, and every subsequent character is an ASCII
///   letter, digit, or `_`;
/// - it is **case-sensitive** (`Checkout` ≠ `checkout`), and `-`/whitespace are **not** allowed.
///
/// Validation runs in the constructor so an invalid name fails fast at the API boundary
/// ([`Engine::create_flow`](crate::Engine::create_flow)) and never enters a Command, log, or
/// storage key. `Deserialize` is derived verbatim (no re-validation) for replay convenience; the
/// boundary guarantee is what prevents invalid names from being persisted in the first place.
/// `FlowName` is a **type alias of [`PlainName`]**: a flow's name obeys exactly the same user-name
/// rules (4..=64, ASCII alnum/`_`, no `-`), so instead of a duplicate
/// newtype with its own validation we reuse [`PlainName`] wholesale — the alias keeps the readable,
/// semantic `FlowName` spelling at call sites while sharing one validation / serde / `as_str`.
pub type FlowName = PlainName;

ulid_id_type!(
    RequestId,
    "Identifies one client request awaiting its acknowledgement — Zeebe's `requestId`. An opaque, \
     never-reused id minted per operation (e.g. `Engine::create_flow` or `Engine::start_for_revision`), \
     carried on the initiating Command and on the outcome Event that resolves it, so the StreamProcessor can \
     route the acknowledgement back to exactly that caller. It is deliberately **not** a target entity \
     id (`ExecutionId`): a single flow or execution can be the target of many concurrent \
     requests, so acks are correlated per-*request*, never per-entity, to avoid aliasing them."
);

// A node in the execution tree is addressed directly by its [`ObjectReference`]: the reference
// carries the object's [`ObjectKind`] (Execution / Thread / Activity / Timer / Task), which is
// exactly the role the former `NodeId`/`NodeKind` enums re-encoded by hand. Consumers dispatch on
// `reference.kind` when they need to branch by node type; the reference itself is the uniform
// handle the storage layer, the cleanup sweep, and the cascade use to address any node. A non-node
// kind (Flow / FlowVersion) simply resolves to nothing at scope/child lookups — it is silent, never
// a panic.
//
// (The `*Id` newtypes above remain the *monotonic-identity* forms minted by the collector and
// stored as `uid`s; `ObjectReference` is the *referencing* form used on `ObjectMeta::owner`, in
// `active_children`, and in command/event payloads.)

/// Monotonic source for identifier kinds. One source is threaded through a single execution
/// (and its StreamProcessor). Each kind has its **own counter** so that, in particular, [`EntryId`]s are
/// contiguous within a stream (1, 2, 3, …) — matching BookKeeper's per-ledger entryId.
///
/// Recovery does not re-run handlers (their output is already in the LogStream), so identifier
/// generation only ever runs once per entry — non-determinism across replays is not a concern.
///
/// Note: [`ExecutionId`], [`TimerId`], and [`ActivityId`] are *not* counted here — they are ULIDs
/// minted in place ([`ExecutionId::new`], `TimerId::new`, `ActivityId::new`) where no shared counter
/// is needed. [`EntryId`] is **not** minted here either — the LogStream assigns
/// positions at `append` time from its own counter. There is no stream counter: a LogStream is a
/// single stream, so its identity ([`StreamId`]) is a property of the log itself, stamped (and for a
/// durable log persisted) by the log — never minted by the engine.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ulid_ids_convert_from_raw_ulid_and_roundtrip() {
        // Each ULID id type wraps a raw Ulid; `From` builds it and the reverse `From`/`Into`
        // unwraps it back (the `get()`-free way to access the underlying Ulid).
        let raw = ulid::Ulid::new();
        let exec: ExecutionId = raw.into();
        let activity: ActivityId = raw.into();
        let timer: TimerId = raw.into();
        let back_exec: ulid::Ulid = exec.into();
        let back_activity: ulid::Ulid = activity.into();
        let back_timer: ulid::Ulid = timer.into();
        assert_eq!(back_exec, raw);
        assert_eq!(back_activity, raw);
        assert_eq!(back_timer, raw);
    }

    #[test]
    fn flow_name_validates_and_roundtrips() {
        // Valid names.
        let n = FlowName::new("checkout_flow").unwrap();
        assert_eq!(n.as_str(), "checkout_flow");
        assert_eq!(n.to_string(), "checkout_flow");
        assert!(FlowName::new("abcd").is_ok()); // 4-char floor is inclusive
        assert!(FlowName::new("OrderFlow23").is_ok());
        assert!(FlowName::new(&"x".repeat(64)).is_ok());

        // Invalid names.
        assert!(FlowName::new("").is_err()); // empty
        assert!(FlowName::new("a").is_err()); // too short (< 4 chars)
        assert!(FlowName::new("abc").is_err()); // too short (< 4 chars)
        assert!(FlowName::new(&"x".repeat(65)).is_err()); // too long
        assert!(FlowName::new("_checkout").is_err()); // leading underscore
        assert!(FlowName::new("1checkout").is_ok()); // leading digit allowed
        assert!(FlowName::new("checkout-flow").is_err()); // hyphen banned
        assert!(FlowName::new("checkout flow").is_err()); // whitespace banned
        assert!(FlowName::new("checkout/flow").is_err()); // slash banned
        assert!(FlowName::new("中文").is_err()); // non-ASCII

        // Case-sensitive: distinct values.
        assert_ne!(
            FlowName::new("Checkout").unwrap(),
            FlowName::new("checkout").unwrap()
        );
    }
}
