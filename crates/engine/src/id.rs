use derive_more::{From, Into};
use serde::{Deserialize, Serialize};

/// Defines a ULID-backed identifier newtype (e.g. [`ExecutionId`], [`ActivityId`], [`TimerId`]) with
/// the conventional derives and constructors, collapsing the per-type boilerplate into one macro.
///
/// The `#[doc = $doc]` attribute carries the type's own documentation. `From`/`Into` (derive_more)
/// give the standard newtype conversions — `From<Ulid>` wraps a raw ULID, `Into<Ulid>` unwraps back
/// to it — and `new()`/`nil()`/`Default`/`Display` are the uniform id surface every such newtype
/// shares. Only the type name and doc differ, so both are macro parameters (mirroring
/// [`id_type!`] for the `i64`-backed position ids).
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

/// Defines a numeric identifier newtype with the standard derives.
macro_rules! id_type {
    ($name:ident, $doc:expr) => {
        #[doc = $doc]
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        pub struct $name(pub i64);

        impl $name {
            pub fn new(v: i64) -> Self {
                Self(v)
            }

            /// The "unset / not yet materialized" sentinel: `-1`, which lies **outside** the valid
            /// 1-based position space (`1, 2, 3, …`). Because it is not a representable position, any
            /// consumer can unambiguously tell an un-stamped id from a real one — unlike an
            /// all-zero sentinel, which for a `u64` position is itself a valid-looking value. Never
            /// persisted as a real position.
            pub fn nil() -> Self {
                Self(-1)
            }

            pub fn get(self) -> i64 {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }
    };
}

id_type!(
    StreamId,
    "Identifies a log stream; a stream may contain entries from multiple executions."
);

ulid_id_type!(
    ActivityId,
    "Identifies the execution of a single state within the execution tree, entered deep inside \
     state handlers and the transition cascade. The durable ordering of activities (which state \
     follows which) is the log's causal `entry_id`, not a numeric activity counter, so a ULID \
     minted in place via `Collector::next_activity` is unambiguous."
);

id_type!(
    EntryId,
    "Identifies a log entry — its monotonic position within a stream (BookKeeper entryId)."
);

ulid_id_type!(
    TimerId,
    "Identifies a timer scheduled by the Engine (an execution `TimeoutSeconds` or a Wait's \
     `Seconds`). Timers are ULIDs minted in place via `Collector::next_timer`; the durable clock for \
     *which* deadline and causal (`entry_id`) ordering comes from the log itself, not a numeric timer \
     counter."
);

/// A node in the execution tree (one row of the store). The tree shapes ownership: every entity
/// other than the root is owned by exactly one parent, and a parent drags down its `active_children`
/// when it terminates. `NodeId` is the uniform handle the storage layer, the cleanup sweep, and the
/// cascade use to address any node without switching on its concrete type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeId {
    /// The root-ish container for one run of the state machine.
    Execution(ExecutionId),
    /// One activation of a single state.
    Activity(ActivityId),
    /// A timer (execution `TimeoutSeconds` or a Wait's `Seconds`).
    Timer(TimerId),
    // M2: Task(TaskId)
}

impl NodeId {
    /// The concrete kind, dropping the embedded id — used by the cascade to route by type without
    /// moving a `NodeId` out.
    pub fn kind(&self) -> NodeKind {
        match *self {
            NodeId::Execution(id) => NodeKind::Execution(id),
            NodeId::Activity(id) => NodeKind::Activity(id),
            NodeId::Timer(id) => NodeKind::Timer(id),
        }
    }
}

/// The discriminant of a [`NodeId`], carrying the typed id. Used by the cascade to branch on the
/// node type without holding the whole `NodeId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Execution(ExecutionId),
    Activity(ActivityId),
    Timer(TimerId),
}

/// Monotonic source for identifier kinds. One source is threaded through a single execution
/// (and its Processor). Each kind has its **own counter** so that, in particular, [`EntryId`]s are
/// contiguous within a stream (1, 2, 3, …) — matching BookKeeper's per-ledger entryId.
///
/// Recovery does not re-run handlers (their output is already in the LogStream), so identifier
/// generation only ever runs once per entry — non-determinism across replays is not a concern.
///
/// Note: [`ExecutionId`], [`TimerId`], and [`ActivityId`] are *not* counted here — they are ULIDs
/// minted in place ([`ExecutionId::new`], `TimerId::new`, `ActivityId::new`) where no shared counter
/// is needed. [`EntryId`] is **not** minted here either — the LogStream assigns
/// positions at `append` time from its own counter, so only `stream` stays as a counter here
/// (`next_entry` is retained solely as a test-only convenience).
#[derive(Debug, Default)]
pub struct IdSource {
    stream: i64,
    entry: i64,
}

impl IdSource {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next_stream(&mut self) -> StreamId {
        self.stream += 1;
        StreamId(self.stream)
    }

    pub fn next_entry(&mut self) -> EntryId {
        self.entry += 1;
        EntryId(self.entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_ids_are_contiguous_per_kind() {
        let mut ids = IdSource::new();
        // Each kind has its own counter, starting at 1 and contiguous.
        assert_eq!(ids.next_entry(), EntryId(1));
        assert_eq!(ids.next_entry(), EntryId(2));
        assert_eq!(ids.next_entry(), EntryId(3));
        assert_eq!(ids.next_stream(), StreamId(1));
        assert_eq!(ids.next_entry(), EntryId(4));
    }

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
}
