use derive_more::{From, Into};
use serde::{Deserialize, Serialize};

use crate::error::ExecutionError;

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
    TimerId,
    "Identifies a timer scheduled by the Engine (an execution `TimeoutSeconds` or a Wait's \
     `Seconds`). Timers are ULIDs minted in place via `Collector::next_timer`; the durable clock for \
     *which* deadline and causal (`entry_id`) ordering comes from the log itself, not a numeric timer \
     counter."
);

ulid_id_type!(
    TaskId,
    "Identifies an in-flight external Task (a `Task` state's call to a connected `Resource`). Tasks \
     are ULIDs minted in place via `Collector::next_task`; the durable causal (`entry_id`) ordering \
     and the completion/failure/termination lifecycle come from the log, not a numeric task counter."
);

/// A user-supplied, **immutable** name identifying a logical flow — the identity under which
/// [`Flow`](crate::Flow)s are addressed (creating the same name again appends a new
/// [`FlowVersion`](crate::FlowVersion) to the same flow). This is the human-readable alias external
/// actors use to address a flow; internal execution references never use it directly — they bind to
/// a [`FlowVersionId`] instead, so a name that is later deleted + re-created (a fresh flow
/// incarnation, a new `flow_id`) can never alias an in-flight execution onto the wrong definition.
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FlowName(String);

impl FlowName {
    /// Validate `raw` against the name rules and wrap it. Returns [`ExecutionError::InvalidDefinition`]
    /// for a malformed name.
    pub fn new(raw: &str) -> Result<Self, ExecutionError> {
        let bytes = raw.as_bytes();
        let valid = !raw.is_empty()
            && raw.len() <= 64
            && bytes[0].is_ascii_alphanumeric()
            && bytes
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'_');
        if !valid {
            return Err(ExecutionError::InvalidDefinition(format!(
                "invalid FlowName {raw:?}: must be 1..=64 chars, start with A-Za-z0-9, and contain only A-Za-z0-9_"
            )));
        }
        Ok(Self(raw.to_string()))
    }

    /// The name as a `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for FlowName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

ulid_id_type!(
    FlowId,
    "Identifies one logical flow (one incarnation of a name) — an **audit / generation identity**, \
     never an addressing key. `Flow` rows are addressed by their immutable [`FlowName`]; `flow_id` is \
     a system-generated, **never-reused** id minted when a name first appears, so the same name \
     re-created after a delete gets a distinct `flow_id` (let audit tell two lives of a name apart). \
     Executions never bind to it — they bind to a [`FlowVersionId`] instead. The analogue of Zeebe's \
     separate notion of a process identity as opposed to a per-version `processDefinitionKey`."
);

ulid_id_type!(
    FlowVersionId,
    "Identifies one immutable published version of a flow (one [`FlowVersion`](crate::FlowVersion)). \
     System-generated and **never reused**; executions bind to a `FlowVersionId` (never the mutable \
     flow name, nor the audit-only [`FlowId`]), so an execution always resolves its machine against \
     exactly the definition it was created on — even after the flow is updated, or its name is \
     deleted and re-created. The analogue of Zeebe's per-version `processDefinitionKey`."
);

ulid_id_type!(
    RequestId,
    "Identifies one client request awaiting its acknowledgement — Zeebe's `requestId`. An opaque, \
     never-reused id minted per operation (e.g. `Engine::create_flow` or `Engine::start_for_revision`), \
     carried on the initiating Command and on the outcome Event that resolves it, so the StreamProcessor can \
     route the acknowledgement back to exactly that caller. It is deliberately **not** a target entity \
     id (`FlowId`/`ExecutionId`): a single flow or execution can be the target of many concurrent \
     requests, so acks are correlated per-*request*, never per-entity, to avoid aliasing them."
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
    /// An in-flight external Task (a `Task` state's call to a connected `Resource`).
    Task(TaskId),
}

impl NodeId {
    /// The concrete kind, dropping the embedded id — used by the cascade to route by type without
    /// moving a `NodeId` out.
    pub fn kind(&self) -> NodeKind {
        match *self {
            NodeId::Execution(id) => NodeKind::Execution(id),
            NodeId::Activity(id) => NodeKind::Activity(id),
            NodeId::Timer(id) => NodeKind::Timer(id),
            NodeId::Task(id) => NodeKind::Task(id),
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
    Task(TaskId),
}

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
        let task: TaskId = raw.into();
        let back_exec: ulid::Ulid = exec.into();
        let back_activity: ulid::Ulid = activity.into();
        let back_timer: ulid::Ulid = timer.into();
        let back_task: ulid::Ulid = task.into();
        assert_eq!(back_exec, raw);
        assert_eq!(back_activity, raw);
        assert_eq!(back_timer, raw);
        assert_eq!(back_task, raw);
    }

    #[test]
    fn flow_name_validates_and_roundtrips() {
        // Valid names.
        let n = FlowName::new("checkout_flow").unwrap();
        assert_eq!(n.as_str(), "checkout_flow");
        assert_eq!(n.to_string(), "checkout_flow");
        assert!(FlowName::new("a").is_ok());
        assert!(FlowName::new("OrderFlow23").is_ok());
        assert!(FlowName::new(&"x".repeat(64)).is_ok());

        // Invalid names.
        assert!(FlowName::new("").is_err()); // empty
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
