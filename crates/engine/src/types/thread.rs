use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::command::TerminationReason;
use crate::types::meta::{ObjectKind, ObjectMeta, ObjectReference};

/// Lifecycle status of a [`Thread`] — the scoped sub-state-machine run a `Parallel` branch or a
/// `Map` item executes.
///
/// Mirrors [`ExecutionStatus`](crate::types::execution::ExecutionStatus) exactly
/// (`Running` -> `Completing` -> `Completed` success, and `Running` -> `Terminating` -> `Terminated`
/// abnormal): a thread is a self-contained sub-run and drains through the winding-down phases while
/// its owned children settle, exactly like a top-level execution. Kept as a **separate type** so a
/// thread's status can never be confused for its owning execution's — a `Thread` is not an
/// `Execution`, and `$states`/drain logic must not treat the two interchangeably.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ThreadStatus {
    Running,
    /// Success finish initiated; waiting on owned children to drain.
    Completing,
    /// Abnormal finish initiated with its final reason already decided; waiting on owned children to
    /// terminate before the terminal ed lands.
    Terminating(TerminationReason),
    Completed,
    Terminated(TerminationReason),
}

impl ThreadStatus {
    pub fn is_running(&self) -> bool {
        matches!(self, ThreadStatus::Running)
    }
    pub fn is_completing(&self) -> bool {
        matches!(self, ThreadStatus::Completing)
    }
    pub fn is_terminating(&self) -> bool {
        matches!(self, ThreadStatus::Terminating(_))
    }
    pub fn is_terminal(&self) -> bool {
        matches!(self, ThreadStatus::Completed | ThreadStatus::Terminated(_))
    }
    /// The terminal termination reason, if this status settled by `Terminating`/`Terminated`.
    pub fn termination_reason(&self) -> Option<&TerminationReason> {
        match self {
            ThreadStatus::Terminating(r) | ThreadStatus::Terminated(r) => Some(r),
            ThreadStatus::Running | ThreadStatus::Completing | ThreadStatus::Completed => None,
        }
    }
}

/// The event-/domain-carried value of a **Thread** — one scoped sub-run of the shared state machine,
/// created for each `Parallel` branch or `Map` item by a container activity.
///
/// A thread is the container-neutral unit of fan-out: `Parallel` calls each one a branch, `Map` calls
/// each one an item, but they share this single entity shape. It is a **distinct type from
/// [`Execution`](crate::types::execution::Execution)**: an `Execution` is always a top-level run a
/// client started (plain user name, no `state_path`, no owner); a `Thread` is always an internal
/// fan-out sub-run (generated `{execution-name}-{suffix}` name, always-a-`state_path`, always owned by
/// its container activity). The role is declared by the type, so no consumer has to infer it from fields.
///
/// It carries only durable identity/lifecycle facts; runtime conveniences (variable scope, in-flight
/// children) live on the storage projection `crate::storage::ThreadRecord`, mirroring
/// `ExecutionRecord`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Thread {
    /// Object identity + shared metadata. **`meta.uid` IS the thread's never-reused identity ulid**
    /// (there is no separate bare id); `meta.name` is a generated `{execution-name}-{suffix}` name
    /// (see `SpawnThread`'s minting rule). Unlike a
    /// top-level `Execution` (which has no owner), `meta.owner` is **always** the container
    /// `Parallel`/`Map` activity that fanned this thread out — the drain cascade and the container's
    /// `active_children` rely on that owning edge.
    pub meta: ObjectMeta,
    /// The execution this thread belongs to — **always** the top-level [`Execution`]'s
    /// `ObjectReference`, regardless of nesting depth (a tree holds exactly one `Execution`, and it
    /// is always the root). This is the flat grouping key for "all events of one top-level run" (the
    /// CCES analogue of Zeebe's `processInstanceKey`), so a query can filter the whole tree by
    /// `execution == R` without recursing the parent chain. It is a query denormalization, **not** a
    /// role signal — the type already declares this is a `Thread`; `execution` never disambiguates
    /// anything. (Renamed from `root_execution`: with `Execution` now root-only by construction, the
    /// `root_` prefix was pure noise.)
    ///
    /// The thread's machine version is **not** duplicated here: every thread shares the owning tree's
    /// top-level run, so its `flow_version` is always that execution's — resolved via
    /// [`crate::storage::resolve_scope_flow_version`] from `execution`, never stored twice.
    pub execution: ObjectReference,
    /// A JSON Pointer (RFC 6901) into the single shared `StateMachine` document locating this
    /// thread's sub-`states` table, e.g. `/states/P1/branches/0/states/P2/item_processor/states`.
    /// **Always present** — a thread's defining property is that it runs a non-top-level portion of
    /// the shared machine, and `resolve_states_map` walks this pointer to resolve its states without
    /// copying any definition. (This is the field that used to be `Option` on `Execution`; for a
    /// `Thread` it is unconditional.)
    pub state_path: jsonptr::PointerBuf,
    /// This thread's **ordinal** within its container Activity — the `Parallel` branch index or the
    /// `Map` item index (0-based, in declaration order). Part of the thread's own identity: a thread
    /// *is* "the i-th branch/item of its container", so the index lives here on the entity, and the
    /// container's ordered fan-out map is projected from it (see the `ThreadCreated` applier) — the
    /// container never re-derives or duplicates the ordinal. Always present: a thread exists only as
    /// a fan-out child, so it never lacks an index.
    pub index: usize,
    pub status: ThreadStatus,
    /// The original input this thread received (a `Parallel` branch's projected `Arguments`, or a
    /// `Map` item's per-item value).
    pub input: Value,
    /// The thread's decided success output (its sub-run's terminal result, storeable once the thread
    /// completes) — the output the container aggregates for its own convergence.
    pub output: Option<Value>,
}

impl Thread {
    /// This thread's canonical [`ObjectReference`] — the `(kind, name, uid)` triple a consumer uses
    /// to address it (`kind = Thread`, `name = meta.name`, `uid = meta.uid`). Mirrors
    /// [`Execution::reference`](crate::types::execution::Execution::reference); Storage keys the row
    /// by this reference and reads it back by reference.
    pub fn reference(&self) -> ObjectReference {
        ObjectReference::new(ObjectKind::Thread, self.meta.name.clone(), self.meta.uid)
    }

    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }
}
