//! The **scope** abstraction — the one place that decides whether a state-owning node is a
//! top-level [`Execution`] or a fan-out [`Thread`].
//!
//! An `Activity` is owned by a succession of states-one-at-a-time, and those states live "inside"
//! exactly one scope: the top-level run (an `Execution`) or a scoped sub-run (a `Thread`). Exactly
//! two consumers cannot know which without reading — resolving an activity's `meta.owner` to find
//! its machine and state definition, and reacting to a settling child of a parent taken from
//! `meta.owner` — and [`ScopeRecord`] + [`load_scope`] exist to serve them.
//!
//! A caller that *does* already know the kind it addressed reads the concrete record instead: a
//! kind-typed command handler (`CompleteThread`), and a container reading its own fan-out children
//! (always `Thread`s, folded from `ThreadCreated`). Routing those through here would let a
//! behavioural dependency on the other kind's shape creep in unnoticed.

use crate::storage::ReadonlyStorageTxn;
use crate::storage::{ExecutionRecord, ThreadRecord};
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::meta::{ObjectKind, ObjectReference};
use crate::types::state_path::StatePath;
use crate::types::variables::Variables;

/// A state-owning node of the execution tree, resolved from an [`ObjectReference`]: either the
/// top-level [`Execution`](crate::types::execution::Execution) or a scoped
/// [`Thread`](crate::types::thread::Thread) (a `Parallel` branch / `Map` item). Exposes only what the
/// kind-agnostic consumers need (see the module docs).
#[derive(Debug, Clone, PartialEq)]
pub enum ScopeRecord {
    Execution(ExecutionRecord),
    Thread(ThreadRecord),
}

impl ScopeRecord {
    /// The flat query anchor of the whole tree — for a `Thread`, its carried `execution` (the
    /// top-level run's id); for the top-level `Execution`, **itself** (a top-level run is its own
    /// root, so the anchor is just its own reference).
    pub fn root_execution(&self) -> ObjectReference {
        match self {
            ScopeRecord::Execution(e) => e.value.reference(),
            ScopeRecord::Thread(t) => t.value.execution.clone(),
        }
    }

    /// The JSON Pointer to this scope's sub-`States` table: `Some` for a `Thread` (its defining
    /// property — it always descends into the shared machine), `None` for a top-level `Execution`
    /// (it resolves against the machine's top-level `States`).
    pub fn state_path(&self) -> Option<&StatePath> {
        match self {
            ScopeRecord::Execution(_) => None,
            ScopeRecord::Thread(t) => Some(&t.value.state_path),
        }
    }

    /// The scope's current variable scope (projection bookkeeping).
    pub fn variables(&self) -> &Variables {
        match self {
            ScopeRecord::Execution(e) => &e.variables,
            ScopeRecord::Thread(t) => &t.variables,
        }
    }

    pub fn is_running(&self) -> bool {
        match self {
            ScopeRecord::Execution(e) => e.value.status.is_running(),
            ScopeRecord::Thread(t) => t.value.status.is_running(),
        }
    }
}

/// Resolve a state-owning node (`Execution` or `Thread`) to its uniform projection record.
/// Returns `Ok(None)` when the node is gone (already terminal); the caller treats that as an
/// idempotent no-op. The reference's [`ObjectKind`] disambiguates structurally — this is the one
/// branch point. A non-scope kind (Flow / FlowVersion / Activity / Timer / Task) is **silently**
/// dropped (resolves to `None`): per the removal of the former `NodeId` role guard, a parent that is
/// not a scope is ignored rather than panicked.
pub async fn load_scope<S: ReadonlyStorageTxn + ?Sized>(
    storage: &S,
    reference: &ObjectReference,
) -> Result<Option<ScopeRecord>, ExecutionError> {
    match reference.kind {
        ObjectKind::Execution => Ok(storage
            .get_execution(reference)
            .await?
            .map(ScopeRecord::Execution)),
        ObjectKind::Thread => Ok(storage
            .get_thread(reference)
            .await?
            .map(ScopeRecord::Thread)),
        // A Flow / FlowVersion / Activity / Timer / Task is not a state-owning scope.
        _ => Ok(None),
    }
}

/// Resolve a scope from a bare [`ObjectReference`] (the owner-style form used on
/// [`ObjectMeta::owner`](crate::types::meta::ObjectMeta::owner) / Activity's `execution`), dispatching
/// on its structural kind.
pub async fn load_scope_ref<S: ReadonlyStorageTxn + ?Sized>(
    storage: &S,
    reference: &ObjectReference,
) -> Result<Option<ScopeRecord>, ExecutionError> {
    load_scope(storage, reference).await
}

/// Resolve the **immutable flow version** a scope's states bind to (its machine definition).
/// For a top-level `Execution` this is its own carried `flow_version`; for a fan-out `Thread` it is
/// **derived** from the thread's root `execution` — the whole tree shares one definition, so a
/// thread never stores its own copy (see [`crate::types::thread::Thread`]). Returns an
/// `InvalidDefinition` error if the owning execution is gone, which can only mean the tree is being
/// torn down.
pub async fn resolve_scope_flow_version<S: ReadonlyStorageTxn + ?Sized>(
    storage: &S,
    scope: &ScopeRecord,
) -> Result<ObjectReference, ExecutionError> {
    match scope {
        ScopeRecord::Execution(e) => Ok(e.value.flow_version.clone()),
        ScopeRecord::Thread(t) => {
            let exec = storage
                .get_execution(&t.value.execution)
                .await?
                .ok_or_else(|| {
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                        "thread {} lost its owning execution {}",
                        t.value.reference(),
                        t.value.execution
                    )))
                })?;
            Ok(exec.value.flow_version.clone())
        }
    }
}
