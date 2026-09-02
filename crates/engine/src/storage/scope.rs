//! The **scope** abstraction — the one place that decides whether a state-owning node is a
//! top-level [`Execution`] or a fan-out [`Thread`].
//!
//! An `Activity` is owned by a succession of states-one-at-a-time, and those states live "inside"
//! exactly one scope: the top-level run (an `Execution`) or a scoped sub-run (a `Thread`). Every
//! state-lifecycle address previously read "the owning execution"; now that address can be either
//! kind, so the [`ScopeRecord`] enum + [`load_scope`] centralize that one branch. Consumers resolve
//! a bare [`ObjectReference`] through `load_scope` (dispatching on its [`ObjectKind`]) and read the
//! uniform accessors below — they never match on kind themselves, which is what keeps
//! role-classification out of the domain path.

use std::collections::HashSet;

use serde_json::Value;

use crate::storage::Storage;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::id::ActivityId;
use crate::types::meta::{ObjectKind, ObjectReference};
use crate::types::variables::Variables;
use crate::{ExecutionRecord, ThreadRecord};

/// A state-owning node of the execution tree, resolved from an [`ObjectReference`]: either the
/// top-level [`Execution`] or a scoped [`Thread`] (a `Parallel` branch / `Map` item).
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

    /// The JSON Pointer to this scope's sub-`states` table: `Some` for a `Thread` (its defining
    /// property — it always descends into the shared machine), `None` for a top-level `Execution`
    /// (it resolves against the machine's top-level `states`).
    pub fn state_path(&self) -> Option<&jsonptr::PointerBuf> {
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

    /// Owned nodes still in flight — drains before this scope's terminal `ed`.
    /// `#[allow(dead_code)]`: a uniform accessor kept on the scope surface; current consumers read
    /// the concrete record's `active_children` directly (e.g. `process_child_completed.rs`), so it is
    /// exercised only once a consumer unifies the Execution/Thread drain arms onto [`ScopeRecord`].
    #[allow(dead_code)]
    pub fn active_children(&self) -> &HashSet<ObjectReference> {
        match self {
            ScopeRecord::Execution(e) => &e.active_children,
            ScopeRecord::Thread(t) => &t.active_children,
        }
    }

    /// The single in-flight state cursor (projection-only).
    pub fn current_activity(&self) -> &Option<ActivityId> {
        match self {
            ScopeRecord::Execution(e) => &e.current_activity,
            ScopeRecord::Thread(t) => &t.current_activity,
        }
    }

    pub fn is_running(&self) -> bool {
        match self {
            ScopeRecord::Execution(e) => e.value.status.is_running(),
            ScopeRecord::Thread(t) => t.value.status.is_running(),
        }
    }

    /// See the note on [`ScopeRecord::active_children`] for why this uniform accessor is
    /// `#[allow(dead_code)]`: it is exercised once a drain consumer unifies onto [`ScopeRecord`].
    #[allow(dead_code)]
    pub fn is_terminal(&self) -> bool {
        match self {
            ScopeRecord::Execution(e) => e.value.is_terminal(),
            ScopeRecord::Thread(t) => t.value.is_terminal(),
        }
    }

    /// The scope's input (the raw input its first state entered with).
    pub fn input(&self) -> &Value {
        match self {
            ScopeRecord::Execution(e) => &e.value.input,
            ScopeRecord::Thread(t) => &t.value.input,
        }
    }

    /// The scope's settled output — `Some` once it reached a terminal state. A top-level `Execution`
    /// and a fan-out `Thread` both carry an `output: Option<Value>`, so the aggregator (a `Parallel`
    /// collecting branch results) reads it through one uniform accessor rather than matching kind.
    pub fn output(&self) -> Option<&Value> {
        match self {
            ScopeRecord::Execution(e) => e.value.output.as_ref(),
            ScopeRecord::Thread(t) => t.value.output.as_ref(),
        }
    }

    /// The scope's terminal termination reason, if it settled by `Terminating`/`Terminated` with one.
    /// Uniform over both kinds: a `Parallel` that scans its branches for failure reads the recorded
    /// `TerminationReason` through this instead of matching the two distinct status enums. Returns
    /// `None` for a `Completed` scope or one still running.
    pub fn termination_reason(&self) -> Option<&crate::types::command::TerminationReason> {
        match self {
            ScopeRecord::Execution(e) => e.value.status.termination_reason(),
            ScopeRecord::Thread(t) => t.value.status.termination_reason(),
        }
    }
}

/// Resolve a state-owning node (`Execution` or `Thread`) to its uniform projection record.
/// Returns `Ok(None)` when the node is gone (already terminal); the caller treats that as an
/// idempotent no-op. The reference's [`ObjectKind`] disambiguates structurally — this is the one
/// branch point. A non-scope kind (Flow / FlowVersion / Activity / Timer / Task) is **silently**
/// dropped (resolves to `None`): per the removal of the former `NodeId` role guard, a parent that is
/// not a scope is ignored rather than panicked.
pub async fn load_scope(
    storage: &dyn Storage,
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
/// [`ObjectMeta::owner`] / Activity's `execution`), dispatching on its structural kind.
pub async fn load_scope_ref(
    storage: &dyn Storage,
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
pub async fn resolve_scope_flow_version(
    storage: &dyn Storage,
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
