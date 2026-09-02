use std::collections::HashSet;
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::Execution;
use crate::log::Timestamp;
use crate::types::id::ActivityId;
use crate::types::meta::ObjectReference;
use crate::types::variables::Variables;

/// The storage projection row of an execution.
///
/// `Execution` is the canonical execution domain entity reconstructed from the stream. Storage
/// wraps it so projection-only bookkeeping — currently `variables`, `active_children`,
/// `current_activity`, and the `created_at`/`updated_at` timing facts — stays separated from the
/// entity value that lifecycle events carry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionRecord {
    /// The canonical execution domain value reconstructed from the event stream.
    ///
    /// Deliberately **not** `#[serde(flatten)]`: `Execution` now carries its domain timing facts
    /// inside an `ObjectMeta` (`meta.created_at`/`updated_at`, stamped at event construction), which
    /// would collide at the same JSON level with this row's entry-timestamp `created_at`/
    /// `updated_at` below. Nesting the value under `value` keeps the two timestamp concepts in
    /// separate namespaces. (`Activity`/`Timer`/`Task` now carry the same two fields, so their rows
    /// nest under `value` too.)
    pub value: Execution,
    /// The execution's current variable scope. `Assign` mutates this projection state through
    /// `VariablesAssigned`; it stays off the event-carried `Execution` so execution lifecycle
    /// events do not repeatedly serialize a mutable scope snapshot.
    pub variables: Variables,
    /// The activity currently in flight for this execution. This is a projection convenience used
    /// by handlers for single-active-state invariants; it is derivable from activity rows and does
    /// not belong in the event-carried execution entity.
    pub current_activity: Option<ActivityId>,
    /// Owned nodes still in flight (active activities / timers / child executions). Completing or
    /// terminating waits for this projection-only set to drain before the terminal `ed` is emitted.
    pub active_children: HashSet<ObjectReference>,
    /// When this row's birth event (the `ExecutionCreated`) landed in the log. Projection-derived
    /// from the applied entry's [`timestamp`](crate::ApplierContext) — never a local
    /// `Timestamp::now()` at apply time — so every replica replaying the same entries computes the
    /// identical value (the timestamp is a deterministic fold of the frozen log record).
    pub created_at: Timestamp,
    /// The latest applied entry's timestamp that touched this row; each mutating applier bumps it on
    /// write. Same determinism note as `created_at`.
    pub updated_at: Timestamp,
}

impl ExecutionRecord {
    pub fn value(&self) -> Execution {
        self.value.clone()
    }

    pub fn from_value(value: Execution, active_children: HashSet<ObjectReference>) -> Self {
        Self {
            value,
            variables: Variables::new(),
            current_activity: None,
            active_children,
            // Zero-stamped here; a creation applier stamps the real entry timestamp (see
            // `ApplierContext::timestamp`).
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.value.is_terminal()
    }

    /// Stamp a fresh row's birth entry moment (creation applier): `created_at == updated_at == at`.
    pub fn born(&mut self, at: Timestamp) {
        self.created_at = at;
        self.updated_at = at;
    }

    /// Record a row write at `at` (a mutation applier): advances `updated_at`, leaves `created_at`.
    pub fn touch(&mut self, at: Timestamp) {
        self.updated_at = at;
    }
}

impl Deref for ExecutionRecord {
    type Target = Execution;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for ExecutionRecord {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
