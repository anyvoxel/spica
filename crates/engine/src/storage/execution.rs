use std::collections::HashSet;
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::ExecutionValue;
use crate::id::{ActivityId, NodeId};
use crate::log::Timestamp;
use crate::variables::Variables;

/// The storage projection row of an execution.
///
/// `ExecutionValue` is the canonical execution domain entity reconstructed from the stream. Storage
/// wraps it so projection-only bookkeeping — currently `variables`, `active_children`,
/// `current_activity`, and the `created_at`/`updated_at` timing facts — stays separated from the
/// entity value that lifecycle events carry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Execution {
    /// The canonical execution domain value reconstructed from the event stream.
    #[serde(flatten)]
    pub value: ExecutionValue,
    /// The execution's current variable scope. `Assign` mutates this projection state through
    /// `VariablesAssigned`; it stays off the event-carried `ExecutionValue` so execution lifecycle
    /// events do not repeatedly serialize a mutable scope snapshot.
    pub variables: Variables,
    /// The activity currently in flight for this execution. This is a projection convenience used
    /// by handlers for single-active-state invariants; it is derivable from activity rows and does
    /// not belong in the event-carried execution entity.
    pub current_activity: Option<ActivityId>,
    /// Owned nodes still in flight (active activities / timers / child executions). Completing or
    /// terminating waits for this projection-only set to drain before the terminal `ed` is emitted.
    pub active_children: HashSet<NodeId>,
    /// When this row's birth event (the `ExecutionCreated`) landed in the log. Projection-derived
    /// from the applied entry's [`timestamp`](crate::ApplierContext) — never a local
    /// `Timestamp::now()` at apply time — so every replica replaying the same entries computes the
    /// identical value (the timestamp is a deterministic fold of the frozen log record).
    pub created_at: Timestamp,
    /// The latest applied entry's timestamp that touched this row; each mutating applier bumps it on
    /// write. Same determinism note as `created_at`.
    pub updated_at: Timestamp,
}

impl Execution {
    pub fn value(&self) -> ExecutionValue {
        self.value.clone()
    }

    pub fn from_value(value: ExecutionValue, active_children: HashSet<NodeId>) -> Self {
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

impl Deref for Execution {
    type Target = ExecutionValue;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for Execution {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
