use std::collections::HashSet;
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::Thread;
use crate::log::Timestamp;
use crate::types::meta::ObjectReference;
use crate::types::variables::Variables;

/// The storage projection row of a [`Thread`] — one scoped sub-run (`Parallel` branch / `Map` item).
///
/// Mirrors [`ExecutionRecord`](crate::storage::ExecutionRecord) exactly: the canonical `Thread`
/// domain entity reconstructed from the stream, wrapped so projection-only bookkeeping
/// (`variables`, `active_children`, `current_activity`, timing facts) stays off the event-carried
/// entity value. A thread is a self-contained sub-run, so it owns its own variable scope and
/// in-flight children, exactly like a top-level execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadRecord {
    /// The canonical thread domain value reconstructed from the event stream. Not `#[serde(flatten)]`
    /// for the same reason as `ExecutionRecord`: `Thread`'s domain timing lives in `meta`, which
    /// would collide with this row's entry-timestamp fields at the same JSON level.
    pub value: Thread,
    /// The thread's current variable scope (see `ExecutionRecord::variables`).
    pub variables: Variables,
    /// The activity currently in flight for this thread (projection convenience; single-active-state
    /// cursor, derivable from activity rows).
    pub current_activity: Option<ObjectReference>,
    /// Owned nodes still in flight (active activities / timers / child threads). A completing or
    /// terminating thread waits for this projection-only set to drain before its terminal `ed`.
    pub active_children: HashSet<ObjectReference>,
    /// Birth entry moment (`ThreadCreated`), projection-derived — never a local `now()`.
    pub created_at: Timestamp,
    /// Latest applied entry's timestamp that touched this row.
    pub updated_at: Timestamp,
}

impl ThreadRecord {
    pub fn value(&self) -> Thread {
        self.value.clone()
    }

    pub fn from_value(value: Thread, active_children: HashSet<ObjectReference>) -> Self {
        Self {
            value,
            variables: Variables::new(),
            current_activity: None,
            active_children,
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.value.is_terminal()
    }

    /// Stamp a fresh row's birth entry moment (`ThreadCreated`): `created_at == updated_at == at`.
    pub fn born(&mut self, at: Timestamp) {
        self.created_at = at;
        self.updated_at = at;
    }

    /// Record a row write at `at` (a mutation applier): advances `updated_at`, leaves `created_at`.
    pub fn with_update_at(&mut self, at: Timestamp) {
        self.updated_at = at;
    }
}

impl Deref for ThreadRecord {
    type Target = Thread;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for ThreadRecord {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
