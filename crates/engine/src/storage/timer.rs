use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::log::Timestamp;
use crate::types::timer::Timer;

/// The storage projection row of a TimerRecord.
///
/// `Timer` is the single source of truth for the timer's shared domain state; storage wraps it
/// so timer domain values and storage ownership stay separated the same way `ActivityRecord` wraps
/// `Activity`. The value is deliberately **not** `#[serde(flatten)]`: `Timer` now carries its own
/// `created_at`/`updated_at` (stamped at event construction), which would collide at the same JSON
/// level with this row's entry-timestamp `created_at`/`updated_at` below. Nesting under `value`
/// keeps the two timestamp concepts in separate namespaces (see
/// [`crate::storage::ExecutionRecord::created_at`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimerRecord {
    /// The canonical timer domain value reconstructed from the event stream.
    pub value: Timer,
    /// When this row's birth event (`TimerActivated`) landed in the log (see
    /// [`crate::storage::ActivityRecord::created_at`] for the deterministic-source note).
    pub created_at: Timestamp,
    /// The latest applied entry's timestamp that touched this row; each mutating applier bumps it.
    pub updated_at: Timestamp,
}

impl TimerRecord {
    pub fn value(&self) -> Timer {
        self.value.clone()
    }

    pub fn from_value(value: Timer) -> Self {
        Self {
            value,
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        }
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

impl Deref for TimerRecord {
    type Target = Timer;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for TimerRecord {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
