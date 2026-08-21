use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::log::Timestamp;
use crate::timer::TimerValue;

/// The storage projection row of a Timer.
///
/// `TimerValue` is the single source of truth for the timer's shared domain state; storage wraps it
/// so timer domain values and storage ownership stay separated the same way `Activity` wraps
/// `ActivityValue`. `#[serde(flatten)]` preserves the existing serialized shape. The row carries the
/// projection-only `created_at`/`updated_at` timing facts (see [`crate::storage::Execution::created_at`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Timer {
    /// The canonical timer domain value reconstructed from the event stream.
    #[serde(flatten)]
    pub value: TimerValue,
    /// When this row's birth event (`TimerActivated`) landed in the log (see
    /// [`crate::storage::Activity::created_at`] for the deterministic-source note).
    pub created_at: Timestamp,
    /// The latest applied entry's timestamp that touched this row; each mutating applier bumps it.
    pub updated_at: Timestamp,
}

impl Timer {
    pub fn value(&self) -> TimerValue {
        self.value.clone()
    }

    pub fn from_value(value: TimerValue) -> Self {
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

impl Deref for Timer {
    type Target = TimerValue;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for Timer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
