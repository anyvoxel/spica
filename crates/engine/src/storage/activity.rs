use std::collections::HashSet;
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::log::Timestamp;
use crate::types::activity::Activity;
use crate::types::meta::ObjectReference;

/// The storage projection row of an ActivityRecord.
///
/// `Activity` is the single source of truth for the ActivityRecord's shared domain state; storage
/// wraps it and adds only projection-only bookkeeping such as `active_children` and the
/// `created_at`/`updated_at` timing facts. The value is deliberately **not** `#[serde(flatten)]`:
/// `Activity` now carries its own `created_at`/`updated_at` (stamped at event construction), which
/// would collide at the same JSON level with this row's entry-timestamp `created_at`/`updated_at`
/// below. Nesting under `value` keeps the two timestamp concepts in separate namespaces.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivityRecord {
    /// The canonical ActivityRecord domain value reconstructed from the event stream.
    pub value: Activity,
    /// Owned nodes still in flight (e.g. this Wait's resume timer). Terminating waits on them.
    pub active_children: HashSet<ObjectReference>,
    /// When this row's birth event (`StateActivating`) landed in the log. Projection-derived from the
    /// applied entry's [`timestamp`](crate::ApplierContext) — never a local `Timestamp::now()` — so
    /// every replica replaying the same entries computes the identical value. A reconstruction
    /// applier carries it over from the loaded row rather than re-stamping it (this is an update, not
    /// a birth).
    pub created_at: Timestamp,
    /// The latest applied entry's timestamp that touched this row; each mutating applier bumps it on
    /// write. Same determinism note as `created_at`.
    pub updated_at: Timestamp,
}

impl ActivityRecord {
    /// Convert the storage projection row into the event-carried entity value, intentionally dropping
    /// projection-only bookkeeping such as `active_children`.
    pub fn value(&self) -> Activity {
        self.value.clone()
    }

    /// Rebuild a storage projection row from the event-carried entity value and the independently
    /// maintained `active_children` set. Stamps zero timestamps; the caller (a creation or
    /// reconstruction applier) sets the real ones explicitly.
    pub fn from_value(value: Activity, active_children: HashSet<ObjectReference>) -> Self {
        Self {
            value,
            active_children,
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
    pub fn with_update_at(&mut self, at: Timestamp) {
        self.updated_at = at;
    }
}

impl Deref for ActivityRecord {
    type Target = Activity;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for ActivityRecord {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
