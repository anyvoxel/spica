use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::log::Timestamp;
use crate::types::command::TimerPurpose;
use crate::types::meta::{ObjectKind, ObjectMeta, ObjectReference};

/// Lifecycle status of a Timer. Kept separate from `ExecutionStatus` / `ActivityStatus` because a
/// timer has a strictly simpler shape — it never initiates its own completion; it is armed by a
/// state or the execution and either fires (`Completed`) or is cancelled (`Cancelled`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimerStatus {
    Active,
    Completed,
    Cancelled,
}

impl TimerStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, TimerStatus::Active)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, TimerStatus::Completed | TimerStatus::Cancelled)
    }
}

/// The event-/domain-carried value of a Timer.
///
/// A timer is a leaf side-effect node armed by an `Execution` (`ExecutionTimeout`) or an
/// `Activity` (`WaitResume` / task retry / task timeout). The value carries only the timer's own
/// domain facts — storage may wrap it to keep the domain/projection boundary explicit, just as it
/// does for `Activity`.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Timer {
    /// Shared identity + timing metadata. `meta.uid` is the timer's stable identity; the domain
    /// `created_at`/`updated_at` (stamped at each lifecycle-transition emit) live inside `meta`.
    pub meta: ObjectMeta,
    /// The execution this timer belongs to — the scope (and, at `StartExecution`'s
    /// `ExecutionTimeout`, the name-prefix) of the timer.
    pub execution: ObjectReference,
    pub purpose: TimerPurpose,
    pub status: TimerStatus,
    /// Absolute wall-clock moment the timer fires. Persisting the absolute deadline (not a relative
    /// duration) keeps the timer row self-contained: a replay can derive "how long is left" from
    /// `deadline - now` without re-arming based on a stale relative count.
    pub deadline: Timestamp,
}

impl Timer {
    /// The timer's stable identity: `meta.uid` is the same ULID that previously stood alone as
    /// `id`, so a retry/cancel that re-emits the same timer keeps its identity.
    pub fn reference(&self) -> ObjectReference {
        ObjectReference::new(ObjectKind::Timer, self.meta.name.clone(), self.meta.uid)
    }
}
