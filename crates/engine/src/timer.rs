use serde::{Deserialize, Serialize};

use crate::command::TimerPurpose;
use crate::id::{NodeId, TimerId};
use crate::log::Timestamp;

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
/// does for `ActivityValue`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimerValue {
    pub id: TimerId,
    /// The node that armed it (its owner). Drained by the owner's cascade.
    pub parent: NodeId,
    pub purpose: TimerPurpose,
    pub status: TimerStatus,
    /// Absolute wall-clock moment the timer fires. Persisting the absolute deadline (not a relative
    /// duration) keeps the timer row self-contained: a replay can derive "how long is left" from
    /// `deadline - now` without re-arming based on a stale relative count.
    pub deadline: Timestamp,
}
