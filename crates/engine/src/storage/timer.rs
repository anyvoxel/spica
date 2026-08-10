use serde::{Deserialize, Serialize};

use crate::command::TimerPurpose;
use crate::id::{NodeId, TimerId};
use crate::log::Timestamp;

/// Lifecycle status of a [`Timer`]. Kept separate from [`super::ExecutionStatus`]/[`super::ActivityStatus`]
/// because a timer has (in M1) a strictly simpler shape — it never initiates its own completion; it
/// is *armed* by a state or the execution and either fires (`Completed`) or is cancelled
/// (`Cancelled`).
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

/// A timer armed by an [`super::Execution`] (`ExecutionTimeout`) or an [`super::Activity`] (`WaitResume`).
/// A leaf — never owns children.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Timer {
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
