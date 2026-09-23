//! The role-aware processing seam that separates the [`StreamProcessor`](crate::StreamProcessor) — the
//! role-agnostic log **driver** — from the per-role *processing* of a single entry (the Zeebe
//! `ProcessingStateMachine` / `LeaderStateMachine` split).
//!
//! The driver owns everything every role shares: tailing the log, routing each [`Entry`] by its
//! [`EntryPayload`], appending produced entries back to the log, and emitting the
//! [`log_event`](crate::stream_processor::log_event) / [`log_reject`](crate::stream_processor::log_reject)
//! execution trace. What *differs* by role — dispatch a [`Command`] into Events, fold an [`Event`]
//! into the projection, advance the resume watermark, deliver acknowledgements — lives behind the
//! installed [`StateMachine`], so the same driver runs as either a **leader** (full processing) or a
//! **follower** (replicate the log without folding the projection). That role-tolerance is the seam a
//! distributed deployment needs for failover.
//!
//! The two roles are **concrete structs** ([`Leader`](crate::leader::Leader) and
//! [`Follower`](crate::follower::Follower)), each exposing only the entry-processing methods its role
//! actually performs — a follower never dispatches, a leader never rounds a batch off at a Noop. The
//! driver matches on the [`StateMachine`] enum and calls the concrete methods directly, rather than
//! routing every call through one role-wide method set that left each role's half empty.

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::follower::Follower;
use crate::hook::Hook;
use crate::leader::Leader;
use crate::log::Entry;
use crate::storage::Storage;
use crate::working::WorkingState;

/// The installed processing role. Modelling it as a **closed enum** (rather than an open trait object)
/// lets the driver [`match`](StateMachine) exhaustively on the concrete role and reach only the
/// methods that role implements, keeping the two roles decoupled from each other's concerns.
pub(crate) enum StateMachine {
    /// Full processing: dispatch Commands, fold Events eagerly at production, drive the schedule.
    Leader(Leader),
    /// Replicate-only: no dispatch, atomic apply per batch at its Noop, serve reads from the
    /// projection. `#[allow(dead_code)]`: the (not-yet-wired) [`Follower`] is itself
    /// `#[allow(dead_code)]` — see there for the multi-node TODO.
    #[allow(dead_code)]
    Follower(Follower),
}

impl StateMachine {
    /// Human-readable role label for the driver's install log.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            StateMachine::Leader(_) => "leader",
            StateMachine::Follower(_) => "follower",
        }
    }
}

/// The injected runtime handles a role reaches through: the projection [`Storage`] and the
/// observation [`Hook`] the driver reports facts to (whose concrete implementation is the request
/// AckHook, owned by the consumer). Bundled so the process entry-points share one signature, even
/// though a given role may touch only a subset (a follower folds no projection and reports no facts,
/// for example).
pub(crate) struct ProcessingHandles {
    pub(crate) storage: Arc<Mutex<Box<dyn Storage>>>,
    pub(crate) hook: Arc<dyn Hook>,
}

/// What the **leader** reports after processing one **command** entry.
///
/// `entries` are what the driver appends to the log (always empty for a follower, which produces
/// nothing, so it never constructs one). Every response — including a `ClaimTasks` grant — rides a
/// durable entry, so nothing is reported outside the appended batch.
pub(crate) struct CommandProcessed {
    pub(crate) entries: Vec<Entry>,
    /// The working projection overlay of this batch, already eager-folded during dispatch. The driver
    /// commits it (with the batch-end watermark) once its append is durable, and drops it — rolling
    /// back — on an append failure, so the txn is the authoritative, crash-consistent fold of the batch.
    pub(crate) work: WorkingState,
}
