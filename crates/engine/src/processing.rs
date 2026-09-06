//! The role-aware processing seam that separates the [`StreamProcessor`](crate::StreamProcessor) — the
//! role-agnostic log **driver** — from the per-role *processing* of a single entry (the Zeebe
//! `ProcessingStateMachine` / `LeaderStateMachine` split).
//!
//! The driver owns everything every role shares: tailing the log, routing each [`Entry`] by its
//! [`EntryPayload`], appending produced entries back to the log, and emitting the
//! [`log_event`](crate::stream_processor::log_event) / [`log_reject`](crate::stream_processor::log_reject)
//! execution trace. What *differs* by role — dispatch a [`Command`] into Events, fold an [`Event`]
//! into the projection, advance the resume watermark, deliver acknowledgements — lives behind
//! [`ProcessingStateMachine`], so the same driver can run as either a **leader** (full processing) or a
//! future **follower** (replicate the log without folding the projection). That role-tolerance is the
//! seam a distributed deployment needs for failover.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::hook::Hook;
use crate::log::{Entry, Timestamp};
use crate::storage::{Storage, StorageTxn};
use crate::types::command::Command;
use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::id::EntryId;
use crate::working::WorkingState;

/// The processing role installed in the driver's [`Box<dyn ProcessingStateMachine>`]. Modelling it as
/// an enum (rather than a free-form string) lets the driver and callers branch on role — e.g. only
/// the [`Leader`](crate::leader::Leader) may own the dispatch seam — and keeps the value set closed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Role {
    /// Full processing: dispatch Commands, fold Events eagerly at production, drive the schedule.
    Leader,
    /// Replicate-only: no dispatch, atomic apply per batch at its Noop, serve reads from the
    /// projection.
    ///
    /// `#[allow(dead_code)]`: constructed only by the (not-yet-wired) [`Follower`](crate::follower::Follower)
    /// role, which is itself `#[allow(dead_code)]` — see there for the multi-node TODO.
    #[allow(dead_code)]
    Follower,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Leader => f.write_str("leader"),
            Role::Follower => f.write_str("follower"),
        }
    }
}

/// The injected runtime handles a [`ProcessingStateMachine`] reaches through: the projection
/// [`Storage`] and the observation [`Hook`] the driver reports facts to (whose concrete
/// implementation is the request AckHook, owned by the consumer). Bundled so the three process
/// entry-points share one signature, even though a given role may touch only a subset (a follower
/// folds no projection and reports no facts, for example).
pub(crate) struct ProcessingHandles {
    pub(crate) storage: Arc<Mutex<Box<dyn Storage>>>,
    pub(crate) hook: Arc<dyn Hook>,
}

/// What a role reports after processing one **command** entry.
///
/// `entries` are what the driver appends to the log (always empty for a follower, which produces
/// nothing). Every response — including a `ClaimTasks` grant — rides a durable entry, so nothing is
/// reported outside the appended batch.
pub(crate) struct CommandProcessed {
    pub(crate) entries: Vec<Entry>,
    /// The working projection overlay of this batch, already eager-folded during dispatch. The driver
    /// commits it (with the batch-end watermark) once its append is durable, and drops it — rolling
    /// back — on an append failure, so the txn is the authoritative, crash-consistent fold of the batch.
    pub(crate) work: WorkingState,
}

/// The per-role processing contract for a single log entry, mirroring Zeebe's `ProcessingStateMachine`
/// (whose leader/follower variants differ only in *how* they treat each entry).
///
/// The driver owns **all** transaction lifecycle: it opens a [`StorageTxn`] (per produced batch for
/// the leader, per replicated batch for the follower), hands it to the role to fold into, and commits
/// the returned watermark — the role never calls `begin_txn`/`commit` itself. This matches the
/// [`Storage`](crate::storage::Storage) contract that only the Box owner (the `StreamProcessor`) may
/// commit. Because `begin_txn` returns an **owned** transaction (no longer borrowing the store's DB
/// handle), the follower's driver can open one txn at a batch's first Event and hold it across the
/// remaining siblings, folding each via [`apply_event`](Self::apply_event) and committing the whole
/// batch atomically at its [`EntryPayload::Noop`](crate::log::EntryPayload::Noop) via
/// [`commit_at_noop`](Self::commit_at_noop).
///
/// Only [`Leader`](crate::leader::Leader) is wired in the single-node milestone; a `Follower` is the
/// `TODO(multi-node)` seam left open by the `Box<dyn ProcessingStateMachine>` held in
/// [`StreamProcessor`](crate::StreamProcessor).
#[async_trait]
pub(crate) trait ProcessingStateMachine: Send + std::any::Any {
    /// The installed role's [`Role`], used by the driver to pick its loop (each role is a distinct
    /// `ProcessingStateMachine` implementation with a distinct control flow).
    fn role(&self) -> Role;

    /// Install the resume position read from [`Storage`] at boot — the last position whose effects are
    /// durable, so the driver resumes the log tail from `position + 1`. The leader advances it at fold
    /// time (the highest fully-applied Command); a follower advances it from each Noop instead, which
    /// is why advancement rather than the *notion* of a resume position is role-specific.
    fn set_resume_position(&mut self, position: i64);

    /// Process (but do not append) one [`Command`] entry. The **leader** dispatches it into produced
    /// `entries`; the driver appends `entries` (terminated by a `Noop`), folds them, and reports
    /// their durable facts. A follower's implementation returns nothing and produces no entries.
    async fn process_command(
        &mut self,
        entry_id: EntryId,
        command: &Command,
        handles: &ProcessingHandles,
    ) -> Result<CommandProcessed, ExecutionError>;

    /// Fold one read-back [`Event`] into the **driver-owned** `txn`. Both roles fold into the same
    /// transaction the driver already opened; they differ in what the driver does with the returned
    /// watermark:
    ///
    /// **Leader** (recovery): the Event is a crash residue — a durably-appended batch that crashed
    /// before its eager fold — so it folds idempotently here and returns the advanced watermark for
    /// the driver to commit immediately. The driver gates on [`is_already_applied`](Self::is_already_applied)
    /// first and only opens a transaction for the residual case.
    ///
    /// **Follower**: folds the Event into the transaction the driver opened at this batch's first
    /// Event and holds across the batch (the driver does *not* commit per Event); returns `None`,
    /// since the whole batch is only committed at its Noop via [`commit_at_noop`](Self::commit_at_noop).
    async fn apply_event(
        &mut self,
        txn: &mut dyn StorageTxn,
        entry_id: EntryId,
        timestamp: Timestamp,
        cause_id: Option<EntryId>,
        event: &Event,
        handles: &ProcessingHandles,
    ) -> Result<Option<i64>, ExecutionError>;

    /// Whether a read-back Event at `entry_id` was already applied at production and can be **skipped**
    /// without opening a transaction. The **leader** answers `entry_id <= watermark` (everything eager
    /// applied); a follower returns `false` — it must fold every replicated Event into its batch.
    fn is_already_applied(&self, entry_id: EntryId) -> bool;

    /// Round off a batch at its [`EntryPayload::Noop`](crate::log::EntryPayload::Noop) — **follower
    /// only**. The batch's Events were already folded incrementally into the driver-owned `txn` by
    /// `apply_event`; this advances the resume watermark to the Noop's position and returns it for
    /// the driver to commit, so the whole batch lands atomically. The **leader** returns `None` (its
    /// batches were already eager-applied at production and its Noops are skipped).
    async fn commit_at_noop(
        &mut self,
        txn: &mut dyn StorageTxn,
        entry_id: EntryId,
        handles: &ProcessingHandles,
    ) -> Result<Option<i64>, ExecutionError>;

    /// Post-commit side effects the driver invokes after it commits a fold's watermark. The **leader**
    /// drains the deferred acknowledgements whose Event was just applied (the Zeebe post-commit
    /// model); a follower has no awaiting callers and does nothing.
    async fn after_commit(&mut self, handles: &ProcessingHandles);
}
