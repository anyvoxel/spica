//! The **follower** role of the engine's [`StateMachine`](crate::processing::StateMachine): a read
//! replica that replicates the log but never dispatches Commands. It folds the replicated Events into
//! its **own** projection (so it can serve reads), folding each atomic batch into a **driver-held
//! transaction** and committing it **as one all-or-nothing fold at the batch's terminating
//! [`EntryPayload::Noop`](crate::log::EntryPayload::Noop)** — the exact sibling-loss hazard the Noop
//! batch marker was built to close (§8 / §4.2 of `docs/durable-execution-recovery-design.md`).
//!
//! How it differs from the [`Leader`](crate::leader::Leader), which eagerly applies its own batches
//! at production time:
//!
//! - it never dispatches nor appends — every Command read back is the leader's decision to replay,
//!   reflected only through the Events that follow in the same batch (which it folds);
//! - `apply_event` folds each replicated Event into the **transaction the driver opened at this
//!   batch's first Event and holds across the batch**, but returns `None` — it does **not** commit
//!   per Event. A crash mid-batch therefore leaves the partial batch un-applied; on restart the
//!   follower resumes from its watermark and re-folds the same entries into a fresh transaction, then
//!   commits once at the Noop (idempotent);
//! - `commit_at_noop` **rounds off** the batch: the Events were already folded incrementally, so it
//!   only advances the resume watermark to the Noop's position, letting the driver commit the whole
//!   batch atomically. Transactions are **driver-owned**: the driver opens one held transaction per
//!   batch, folds each Event in via `apply_event`, and commits the returned watermark.
//! - Rejections are audited by the driver's `log_reject` trace — a follower has no awaiting caller.
//!
//! This is a **single-node stub**: nothing installs a `Follower` yet, and the open multi-node design
//! questions (how a follower learns which Commands are undecided, take-over, fencing) are unresolved.
//! `stream_processor::run` reads the resume watermark from `Storage` and selects the role at the
//! [`StateMachine`](crate::processing::StateMachine) variant, so a `Follower` swapped in for the
//! `Leader` would "just work" structurally — but that wiring is explicitly out of scope here. See
//! `TODO(multi-node)` markers.
use tracing::debug;

use crate::applier::{ApplierContext, dispatch_event};
use crate::log::Timestamp;
use crate::processing::ProcessingHandles;
use crate::storage::StorageTxn;
use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::id::EntryId;

/// The follower processing state machine: replicate the log, folding each batch's Events **into one
/// driver-owned transaction** held open across the batch, and commit that transaction atomically at
/// the batch's Noop. Owns none of the leader's machinery — no dispatch table, no machine cache, no
/// eval environment, no pending acks.
///
/// `#[allow(dead_code)]`: the single-node milestone installs only [`Leader`](crate::leader::Leader);
/// nothing constructs a `Follower` yet, and `Follower::new` is exercised only by the cfg(test) unit
/// below. `TODO(multi-node)`: wire the role selection (and take-over) that gives this type a caller.
#[allow(dead_code)]
pub(crate) struct Follower {
    /// Resume watermark — the last Noop (batch boundary) whose whole batch is durably applied.
    /// Installed from [`Storage`] at boot (`set_resume_position`) and advanced at each
    /// `commit_at_noop` commit.
    watermark: i64,
}

/// See the struct doc for the `#[allow(dead_code)]` rationale: this role has no caller yet.
#[allow(dead_code)]
impl Follower {
    /// Build an empty follower. The resume watermark is installed later, when the driver reads it
    /// from [`Storage`] at boot (`set_resume_position`).
    pub(crate) fn new() -> Self {
        Self { watermark: 0 }
    }
}

impl Default for Follower {
    fn default() -> Self {
        Self::new()
    }
}

impl Follower {
    /// Install the resume position read from [`Storage`] at boot — the last position whose effects
    /// are durable, so the driver resumes the log tail from `position + 1`. The follower advances it
    /// from each Noop's `commit_at_noop` (a leader advances it at fold time instead).
    pub(crate) fn set_resume_position(&mut self, position: i64) {
        self.watermark = position;
    }

    pub(crate) async fn apply_event(
        &mut self,
        txn: &mut dyn StorageTxn,
        entry_id: EntryId,
        timestamp: Timestamp,
        _cause_id: Option<EntryId>,
        event: &Event,
        _handles: &ProcessingHandles,
    ) -> Result<Option<i64>, ExecutionError> {
        // Fold this Event into the **driver-held** transaction for the in-flight batch, but do **not**
        // commit — the batch isn't whole until its Noop, so returning `None` leaves the watermark
        // untouched and lets the driver commit the whole batch atomically at
        // `commit_at_noop`. Grouping siblings this way is what makes the batch's apply atomic — a
        // crash mid-batch leaves *none* of the transaction's writes committed (all are re-read and
        // re-folded on restart), rather than dropping the tail siblings.
        //
        // TODO(multi-node): a follower replays Events into its own projection and never owns the
        // schedule — it derives no timer side effects downstream (its consumer would need to observe
        // and co-ordinate with the leader's timer arms).
        debug!(entry_id = %entry_id, "follower folding event");
        let mut ctx = ApplierContext {
            storage: &mut *txn,
            timestamp,
        };
        dispatch_event(&mut ctx, event).await?;
        Ok(None)
    }

    pub(crate) async fn commit_at_noop(
        &mut self,
        _txn: &mut dyn StorageTxn,
        entry_id: EntryId,
        _handles: &ProcessingHandles,
    ) -> Result<Option<i64>, ExecutionError> {
        // The batch's Events were already folded into the driver-held transaction by `apply_event`;
        // all that remains is to **round it off** — advance the resume watermark to the Noop's
        // position so the driver commits the whole batch atomically. The watermark is written by the
        // driver's `commit` in the same atomic transaction as the fold, keeping it
        // never-ahead-of-projection, exactly like the leader.
        self.watermark = entry_id.get();
        debug!(watermark = self.watermark, "follower applied batch at noop");
        Ok(Some(self.watermark))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use spica_storage::InMemoryStorage;
    use tokio::sync::Mutex;

    use crate::Storage;
    use crate::engine::NoopHook;
    use crate::types::event::FlowVersionCreated;
    use crate::types::flow_version::FlowVersion;
    use crate::types::id::{FlowName, RequestId};
    use crate::types::meta::{ObjectKind, ObjectMeta, ObjectName, ObjectReference, OwnerReference};

    use super::*;

    fn handles(storage: Arc<Mutex<Box<dyn Storage>>>) -> ProcessingHandles {
        ProcessingHandles {
            storage,
            hook: Arc::new(NoopHook),
        }
    }

    /// A `FlowVersionCreated` event that folds cleanly into the in-memory store (its applier touches
    /// only `get_flow_by_name` / `put_flow_version` / `put_flow`), used as a batch's sibling. Each
    /// sibling carries a **distinct** version, so the two land on two distinct rows and the commit's
    /// scope can be read back row by row.
    fn flow_version_event(version: u32) -> (EntryId, Timestamp, Event) {
        (
            EntryId::new(2),
            Timestamp::now(),
            Event::FlowVersionCreated(FlowVersionCreated {
                request_id: RequestId::new(),
                flow_version: FlowVersion {
                    meta: ObjectMeta::builder(ObjectKind::FlowVersion, ulid::Ulid::new())
                        .name(FlowVersion::version_name(
                            &FlowName::new("flow").expect("literal name is valid"),
                            version,
                        ))
                        .at(Timestamp::now())
                        .build()
                        .with_owner(OwnerReference::new(
                            ObjectKind::Flow,
                            ObjectName::plain("flow").expect("literal name is valid"),
                            ulid::Ulid::nil(),
                        )),
                    version,
                    definition: String::new(),
                    checksum: FlowVersion::definition_checksum(""),
                },
            }),
        )
    }

    /// The reference the event's applier files its row under — the key a read addresses.
    fn version_ref(event: &Event) -> ObjectReference {
        let Event::FlowVersionCreated(created) = event else {
            panic!("the fixture emits a flow-version create; got {event:?}");
        };
        created.flow_version.reference()
    }

    /// Read a row through the store's **committed** face — the only face the Noop commit writes, so a
    /// `None` here is what "this entry has not been folded yet" means to a reader.
    async fn committed_version(
        storage: &Mutex<Box<dyn Storage>>,
        reference: &ObjectReference,
    ) -> Option<FlowVersion> {
        storage
            .lock()
            .await
            .get_flow_version(reference)
            .await
            .expect("the in-memory store reads")
    }

    async fn resume_position(storage: &Mutex<Box<dyn Storage>>) -> i64 {
        storage
            .lock()
            .await
            .last_processed_position()
            .await
            .expect("the in-memory store reads")
    }

    /// Two sibling Events of one batch followed by their Noop. The test drives a `Follower` the way
    /// the driver does — opening **one** transaction at the batch's first Event, holding it across
    /// both siblings (`apply_event` folds, never commits), and committing once at the Noop — and
    /// asserts nothing lands before the Noop, then the whole batch commits in one atomic fold with `W`
    /// advanced to the Noop's position.
    #[tokio::test]
    async fn follower_applies_batch_atomically_at_noop() {
        let storage: Arc<Mutex<Box<dyn Storage>>> =
            Arc::new(Mutex::new(Box::new(InMemoryStorage::new())));
        let h = handles(storage.clone());
        let mut follower = Follower::new();

        let (id1, ts1, ev1) = flow_version_event(1);
        let (id2, ts2, ev2) = flow_version_event(2);
        let (v1, v2) = (version_ref(&ev1), version_ref(&ev2));

        // As the driver would, open **one** owned transaction at the batch's first Event and hold it
        // across the batch. The guard is dropped at the end of this statement — `begin_txn` no longer
        // borrows the store, so the txn survives on its own (this is exactly the decoupling that lets
        // a follower fold a whole batch without serializing concurrent readers).
        let mut txn = storage.lock().await.begin_txn().unwrap();
        let w1 = follower
            .apply_event(&mut *txn, id1, ts1, Some(EntryId::new(1)), &ev1, &h)
            .await
            .unwrap();
        assert_eq!(w1, None, "follower never commits mid-batch");
        let w2 = follower
            .apply_event(&mut *txn, id2, ts2, Some(EntryId::new(1)), &ev2, &h)
            .await
            .unwrap();
        assert_eq!(w2, None, "follower never commits mid-batch");

        // Before the Noop nothing is committed: neither sibling's row is readable through the store's
        // committed face, and W is unchanged.
        assert!(
            committed_version(&storage, &v1).await.is_none(),
            "no fold before the Noop"
        );
        assert!(
            committed_version(&storage, &v2).await.is_none(),
            "no fold before the Noop"
        );
        assert_eq!(
            resume_position(&storage).await,
            0,
            "resume position unchanged before the Noop"
        );

        // The Noop closes the batch: `commit_at_noop` rounds it off (returns the watermark to commit),
        // and the driver commits the still-open, now-whole transaction atomically.
        let w = follower
            .commit_at_noop(&mut *txn, EntryId::new(3), &h)
            .await
            .unwrap();
        txn.commit(w).unwrap();
        assert_eq!(
            w,
            Some(3),
            "commit_at_noop returns the Noop position to commit"
        );
        assert_eq!(follower.watermark, 3, "W advanced to the Noop position");
        // The single commit landed the whole batch: both siblings' rows became readable together,
        // and nothing was written before the Noop (asserted above).
        assert!(
            committed_version(&storage, &v1).await.is_some(),
            "the first sibling folded in the single commit"
        );
        assert!(
            committed_version(&storage, &v2).await.is_some(),
            "the second sibling folded in the single commit"
        );
        assert_eq!(
            resume_position(&storage).await,
            3,
            "W persisted to the Noop position"
        );
    }
}
