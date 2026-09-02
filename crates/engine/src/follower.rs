//! The **follower** role of [`ProcessingStateMachine`](crate::processing::ProcessingStateMachine): a
//! read replica that replicates the log but never dispatches Commands. It folds the replicated
//! Events into its **own** projection (so it can serve reads), folding each atomic batch into a
//! **driver-held transaction** and committing it **as one all-or-nothing fold at the batch's
//! terminating [`EntryPayload::Noop`](crate::log::EntryPayload::Noop)** — the exact sibling-loss
//! hazard the Noop batch marker was built to close (§8 / §4.2 of
//! `docs/durable-execution-recovery-design.md`).
//!
//! How it differs from the [`Leader`](crate::leader::Leader), which eagerly applies its own batches
//! at production time:
//!
//! - `process_command` produces **nothing** — a follower neither dispatches nor appends, so the
//!   driver's "empty batch" arm sends nothing to the log.
//! - `apply_event` folds each replicated Event into the **transaction the driver opened at this
//!   batch's first Event and holds across the batch**, but returns `None` — it does **not** commit
//!   per Event. A crash mid-batch therefore leaves the partial batch un-applied; on restart the
//!   follower resumes from its watermark and re-folds the same entries into a fresh transaction, then
//!   commits once at the Noop (idempotent).
//! - `commit_at_noop` **rounds off** the batch: the Events were already folded incrementally, so it
//!   only advances the resume watermark to the Noop's position, letting the driver commit the whole
//!   batch atomically — the same granularity as the leader's eager apply. Transactions are
//!   **driver-owned**: the driver opens one held transaction per batch (now that `begin_txn` no longer
//!   borrows the store), folds each Event in via `apply_event`, and commits the returned watermark.
//! - `apply_batch` is a no-op (a follower produces no batches to apply), as is `after_commit` (a
//!   follower has no awaiting callers); `is_already_applied` is always `false` (it folds every
//!   replicated Event).
//! - Rejections are audited by the driver's `log_reject` trace — a follower has no awaiting caller.
//!
//! This is a **single-node stub**: nothing installs a `Follower` yet, and the open multi-node design
//! questions (how a follower learns which Commands are undecided, take-over, fencing) are unresolved.
//! `stream_processor::run` reads the resume watermark from `Storage` and the driver is role-agnostic,
//! so a `Follower` swapped in for the `Leader` would "just work" structurally — but that wiring is
//! explicitly out of scope here. See [`ProcessingStateMachine`] and `TODO(multi-node)` markers.
use async_trait::async_trait;
use tracing::debug;

use crate::applier::{ApplierContext, EventDispatcher};
use crate::log::Timestamp;
use crate::processing::{CommandProcessed, ProcessingHandles, ProcessingStateMachine, Role};
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
    /// [`ProcessingStateMachine::commit_at_noop`] commit.
    watermark: i64,
    /// Table-driven event applier (fold an [`Event`] into [`Storage`]) — reused across the events of
    /// one in-flight batch, all folded into the same driver-owned transaction.
    dispatcher: EventDispatcher,
}

/// See the struct doc for the `#[allow(dead_code)]` rationale: this role has no caller yet.
#[allow(dead_code)]
impl Follower {
    /// Build an empty follower. The resume watermark is installed later, when the driver reads it
    /// from [`Storage`] at boot (`set_resume_position`).
    pub(crate) fn new() -> Self {
        Self {
            watermark: 0,
            dispatcher: EventDispatcher::new(),
        }
    }
}

impl Default for Follower {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ProcessingStateMachine for Follower {
    fn role(&self) -> Role {
        Role::Follower
    }

    fn set_resume_position(&mut self, position: i64) {
        self.watermark = position;
    }

    async fn process_command(
        &mut self,
        _entry_id: EntryId,
        _command: &crate::types::command::Command,
        _handles: &ProcessingHandles,
    ) -> Result<CommandProcessed, ExecutionError> {
        // A follower never dispatches and never produces entries: every Command read back (a
        // client's root command or a leader's follow-up) is someone else's decision to replay, not
        // ours to re-run. Returning an empty `CommandProcessed` makes the driver append nothing (no
        // Noop either), so the follower stays write-silent on the log. Its decision is reflected
        // only through the Events that follow in the same batch, which we buffer here.
        Ok(CommandProcessed {
            entries: Vec::new(),
            grants: Vec::new(),
        })
    }

    async fn apply_batch(
        &mut self,
        _txn: &mut dyn StorageTxn,
        _batch: &[crate::log::Entry],
        _handles: &ProcessingHandles,
    ) -> Result<Option<i64>, ExecutionError> {
        // A follower produces no batches, so there is never a just-appended batch of its own to apply
        // eagerly; its apply happens on read-back, at the batch's Noop (`commit_at_noop`).
        Ok(None)
    }

    async fn apply_event(
        &mut self,
        txn: &mut dyn StorageTxn,
        entry_id: EntryId,
        timestamp: Timestamp,
        _cause_id: Option<EntryId>,
        event: &Event,
        handles: &ProcessingHandles,
    ) -> Result<Option<i64>, ExecutionError> {
        // Fold this Event into the **driver-held** transaction for the in-flight batch, but do **not**
        // commit — the batch isn't whole until its Noop, so returning `None` leaves the watermark
        // untouched and lets the driver commit the whole batch atomically at
        // `commit_at_noop`. Grouping siblings this way is what makes the batch's apply atomic — a
        // crash mid-batch leaves *none* of the transaction's writes committed (all are re-read and
        // re-folded on restart), rather than dropping the tail siblings.
        //
        // TODO(multi-node): a follower should not arm timers it does not own (the leader dispatches
        // and owns the schedule); feeding `handles.scheduler` below folds timer-arming Events into
        // this replica's projection. Until leader/follower scheduling is coordinated, a deployed
        // follower must be built with a no-op scheduler.
        debug!(entry_id = %entry_id, "follower folding event");
        let mut ctx = ApplierContext {
            storage: &mut *txn,
            scheduler: handles.scheduler.as_ref(),
            cause_id: entry_id,
            timestamp,
        };
        self.dispatcher.apply(&mut ctx, event).await?;
        Ok(None)
    }

    fn is_already_applied(&self, _entry_id: EntryId) -> bool {
        // A follower never skips a replicated Event — each one must be folded into its in-flight
        // batch transaction. The driver therefore always applies Events for us.
        false
    }

    async fn commit_at_noop(
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

    async fn after_commit(&mut self, _handles: &ProcessingHandles) {
        // A follower has no awaiting callers and defers nothing, so there are no post-commit
        // side effects to fire here.
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex as StdMutex};

    use tokio::sync::Mutex;

    use crate::Storage;
    use crate::engine::AckRouter;
    use crate::storage::{
        ActivityRecord, ExecutionRecord, StorageTxn, TaskRecord, ThreadRecord, TimerRecord,
    };
    use crate::types::flow::Flow;
    use crate::types::flow_version::FlowVersion;
    use crate::types::id::{FlowName, RequestId};
    use crate::types::meta::ObjectReference;

    use super::*;

    /// A no-op scheduler: applying timer-arming Events must not actually arm anything in this unit
    /// test (and, per the `TODO(multi-node)` in `apply_event`/`commit_at_noop`, a real follower
    /// shouldn't arm timers either).
    #[derive(Default)]
    struct NullScheduler;
    #[async_trait]
    impl crate::scheduler::Scheduler for NullScheduler {
        fn attach_sink(&self, _sink: Arc<dyn crate::scheduler::TimerSink>) {}
        fn schedule(&self, _t: &ObjectReference, _d: Timestamp, _c: EntryId) {}
        fn cancel(&self, _t: &ObjectReference) {}
    }

    /// The durable state a [`FakeStore`] commit materializes: the resume watermark plus a count of
    /// projection writes folded into the batch. A write only lands when its transaction **commits**,
    /// so this is the observability point for "buffered until the Noop".
    #[derive(Default)]
    struct FakeState {
        committed_watermark: Option<i64>,
        committed_writes: usize,
    }

    /// A transaction-shaped write buffer whose writes are invisible until `commit` folds them into
    /// [`FakeState`] all together.
    struct FakeTxn {
        state: Arc<StdMutex<FakeState>>,
        pending_writes: usize,
    }

    #[async_trait]
    impl StorageTxn for FakeTxn {
        // The follower's fold touches only the flow rows the test event's applier needs; everything
        // else is unused by this test, so it panics rather than silently passing with wrong behavior.
        async fn get_flow_by_name(&mut self, _n: FlowName) -> Result<Option<Flow>, ExecutionError> {
            Ok(None)
        }
        async fn put_flow(&mut self, _f: Flow) -> Result<(), ExecutionError> {
            self.pending_writes += 1;
            Ok(())
        }
        async fn put_flow_version(&mut self, _v: FlowVersion) -> Result<(), ExecutionError> {
            self.pending_writes += 1;
            Ok(())
        }
        fn commit(self: Box<Self>, watermark: Option<i64>) -> Result<(), ExecutionError> {
            let mut s = self.state.lock().unwrap();
            s.committed_watermark = watermark;
            s.committed_writes += self.pending_writes;
            Ok(())
        }

        async fn get_execution(
            &mut self,
            _reference: &ObjectReference,
        ) -> Result<Option<ExecutionRecord>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn get_thread(
            &mut self,
            _reference: &ObjectReference,
        ) -> Result<Option<ThreadRecord>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn get_activity(
            &mut self,
            _reference: &ObjectReference,
        ) -> Result<Option<ActivityRecord>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn get_timer(
            &mut self,
            _reference: &ObjectReference,
        ) -> Result<Option<TimerRecord>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn get_task(
            &mut self,
            _reference: &ObjectReference,
        ) -> Result<Option<TaskRecord>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn get_children(
            &mut self,
            _id: ObjectReference,
        ) -> Result<HashSet<ObjectReference>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn get_flow_version(
            &mut self,
            _id: &ObjectReference,
        ) -> Result<Option<FlowVersion>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn flow_version_of(
            &mut self,
            _name: FlowName,
            _v: u32,
        ) -> Result<Option<FlowVersion>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn put_execution(&mut self, _e: ExecutionRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn put_thread(&mut self, _t: ThreadRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn put_activity(&mut self, _a: ActivityRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn put_timer(&mut self, _t: TimerRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn put_task(&mut self, _t: TaskRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn remove_child(
            &mut self,
            _p: ObjectReference,
            _c: ObjectReference,
        ) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn add_child(
            &mut self,
            _p: ObjectReference,
            _c: ObjectReference,
        ) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
    }

    /// A minimal in-crate [`Storage`] whose fold commits are observable. A real store can't be used
    /// here: `spica-storage` implements the *external* `spica_engine::Storage`, a distinct copy of
    /// this crate's trait, so the trait bounds wouldn't line up in an in-crate unit test.
    struct FakeStore(Arc<StdMutex<FakeState>>);

    #[async_trait]
    impl Storage for FakeStore {
        fn begin_txn(&self) -> Result<Box<dyn StorageTxn>, ExecutionError> {
            Ok(Box::new(FakeTxn {
                state: self.0.clone(),
                pending_writes: 0,
            }))
        }
        async fn last_processed_position(&self) -> Result<i64, ExecutionError> {
            Ok(self.0.lock().unwrap().committed_watermark.unwrap_or(0))
        }
        async fn put_last_processed_position(&mut self, p: i64) -> Result<(), ExecutionError> {
            self.0.lock().unwrap().committed_watermark = Some(p);
            Ok(())
        }

        async fn get_execution(
            &self,
            _reference: &ObjectReference,
        ) -> Result<Option<ExecutionRecord>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn get_thread(
            &self,
            _reference: &ObjectReference,
        ) -> Result<Option<ThreadRecord>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn get_activity(
            &self,
            _reference: &ObjectReference,
        ) -> Result<Option<ActivityRecord>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn get_timer(
            &self,
            _reference: &ObjectReference,
        ) -> Result<Option<TimerRecord>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn get_task(
            &self,
            _reference: &ObjectReference,
        ) -> Result<Option<TaskRecord>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn get_children(
            &self,
            _id: ObjectReference,
        ) -> Result<HashSet<ObjectReference>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn activatable_tasks(
            &self,
            _r: &str,
            _l: usize,
        ) -> Result<Vec<TaskRecord>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn put_execution(&mut self, _e: ExecutionRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn put_thread(&mut self, _t: ThreadRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn put_activity(&mut self, _a: ActivityRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn put_timer(&mut self, _t: TimerRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn put_task(&mut self, _t: TaskRecord) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn remove_child(
            &mut self,
            _p: ObjectReference,
            _c: ObjectReference,
        ) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn add_child(
            &mut self,
            _p: ObjectReference,
            _c: ObjectReference,
        ) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn get_flow_by_name(&self, _n: FlowName) -> Result<Option<Flow>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn put_flow(&mut self, _f: Flow) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn get_flow_version(
            &self,
            _id: &ObjectReference,
        ) -> Result<Option<FlowVersion>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn put_flow_version(&mut self, _v: FlowVersion) -> Result<(), ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
        async fn flow_version_of(
            &self,
            _name: FlowName,
            _v: u32,
        ) -> Result<Option<FlowVersion>, ExecutionError> {
            unimplemented!("not exercised by the follower batch test")
        }
    }

    fn handles(storage: Arc<Mutex<Box<dyn Storage>>>) -> ProcessingHandles {
        ProcessingHandles {
            storage,
            scheduler: Arc::new(NullScheduler),
            ack: Arc::new(Mutex::new(AckRouter::new())),
        }
    }

    /// A `FlowVersionCreated` event that folds cleanly into the fake store (its applier touches only
    /// `get_flow_by_name` / `put_flow_version` / `put_flow`), used as a batch's sibling.
    fn flow_version_event() -> (EntryId, Timestamp, Event) {
        (
            EntryId::new(2),
            Timestamp::now(),
            Event::FlowVersionCreated {
                request_id: RequestId::new(),
                flow_version: FlowVersion {
                    meta: crate::types::meta::ObjectMeta::born_named(
                        crate::types::meta::ObjectKind::FlowVersion,
                        FlowVersion::version_name(
                            &FlowName::new("flow").expect("literal name is valid"),
                            1,
                        ),
                        ulid::Ulid::new(),
                        Timestamp::now(),
                    )
                    .with_owner(crate::types::meta::OwnerReference::new(
                        crate::types::meta::ObjectKind::Flow,
                        crate::types::meta::ObjectName::plain("flow")
                            .expect("literal name is valid"),
                        ulid::Ulid::nil(),
                    )),
                    version: 1,
                    definition: String::new(),
                },
            },
        )
    }

    /// Two sibling Events of one batch followed by their Noop. The test drives a `Follower` the way
    /// the driver does — opening **one** transaction at the batch's first Event, holding it across
    /// both siblings (`apply_event` folds, never commits), and committing once at the Noop — and
    /// asserts nothing lands before the Noop, then the whole batch commits in one atomic fold with `W`
    /// advanced to the Noop's position.
    #[tokio::test]
    async fn follower_applies_batch_atomically_at_noop() {
        let state = Arc::new(StdMutex::new(FakeState::default()));
        let storage: Arc<Mutex<Box<dyn Storage>>> =
            Arc::new(Mutex::new(Box::new(FakeStore(state.clone()))));
        let h = handles(storage.clone());
        let mut follower = Follower::new();

        let (id1, ts1, ev1) = flow_version_event();
        let (id2, ts2, ev2) = flow_version_event();

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

        // Before the Noop nothing is committed: the projection is untouched and W is unchanged.
        {
            let s = state.lock().unwrap();
            assert_eq!(s.committed_watermark, None, "no watermark before the Noop");
            assert_eq!(s.committed_writes, 0, "no fold before the Noop");
        }
        let probe = storage
            .lock()
            .await
            .last_processed_position()
            .await
            .unwrap();
        assert_eq!(probe, 0, "resume position unchanged before the Noop");

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
        {
            let s = state.lock().unwrap();
            assert_eq!(
                s.committed_watermark,
                Some(3),
                "W persisted to the Noop position"
            );
            // Each sibling's applier writes both a flow-version and a flow row, so at least the two
            // siblings' rows landed in the single commit — nothing was written before the Noop.
            assert!(
                s.committed_writes >= 2,
                "both siblings folded in the single commit"
            );
        }
    }
}
