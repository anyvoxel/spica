//! The **container side** of a child settling: what an owner does when one of its owned nodes reaches
//! a terminal state.
//!
//! A settling node names only its owner. What that settle *means* is the owner's business, so the
//! reaction lives here rather than in the settling node's own handler — a `Task`'s handler used to
//! know that its parent was an `Activity` and that the parent therefore needed a `CompleteState`. Now
//! the child hands its settle to its owner's [`Container`] and the owner decides.
//!
//! Only [`ActivityContainer`] exists so far, and only the `Task` lifecycle routes through it; the
//! remaining owners still settle via [`child_settled`](super::child_completed::child_settled).

use crate::ActivityStatus;
use crate::handler::{Collector, HandlerContext};
use crate::storage::{ActivityRecord, ReadonlyStorageTxn};
use crate::types::command::{Command, CompleteState};
use crate::types::meta::{ObjectKind, ObjectReference};

/// A node that owns children: the reaction point for one of them settling. The implementor is chosen
/// from the owner's *kind* at the call site (a task's owner is always an `Activity`, a root thread's
/// is the `Execution`, …), so the concrete type is known statically and this is never used as `dyn`.
pub(crate) trait Container {
    /// Resolve the container that owns `owner`'s children, `None` when the owner's row is missing.
    ///
    /// The caller runs this *before* producing the settle's effects, so an ownerless settle is caught
    /// while it can still be answered — a `Reject` for a worker-reported settle — rather than
    /// discovered as a silent no-op after the terminal event is already on the log.
    ///
    /// `None` answers a *missing row* only. Which impl runs is decided by the owner's kind at the call
    /// site, so an owner of another kind is a broken invariant rather than an ownerless settle, and an
    /// impl meets it by panicking instead of folding it into the `None` the caller reports.
    ///
    /// This read is the *existence* check only. The hooks re-read the row: the settle's own batch
    /// rewrites it (draining the child, folding the payload), so the row read here is stale by the
    /// time a hook decides on it.
    async fn open(storage: &dyn ReadonlyStorageTxn, owner: ObjectReference) -> Option<Self>
    where
        Self: Sized;

    /// An owned `child` reached a **success** terminal. Fired on *every* settled child, not only the
    /// last one: a container with open slots (a `Map`'s `MaxConcurrency`) decides per settle whether
    /// to replenish, converge or fail, so the hook cannot assume it is the final settle.
    async fn after_child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &ObjectReference,
    );

    /// An owned `child` reached an **abnormal** terminal (cancelled, terminated, timed out). The
    /// reason is deliberately not carried: the container that is finishing already holds its own
    /// `Terminating(reason)` on its row — the single source of truth for why *it* is going down.
    async fn after_child_terminated(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &ObjectReference,
    );
}

/// The container for an `Activity`'s children.
pub(crate) struct ActivityContainer {
    activity: ObjectReference,
}

impl ActivityContainer {
    /// The half both outcomes share: an activity that is already finishing and has just drained its
    /// last child advances its own finish. Returns `true` when it emitted the Continue command, so the
    /// caller knows the outcome-specific arm has nothing left to do.
    fn advance_if_drained(&self, out: &mut Collector<'_>, act: &ActivityRecord) -> bool {
        if !act.active_children.is_empty() {
            return false; // more children in flight — the last one to settle advances the finish.
        }
        match &act.value.status {
            ActivityStatus::Completing => {
                out.append_command(Command::ContinueComplete {
                    owner: self.activity.clone(),
                });
                true
            }
            ActivityStatus::Terminating(_) => {
                out.append_command(Command::ContinueTerminate {
                    owner: self.activity.clone(),
                });
                true
            }
            _ => false,
        }
    }
}

impl Container for ActivityContainer {
    async fn open(storage: &dyn ReadonlyStorageTxn, owner: ObjectReference) -> Option<Self> {
        // The call site picks this impl from the owner's kind, so an owner of another kind means the
        // tree itself is corrupt: a programming error with no answer to give, unlike a missing row,
        // which is a real state the caller reports.
        if owner.kind != ObjectKind::Activity {
            // TODO：这里是应该使用 unreachable 还是应该返回一个错误然后走 Reject，还没想清楚
            unreachable!(
                "a {:?} owner {owner} cannot own activity children",
                owner.kind
            );
        }
        // TODO：如果 owner 不存在，是不是也应该是一个 Reject？
        storage.get_activity(&owner).await.ok().flatten()?;
        Some(Self { activity: owner })
    }

    async fn after_child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &ObjectReference,
    ) {
        let Some(act) = ctx
            .storage
            .get_activity(&self.activity)
            .await
            .ok()
            .flatten()
        else {
            return; // gone already — nothing to complete or advance.
        };
        if self.advance_if_drained(out, &act) {
            return;
        }
        if !act.value.status.is_running() {
            return; // terminal, or finishing with children still attached: not this settle's move.
        }

        // TODO(Wait/Parallel/Map): this arm completes the activity unconditionally, which holds only
        // for the states whose completion trigger *is* the settled child — a `Task`'s in-flight task,
        // a `Wait`'s resume timer. A fan-out container settling one branch of many must instead
        // replenish an open slot or aggregate its result, so the decision has to be split before
        // those states can route their settles through here.
        out.append_command(Command::CompleteState(CompleteState {
            activity: self.activity.clone(),
            // The `Task`'s raw result is the worker's payload, folded onto the activity as
            // `raw_output` by the `TaskCompleted` applier within this same batch (emitted events fold
            // into the overlay eagerly at `append_event`). The base `complete` step's `$states.result`
            // reads that very field, so taking it from the row keeps one source of truth while the
            // command still carries the result itself, keeping the log self-describing.
            output: act
                .value
                .raw_output
                .clone()
                .unwrap_or(serde_json::Value::Null),
        }));
        tracing::debug!(
            activity = %self.activity,
            child = %child,
            "child completed; completing the owning activity"
        );
    }

    async fn after_child_terminated(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &ObjectReference,
    ) {
        let Some(act) = ctx
            .storage
            .get_activity(&self.activity)
            .await
            .ok()
            .flatten()
        else {
            return; // gone already — nothing to advance.
        };
        if self.advance_if_drained(out, &act) {
            return;
        }
        if act.value.status.is_running() {
            // An activity still Running while a child terminates is the anomalous case (a termination
            // is driven by the activity's own teardown sweep), and it asks for no reaction: whatever
            // finishes this activity still owns the next move.
            tracing::debug!(
                activity = %self.activity,
                child = %child,
                "child terminated under a Running activity; no reaction"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use serde_json::{Value, json};
    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_storage::InMemoryStorage;

    use super::{ActivityContainer, Container};
    use crate::StatePath;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, OverlaySink};
    use crate::handlers::dispatch::build_state_handlers;
    use crate::storage::{ActivityRecord, Storage};
    use crate::types::command::{Command, CompleteState, TerminationReason};
    use crate::types::id::EntryId;
    use crate::types::meta::{ObjectKind, ObjectMeta, ObjectName, ObjectReference};
    use crate::working::WorkingState;
    use crate::{Activity, ActivityStatus, EntryPayload, Timestamp};

    /// The instant every stamp reads: one `ManualClock` reading serves the seeded activity's meta and
    /// the collector's envelopes alike.
    fn at() -> Timestamp {
        Timestamp::from_millis(1_000)
    }

    fn reference(kind: ObjectKind, name: &str, uid: u64) -> ObjectReference {
        ObjectReference::new(
            kind,
            ObjectName::from_parsed(name).expect("a static literal is a valid object name"),
            ulid::Ulid::from(u128::from(uid)),
        )
    }

    fn activity_ref() -> ObjectReference {
        reference(ObjectKind::Activity, "execution-0", 90)
    }

    /// The child handed to the container — a settled `Task`, the only kind that routes here so far.
    fn task_ref() -> ObjectReference {
        reference(ObjectKind::Task, "execution-0", 91)
    }

    /// A seeded activity row with `children` live children still attached. `raw_output` stands in for
    /// what the `TaskCompleted` applier folds onto the row in the settle's own batch.
    fn seeded_activity(
        status: ActivityStatus,
        raw_output: Option<Value>,
        children: usize,
    ) -> ActivityRecord {
        let mut path = jsonptr::PointerBuf::new();
        path.push_back("States");
        path.push_back("P");
        let activity = Activity {
            meta: ObjectMeta::builder(activity_ref().uid)
                .name(activity_ref().name)
                .at(at())
                .build()
                .with_owner(reference(ObjectKind::Thread, "execution-1", 80)),
            execution: reference(ObjectKind::Execution, "execution", 70),
            state_path: StatePath::from(path),
            status,
            raw_input: json!({ "in": 1 }),
            input: Some(json!({ "in": 1 })),
            raw_output,
            activity_state: None,
            retry_state: None,
            output: None,
        };
        let live = (0..children)
            .map(|n| reference(ObjectKind::Timer, "deadline", 200 + n as u64))
            .collect::<HashSet<_>>();
        let mut row = ActivityRecord::from_value(activity, live);
        row.born(at());
        row
    }

    /// Drive one hook over a working overlay seeded with a single activity row — the leader's shape, so
    /// the assertion reads the command the container emitted, not merely an intent.
    async fn settle(activity: ActivityRecord, terminated: bool) -> Vec<EntryPayload> {
        let mut store = InMemoryStorage::new();
        store
            .put_activity(activity)
            .await
            .expect("the in-memory store seeds an activity row");

        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(at()));
        let ids: Arc<dyn IdGenerator> = Arc::new(CountingIdGenerator::new());
        let work = WorkingState::new(store.begin_txn().expect("the in-memory store begins a txn"));
        let mut out = Collector::new(
            EntryId::new(1),
            Some(OverlaySink::new(&work)),
            clock.clone(),
            ids.clone(),
        );
        let mut env = EvalEnv::new();
        let mut definitions = HashMap::new();
        let state_handlers = build_state_handlers();
        let container = ActivityContainer::open(&work, activity_ref())
            .await
            .expect("the seeded activity resolves its container");
        {
            let mut ctx = HandlerContext {
                env: &mut env,
                storage: &work,
                clock,
                ids,
                definitions: &mut definitions,
                state_handlers: &state_handlers,
            };
            if terminated {
                container
                    .after_child_terminated(&mut ctx, &mut out, &task_ref())
                    .await;
            } else {
                container
                    .after_child_completed(&mut ctx, &mut out, &task_ref())
                    .await;
            }
        }
        out.into_entries()
            .into_iter()
            .map(|entry| entry.payload)
            .collect()
    }

    /// A settle with no live owner has no container at all: that `None` is what a handler answers
    /// *before* it writes the terminal event, and it is why the resolution happens up front.
    #[tokio::test]
    async fn an_owner_that_does_not_exist_has_no_container() {
        let store = InMemoryStorage::new();
        let work = WorkingState::new(store.begin_txn().expect("the in-memory store begins a txn"));
        assert!(
            ActivityContainer::open(&work, activity_ref())
                .await
                .is_none(),
            "a missing activity row must not yield a container"
        );
    }

    /// A row of the wrong kind is a broken invariant, not an ownerless settle: the kind is what picks
    /// this impl, so folding it into the same `None` a missing row answers with would hide a corrupt
    /// tree behind a case the caller is expected to survive.
    #[tokio::test]
    #[should_panic(expected = "cannot own activity children")]
    async fn a_non_activity_owner_panics() {
        let store = InMemoryStorage::new();
        let work = WorkingState::new(store.begin_txn().expect("the in-memory store begins a txn"));
        ActivityContainer::open(&work, task_ref()).await;
    }

    /// A settled `Task` under a `Running` activity completes that activity, carrying the payload the
    /// applier folded onto its `raw_output` — the `CompleteState` the task handler used to emit itself,
    /// now emitted by the owner that the settle actually concerns.
    #[tokio::test]
    async fn a_settled_child_completes_a_running_activity() {
        let payload = json!({ "answer": 42 });
        let chain = settle(
            seeded_activity(ActivityStatus::Running, Some(payload.clone()), 0),
            false,
        )
        .await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::CompleteState(
                CompleteState {
                    activity: activity_ref(),
                    output: payload,
                }
            ))]
        );
    }

    /// A `Running` activity that has no folded payload still completes on a settled child, with a null
    /// result — the state's `Output`/`$states.result` fallback owns what that means.
    #[tokio::test]
    async fn a_settled_child_without_a_payload_completes_with_null() {
        let chain = settle(seeded_activity(ActivityStatus::Running, None, 0), false).await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::CompleteState(
                CompleteState {
                    activity: activity_ref(),
                    output: Value::Null,
                }
            ))]
        );
    }

    /// A `Completing` activity whose last child just drained advances its own finish — the Continue the
    /// generic child-settled reaction used to issue.
    #[tokio::test]
    async fn a_settled_child_advances_a_drained_completing_activity() {
        let chain = settle(seeded_activity(ActivityStatus::Completing, None, 0), false).await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueComplete {
                owner: activity_ref(),
            })]
        );
    }

    /// A `Completing` activity with a child still in flight emits nothing: the last settle to land is
    /// the one that advances it, so an earlier one must not race ahead of the drain.
    #[tokio::test]
    async fn a_settled_child_leaves_an_undrained_completing_activity_alone() {
        let chain = settle(seeded_activity(ActivityStatus::Completing, None, 1), false).await;
        assert!(
            chain.is_empty(),
            "undrained Completing activity must wait: {chain:?}"
        );
    }

    /// A terminated child drains a `Terminating` owner into its ContinueTerminate, whose reason is read
    /// back off the owner's own status rather than carried.
    #[tokio::test]
    async fn a_terminated_child_advances_a_drained_terminating_activity() {
        let chain = settle(
            seeded_activity(
                ActivityStatus::Terminating(TerminationReason::Cancelled),
                None,
                0,
            ),
            true,
        )
        .await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueTerminate {
                owner: activity_ref(),
            })]
        );
    }

    /// A terminated child under a still-`Running` activity asks for no reaction: whatever finishes that
    /// activity owns the next move.
    #[tokio::test]
    async fn a_terminated_child_leaves_a_running_activity_alone() {
        let chain = settle(seeded_activity(ActivityStatus::Running, None, 0), true).await;
        assert!(
            chain.is_empty(),
            "a Running activity must not react: {chain:?}"
        );
    }
}
