//! The `Timer` element — one armed deadline: the fire that ends it and the cancel that stops
//! it. It owns no children, so like `Task` it carries no `Container` impl — it is always the
//! child that settles, and the settle is handed to its owner's container.

use crate::RejectionType;
use crate::elements::{ActivityElementBehavior, Container, ExecutionElementBehavior};
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::event::Event;
use crate::types::meta::{HasRawObjectRef, ObjectRef, TimerOwner};
use crate::types::timer::TimerKind;

pub(crate) struct TimerElementBehavior;

impl TimerElementBehavior {
    /// Handles `TriggerTimer`: a timer's deadline elapsed. A fire for a timer that is gone or no longer
    /// `Active` is refused, not acted on — the durable record of a fire that lost its race.
    ///
    /// The fire is recorded and then handed to the owner's [`Container`], exactly as a cancel is
    /// ([`Self::cancel`]): the owner's *kind* names the container, and what the settle *means* to that
    /// owner is the owner's own affair. So a `Wait`'s `Seconds` resumes the state that armed it, a `Task`'s
    /// `TimeoutSeconds` fails it, and a run's own `TimeoutSeconds` terminates the run — three effects
    /// obtained by two containers rather than by a third field on the timer. The timer itself carries no
    /// purpose: *who* armed it is `meta.owner`, and *why* is that owner's `TimeoutSeconds`/`Seconds`.
    pub(crate) async fn trigger(
        timer: &ObjectRef<TimerKind>,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        // A timer row is written by the batch that arms it, and the scheduler only learns to fire it
        // once that batch is durable — so a miss is the log and the projection disagreeing (a forged
        // command, or a corrupt store), not a fire that outlived its timer. Nothing ever removes a row,
        // so an already-fired timer keeps its row and lands in the status guard below instead.
        let act = match ctx.storage.get_timer(timer).await? {
            Some(t) => t,
            None => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("trigger_timer: timer {timer} does not exist; fire refused"),
                ));
            }
        };

        // A duplicate fire, or one racing the `CancelTimer` that swept the timer first: the transition
        // refuses a timer past `Active`, leaving it untouched. The fire is recorded rather than
        // swallowed, so the durable log explains why the deadline went unenforced.
        let mut fired = act.clone();
        if let Err(why) = fired.mark_triggered(ctx.now()) {
            tracing::warn!(
                timer = %timer,
                status = ?act.status,
                owner = ?act.meta.owner,
                "fire arrived for a timer that is no longer Active; refused: {why}"
            );
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("trigger_timer: timer {timer} cannot fire: {why}"),
            ));
        }

        out.append_event(Event::TimerTriggered { timer: fired })
            .await;

        // Both containers below re-read their owner, and the settle's own batch has just rewritten the
        // timer row (`TimerTriggered` folds `Completed` in and detaches the child) — so what each hook
        // decides is decided against the post-fire projection, never against the row read above.
        //
        // A container is resolved *after* the fire is recorded — the fire is the fact, and it stands
        // even for an owner that is gone — but *before* the reaction, so the absent owner is logged
        // rather than left indistinguishable from a reaction that did nothing.
        match act.meta.owner.clone() {
            TimerOwner::Activity(owner) => {
                let container = match ActivityElementBehavior::open(ctx.storage, owner).await {
                    Ok(container) => container,
                    // The fire is the fact, and it stands even for a gone owner: `TimerTriggered` is
                    // already on this batch, so refusing here would drop it and leave the timer armed.
                    // Only the reaction is skipped.
                    Err(ProcessingError::Rejected(..)) => {
                        tracing::warn!(
                            timer = %timer,
                            "fire arrived for a timer whose owning activity is gone; no reaction"
                        );
                        return Ok(());
                    }
                    Err(err) => return Err(err),
                };
                container
                    .after_child_completed(ctx, out, timer.as_raw_object_ref())
                    .await?;
            }
            TimerOwner::Execution(owner) => {
                let container = match ExecutionElementBehavior::open(ctx.storage, owner).await {
                    Ok(container) => container,
                    // As above: the fire outlives its owner.
                    Err(ProcessingError::Rejected(..)) => {
                        tracing::warn!(
                            timer = %timer,
                            "fire arrived for a timer whose owning execution is gone; no reaction"
                        );
                        return Ok(());
                    }
                    Err(err) => return Err(err),
                };
                container
                    .after_child_completed(ctx, out, timer.as_raw_object_ref())
                    .await?;
            }
        }

        Ok(())
    }

    /// Handles `CancelTimer`: marks an armed timer cancelled. After recording the timer's terminal state,
    /// the settle is handed to the owner's [`Container`], whose re-read of the owner lets a
    /// Completing/Terminating one converge in the same batch — a cancel is often the last thing draining
    /// it. The owner slot's own type ([`TimerOwner`]) picks that container statically: an activity's
    /// deadline drains through [`ActivityElementBehavior`], a run's own through [`ExecutionElementBehavior`].
    ///
    /// Every refusal is a command that owes one followup entry and has no `Event` to give — but on three
    /// different footings. A timer row the store has never seen is an invariant violation: only a sweep
    /// that just read the timer off a live owner's ownership edges issues this command, and the row is
    /// born in the same batch as that very child edge (see `TimerActivatedApplier`). A timer that is no
    /// longer `Active` is instead a *race*: the scheduler's fire is deliberately decoupled from the
    /// dispatch loop (`spica-scheduler`), so a `TriggerTimer` can overtake a sweep's `CancelTimer` and
    /// leave it arriving at a timer that already fired — hence `InvalidState` rather than the `NotFound`
    /// the absent row earns. That judgement is not this handler's to make: it lives on the transition
    /// itself ([`Timer::mark_cancelled`]), and this step only relays the reason it returns. An **owner
    /// that is gone** is refused by that container's own `open` with [`RejectionType::NotFound`]: the
    /// timer itself is live, but an ownerless cancel has nothing left to drain into.
    ///
    /// A read that *faults* — the timer's or the owner's — is none of those: it is the engine's own
    /// failure, so it propagates as [`ProcessingError::Unexpected`] for the leader to retry before giving
    /// up.
    pub(crate) async fn cancel(
        timer: &ObjectRef<TimerKind>,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let Some(row) = ctx.storage.get_timer(timer).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("timer {timer} not found; cancel dropped"),
            ));
        };
        // The transition owns its own precondition: a timer that already fired or was cancelled is
        // declined by `mark_cancelled` and left untouched, so this step cannot rewrite a terminal
        // timer's state. The reason it names is folded into this command's durable rejection.
        let mut timer_value = row;
        if let Err(reason) = timer_value.mark_cancelled(ctx.now()) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("timer {timer} cannot be cancelled: {reason}"),
            ));
        }
        // The owner's container is resolved *before* the cancel is written, so an ownerless cancel is
        // refused while it can still be answered rather than discovered as a no-op after
        // `TimerCancelled` is already on the log. The slot's own type is the owner's kind, so each arm
        // names its container — and the `Errs` below stay distinct: a missing row is this command's
        // refusal, a fault on that read is the engine's (see `Container::open`).
        match timer_value.meta.owner.clone() {
            TimerOwner::Activity(owner) => {
                let container = ActivityElementBehavior::open(ctx.storage, owner.clone()).await?;
                out.append_event(Event::TimerCancelled { timer: timer_value })
                    .await;
                container
                    .after_child_terminated(ctx, out, timer.as_raw_object_ref())
                    .await?;
            }
            TimerOwner::Execution(owner) => {
                let container = ExecutionElementBehavior::open(ctx.storage, owner.clone()).await?;
                out.append_event(Event::TimerCancelled { timer: timer_value })
                    .await;
                container
                    .after_child_terminated(ctx, out, timer.as_raw_object_ref())
                    .await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod cancel_tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use serde_json::json;
    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_storage::InMemoryStorage;
    use spica_testing::MockReadonlyStorageTxn;

    use super::TimerElementBehavior;
    use crate::StatePath;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, OverlaySink, ProcessingError};
    use crate::handlers::build_state_handlers;
    use crate::handlers::fixtures::{Seeded, at, object_ref, path, seed_edges};
    use crate::storage::Storage;
    use crate::types::command::{Command, TerminationReason};
    use crate::types::event::Event;
    use crate::types::execution::{Execution, ExecutionKind, ExecutionStatus};
    use crate::types::flow_version::FlowVersionKind;
    use crate::types::id::EntryId;
    use crate::types::meta::{
        HasRawObjectRef, NoOwner, ObjectMeta, ObjectRef, RawObjectRef, ThreadOwner, TimerOwner,
    };
    use crate::types::reject::RejectionType;
    use crate::types::thread::{Thread, ThreadKind, ThreadStatus};
    use crate::types::timer::TimerKind;
    use crate::working::WorkingState;
    use crate::{
        Activity, ActivityKind, ActivityStatus, EntryPayload, FlowKind, FlowVersion, StorageError,
        Timer, TimerStatus, Timestamp,
    };

    /// The timer's owner slot filled by an activity — the scope a state's deadline is armed under.
    fn activity_ref() -> ObjectRef<ActivityKind> {
        object_ref("execution-0", 90)
    }

    /// The same slot filled by a run — the scope its own `TimeoutSeconds` is armed under.
    fn execution_ref() -> ObjectRef<ExecutionKind> {
        object_ref("execution", 70)
    }

    /// The owning activity's own owner — a `Thread`, the only kind an activity's slot admits.
    fn thread_owner() -> ObjectRef<ThreadKind> {
        object_ref("execution-1", 80)
    }

    /// The cancel target. Its name is load-bearing: the cancelled timer keeps its own meta, and a
    /// renamed timer is one its owner can no longer match against the child it owns.
    fn timer_ref() -> ObjectRef<TimerKind> {
        object_ref("execution-0", 92)
    }

    /// The armed timer a sweep cancels, owned by `owner` through the slot's own variant.
    fn seeded_timer(owner: TimerOwner, status: TimerStatus) -> Timer {
        Timer {
            meta: ObjectMeta::builder(timer_ref().uid())
                .name(timer_ref().name().clone())
                .at(at())
                .with_owner(owner),
            execution: execution_ref(),
            status,
            deadline: Timestamp::from_millis(2_000),
        }
    }

    /// The activity a state's deadline hangs under, with `children` still live so the drain the
    /// cancel triggers is observable: the container may only converge an owner with nothing in flight.
    fn seeded_activity(
        status: ActivityStatus,
        children: HashSet<RawObjectRef>,
    ) -> Seeded<Activity> {
        let activity = Activity {
            meta: ObjectMeta::builder(activity_ref().uid())
                .name(activity_ref().name().clone())
                .at(at())
                .with_owner(thread_owner()),
            execution: execution_ref(),
            state_path: path("States/P"),
            status,
            raw_input: json!({ "in": 1 }),
            input: Some(json!({ "in": 1 })),
            raw_output: None,
            activity_state: None,
            retry_state: None,
            output: None,
        };
        let row = activity;
        Seeded::owning(row, children)
    }

    /// The run its own `TimeoutSeconds` timer hangs under. A run is the root of its tree, so it owns no
    /// ownerless slot; its children are the deadline and (once running) its root thread.
    fn seeded_execution(
        status: ExecutionStatus,
        children: HashSet<RawObjectRef>,
    ) -> Seeded<Execution> {
        let row = Execution {
            meta: ObjectMeta::builder(execution_ref().uid())
                .name(execution_ref().name().clone())
                .at(at())
                .with_owner(NoOwner::new()),
            flow_version: object_ref::<FlowVersionKind>("flow-1", 60),
            status,
            deadline: None,
            input: json!({}),
            output: None,
        };
        Seeded::owning(row, children)
    }

    /// The version the seeded scope resolves its machine through — the reference the run's own
    /// `flow_version` names, so the seeded version row has to answer to it.
    fn version_ref() -> ObjectRef<FlowVersionKind> {
        object_ref("flow-1", 60)
    }

    /// The scope a `Running` owner's settle is handed through: the thread the activity is owned by,
    /// carrying the input its own settle-time variables are read from.
    fn seeded_scope() -> Thread {
        Thread {
            meta: ObjectMeta::builder(thread_owner().uid())
                .name(thread_owner().name().clone())
                .at(at())
                .with_owner(ThreadOwner::Execution(execution_ref())),
            execution: execution_ref(),
            state_path: StatePath::root(),
            start_at: "P".to_string(),
            index: 0,
            status: ThreadStatus::Running,
            input: json!({ "in": 1 }),
            output: None,
        }
    }

    /// The run the scope resolves its machine through — the one row between a thread and its
    /// definition. Its `flow_version` is what [`version_ref`] has to answer to.
    fn seeded_run() -> Execution {
        Execution {
            meta: ObjectMeta::builder(execution_ref().uid())
                .name(execution_ref().name().clone())
                .at(at())
                .with_owner(NoOwner::new()),
            flow_version: version_ref(),
            status: ExecutionStatus::Running,
            deadline: None,
            input: json!({ "in": 1 }),
            output: None,
        }
    }

    /// The definition the seeded run binds to: `P` is a `Task`, so a settle under a `Running` owner is
    /// handed to a state that owns children. This is what keeps a *cancelled* deadline meaningful —
    /// the state reads the timer's own row, finds it is no longer a fire, and stops there.
    fn seeded_deadline_version() -> FlowVersion {
        FlowVersion {
            meta: ObjectMeta::builder(version_ref().uid())
                .name(version_ref().name().clone())
                .at(at())
                .with_owner(object_ref::<FlowKind>("deadline_flow", 50)),
            version: 1,
            definition: json!({
                "StartAt": "P",
                "States": { "P": { "Type": "Task", "Resource": "service-a", "End": true } }
            })
            .to_string(),
            checksum: 0,
        }
    }

    /// The handler exercised against a **mock** read-only store: no `InMemoryStorage`, no working
    /// overlay, so these pin the handler's *own* decision and nothing else. The store answers exactly
    /// the reads the handler makes and panics on any other, which is itself an assertion about how far
    /// the handler got — every guard case below must never reach the owner read.
    async fn cancel_over(
        store: &MockReadonlyStorageTxn,
    ) -> Result<Vec<EntryPayload>, ProcessingError> {
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(at()));
        let ids: Arc<dyn IdGenerator> = Arc::new(CountingIdGenerator::new());
        let mut out = Collector::new(EntryId::new(1), None, clock.clone(), ids.clone());
        let mut env = EvalEnv::new();
        let mut definitions = HashMap::new();
        let state_handlers = build_state_handlers();
        let mut ctx = HandlerContext {
            env: &mut env,
            storage: store,
            clock,
            ids,
            definitions: &mut definitions,
            state_handlers: &state_handlers,
        };
        TimerElementBehavior::cancel(&timer_ref(), &mut ctx, &mut out).await?;
        Ok(out.into_entries().into_iter().map(|e| e.payload).collect())
    }

    /// The handler over a **working overlay** — the leader's shape, where the `TimerCancelled` this
    /// step emits folds back into the store it is building. That fold is what detaches the timer from
    /// its owner, so the container's own re-read of the owner sees the drained row and reacts to it;
    /// over a mock store the same reaction is unreachable, since nothing ever folds.
    ///
    /// The scope rows are seeded too, because resolving *which* reaction that is happens before the arm
    /// that decides it: an activity owner's settle is relayed to the state its definition puts at its
    /// own `state_path`, whatever status the activity is in when the settle lands.
    async fn cancel_over_overlay(
        timer: Timer,
        activity: Option<Seeded<Activity>>,
        execution: Option<Seeded<Execution>>,
    ) -> Vec<EntryPayload> {
        let mut store = InMemoryStorage::new();
        if let Some(activity) = activity {
            let owner = activity.row.meta.raw_object_ref();
            seed_edges(&mut store, owner, activity.children).await;
            store
                .put_activity(activity.row)
                .await
                .expect("the in-memory store seeds an activity row");
            store
                .put_thread(seeded_scope())
                .await
                .expect("the in-memory store seeds a thread row");
            store
                .put_execution(seeded_run())
                .await
                .expect("the in-memory store seeds an execution row");
            store
                .put_flow_version(seeded_deadline_version())
                .await
                .expect("the in-memory store seeds a flow version row");
        }
        if let Some(execution) = execution {
            let owner = execution.row.meta.raw_object_ref();
            seed_edges(&mut store, owner, execution.children).await;
            store
                .put_execution(execution.row)
                .await
                .expect("the in-memory store seeds an execution row");
        }
        store
            .put_timer(timer)
            .await
            .expect("the in-memory store seeds a timer row");
        drive(store).await
    }

    /// Drive the cancel over an already-seeded store, folding what it emits back in — the leader's
    /// shape, so the assertion reads the commands the container emitted, not merely an intent.
    async fn drive(store: InMemoryStorage) -> Vec<EntryPayload> {
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
        {
            let mut ctx = HandlerContext {
                env: &mut env,
                storage: &work,
                clock,
                ids,
                definitions: &mut definitions,
                state_handlers: &state_handlers,
            };
            TimerElementBehavior::cancel(&timer_ref(), &mut ctx, &mut out)
                .await
                .expect("a live timer under a live owner is a clean cancel");
        }
        out.into_entries().into_iter().map(|e| e.payload).collect()
    }

    /// No row: the *command's* failure, refused with the classification the leader records. The
    /// dispatch is only ever issued by a sweep that just read this timer off a live owner's
    /// ownership edges, so a missing row is the projection disagreeing with itself — not a duplicate
    /// cancel to swallow. The owner is never read: a refusal must not depend on the container
    /// resolving.
    #[tokio::test]
    async fn a_missing_row_is_the_commands_own_refusal() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_timer()
            .times(1)
            .return_const(Ok::<_, StorageError>(None));
        let err = cancel_over(&store)
            .await
            .expect_err("a cancel with no timer row must be refused");
        let ProcessingError::Rejected(ty, reason) = err else {
            panic!("a missing row is the command's failure, not the engine's: {err:?}");
        };
        assert_eq!(ty, RejectionType::NotFound);
        assert!(
            reason.contains(&timer_ref().to_string()),
            "the refusal names the timer it could not find: {reason}"
        );
    }

    /// A timer that already fired or was already cancelled is refused rather than swallowed: the
    /// scheduler's fire is decoupled from the dispatch loop, so a `TriggerTimer` overtaking this
    /// sweep's `CancelTimer` lands here. Both terminal ends are covered — the refusal names the state
    /// it found, so the two are told apart by the reason alone. The owner is never read: a refusal
    /// must not depend on the container resolving.
    #[tokio::test]
    async fn an_already_terminal_timer_is_a_wrong_state_refusal() {
        for status in [TimerStatus::Cancelled, TimerStatus::Completed] {
            let mut store = MockReadonlyStorageTxn::new();
            store
                .expect_get_timer()
                .times(1)
                .return_const(Ok(Some(seeded_timer(
                    TimerOwner::Activity(activity_ref()),
                    status,
                ))));
            let err = cancel_over(&store)
                .await
                .expect_err("a cancel of a terminal timer must be refused");
            let ProcessingError::Rejected(ty, reason) = err else {
                panic!("a terminal timer is the command's refusal, not the engine's: {err:?}");
            };
            assert_eq!(ty, RejectionType::InvalidState);
            assert!(
                reason.contains(&timer_ref().to_string()),
                "the refusal names the timer it could not cancel: {reason}"
            );
            assert!(
                reason.contains(&format!("{status:?}")),
                "the refusal names the state the timer was found in: {reason}"
            );
        }
    }

    /// An owner that is gone has nothing to drain the cancel into, so `Container::open` refuses it
    /// with [`RejectionType::NotFound`] naming the owner — the resolution the handler used to do
    /// inline, now the seam's own contract. The timer itself is live in both cases, so this is not a
    /// caller naming something absent. The owner is read exactly **once**: resolution finds nothing,
    /// so the settle never re-reads.
    #[tokio::test]
    async fn a_gone_owner_is_refused_by_open() {
        for owner in [
            TimerOwner::Activity(activity_ref()),
            TimerOwner::Execution(execution_ref()),
        ] {
            let mut store = MockReadonlyStorageTxn::new();
            store
                .expect_get_timer()
                .times(1)
                .return_const(Ok(Some(seeded_timer(owner.clone(), TimerStatus::Active))));
            match &owner {
                TimerOwner::Activity(_) => {
                    store
                        .expect_get_activity()
                        .times(1)
                        .return_const(Ok::<_, StorageError>(None));
                }
                TimerOwner::Execution(_) => {
                    store
                        .expect_get_execution()
                        .times(1)
                        .return_const(Ok::<_, StorageError>(None));
                }
            }
            let gone = match &owner {
                TimerOwner::Activity(activity) => activity.to_string(),
                TimerOwner::Execution(execution) => execution.to_string(),
            };
            let err = cancel_over(&store)
                .await
                .expect_err("a cancel with no owner to drain must be refused");
            let ProcessingError::Rejected(ty, reason) = err else {
                panic!("a missing owner is a refusal, not a read fault: {err:?}");
            };
            assert_eq!(ty, RejectionType::NotFound);
            assert!(
                reason.contains(&gone),
                "the refusal names the owner that is gone: {reason}"
            );
        }
    }

    /// A **fault on the owner read** is the engine's failure, exactly as a fault on the timer read is
    /// — and this is what `Container::open`'s `Result` is for. Folding it into `None` would make a
    /// store that hiccuped read as an owner that is gone, turning a retryable fault into a refusal.
    /// The owner is read exactly once: the resolution faulted, so the settle never runs.
    #[tokio::test]
    async fn a_faulted_owner_read_surfaces_as_unexpected() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_timer()
            .times(1)
            .return_const(Ok(Some(seeded_timer(
                TimerOwner::Activity(activity_ref()),
                TimerStatus::Active,
            ))));
        store
            .expect_get_activity()
            .times(1)
            .return_once(|_| Err(StorageError::Backend("injected storage fault".to_string())));
        let err = cancel_over(&store)
            .await
            .expect_err("a store that cannot read is not the command's failure");
        assert!(
            matches!(err, ProcessingError::Unexpected(_)),
            "an owner read fault is the engine's, never a refusal: {err:?}"
        );
    }

    /// The cancel's *whole* tail over the leader's overlay: the emitted `TimerCancelled` folds back
    /// into the store, which detaches the timer from its owner, so the container's re-read finds the
    /// drained `Terminating` owner and hands it its `ContinueTerminate` in the **same** batch. Both
    /// owner kinds are covered — an activity's deadline and a run's own — since only the slot's type
    /// decides which container runs.
    #[tokio::test]
    async fn a_cancelled_timer_drains_its_terminating_owner_in_the_same_batch() {
        let term = ActivityStatus::Terminating(TerminationReason::Cancelled);
        let worlds = [
            (
                TimerOwner::Activity(activity_ref()),
                Some(seeded_activity(
                    term,
                    HashSet::from([timer_ref().into_raw_object_ref()]),
                )),
                None,
                activity_ref().into_raw_object_ref(),
            ),
            (
                TimerOwner::Execution(execution_ref()),
                None,
                Some(seeded_execution(
                    ExecutionStatus::Terminating(TerminationReason::Cancelled),
                    HashSet::from([timer_ref().into_raw_object_ref()]),
                )),
                execution_ref().into_raw_object_ref(),
            ),
        ]
        .map(|(owner, activity, execution, drained)| {
            (
                seeded_timer(owner, TimerStatus::Active),
                activity,
                execution,
                drained,
            )
        });

        for (timer, activity, execution, drained) in worlds {
            let chain = cancel_over_overlay(timer, activity, execution).await;
            assert_eq!(
                chain.len(),
                2,
                "the cancel and the drain it triggers, and nothing besides: {chain:?}"
            );
            let EntryPayload::Event(Event::TimerCancelled { timer }) = &chain[0] else {
                panic!("a cancelled timer emits TimerCancelled: {chain:?}");
            };
            assert_eq!(timer.status, TimerStatus::Cancelled);
            assert_eq!(timer.meta.updated_at, at());
            assert_eq!(
                chain[1],
                EntryPayload::Command(Command::ContinueTerminate { owner: drained }),
                "the drained owner converges in this batch"
            );
        }
    }

    /// A live timer under an owner that is **not** finishing: the handler's own event and nothing
    /// besides — the owner is still `Running`, so it has no finish to advance. The container does hand
    /// the settle to the state that owns the deadline, but a deadline a sweep *cancelled* is no longer
    /// a fire: the state reads it, finds it stopped bounding nothing, and stops there too.
    #[tokio::test]
    async fn an_active_timer_emits_its_own_cancel() {
        let chain = cancel_over_overlay(
            seeded_timer(TimerOwner::Activity(activity_ref()), TimerStatus::Active),
            Some(seeded_activity(
                ActivityStatus::Running,
                HashSet::from([timer_ref().into_raw_object_ref()]),
            )),
            None,
        )
        .await;
        assert_eq!(
            chain.len(),
            1,
            "a Running owner has no finish to advance: {chain:?}"
        );
        let EntryPayload::Event(Event::TimerCancelled { timer }) = &chain[0] else {
            panic!("a cancelled timer emits TimerCancelled: {chain:?}");
        };
        assert_eq!(timer.status, TimerStatus::Cancelled);
        assert_eq!(timer.meta.name, *timer_ref().name());
        assert_eq!(timer.meta.updated_at, at());
    }
}
