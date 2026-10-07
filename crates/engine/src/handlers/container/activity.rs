use serde_json::Value;

use super::Container;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::storage::{ActivityRecord, ReadonlyStorageTxn, ThreadRecord};
use crate::types::activity::ActivityKind;
use crate::types::command::{Command, CompleteState};
use crate::types::error::ExecutionError;
use crate::types::meta::{HasRawObjectRef, ObjectKind, ObjectRef, RawObjectRef};
use crate::{ActivityStatus, RejectionType};
use spica_asl::State;

/// The container for an `Activity`'s children.
pub(crate) struct ActivityContainer {
    activity: ObjectRef<ActivityKind>,
}

impl ActivityContainer {
    /// Hand the settle to the state that fronts this activity — its own
    /// [`StateHandler::child_completed`](crate::handlers::state_handler::StateHandler::child_completed),
    /// the one place a per-state reaction lives.
    async fn hand_to_state(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        act: &ActivityRecord,
        thread: &ThreadRecord,
        state_def: &State,
        child: &RawObjectRef,
    ) {
        let activity = self.activity.clone();
        let activity_value = act.value();
        let variables = thread.variables.clone();
        // Every `State` variant has a registered factory (see `build_state_handlers`), so a miss here is
        // an engine regression — fail loud rather than leave the activity stuck `Running`.
        let handler = ctx
            .state_handlers
            .create(state_def)
            .expect("state type has no registered handler: engine regression, not a flow error");
        handler
            .child_completed(
                ctx,
                out,
                activity,
                &activity_value,
                &variables,
                child.clone(),
            )
            .await;
    }

    /// A `Wait` owns exactly one child — its resume timer — and that timer's settle **is** its
    /// completion trigger: the state resumes and finishes on the activity's processed input, since a
    /// `Wait` produces no distinct raw output.
    ///
    /// So a `Wait` can only ever be settled while `Running`: `Completing` is entered *by* this very
    /// settle, and nothing else can reach it. `Terminating` still advances, since a teardown cancels
    /// the timer it armed; every other status is the row and the lifecycle disagreeing rather than a
    /// settle to react to.
    fn settle_wait(
        &self,
        out: &mut Collector<'_>,
        act: &ActivityRecord,
    ) -> Result<(), ProcessingError> {
        match &act.value.status {
            ActivityStatus::Running => {
                out.append_command(Command::CompleteState(CompleteState {
                    activity: self.activity.clone(),
                    output: act.value.input.clone().unwrap_or(Value::Null),
                }));
                Ok(())
            }
            ActivityStatus::Terminating(_) => {
                out.append_command(Command::ContinueTerminate {
                    owner: self.activity.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            status => Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "activity_container: activity {} settled its resume timer while {}; a `Wait` \
                     completes on that very settle",
                    self.activity,
                    status.phase()
                ),
            )),
        }
    }

    /// A `Task` owns two children — the invocation and the `TimeoutSeconds` deadline that bounds it —
    /// and only those two kinds can settle under it, so the *pair* is the arm, exactly as the run's own
    /// container decides on `(child kind, run status)`. Which of the two settled, and in which status,
    /// is what the state's own hook knows: a landed attempt completes it, a fired deadline terminates
    /// it, and a deadline a sweep cancelled fails nothing.
    async fn settle_task(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        act: &ActivityRecord,
        thread: &ThreadRecord,
        state_def: &State,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        match (child.kind, &act.value.status) {
            (ObjectKind::Task | ObjectKind::Timer, ActivityStatus::Running) => {
                self.hand_to_state(ctx, out, act, thread, state_def, child)
                    .await;
                Ok(())
            }
            (ObjectKind::Task | ObjectKind::Timer, ActivityStatus::Completing) => {
                out.append_command(Command::ContinueComplete {
                    owner: self.activity.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            (ObjectKind::Task | ObjectKind::Timer, ActivityStatus::Terminating(_)) => {
                out.append_command(Command::ContinueTerminate {
                    owner: self.activity.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            // A finished `Task` has nothing left to move. What reaches it here is a *late* cleanup: the
            // exit that gave up on the in-flight call never opens the sweep that would have taken it
            // down, so it disposes of it by resolving to a `CancelTask` instead — and that cancellation
            // lands after the activity is already done. The child's own terminal is written by then, so
            // refusing the settle would drop the cancellation of a call that is still live.
            (
                ObjectKind::Task | ObjectKind::Timer,
                ActivityStatus::Completed | ActivityStatus::Terminated(_),
            ) => Ok(()),
            (kind, status) => Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "activity_container: activity {} settled a {kind} child {child} while {}; a `Task` \
                     owns only its invocation and its deadline",
                    self.activity,
                    status.phase()
                ),
            )),
        }
    }

    /// A `Parallel`/`Map` owns the branches/items it fanned out, and its own hook decides per settle
    /// whether to replenish an open slot, converge, or fail — so a `Running` settle is always handed to
    /// it: a `Parallel` fills every branch up front and converges once the last drains, a `Map` refills
    /// a freed `MaxConcurrency` slot from the never-spawned tail before it converges.
    ///
    /// A settle landing once the fan-out is finishing advances that finish instead — the hook would be
    /// replenishing under a state that already decided to stop — and one landing after it finished is a
    /// late cleanup of a child the fan-out no longer waits for, which no arm of the state can mean.
    async fn settle_fanout(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        act: &ActivityRecord,
        thread: &ThreadRecord,
        state_def: &State,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        match &act.value.status {
            ActivityStatus::Running => {
                self.hand_to_state(ctx, out, act, thread, state_def, child)
                    .await;
                Ok(())
            }
            ActivityStatus::Completing => {
                out.append_command(Command::ContinueComplete {
                    owner: self.activity.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            ActivityStatus::Terminating(_) => {
                out.append_command(Command::ContinueTerminate {
                    owner: self.activity.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            ActivityStatus::Completed | ActivityStatus::Terminated(_) => Ok(()),
        }
    }
}

impl Container for ActivityContainer {
    type Owner = ActivityKind;

    async fn open(
        storage: &dyn ReadonlyStorageTxn,
        owner: ObjectRef<Self::Owner>,
    ) -> Result<Self, ProcessingError> {
        // The activity is the seam's own existence check (see `Container::open`): an activity that is
        // gone is refused here, uniformly for every caller whose settle would have nothing to land on.
        // The call site picks this impl from the owner's kind, and the parameter's own type is that
        // kind — an owner of another kind is unrepresentable rather than a case to check.
        if storage.get_activity(&owner).await?.is_none() {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("activity_container: activity {owner} is gone"),
            ));
        }
        Ok(Self { activity: owner })
    }

    async fn after_child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        // The activity was resolved *before* the settle's own terminal event was written, and this
        // re-read runs in that same batch — so a row that is gone here is the lifecycle disagreeing with
        // the log rather than a settle to ignore. Refused, never no-op'ed: the terminal is already on
        // this batch, and a silent success would leave it unexplained. A fault is the leader's to retry.
        let Some(act) = ctx.storage.get_activity(&self.activity).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "activity_container: activity {} is gone from under its settled child",
                    self.activity
                ),
            ));
        };

        // What a settle *means* is the state's own affair, so the state's **type** is what picks the arm
        // below — and each arm then decides by the activity's own status, because which statuses a state
        // can even be settled in differ per state: a `Wait` completes on the very settle, so nothing
        // else can reach it, while a `Task`'s abandoned call is cancelled *after* the activity is done.
        // Resolving it needs the scope: an activity's owner slot admits only a `Thread`, so that row is
        // read directly, and the run between it and its definition resolves the machine.
        let owner = act.value.meta.owner.clone();
        let Some(thread) = ctx.storage.get_thread(&owner).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!(
                    "activity_container: owning thread {owner} of activity {} does not exist",
                    self.activity
                ),
            ));
        };
        // A settle whose definition cannot be resolved cannot tell what the settle means, so the batch
        // is refused rather than decided blind. `machine_for_thread` mixes a missing definition (domain)
        // with the storage fault under it, so its `Infra` is split out — a fault stays the leader's
        // retry.
        let machine = match ctx.machine_for_thread(&thread).await {
            Ok(machine) => machine,
            Err(ExecutionError::Infra(e)) => return Err(ProcessingError::Unexpected(e.into())),
            Err(e) => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("activity_container: thread {owner} cannot resolve its machine: {e}"),
                ));
            }
        };
        // The state that fronts this activity is the one its own `state_path` names inside that machine.
        let state_path = act.value.state_path.clone();
        let state_def = match machine.state_at(&state_path) {
            Ok(state_def) => state_def,
            Err(e) => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!(
                        "activity_container: state {state_path:?} is not defined by the machine that \
                         thread {owner} binds to: {e}"
                    ),
                ));
            }
        };

        match state_def {
            State::Wait(_) => self.settle_wait(out, &act),
            State::Task(_) => {
                self.settle_task(ctx, out, &act, &thread, state_def, child)
                    .await
            }
            State::Parallel(_) | State::Map(_) => {
                self.settle_fanout(ctx, out, &act, &thread, state_def, child)
                    .await
            }
            // Only a state that owns children can have one settle: a `Task` its invocation and its
            // `TimeoutSeconds` deadline, a `Wait` its resume timer, a `Parallel`/`Map` its fan-out.
            // Every other state type has nothing a settle could come from, so the row and the lifecycle
            // disagree.
            State::Choice(_) | State::Pass(_) | State::Succeed(_) | State::Fail(_) => {
                Err(ProcessingError::Rejected(
                    RejectionType::InvalidState,
                    format!(
                        "activity_container: activity {} settled a child {child}, but state \
                         {state_path:?} owns no children",
                        self.activity
                    ),
                ))
            }
        }
    }

    /// The activity's reaction is the completed hook **verbatim**: how a child settled is not what
    /// decides an activity's next move, only *that* it settled. A terminated child is exactly how a
    /// `Map`/`Parallel` learns one of its items/branches went down (`child_completed` reads the child's
    /// own terminal status to tell a finished item from a failed one), so routing the two outcomes to
    /// different arms would leave a fan-out waiting forever on a settle it was never shown.
    async fn after_child_terminated(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        self.after_child_completed(ctx, out, child).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use serde_json::{Value, json};
    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_storage::InMemoryStorage;
    use spica_testing::MockReadonlyStorageTxn;

    use super::{ActivityContainer, Container};
    use crate::RejectionType;
    use crate::StatePath;
    use crate::StorageError;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, OverlaySink, ProcessingError};
    use crate::handlers::dispatch::build_state_handlers;
    use crate::handlers::fixtures::{at, object_ref};
    use crate::storage::{
        ActivityRecord, ExecutionRecord, ReadonlyStorageTxn, Storage, ThreadRecord,
    };
    use crate::types::activity::ActivityKind;
    use crate::types::command::{Command, CompleteState, TerminationReason};
    use crate::types::execution::ExecutionKind;
    use crate::types::flow_version::FlowVersionKind;
    use crate::types::id::EntryId;
    use crate::types::meta::{
        HasRawObjectRef, NoOwner, ObjectMeta, ObjectRef, RawObjectRef, ThreadOwner,
    };
    use crate::types::task::TaskKind;
    use crate::types::thread::{Thread, ThreadKind, ThreadStatus};
    use crate::types::timer::TimerKind;
    use crate::working::WorkingState;
    use crate::{
        Activity, ActivityStatus, EntryPayload, Execution, ExecutionStatus, FlowKind, FlowVersion,
    };

    fn activity_ref() -> ObjectRef<ActivityKind> {
        object_ref("execution-0", 90)
    }

    /// The owning activity's own owner — a `Thread`, the only kind an activity's slot admits.
    fn thread_owner() -> ObjectRef<ThreadKind> {
        object_ref("execution-1", 80)
    }

    /// The child handed to the container — a settled `Task`, the only kind that routes here so far.
    fn task_ref() -> ObjectRef<TaskKind> {
        object_ref("execution-0", 91)
    }

    /// The invocation of a `Task` state, settled.
    fn task_child() -> RawObjectRef {
        task_ref().into_raw_object_ref()
    }

    /// The other child a `Task` state owns: the `TimeoutSeconds` deadline that bounds the invocation.
    fn timer_child() -> RawObjectRef {
        object_ref::<TimerKind>("deadline", 91).into_raw_object_ref()
    }

    /// A child of a kind no state settled here owns — a branch/item thread, the fan-out's own child.
    fn thread_child() -> RawObjectRef {
        object_ref::<ThreadKind>("execution-2", 92).into_raw_object_ref()
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
            meta: ObjectMeta::builder(activity_ref().uid())
                .name(activity_ref().name().clone())
                .at(at())
                .with_owner(thread_owner()),
            execution: object_ref::<ExecutionKind>("execution", 70),
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
            .map(|n| object_ref::<TimerKind>("deadline", 200 + n as u64).into_raw_object_ref())
            .collect::<HashSet<_>>();
        let mut row = ActivityRecord::from_value(activity, live);
        row.born(at());
        row
    }

    /// The run the seeded activity and its scope belong to.
    fn execution_ref() -> ObjectRef<ExecutionKind> {
        object_ref("execution", 70)
    }

    /// The version the seeded run binds to — the reference `machine_for_thread` resolves through the
    /// run's row, so the seeded version row has to answer to it.
    fn version_ref() -> ObjectRef<FlowVersionKind> {
        object_ref("execution", 60)
    }

    /// The scope a `Running` activity's settle is handed through: the thread the activity is owned by,
    /// carrying the input its own settle-time variables are read from.
    fn seeded_scope() -> ThreadRecord {
        let thread = Thread {
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
        };
        let mut row = ThreadRecord::from_value(thread, HashSet::new());
        row.born(at());
        row
    }

    /// The run the scope resolves its machine through — the one row between a thread and its definition.
    fn seeded_run() -> ExecutionRecord {
        let execution = Execution {
            meta: ObjectMeta::builder(execution_ref().uid())
                .name(execution_ref().name().clone())
                .at(at())
                .with_owner(NoOwner::new()),
            flow_version: version_ref(),
            status: ExecutionStatus::Running,
            deadline: None,
            input: json!({ "in": 1 }),
            output: None,
        };
        let mut row = ExecutionRecord::from_value(execution, HashSet::new());
        row.born(at());
        row
    }

    /// The definition the seeded run binds to. `definition` decides which state type the container's
    /// `Running` arm finds at the activity's own `state_path`.
    fn seeded_version(definition: &str) -> FlowVersion {
        FlowVersion {
            meta: ObjectMeta::builder(version_ref().uid())
                .name(version_ref().name().clone())
                .at(at())
                .with_owner(object_ref::<FlowKind>("execution_flow", 50)),
            version: 1,
            definition: definition.to_string(),
            checksum: 0,
        }
    }

    /// A childless definition: `P` is a `Pass`, so the settled activity's state owns nothing a settle
    /// could come from.
    fn childless_definition() -> String {
        json!({
            "StartAt": "P",
            "States": { "P": { "Type": "Pass", "End": true } }
        })
        .to_string()
    }

    /// A definition whose `P` owns the child the `settle` driver hands over: a `Task` state, whose
    /// reaction to its own invocation settling is the one this container relays.
    fn task_definition() -> String {
        json!({
            "StartAt": "P",
            "States": { "P": { "Type": "Task", "Resource": "service-a", "End": true } }
        })
        .to_string()
    }

    /// A definition whose `P` is a `Wait` — the state whose one child *is* its completion trigger.
    fn wait_definition() -> String {
        json!({
            "StartAt": "P",
            "States": { "P": { "Type": "Wait", "Seconds": 30, "End": true } }
        })
        .to_string()
    }

    /// A definition whose `P` is a fan-out, the state that owns the branches it spawns.
    fn fanout_definition() -> String {
        json!({
            "StartAt": "P",
            "States": { "P": { "Type": "Parallel", "End": true, "Branches": [
                { "StartAt": "B", "States": { "B": { "Type": "Pass", "End": true } } }
            ] } }
        })
        .to_string()
    }

    /// A store holding this activity row alone — all the `Completing`/`Terminating` arms read.
    async fn store_over(activity: ActivityRecord) -> InMemoryStorage {
        let mut store = InMemoryStorage::new();
        store
            .put_activity(activity)
            .await
            .expect("the in-memory store seeds an activity row");
        store
    }

    /// A store holding the activity, its owning scope and the run `definition` resolves through — the
    /// three rows the `Running` arm reads before it hands the settle to a state.
    async fn store_in_scope(activity: ActivityRecord, definition: &str) -> InMemoryStorage {
        let mut store = store_over(activity).await;
        store
            .put_thread(seeded_scope())
            .await
            .expect("the in-memory store seeds a thread row");
        store
            .put_execution(seeded_run())
            .await
            .expect("the in-memory store seeds an execution row");
        store
            .put_flow_version(seeded_version(definition))
            .await
            .expect("the in-memory store seeds a flow version row");
        store
    }

    /// Drive one hook over a working overlay seeded with `store` — the leader's shape, so the assertion
    /// reads the command the container emitted, not merely an intent. The hook's own outcome comes back
    /// too: an arm that refuses is as much the container's answer as one that emits.
    async fn settle(
        store: InMemoryStorage,
        child: &RawObjectRef,
        terminated: bool,
    ) -> (Result<(), ProcessingError>, Vec<EntryPayload>) {
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
        let mut ctx = HandlerContext {
            env: &mut env,
            storage: &work,
            clock,
            ids,
            definitions: &mut definitions,
            state_handlers: &state_handlers,
        };
        let result = if terminated {
            container
                .after_child_terminated(&mut ctx, &mut out, child)
                .await
        } else {
            container
                .after_child_completed(&mut ctx, &mut out, child)
                .await
        };
        drop(ctx);
        let chain = out
            .into_entries()
            .into_iter()
            .map(|entry| entry.payload)
            .collect();
        (result, chain)
    }

    /// Drive the **completed** hook against a mock store answering the one owner read, with the container
    /// built directly — the shape a vanished owner needs, since `open` refuses that same missing row
    /// before any hook could be reached.
    async fn hook_over(
        store: &dyn ReadonlyStorageTxn,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(at()));
        let ids: Arc<dyn IdGenerator> = Arc::new(CountingIdGenerator::new());
        let mut out = Collector::new(EntryId::new(1), None, clock.clone(), ids.clone());
        let mut env = EvalEnv::new();
        let mut definitions = HashMap::new();
        let state_handlers = build_state_handlers();
        let container = ActivityContainer {
            activity: activity_ref(),
        };
        let mut ctx = HandlerContext {
            env: &mut env,
            storage: store,
            clock,
            ids,
            definitions: &mut definitions,
            state_handlers: &state_handlers,
        };
        container
            .after_child_completed(&mut ctx, &mut out, child)
            .await
    }

    /// A settle with no live owner is refused up front: `open` names the gone owner, so a caller whose
    /// settle would have nothing to land on answers the absence before it writes its terminal.
    #[tokio::test]
    async fn a_missing_owner_has_no_container() {
        let store = InMemoryStorage::new();
        let work = WorkingState::new(store.begin_txn().expect("the in-memory store begins a txn"));
        let Err(ProcessingError::Rejected(ty, reason)) =
            ActivityContainer::open(&work, activity_ref()).await
        else {
            panic!("an open that did not read a missing row");
        };
        assert_eq!(ty, RejectionType::NotFound);
        assert!(
            reason.contains("is gone"),
            "the refusal names the gone owner: {reason}"
        );
    }

    /// A read that **faults** stays a fault, never a missing owner. The two answers are what the callers
    /// refuse on: a missing row is a decision they record, a fault is one the leader retries — so folding
    /// the fault into the missing-owner arm would turn a hiccup into a refusal. Pinned here rather than
    /// per caller, since every handler answers it through this one contract.
    #[tokio::test]
    async fn a_faulted_owner_read_is_not_a_missing_owner() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_activity()
            .times(1)
            .return_once(|_| Err(StorageError::Backend("injected storage fault".to_string())));
        assert!(
            matches!(
                ActivityContainer::open(&store, activity_ref()).await,
                Err(ProcessingError::Unexpected(_))
            ),
            "a fault must surface as Unexpected, not read as a missing row"
        );
    }

    /// An activity gone from under its settled child refuses the settle instead of no-op'ing it: the
    /// terminal is already on this attempt's batch, so a silent success would leave the log saying the
    /// child settled with nothing after it.
    #[tokio::test]
    async fn a_vanished_activity_refuses_the_settle() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_activity()
            .times(1)
            .return_once(|_| Ok(None));
        let result = hook_over(&store, task_ref().as_raw_object_ref()).await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a vanished activity must refuse the settle: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("is gone from under its settled child"),
            "the refusal names what it could not relay to: {reason}"
        );
    }

    /// A settle that reaches an activity whose state owns no children has nothing to mean: a `Pass`
    /// (like a `Choice`/`Succeed`/`Fail`) never gains a child, so the row and the lifecycle disagree and
    /// the batch is refused rather than left `Running` with nothing to advance it.
    #[tokio::test]
    async fn a_settled_child_under_a_childless_state_is_refused() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Running, None, 0),
            &childless_definition(),
        )
        .await;
        let (result, chain) = settle(store, &task_child(), false).await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a settle under a childless state must be refused: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("owns no children"),
            "the refusal names what the state lacks: {reason}"
        );
        assert!(chain.is_empty(), "the refusal emits nothing: {chain:?}");
    }

    /// A settle under a `Running` activity whose state owns children is relayed to that state: the
    /// container decides nothing itself, so what lands is exactly the state's own reaction — here a
    /// `Task` whose invocation settled completes the state with the output the row already carries.
    #[tokio::test]
    async fn a_settled_child_under_a_running_activity_reaches_its_state() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Running, Some(json!({ "out": 1 })), 0),
            &task_definition(),
        )
        .await;
        let (result, chain) = settle(store, &task_child(), false).await;
        result.expect("the hook relays the settle");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::CompleteState(
                CompleteState {
                    activity: activity_ref(),
                    output: json!({ "out": 1 }),
                }
            ))],
            "the state's own reaction is what the container relays"
        );
    }

    /// A `Running` activity whose run cannot be resolved is refused: the state that would decide the
    /// settle cannot be found, so nothing can advance the activity — and the same holds for a settle
    /// under an activity that is *already* finishing, since the state's type is what picks the arm.
    #[tokio::test]
    async fn a_settled_child_under_an_unresolvable_scope_is_refused() {
        for status in [
            ActivityStatus::Running,
            ActivityStatus::Completing,
            ActivityStatus::Terminating(TerminationReason::Cancelled),
        ] {
            let mut store = store_over(seeded_activity(status, None, 0)).await;
            store
                .put_thread(seeded_scope())
                .await
                .expect("the in-memory store seeds a thread row");
            let (result, _) = settle(store, &task_child(), false).await;
            let Err(ProcessingError::Rejected(ty, reason)) = result else {
                panic!("a settle whose definition cannot be resolved must be refused: {result:?}");
            };
            assert_eq!(ty, RejectionType::NotFound);
            assert!(
                reason.contains("cannot resolve its machine"),
                "the refusal names the definition it could not reach: {reason}"
            );
        }
    }

    /// A `Completing` activity with a child still in flight still advances: the Continue names the
    /// activity this settle belongs to, and its own handler takes down whatever is left and stops short
    /// of advancing — so the settle that drains the activity last is the one that completes it.
    #[tokio::test]
    async fn a_settled_child_advances_a_completing_activity_before_the_drain() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Completing, None, 1),
            &task_definition(),
        )
        .await;
        let (result, chain) = settle(store, &task_child(), false).await;
        result.expect("the hook relays the settle");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueComplete {
                owner: activity_ref().into_raw_object_ref(),
            })]
        );
    }

    /// The same for a fan-out: `Completing` is not the state's to decide, whichever state fronts the
    /// activity, so the Continue is emitted without consulting it.
    #[tokio::test]
    async fn a_settled_child_advances_a_completing_fanout() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Completing, None, 1),
            &fanout_definition(),
        )
        .await;
        let (result, chain) = settle(store, &thread_child(), false).await;
        result.expect("the hook relays the settle");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueComplete {
                owner: activity_ref().into_raw_object_ref(),
            })]
        );
    }

    /// A terminated child advances a `Terminating` owner into its ContinueTerminate — child still in
    /// flight or not: the Continue names the activity this settle belongs to, and its own handler takes
    /// down whatever is left and stops short of advancing.
    #[tokio::test]
    async fn a_terminated_child_advances_a_terminating_activity_before_the_drain() {
        let store = store_in_scope(
            seeded_activity(
                ActivityStatus::Terminating(TerminationReason::Cancelled),
                None,
                1,
            ),
            &task_definition(),
        )
        .await;
        let (result, chain) = settle(store, &task_child(), true).await;
        result.expect("the hook relays the settle");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueTerminate {
                owner: activity_ref().into_raw_object_ref(),
            })]
        );
    }

    /// A *completed* child under a `Terminating` activity advances that teardown just the same: the
    /// activity's direction was fixed when the teardown began, and a bystander's settle does not reopen
    /// it.
    #[tokio::test]
    async fn a_completed_child_advances_a_terminating_activity() {
        let store = store_in_scope(
            seeded_activity(
                ActivityStatus::Terminating(TerminationReason::Cancelled),
                None,
                0,
            ),
            &task_definition(),
        )
        .await;
        let (result, chain) = settle(store, &task_child(), false).await;
        result.expect("the hook relays the settle");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueTerminate {
                owner: activity_ref().into_raw_object_ref(),
            })]
        );
    }

    /// A settled child under an activity that has already finished is left alone: the activity will
    /// never act again, and what reaches it here is a late cleanup — the exit that gave up on an
    /// in-flight call disposes of it by resolving to a `CancelTask`, which lands after the finish.
    /// Refusing that would drop the cancellation of a child that is still live.
    #[tokio::test]
    async fn a_settled_child_under_a_finished_activity_is_left_alone() {
        for status in [
            ActivityStatus::Completed,
            ActivityStatus::Terminated(TerminationReason::Cancelled),
        ] {
            let store = store_in_scope(seeded_activity(status, None, 0), &task_definition()).await;
            let (result, chain) = settle(store, &task_child(), false).await;
            result.expect("a finished activity has nothing to move");
            assert!(
                chain.is_empty(),
                "a finished activity emits nothing: {chain:?}"
            );
        }
    }

    /// A `Wait`'s one child *is* its completion trigger, so a `Running` settle completes the state on
    /// the activity's processed input — the arm the state's own hook used to be, and the reason the
    /// container holds it directly.
    #[tokio::test]
    async fn a_settled_child_completes_a_running_wait_on_its_input() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Running, None, 0),
            &wait_definition(),
        )
        .await;
        let (result, chain) = settle(store, &timer_child(), false).await;
        result.expect("the hook relays the settle");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::CompleteState(
                CompleteState {
                    activity: activity_ref(),
                    output: json!({ "in": 1 }),
                }
            ))],
            "a `Wait` finishes on its input, which is the whole of its raw result"
        );
    }

    /// A teardown still cancels the timer it armed, so a `Wait` under a `Terminating` activity advances
    /// that teardown like any other state would.
    #[tokio::test]
    async fn a_terminated_child_advances_a_terminating_wait() {
        let store = store_in_scope(
            seeded_activity(
                ActivityStatus::Terminating(TerminationReason::Cancelled),
                None,
                0,
            ),
            &wait_definition(),
        )
        .await;
        let (result, chain) = settle(store, &timer_child(), true).await;
        result.expect("the hook relays the settle");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueTerminate {
                owner: activity_ref().into_raw_object_ref(),
            })]
        );
    }

    /// A `Wait` cannot be settled in any other status — `Completing` is entered *by* this very settle —
    /// so those cells are the row and the lifecycle disagreeing rather than a settle to react to. The
    /// refusal names the status it found, so the cells are told apart by the reason alone.
    #[tokio::test]
    async fn a_wait_settled_in_any_other_status_is_refused() {
        for status in [
            ActivityStatus::Completing,
            ActivityStatus::Completed,
            ActivityStatus::Terminated(TerminationReason::Cancelled),
        ] {
            let store = store_in_scope(seeded_activity(status, None, 0), &wait_definition()).await;
            let (result, chain) = settle(store, &timer_child(), false).await;
            let Err(ProcessingError::Rejected(ty, reason)) = result else {
                panic!("a `Wait` settled in any other status must be refused: {result:?}");
            };
            assert_eq!(ty, RejectionType::InvalidState);
            assert!(
                reason.contains("settled its resume timer while"),
                "the refusal names what it could not relay to: {reason}"
            );
            assert!(chain.is_empty(), "the refusal emits nothing: {chain:?}");
        }
    }

    /// A `Task` owns exactly two children, so a settle of any other kind is the row and the lifecycle
    /// disagreeing — the arm carries the pair precisely so this cannot be answered as if it were one of
    /// the two the state knows.
    #[tokio::test]
    async fn a_settled_foreign_child_under_a_task_is_refused() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Running, None, 0),
            &task_definition(),
        )
        .await;
        let (result, chain) = settle(store, &thread_child(), false).await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a foreign child under a `Task` must be refused: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("owns only its invocation and its deadline"),
            "the refusal names what the state owns: {reason}"
        );
        assert!(chain.is_empty(), "the refusal emits nothing: {chain:?}");
    }
}
