use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::handlers::container::{ActivityContainer, Container};
use crate::types::event::Event;
use crate::types::meta::{HasRawObjectRef, ObjectRef};
use crate::types::reject::RejectionType;
use crate::types::task::TaskKind;

/// Handles `CancelTask`: an in-flight `Task` is cancelled because its owning activity/execution is
/// being torn down. Emits `TaskCancelled`, which marks the task `Cancelled` in storage and drains it
/// from its parent; the physical work on the worker is deliberately left running (the worker owns
/// cancellation of its own call), and a later `CompleteTask` for this task is swallowed by the
/// `CompleteTaskHandler`'s non-`Running` guard — exactly the race guard a `CancelTimer` + late
/// `TriggerTimer` already uses.
///
/// The owning activity's container is resolved *before* the cancel is written, because this settle is
/// the sweep's own last move for a `Task` child and it is what lets the `Terminating` owner that
/// issued the sweep converge in the same batch. An owner that is already gone makes the cancel
/// pointless, so it is refused rather than written against a row nothing can drain.
///
/// A `Task` row the store has never seen is likewise nothing to cancel, but it is an invariant
/// violation rather than an idempotent replay — the sweep read that child off a live owner — so it is
/// refused with a `NotFound` [`Reject`](crate::Reject) rather than dropped silently: every command
/// owes one followup entry, and this dispatch has no `Event` to give. A **missing owner** is refused
/// by that container's own `open` with [`RejectionType::NotFound`]: the task itself is live, but an
/// ownerless cancel has nothing left to drain into.
///
/// A read that *faults* is neither: it is the engine's own failure, so it propagates as
/// [`ProcessingError::Unexpected`] for the leader to retry before giving up.
#[derive(Default)]
pub struct CancelTaskHandler;

impl CancelTaskHandler {
    pub(crate) async fn handle(
        &self,
        task: &ObjectRef<TaskKind>,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let Some(mut task_value) = ctx.storage.get_task(task).await?.map(|task| task.value())
        else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("task {task} not found; cancel dropped"),
            ));
        };
        // The transition owns its own precondition: a task that already settled (or was already
        // cancelled) has no cancellation to take, and is left untouched rather than rewritten into a
        // cancelled one. Checked before the owner read below — this refusal needs nothing but the row
        // already in hand.
        if let Err(reason) = task_value.mark_cancelled(ctx.now()) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("task {task} cannot be cancelled: {reason}"),
            ));
        }
        let owner = task_value.meta.owner.clone();
        let container = ActivityContainer::open(ctx.storage, owner.clone()).await?;
        out.append_event(Event::TaskCancelled { task: task_value })
            .await;
        container
            .after_child_terminated(ctx, out, task.as_raw_object_ref())
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use serde_json::json;
    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};

    use super::CancelTaskHandler;
    use crate::StatePath;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, ProcessingError};
    use crate::handlers::dispatch::build_state_handlers;
    use crate::handlers::fixtures::{at, object_ref};
    use crate::storage::{ActivityRecord, ExecutionRecord, TaskRecord, ThreadRecord};
    use crate::types::event::Event;
    use crate::types::execution::ExecutionKind;
    use crate::types::flow_version::FlowVersionKind;
    use crate::types::id::EntryId;
    use crate::types::meta::HasRawObjectRef;
    use crate::types::meta::{NoOwner, ObjectMeta, ObjectRef, RawObjectRef, ThreadOwner};
    use crate::types::reject::RejectionType;
    use crate::types::task::TaskKind;
    use crate::types::thread::{Thread, ThreadStatus};
    use crate::{
        Activity, ActivityKind, ActivityStatus, Command, EntryPayload, Execution, ExecutionStatus,
        FlowKind, FlowVersion, StorageError, Task, TaskStatus, TerminationReason, ThreadKind,
        Timestamp,
    };

    /// The task's owner slot: an activity, the only kind it admits.
    fn activity_ref() -> ObjectRef<ActivityKind> {
        object_ref("execution-0", 90)
    }

    /// The owning activity's own owner — a `Thread`, the only kind an activity's slot admits.
    fn thread_owner() -> ObjectRef<ThreadKind> {
        object_ref("execution-1", 80)
    }

    /// The cancel target. Its name is load-bearing: the cancelled task keeps its own meta, and a
    /// renamed task is one its owner can no longer match against the child it owns.
    fn task_ref() -> ObjectRef<TaskKind> {
        object_ref("execution-0", 91)
    }

    /// The live task the sweep cancels, owned by [`activity_ref`] and leased to `w1`.
    fn seeded_task() -> TaskRecord {
        let value = Task {
            meta: ObjectMeta::builder(task_ref().uid())
                .name(task_ref().name().clone())
                .at(at())
                .with_owner(activity_ref()),
            execution: object_ref::<ExecutionKind>("execution", 70),
            resource: "service-a".to_string(),
            arguments: json!({ "in": 1 }),
            status: TaskStatus::Running,
            deadline: None,
            worker_id: Some("w1".to_string()),
            lease_expires_at: Some(Timestamp::from_millis(2_000)),
            retry_plan: Vec::new(),
            retry_state: Default::default(),
        };
        let mut row = TaskRecord::from_value(value);
        row.born(at());
        row
    }

    /// The owning activity, with the task as its one live child so the drain the cancel triggers is
    /// observable: the container may only converge an owner that has nothing left in flight.
    fn seeded_activity(status: ActivityStatus, children: HashSet<RawObjectRef>) -> ActivityRecord {
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
            raw_output: None,
            activity_state: None,
            retry_state: None,
            output: None,
        };
        let mut row = ActivityRecord::from_value(activity, children);
        row.born(at());
        row
    }

    /// The handler exercised against a **mock** read-only store: no `InMemoryStorage` and no working
    /// overlay, so nothing the handler emits is folded back — these pin the handler's *own* decision,
    /// and the owner container's, against rows that never move. The store answers exactly the reads the
    /// run makes and panics on any other, which is itself an assertion about how far it got.
    mod mocked {
        use super::*;
        use spica_testing::MockReadonlyStorageTxn;

        /// Run the handler over the mock with a collector that carries **no** overlay: nothing the
        /// handler emits is folded back into a store, so the entries returned are exactly what the
        /// handler produced. The owner's container reads the same mocked rows — and the scope it
        /// resolves the settled child's state through — with the cancelled child still attached, since
        /// no fold ever detached it.
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
            CancelTaskHandler
                .handle(&task_ref(), &mut ctx, &mut out)
                .await?;
            Ok(out.into_entries().into_iter().map(|e| e.payload).collect())
        }

        /// A mocked store whose task read answers `task` and whose owner read answers `activity` on
        /// **both** reads the handler makes of it: `ActivityContainer::open` resolves the owner, then
        /// the settle re-reads it — a case that gets past container resolution owes it two answers.
        fn store_with_owner(
            task: Result<Option<TaskRecord>, StorageError>,
            activity: ActivityRecord,
        ) -> MockReadonlyStorageTxn {
            let mut store = MockReadonlyStorageTxn::new();
            store
                .expect_get_task()
                .withf(|r| r == &task_ref())
                .times(1)
                .return_once(move |_| task);
            store
                .expect_get_activity()
                .withf(|r| r == &activity_ref())
                .times(2)
                .return_const(Ok(Some(activity)));
            store
        }

        /// A mocked store for a case that must be decided **before** the owner is looked at: the task
        /// read answers `task`, and any owner read would find no expectation — the mock panics, which
        /// is the assertion that the refusal came first.
        fn store_without_owner(
            task: Result<Option<TaskRecord>, StorageError>,
        ) -> MockReadonlyStorageTxn {
            let mut store = MockReadonlyStorageTxn::new();
            store
                .expect_get_task()
                .withf(|r| r == &task_ref())
                .times(1)
                .return_once(move |_| task);
            store
        }

        /// The live rows the happy-path cases start from: a `Running` task owned by an activity that
        /// still holds it as a child.
        fn live_world() -> (TaskRecord, ActivityRecord) {
            (
                seeded_task(),
                seeded_activity(
                    ActivityStatus::Running,
                    HashSet::from([task_ref().into_raw_object_ref()]),
                ),
            )
        }

        /// The run the scope belongs to — the row between a thread and the definition it binds to.
        fn execution_ref() -> ObjectRef<ExecutionKind> {
            object_ref("execution", 70)
        }

        /// The definition's own reference: the version the seeded run names.
        fn version_ref() -> ObjectRef<FlowVersionKind> {
            object_ref("execution_flow", 60)
        }

        /// The owner's owning scope: a thread, the only kind an activity's owner slot admits.
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

        /// The run the scope resolves its machine through. Its `flow_version` is what [`version_ref`]
        /// has to answer to.
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

        /// The definition the seeded run binds to: `P` is the `Task` the owner's `state_path` names.
        fn seeded_version() -> FlowVersion {
            FlowVersion {
                meta: ObjectMeta::builder(version_ref().uid())
                    .name(version_ref().name().clone())
                    .at(at())
                    .with_owner(object_ref::<FlowKind>("execution_flow", 50)),
                version: 1,
                definition: json!({
                    "StartAt": "P",
                    "States": { "P": { "Type": "Task", "Resource": "service-a", "End": true } }
                })
                .to_string(),
                checksum: 0,
            }
        }

        /// The three rows a settle is resolved through **before** any arm can be picked: the thread the
        /// activity is owned by, the run under it, and the definition that run binds to. The container
        /// reads the *type* of the state at the activity's `state_path` off that definition, so even an
        /// owner whose arm ignores the state entirely owes these reads.
        fn expect_scope(store: &mut MockReadonlyStorageTxn) {
            store
                .expect_get_thread()
                .times(1)
                .return_const(Ok(Some(seeded_scope())));
            store
                .expect_get_execution()
                .times(1)
                .return_const(Ok(Some(seeded_run())));
            store
                .expect_get_flow_version()
                .times(1)
                .return_const(Ok(Some(seeded_version())));
        }

        /// A live task under a live owner: the handler's own event, and then the Continue that advances
        /// the owner — the cancel moment stamped, the identity its owner matches the child by preserved.
        ///
        /// The owner is seeded `Terminating`, which is the only shape this settle really reaches in:
        /// the state's own teardown sweep is the single emitter of `CancelTask`, so the owner has
        /// always begun its finish by the time the cancel lands. The Continue names the owner without
        /// claiming it is drained — the child is still attached here (no overlay to detach it) — so the
        /// command's own handler sweeps what is left and stops short of advancing.
        #[tokio::test]
        async fn a_live_task_emits_its_own_cancel_and_the_continue_that_advances_its_owner() {
            let (task, mut activity) = live_world();
            activity.value.status = ActivityStatus::Terminating(TerminationReason::Cancelled);
            let mut store = store_with_owner(Ok(Some(task)), activity);
            expect_scope(&mut store);
            let chain = cancel_over(&store)
                .await
                .expect("a live task under a live owner is a clean settle");
            assert_eq!(
                chain.len(),
                2,
                "the handler emits its cancel and the owner's Continue: {chain:?}"
            );
            let EntryPayload::Event(Event::TaskCancelled { task }) = &chain[0] else {
                panic!("a cancelled task emits TaskCancelled: {chain:?}");
            };
            assert_eq!(task.status, TaskStatus::Cancelled);
            assert_eq!(task.meta.name, *task_ref().name());
            assert_eq!(task.meta.owner, activity_ref());
            assert_eq!(task.meta.updated_at, at());
            assert_eq!(
                chain[1],
                EntryPayload::Command(Command::ContinueTerminate {
                    owner: activity_ref().into_raw_object_ref(),
                }),
                "the settle advances the terminating owner"
            );
        }

        /// No row: the *command's* failure, refused with the classification the leader records — and
        /// the refusal names the task, so the durable entry says what could not be found. The owner is
        /// never read: a refusal must not depend on the container resolving.
        #[tokio::test]
        async fn a_missing_row_is_the_commands_own_refusal() {
            let err = cancel_over(&store_without_owner(Ok(None)))
                .await
                .expect_err("a cancel with no task row must be refused");
            let ProcessingError::Rejected(ty, reason) = err else {
                panic!("a missing row is the command's failure, not the engine's: {err:?}");
            };
            assert_eq!(ty, RejectionType::NotFound);
            assert!(
                reason.contains(&task_ref().to_string()),
                "the refusal names the task it could not find: {reason}"
            );
        }

        /// A fault on the read is the *engine's* failure: the handler must not fold it into a domain
        /// answer, so it surfaces as `Unexpected` — what the leader retries before refusing (see
        /// `Leader::process_command`). Nothing may be emitted on the way out, and the owner is never
        /// read either.
        #[tokio::test]
        async fn a_faulted_read_surfaces_as_unexpected() {
            let err = cancel_over(&store_without_owner(Err(StorageError::Backend(
                "injected storage fault".to_string(),
            ))))
            .await
            .expect_err("a store that cannot read is not the command's failure");
            assert!(
                matches!(err, ProcessingError::Unexpected(_)),
                "a read fault is the engine's, never a refusal: {err:?}"
            );
        }

        /// A **fault on the owner read** is the engine's failure, exactly as a fault on the task read
        /// is — and this is what `Container::open`'s `Result` is for. Folding it into `None` would make
        /// a store that hiccuped read as an owner that is gone, turning a retryable fault into a
        /// refusal. The owner is read exactly once: the resolution faulted, so the settle never runs.
        #[tokio::test]
        async fn a_faulted_owner_read_surfaces_as_unexpected() {
            let (task, _) = live_world();
            let mut store = MockReadonlyStorageTxn::new();
            store
                .expect_get_task()
                .times(1)
                .return_once(move |_| Ok(Some(task)));
            store.expect_get_activity().times(1).return_once(move |_| {
                Err(StorageError::Backend("injected storage fault".to_string()))
            });
            let err = cancel_over(&store)
                .await
                .expect_err("a store that cannot read is not the command's failure");
            assert!(
                matches!(err, ProcessingError::Unexpected(_)),
                "an owner read fault is the engine's, never a refusal: {err:?}"
            );
        }

        /// An owner that is gone has nothing to drain the task, so `Container::open` refuses it with
        /// [`RejectionType::NotFound`] naming the owner — the resolution the handler used to do
        /// inline, now the seam's own contract. The owner is read exactly **once** here: resolution
        /// finds nothing, so the settle never re-reads.
        #[tokio::test]
        async fn a_gone_owner_is_refused_by_open() {
            let (task, _) = live_world();
            let mut store = MockReadonlyStorageTxn::new();
            store
                .expect_get_task()
                .times(1)
                .return_once(move |_| Ok(Some(task)));
            store
                .expect_get_activity()
                .times(1)
                .return_const(Ok::<_, StorageError>(None));
            let err = cancel_over(&store)
                .await
                .expect_err("a cancel with no owner to drain must be refused");
            let ProcessingError::Rejected(ty, reason) = err else {
                panic!("a missing owner is a refusal, not a read fault: {err:?}");
            };
            assert_eq!(ty, RejectionType::NotFound);
            assert!(
                reason.contains(&activity_ref().to_string()),
                "the refusal names the owner that is gone: {reason}"
            );
        }
    }
}
