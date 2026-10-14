//! The `Task` element — one external call a worker executes on a state's behalf: its
//! invocation, the worker's report, its deadlines and leases, and the cancellation that a
//! teardown reaches it with.
//!
//! Two faces of one element, gathered here from where they were split before: what the engine
//! does *to* a task (activate it, cancel it, lease it) and what a worker's report *makes of* it
//! (complete, fail — including the retry it re-queues). Unlike `Execution`/`Thread`/`Activity`
//! it owns no children, so it carries no `Container` impl: it is always the child that settles.

use std::time::Duration;

use crate::elements::{ActivityElementBehavior, Container};
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::{
    ActivateTask, ClaimTasks, Command, CompleteTask, FailTask, TerminateState, TerminationReason,
};
use crate::types::event::{Event, TaskCompleted, TaskFailed, TasksClaimed};
use crate::types::meta::{HasRawObjectRef, ObjectRef};
use crate::types::task::{FailureOutcome, TaskKind};
use crate::{RejectionType, RetryState, Task, TaskStatus};

pub(crate) struct TaskElementBehavior;

impl TaskElementBehavior {
    /// The side-effect handler that invokes a `Task` state's `Resource`: emits only `TaskActivated`,
    /// recording the logical invocation (owning `parent` activity, `resource` URI, and projected
    /// `arguments`) in the stream. The follow-on `CompleteTask` is synthesized later by a worker's
    /// [`TaskApi::poll_tasks`](crate::task_api::TaskApi::poll_tasks) when the external call settles — so the
    /// stream advances while the task "runs" and an in-dispatch external await inside this handler
    /// can't block the cascade's serial dispatch (e.g. an external `TerminateExecution` from
    /// [`crate::Engine::cancel_execution`] must be able to reach the stream while a `Task` is in flight).
    ///
    /// The task is simply made claimable (`Pending`): a worker that serves `resource` will pull it via
    /// `TaskApi::poll_tasks`; an unserved task stays queued (Zeebe semantics) and, if the state sets
    /// `TimeoutSeconds`, the deadline eventually terminates the state that armed it. No worker-presence
    /// prediction happens here — a worker may appear at any time and claim the task.
    pub(crate) async fn activate(
        p: &ActivateTask,
        _ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let ActivateTask {
            execution,
            owner,
            task,
            resource,
            arguments,
            retry_plan,
            deadline,
        } = p;
        out.append_event(Event::TaskActivated {
            task: Task {
                // The execution anchor is carried from the command (finding #13), so the task is
                // traceable to / nameable from its root run even in a branch.
                execution: execution.clone(),
                resource: resource.clone(),
                arguments: arguments.clone(),
                status: TaskStatus::Pending,
                // The state's `TimeoutSeconds` instant, decided once by the state handler; the
                // `TimeoutSeconds` timer armed from the same value is what enforces it.
                deadline: *deadline,
                worker_id: None,
                lease_expires_at: None,
                retry_plan: retry_plan.clone(),
                retry_state: RetryState::default(),
                // Birth: `created_at == updated_at == now` (invocation moment). The `meta.name` is
                // carried from the command's task reference (already execution-based per finding
                // #13), not re-derived as `child-<uid>`; the owner is the invoking activity the
                // command's own slot names — a foreign kind cannot be built, let alone dispatched.
                meta: crate::types::meta::ObjectMeta::builder(task.uid())
                    .name(task.name().clone())
                    .at(out.now())
                    .with_owner(owner.clone()),
            },
        })
        .await;

        Ok(())
    }

    /// Handles `CompleteTask`: a worker reported its claimed task **completed** (Zeebe `CompleteJob`).
    ///
    /// A request/**response** boundary (mirroring `CreateFlow`/`CreateExecution`): the worker mints a
    /// `request_id` and the outcome is echoed back through it — the applied [`Event::TaskCompleted`] on
    /// success (an injected `Hook` observer wakes the awaiter), or a [`Reject`](crate::Reject) when the
    /// settlement guard refuses (via `Collector::reject`). Without this a `CompleteTask` would be
    /// fire-and-forget; with it, `TaskApi::complete` awaits and reports the *actual* processing result
    /// to the completing worker.
    ///
    /// Idempotent and lease-guarded: the authoritative settlement guard is that the task is currently
    /// `Running` (leased) **to the reporting `worker_id`**. A late `CompleteTask` after a cancel, a
    /// timeout, or a lease that already expired (and was re-leased to another worker) is rejected as a
    /// `Reject`, so the state advances exactly once even under Zeebe's at-least-once re-claims — and the
    /// rejected worker is told *why* instead of hitting a silent no-op.
    ///
    /// On success it emits `TaskCompleted` and hands the settle to the owning activity's container, which
    /// resumes the state via `CompleteState` (its `complete` runs the success projection and routes to
    /// `Next`/`End`); that container is resolved *before* the event is emitted, so a task whose owning
    /// activity is gone is refused rather than logged as a settle nothing can resume. A *failed*
    /// settlement belongs to `TaskElementBehavior::fail` (`Command::FailTask`), which owns the
    /// `Retry`/`Catch`/terminate policy.
    pub(crate) async fn complete(
        p: &CompleteTask,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let CompleteTask {
            request_id,
            task,
            worker_id,
            output,
        } = p;

        let act = match ctx.storage.get_task(task).await {
            Ok(Some(t)) => t,
            // A task that never existed in scope is a genuine refusal, not a silent no-op: the
            // awaiting worker must learn it settled nothing rather than hang on an unmatchable ack.
            Ok(None) => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("task {task} not found or not activated"),
                ));
            }
            // A read fault is the engine's, not the command's: returned so the leader can retry it
            // rather than answering the awaiting worker with a silent nothing.
            Err(e) => return Err(e.into()),
        };
        // Settlement guard: only a `Running` (leased) task has a settlement to take, and it must be
        // leased to the reporting worker right now. The whole guard lives in the transition itself
        // (`mark_completed`), so the caller cannot honor one half and forget the other: a task not
        // currently Running (still Pending, or already settled) and one leased to a *different* worker
        // are both refusals of the request's precondition — the caller's handle on the task is not the
        // live one — so both are `InvalidState`, told apart by their reason. Both keep the state
        // advancing exactly once (duplicate/foreign settles are refused, never applied twice).
        let mut task_value = act.clone();
        if let Err(reason) = task_value.mark_completed(worker_id, ctx.now()) {
            tracing::warn!(
                task = %act.meta.raw_object_ref(),
                reported = %worker_id,
                leased = ?act.worker_id,
                "task settlement refused: {reason}"
            );
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("task {task} cannot be completed: {reason}"),
            ));
        }

        let activity_id = act.meta.owner.clone();
        // The container is resolved *before* anything is emitted: a task's settle has no meaning apart
        // from the activity it resumes, so an ownerless settle is refused by the container's own `open`
        // (`NotFound`, naming the gone owner) — while the worker is still waiting on an answer — rather
        // than discovered as a no-op after `TaskCompleted` is already on the log, which would strand the
        // owning state with nothing left to resume it. Deliberately neither `ProcessingError` — which
        // the log reserves for a command that outlived its retry budget — nor `Unexpected`, whose retry
        // could only repeat this conclusion later: a row is never removed, so the owning activity cannot
        // come back, and the worker is blocked on the ack.
        let container = ActivityElementBehavior::open(ctx.storage, activity_id.clone()).await?;

        // Emit the completed task entity (lease cleared, status terminal) — the very value the
        // transition above advanced — and resume the owning Task state's `complete`. The concrete
        // output travels alongside (feeds the activity's raw_output). The success settlement echoes
        // the worker's own request id back on the event; the `AckHook` observer wakes the awaiting
        // `TaskApi::complete` with it (the request/response contract that lets it report this
        // settlement was applied).
        out.append_event(Event::TaskCompleted(TaskCompleted {
            request_id: *request_id,
            task: task_value,
            output: output.clone(),
        }))
        .await;
        // The activity's own `TimeoutSeconds` child needs no sweep here: the Task state declares it
        // supervisory, so the base complete step cancels it before it finishes. (A claim leaves no
        // child at all — the delivery lease is a field on the task, not a timer.)
        //
        // The settle is handed to the activity that owns the task rather than acted on here: what a
        // settled task means for its owner is the owner's business, so the child only names its owner
        // and the owner's container decides.
        container
            .after_child_completed(ctx, out, task.as_raw_object_ref())
            .await?;

        Ok(())
    }

    /// Handles `FailTask`: a claimed task was reported **failed** (Zeebe `FailJob`). A state's own
    /// `TimeoutSeconds` does not take this route — a deadline is the state's failure, not the task's, so it
    /// terminates the state itself (`TimerElementBehavior::trigger`).
    ///
    /// The handler is only orchestration: the task decides its own next life
    /// ([`Task::handle_failure`](crate::Task::handle_failure) — re-queued for a retry, or terminally
    /// failed, with the lease guard that admits the report), and a terminal failure is then handed to the
    /// owning activity's **own** terminate flow, where the state's `Catch` policy gets its say
    /// ([`StateHandler::on_failed`](crate::elements::state_handler::StateHandler::on_failed)). A retry notifies
    /// nothing: the task is simply claimable again past its backoff gate.
    pub(crate) async fn fail(
        p: &FailTask,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let FailTask {
            task,
            worker_id,
            error,
        } = p;

        let act = match ctx.storage.get_task(task).await? {
            Some(t) => t,
            // The task a report names is born in the batch that activates it and nothing ever removes a
            // row, so a miss means the report was forged into the log or the projection is corrupt — the
            // command's own precondition. A task that never activated leaves nothing to fail, and the
            // refusal is the entry this command owes either way.
            None => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("fail_task: task {task} does not exist; failure refused"),
                ));
            }
        };

        // The one transition this command performs, with its whole precondition (settled once, and
        // leased to the reporter) inside it — so a refused report writes nothing.
        let mut task_value = act.clone();
        let outcome = match task_value.handle_failure(worker_id, error, ctx.now()) {
            Ok(outcome) => outcome,
            Err(why) => {
                tracing::warn!(
                    task = %act.meta.raw_object_ref(),
                    reported = %worker_id,
                    status = ?act.status,
                    "failure reported against a task that cannot take it; refused: {why}"
                );
                return Err(ProcessingError::Rejected(
                    RejectionType::InvalidState,
                    format!(
                        "fail_task: task {} cannot take the failure reported by {worker_id}: {why}",
                        act.meta.raw_object_ref()
                    ),
                ));
            }
        };
        out.append_event(Event::TaskFailed(TaskFailed {
            task: task_value,
            error: error.clone(),
        }))
        .await;

        // A retry needs nothing more: the task is claimable again past its backoff gate, and whoever
        // re-claims it runs the next attempt. Only an exhausted failure has to reach the state, whose
        // own terminate decides `Catch`-versus-fail.
        if outcome == FailureOutcome::Terminal {
            // The slot is an `ObjectRef<ActivityKind>`, so "the owner is an activity" is a type fact,
            // not a runtime check — the task's own owner names the activity it fails.
            out.append_command(Command::TerminateState(TerminateState {
                activity: act.meta.owner.clone(),
                reason: TerminationReason::Failed {
                    error: error.clone(),
                },
            }));
        }

        Ok(())
    }

    /// Handles `CancelTask`: an in-flight `Task` is cancelled because its owning activity/execution is
    /// being torn down. Emits `TaskCancelled`, which marks the task `Cancelled` in storage and drains it
    /// from its parent; the physical work on the worker is deliberately left running (the worker owns
    /// cancellation of its own call), and a later `CompleteTask` for this task is swallowed by the
    /// `TaskElementBehavior::complete`'s non-`Running` guard — exactly the race guard a `CancelTimer` + late
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
    pub(crate) async fn cancel(
        task: &ObjectRef<TaskKind>,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let Some(mut task_value) = ctx.storage.get_task(task).await? else {
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
        let container = ActivityElementBehavior::open(ctx.storage, owner.clone()).await?;
        out.append_event(Event::TaskCancelled { task: task_value })
            .await;
        container
            .after_child_terminated(ctx, out, task.as_raw_object_ref())
            .await?;

        Ok(())
    }

    /// Handles `ClaimTasks` — the durable claim behind `TaskApi::poll_tasks` (Zeebe `ActivateJobs`).
    ///
    /// Runs in the StreamProcessor's serialized, lock-holding command arm, so discovery and leasing are
    /// decided against the same projection snapshot the fold writes. It discovers up to `max_tasks`
    /// claimable tasks of `resource` ([`Task::is_claimable_at`] — pending with backoff lapsed, or a
    /// lease that has expired) and leases each to `worker_id`, reporting one batched `TasksClaimed`
    /// **durable** response. The awaiting `poll_tasks` resolves its grant from that durable event — the
    /// discovery-time `Task` snapshot embedded in it — so the exact set is re-derived from the log,
    /// never re-read.
    ///
    /// The returned set is the handler's *discovery-time* grant (direct return, not re-read): a narrow
    /// race — a concurrent pull that discovers the same task before this one's `TasksClaimed` is applied —
    /// can hand a task to two workers. The `TasksClaimed` applier re-decides claimability per entry with
    /// the *entry's* timestamp, so the lease stake is exactly-once regardless, and the *state* never
    /// advances twice even if the *work* is at-least-once (handlers must be idempotent, Zeebe's contract).
    ///
    /// A `lease_seconds` the clock cannot represent is refused with `InvalidArgument` rather than answered
    /// with an empty set: the payload is malformed, and that is Zeebe's own classification for a bad job
    /// timeout.
    pub(crate) async fn claim(
        p: &ClaimTasks,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let ClaimTasks {
            request_id,
            worker_id,
            resource,
            max_tasks,
            lease_seconds,
        } = p;
        // Read *now* once, at the moment the lease window is decided: the persisted `lease_expires_at` is
        // what a restarted engine and every later poll reconstruct claimability from. An overflow is the
        // caller's own `lease_seconds` being unrepresentable, so it is refused rather than answered with
        // an empty set — an empty `TasksClaimed` is indistinguishable from "nothing claimable now",
        // hiding the malformed lease behind a legitimate-looking answer.
        let now = ctx.now();
        let Some(lease_expires_at) = now.checked_add(Duration::from_secs(*lease_seconds)) else {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidArgument,
                format!(
                    "worker {worker_id} asked for a {lease_seconds}s lease on {resource}, which \
                     overflows the clock; claim refused"
                ),
            ));
        };
        // Discover claimable tasks of `resource` under the command arm's storage lock — the same
        // snapshot the fold writes against — so allocation is decided against authoritative state. A
        // read that *faults* is the engine's own failure, not a domain answer: folding it into an empty
        // grant would dress a store hiccup up as "nothing claimable", so it propagates as `Unexpected`
        // — the leader retries it, and the dispatch that exhausts its attempts records the command's
        // single `Reject` (`ProcessingError`). The poll is never answered with a lie.
        let tasks = ctx
            .storage
            .activatable_tasks(resource, now, *max_tasks)
            .await?;
        let mut claimed = Vec::new();
        for t in tasks {
            let mut value = t;
            // The claim moment is the very reading the lease window was computed from, so the row's
            // transition stamp and its `lease_expires_at` share one base.
            value.claim(worker_id, lease_expires_at, now);
            claimed.push(value);
        }
        // One batched claim fact for the whole poll — every appended `ClaimTasks` answers its awaiting
        // caller with a durable `TasksClaimed` (all entries share this single causal batch).
        out.append_event(Event::TasksClaimed(TasksClaimed {
            request_id: *request_id,
            tasks: claimed,
        }))
        .await;

        Ok(())
    }
}

#[cfg(test)]
mod cancel_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use serde_json::json;
    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};

    use super::TaskElementBehavior;
    use crate::StatePath;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, ProcessingError};
    use crate::handlers::build_state_handlers;
    use crate::handlers::fixtures::{at, object_ref};
    use crate::types::event::Event;
    use crate::types::execution::ExecutionKind;
    use crate::types::flow_version::FlowVersionKind;
    use crate::types::id::EntryId;
    use crate::types::meta::HasRawObjectRef;
    use crate::types::meta::{NoOwner, ObjectMeta, ObjectRef, ThreadOwner};
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
    fn seeded_task() -> Task {
        Task {
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
        }
    }

    /// The owning activity — the row whose settle the cancel is handed to, resolved before the
    /// cancellation is written.
    fn seeded_activity(status: ActivityStatus) -> Activity {
        let mut path = jsonptr::PointerBuf::new();
        path.push_back("States");
        path.push_back("P");
        Activity {
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
        }
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
            TaskElementBehavior::cancel(&task_ref(), &mut ctx, &mut out).await?;
            Ok(out.into_entries().into_iter().map(|e| e.payload).collect())
        }

        /// A mocked store whose task read answers `task` and whose owner read answers `activity` on
        /// **both** reads the handler makes of it: `ActivityElementBehavior::open` resolves the owner, then
        /// the settle re-reads it — a case that gets past container resolution owes it two answers.
        fn store_with_owner(
            task: Result<Option<Task>, StorageError>,
            activity: Activity,
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
        fn store_without_owner(task: Result<Option<Task>, StorageError>) -> MockReadonlyStorageTxn {
            let mut store = MockReadonlyStorageTxn::new();
            store
                .expect_get_task()
                .withf(|r| r == &task_ref())
                .times(1)
                .return_once(move |_| task);
            store
        }

        /// The live rows the happy-path cases start from: a `Running` task owned by an activity that
        /// fronts the state the cancellation is settled against. The ownership edge between them lives
        /// in the store, not on the row, and the mocked store answers no child read — the settle here
        /// never asks for one.
        fn live_world() -> (Task, Activity) {
            (seeded_task(), seeded_activity(ActivityStatus::Running))
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

        /// The run the scope resolves its machine through. Its `flow_version` is what [`version_ref`]
        /// has to answer to.
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
            activity.status = ActivityStatus::Terminating(TerminationReason::Cancelled);
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

#[cfg(test)]
mod claim_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_testing::MockReadonlyStorageTxn;

    use super::TaskElementBehavior;
    use crate::StorageError;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, ProcessingError};
    use crate::handlers::build_state_handlers;
    use crate::handlers::fixtures::at;
    use crate::types::command::ClaimTasks;
    use crate::types::id::{EntryId, RequestId};
    use crate::types::reject::RejectionType;

    /// The handler exercised against a **mock** read-only store carrying **no** expectations: every
    /// read it makes panics, so the case below is itself an assertion about how far the handler got —
    /// the lease window is decided from the command's own payload, before any storage look.
    async fn claim_over(
        store: &MockReadonlyStorageTxn,
        lease_seconds: u64,
    ) -> Result<(), ProcessingError> {
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
        TaskElementBehavior::claim(
            &ClaimTasks {
                request_id: RequestId::nil(),
                worker_id: "w1".to_string(),
                resource: "service-a".to_string(),
                max_tasks: 10,
                lease_seconds,
            },
            &mut ctx,
            &mut out,
        )
        .await
    }

    /// A lease the clock cannot represent is the *command's* refusal, recorded rather than answered
    /// with an empty grant: an empty `TasksClaimed` is exactly what a poll that found nothing
    /// claimable looks like, so granting one would hide a malformed `lease_seconds` behind a
    /// legitimate answer. The classification is `InvalidArgument` because the payload itself is at
    /// fault, not the state of the tasks (Zeebe rejects a bad job timeout the same way).
    #[tokio::test]
    async fn an_unrepresentable_lease_is_the_commands_own_refusal() {
        let store = MockReadonlyStorageTxn::new();
        let err = claim_over(&store, u64::MAX)
            .await
            .expect_err("a lease the clock cannot add must be refused");
        let ProcessingError::Rejected(ty, reason) = err else {
            panic!("a malformed lease is the command's failure, not the engine's: {err:?}");
        };
        assert_eq!(ty, RejectionType::InvalidArgument);
        assert!(
            reason.contains(&u64::MAX.to_string()),
            "the refusal names the lease it was given: {reason}"
        );
        assert!(
            reason.contains("service-a"),
            "the refusal names the resource the poll was for: {reason}"
        );
    }

    /// A **faulted discovery read** is the *engine's* failure, exactly as a faulted row read is in the
    /// sibling handlers: it must not be folded into an empty grant, which would be indistinguishable
    /// from a poll that legitimately found nothing claimable. Surfacing it as `Unexpected` is what
    /// gets the command retried and, exhausted, recorded as a `Reject` (`ProcessingError`) — the
    /// command still answers its caller, and answers truthfully.
    #[tokio::test]
    async fn a_faulted_discovery_read_surfaces_as_unexpected() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_activatable_tasks()
            .times(1)
            .return_once(|_, _, _| {
                Err(StorageError::Backend("injected storage fault".to_string()))
            });
        let err = claim_over(&store, 30)
            .await
            .expect_err("a store that cannot read is not the command's failure");
        assert!(
            matches!(err, ProcessingError::Unexpected(_)),
            "a discovery read fault is the engine's, never an empty grant: {err:?}"
        );
    }
}
