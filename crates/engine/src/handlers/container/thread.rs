use serde_json::Value;

use super::Container;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::storage::ReadonlyStorageTxn;
use crate::types::activity::ActivityKind;
use crate::types::command::{Command, CompleteThread, TerminateThread};
use crate::types::meta::{HasRawObjectRef, ObjectRef, RawObjectRef};
use crate::types::thread::{ThreadKind, ThreadStatus};
use crate::{ActivityStatus, RejectionType};

/// The container for a `Thread`'s children — the activity states that run inside one scope.
pub(crate) struct ThreadContainer {
    thread: ObjectRef<ThreadKind>,
}

impl Container for ThreadContainer {
    type Owner = ThreadKind;

    async fn open(
        storage: &dyn ReadonlyStorageTxn,
        owner: ObjectRef<Self::Owner>,
    ) -> Result<Self, ProcessingError> {
        // The thread is the seam's own existence check (see `Container::open`): a thread that is gone is
        // refused here, uniformly for every caller whose settle would have nothing to land on.
        if storage.get_thread(&owner).await?.is_none() {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("thread_container: thread {owner} is gone"),
            ));
        }
        Ok(Self { thread: owner })
    }

    // Both hooks decide on one pair: *how the child settled* (which hook) and *where the thread
    // stands* (its status). A thread's child is always an `Activity` (see `TerminateThreadHandler`), so
    // the child's kind adds no choice — the table is the same in both hooks, only the `Running` arm
    // differs, since a thread still running has to be told what the settle *means* while one already
    // finishing only has to be advanced. Whether the thread has *drained* is not checked here: a
    // `Continue*` cell names the thread this settle advances, and that command's own handler takes down
    // anything still attached and stops short of advancing, so the settle that drains the thread last is
    // the one that advances it.

    async fn after_child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        // The thread was resolved *before* the settle's own terminal event was written, and this re-read
        // runs in that same batch — so a row that is gone here is the lifecycle disagreeing with the log
        // rather than a settle to ignore. Refused, never no-op'ed: the terminal is already on this batch,
        // and a silent success would leave it unexplained. A fault is the leader's to retry.
        let Some(thread) = ctx.storage.get_thread(&self.thread).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "thread_container: thread {} is gone from under its settled child",
                    self.thread
                ),
            ));
        };
        match &thread.value.status {
            // The scope's own success: a `Succeed` is a thread's only natural end, so the thread
            // completes on the settled child's output — read off that child's own row, the single source
            // of truth, rather than carried on the call. A terminal that projects no value folds as no
            // `output` at all, hence the `Null` fallback: a null-returning terminal still completes its
            // thread, where a none-event would strand it.
            ThreadStatus::Running => {
                let output = ctx
                    .storage
                    .get_activity(&child.clone().typed::<ActivityKind>())
                    .await?
                    .map(|act| act.value.output.clone().unwrap_or(Value::Null))
                    .unwrap_or(Value::Null);
                out.append_command(Command::CompleteThread(CompleteThread {
                    thread: self.thread.clone(),
                    output,
                }));
                Ok(())
            }
            // Already finishing: this settle advances the thread, whether it was a `Completing`
            // thread's bystander landing or a `Terminating` one's swept child.
            ThreadStatus::Completing => {
                out.append_command(Command::ContinueComplete {
                    owner: self.thread.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            ThreadStatus::Terminating(_) => {
                out.append_command(Command::ContinueTerminate {
                    owner: self.thread.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            // A terminal thread has no child left to settle: the row and the lifecycle disagree, and the
            // batch is refused rather than advanced.
            status => Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "thread_container: thread {} settled a completed child {child} while {status:?}",
                    self.thread
                ),
            )),
        }
    }

    /// A state activity under this thread reached an abnormal terminal (a leaf `Fail`, or a teardown's
    /// `Cancelled` sweep) — the same table as the success hook, so the thread is taken down with the
    /// reason its child failed with while it is still `Running`, and advanced when it is already
    /// finishing.
    async fn after_child_terminated(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        let Some(thread) = ctx.storage.get_thread(&self.thread).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "thread_container: thread {} is gone from under its settled child",
                    self.thread
                ),
            ));
        };
        match &thread.value.status {
            // The thread is the scope the failed state ran in, so it goes down with the same reason —
            // read off the settled child's own row, the single source of truth, rather than carried on
            // the call. An abnormal hook whose child is not in an abnormal terminal, or whose row is
            // already gone, has no reason to propagate: a running thread is left alone rather than taken
            // down with a reason this container would have to invent.
            ThreadStatus::Running => {
                let reason = match ctx
                    .storage
                    .get_activity(&child.clone().typed::<ActivityKind>())
                    .await?
                {
                    Some(act) => match &act.value.status {
                        ActivityStatus::Terminated(reason) => Some(reason.clone()),
                        _ => None,
                    },
                    None => None,
                };
                let Some(reason) = reason else {
                    tracing::debug!(
                        thread = %self.thread,
                        child = %child,
                        "settled child carries no termination reason; no reaction"
                    );
                    return Ok(());
                };
                out.append_command(Command::TerminateThread(TerminateThread {
                    thread: self.thread.clone(),
                    reason,
                }));
                Ok(())
            }
            ThreadStatus::Completing => {
                out.append_command(Command::ContinueComplete {
                    owner: self.thread.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            ThreadStatus::Terminating(_) => {
                out.append_command(Command::ContinueTerminate {
                    owner: self.thread.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            status => Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "thread_container: thread {} settled a terminated child {child} while {status:?}",
                    self.thread
                ),
            )),
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
    use spica_testing::MockReadonlyStorageTxn;

    use super::{Container, ThreadContainer};
    use crate::RejectionType;
    use crate::StatePath;
    use crate::StorageError;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, OverlaySink, ProcessingError};
    use crate::handlers::dispatch::build_state_handlers;
    use crate::handlers::fixtures::{at, object_ref};
    use crate::storage::{ActivityRecord, ReadonlyStorageTxn, Storage, ThreadRecord};
    use crate::types::activity::{ActivityKind, ActivityStatus};
    use crate::types::command::{Command, CompleteThread, TerminateThread, TerminationReason};
    use crate::types::execution::ExecutionKind;
    use crate::types::id::EntryId;
    use crate::types::meta::{HasRawObjectRef, ObjectMeta, ObjectRef, RawObjectRef, ThreadOwner};
    use crate::types::thread::{Thread, ThreadKind, ThreadStatus};
    use crate::types::timer::TimerKind;
    use crate::working::WorkingState;
    use crate::{Activity, EntryPayload};

    fn thread_ref() -> ObjectRef<ThreadKind> {
        object_ref("execution-1", 80)
    }

    fn execution_ref() -> ObjectRef<ExecutionKind> {
        object_ref("execution", 70)
    }

    /// The settled child handed to the container — a one-state activity under the thread.
    fn child_ref() -> ObjectRef<ActivityKind> {
        object_ref("execution-0", 90)
    }

    fn state_path() -> StatePath {
        let mut p = jsonptr::PointerBuf::new();
        p.push_back("States");
        p.push_back("P");
        StatePath::from(p)
    }

    /// A seeded thread row with `children` live children still attached (any ref is fine here — the
    /// drain decision reads the set, not the members).
    fn seeded_thread(status: ThreadStatus, children: usize) -> ThreadRecord {
        let thread = Thread {
            meta: ObjectMeta::builder(thread_ref().uid())
                .name(thread_ref().name().clone())
                .at(at())
                .with_owner(ThreadOwner::Execution(execution_ref())),
            execution: execution_ref(),
            state_path: state_path(),
            start_at: "P".to_string(),
            index: 0,
            status,
            input: json!({ "in": 1 }),
            output: None,
        };
        let live = (0..children)
            .map(|n| object_ref::<TimerKind>("deadline", 200 + n as u64).into_raw_object_ref())
            .collect::<HashSet<_>>();
        let mut row = ThreadRecord::from_value(thread, live);
        row.born(at());
        row
    }

    /// A settled child activity row owned by [`thread_ref`]-shaped owner slot — the row the
    /// terminated hook reads the failure's reason back off of.
    fn seeded_child(status: ActivityStatus) -> ActivityRecord {
        let activity = Activity {
            meta: ObjectMeta::builder(child_ref().uid())
                .name(child_ref().name().clone())
                .at(at())
                .with_owner(thread_ref()),
            execution: execution_ref(),
            state_path: state_path(),
            status,
            raw_input: json!({}),
            input: None,
            raw_output: None,
            output: None,
            activity_state: None,
            retry_state: None,
        };
        let mut row = ActivityRecord::from_value(activity, HashSet::new());
        row.born(at());
        row
    }

    /// Drive one hook over a working overlay seeded with the thread and (optionally) its settled
    /// child — the leader's shape, so the assertion reads the command the container emitted.
    async fn drive(
        thread: ThreadRecord,
        child: Option<ActivityRecord>,
        terminated: bool,
    ) -> Vec<EntryPayload> {
        let mut store = InMemoryStorage::new();
        store
            .put_thread(thread)
            .await
            .expect("the in-memory store seeds a thread row");
        if let Some(child) = child {
            store
                .put_activity(child)
                .await
                .expect("the in-memory store seeds the child activity row");
        }

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
        let container = ThreadContainer::open(&work, thread_ref())
            .await
            .expect("the seeded thread resolves its container");
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
                    .after_child_terminated(&mut ctx, &mut out, &child_ref().into_raw_object_ref())
                    .await
                    .expect("the hook relays the settle");
            } else {
                container
                    .after_child_completed(&mut ctx, &mut out, &child_ref().into_raw_object_ref())
                    .await
                    .expect("the hook relays the settle");
            }
        }
        out.into_entries()
            .into_iter()
            .map(|entry| entry.payload)
            .collect()
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
        let container = ThreadContainer {
            thread: thread_ref(),
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
            ThreadContainer::open(&work, thread_ref()).await
        else {
            panic!("an open that did not read a missing row");
        };
        assert_eq!(ty, RejectionType::NotFound);
        assert!(
            reason.contains("is gone"),
            "the refusal names the gone owner: {reason}"
        );
    }

    /// A read that **faults** stays a fault, never a missing owner (see `ActivityContainer`'s sibling).
    #[tokio::test]
    async fn a_faulted_owner_read_is_not_a_missing_owner() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_thread()
            .times(1)
            .return_once(|_| Err(StorageError::Backend("injected storage fault".to_string())));
        assert!(
            matches!(
                ThreadContainer::open(&store, thread_ref()).await,
                Err(ProcessingError::Unexpected(_))
            ),
            "a fault must surface as Unexpected, not read as a missing row"
        );
    }

    /// A terminated state under a still-`Running` thread is the scope's own failure: the thread is the
    /// state's scope, so it is taken down with the state's reason.
    #[tokio::test]
    async fn a_terminated_child_terminates_a_running_thread() {
        let reason = TerminationReason::Cancelled;
        let chain = drive(
            seeded_thread(ThreadStatus::Running, 0),
            Some(seeded_child(ActivityStatus::Terminated(reason.clone()))),
            true,
        )
        .await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::TerminateThread(
                TerminateThread {
                    thread: thread_ref(),
                    reason,
                }
            ))]
        );
    }

    /// A settled child that is *not* an abnormal terminal (a `Completed` state) does not take the
    /// scope down — a thread's normal advance is not its end.
    #[tokio::test]
    async fn a_completed_child_reaches_a_running_thread_without_terminating() {
        let chain = drive(
            seeded_thread(ThreadStatus::Running, 0),
            Some(seeded_child(ActivityStatus::Completed)),
            true,
        )
        .await;
        assert!(
            chain.is_empty(),
            "a completed child must not terminate its scope: {chain:?}"
        );
    }

    /// A thread gone from under its settled child refuses the settle instead of no-op'ing it: the
    /// terminal is already on this attempt's batch, so a silent success would leave the log saying the
    /// child settled with nothing after it.
    #[tokio::test]
    async fn a_vanished_thread_refuses_the_settle() {
        let mut store = MockReadonlyStorageTxn::new();
        store.expect_get_thread().times(1).return_once(|_| Ok(None));
        let result = hook_over(&store, child_ref().as_raw_object_ref()).await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a vanished thread must refuse the settle: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("is gone from under its settled child"),
            "the refusal names what it could not relay to: {reason}"
        );
    }

    /// A settled child under a thread that is already terminal has nothing to mean — the row and the
    /// lifecycle disagree, so the batch is refused rather than advanced.
    #[tokio::test]
    async fn a_settled_child_under_a_terminal_thread_is_refused() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_thread()
            .times(1)
            .return_once(|_| Ok(Some(seeded_thread(ThreadStatus::Completed, 0))));
        let result = hook_over(&store, child_ref().as_raw_object_ref()).await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a settled child under a terminal thread must be refused: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("while Completed"),
            "the refusal names the status the thread was found in: {reason}"
        );
    }

    /// A settled child row that is already gone leaves the running thread alone — nothing to read a
    /// reason from.
    #[tokio::test]
    async fn a_terminated_child_with_no_readable_row_does_nothing() {
        let chain = drive(seeded_thread(ThreadStatus::Running, 0), None, true).await;
        assert!(
            chain.is_empty(),
            "a missing child must not terminate the scope: {chain:?}"
        );
    }

    /// A completed state under a still-`Running` thread is the scope's own success — a `Succeed` is a
    /// thread's only natural end — so the thread completes with the child's output.
    #[tokio::test]
    async fn a_completed_child_completes_a_running_thread() {
        let mut child = seeded_child(ActivityStatus::Completed);
        child.value.output = Some(json!({ "done": 1.0 }));
        let chain = drive(seeded_thread(ThreadStatus::Running, 0), Some(child), false).await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::CompleteThread(
                CompleteThread {
                    thread: thread_ref(),
                    output: json!({ "done": 1.0 }),
                }
            ))]
        );
    }

    /// A `Succeed` that projects no `Output` (input `Null`) completes the running thread with a
    /// `Null` result. The terminal's success is the thread's end regardless of whether it carried a
    /// value — a null-rejecting re-read must not strand it (durable `Succeed` resume regression).
    #[tokio::test]
    async fn a_completed_child_without_output_completes_the_running_thread_with_null() {
        let chain = drive(
            seeded_thread(ThreadStatus::Running, 0),
            Some(seeded_child(ActivityStatus::Completed)),
            false,
        )
        .await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::CompleteThread(
                CompleteThread {
                    thread: thread_ref(),
                    output: Value::Null,
                }
            ))]
        );
    }

    /// A `Completing` thread with a child still in flight still advances: the Continue names the thread
    /// this settle belongs to, and its own handler takes down whatever is left and stops short of
    /// advancing — so the settle that drains the thread last is the one that completes it.
    #[tokio::test]
    async fn a_settled_child_advances_a_completing_thread_before_the_drain() {
        let chain = drive(
            seeded_thread(ThreadStatus::Completing, 1),
            Some(seeded_child(ActivityStatus::Completed)),
            false,
        )
        .await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueComplete {
                owner: thread_ref().into_raw_object_ref(),
            })]
        );
    }

    /// A terminated child advances a `Terminating` owner into its ContinueTerminate — child still in
    /// flight or not: the Continue names the thread this settle belongs to, and its own handler takes
    /// down whatever is left and stops short of advancing, so the settle that drains the thread last is
    /// the one that terminates it.
    #[tokio::test]
    async fn a_terminated_child_advances_a_terminating_thread_before_the_drain() {
        let chain = drive(
            seeded_thread(ThreadStatus::Terminating(TerminationReason::Cancelled), 1),
            Some(seeded_child(ActivityStatus::Terminated(
                TerminationReason::Cancelled,
            ))),
            true,
        )
        .await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueTerminate {
                owner: thread_ref().into_raw_object_ref(),
            })]
        );
    }

    /// A *completed* child under a `Terminating` thread advances that teardown just the same: the
    /// thread's direction was fixed when the teardown began, and a bystander's settle does not reopen it.
    #[tokio::test]
    async fn a_completed_child_advances_a_terminating_thread() {
        let chain = drive(
            seeded_thread(ThreadStatus::Terminating(TerminationReason::Cancelled), 0),
            Some(seeded_child(ActivityStatus::Completed)),
            false,
        )
        .await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueTerminate {
                owner: thread_ref().into_raw_object_ref(),
            })]
        );
    }
}
