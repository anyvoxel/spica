use serde_json::Value;

use super::Container;
use crate::handler::{Collector, HandlerContext};
use crate::storage::ReadonlyStorageTxn;
use crate::types::activity::ActivityKind;
use crate::types::command::{Command, CompleteThread, TerminateThread};
use crate::types::meta::{HasRawObjectRef, ObjectRef, RawObjectRef};
use crate::types::thread::{ThreadKind, ThreadStatus};
use crate::{ActivityStatus, StorageError};

/// The container for a `Thread`'s children — the activity states that run inside one scope.
pub(crate) struct ThreadContainer {
    thread: ObjectRef<ThreadKind>,
}

impl ThreadContainer {
    /// The half both outcomes share: a thread that is already finishing and has just drained its last
    /// child advances its own finish. Returns `true` when it emitted the Continue command, so the
    /// caller knows the outcome-specific arm has nothing left to do.
    fn advance_if_drained(
        &self,
        out: &mut Collector<'_>,
        thread: &crate::storage::ThreadRecord,
    ) -> bool {
        if !thread.active_children.is_empty() {
            return false; // more children in flight — the last one to settle advances the finish.
        }
        match &thread.value.status {
            ThreadStatus::Completing => {
                out.append_command(Command::ContinueComplete {
                    owner: self.thread.as_raw_object_ref().clone(),
                });
                true
            }
            ThreadStatus::Terminating(_) => {
                out.append_command(Command::ContinueTerminate {
                    owner: self.thread.as_raw_object_ref().clone(),
                });
                true
            }
            _ => false,
        }
    }
}

impl Container for ThreadContainer {
    type Owner = ThreadKind;

    async fn open(
        storage: &dyn ReadonlyStorageTxn,
        owner: ObjectRef<Self::Owner>,
    ) -> Result<Option<Self>, StorageError> {
        // See `ActivityContainer::open`: the call site's owner kind is this impl's own parameter type.
        if storage.get_thread(&owner).await?.is_none() {
            return Ok(None);
        }
        Ok(Some(Self { thread: owner }))
    }

    async fn after_child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) {
        let Some(thread) = ctx.storage.get_thread(&self.thread).await.ok().flatten() else {
            return; // gone already — nothing to advance.
        };
        if self.advance_if_drained(out, &thread) {
            return;
        }
        if !thread.value.status.is_running() {
            return; // finishing with children still attached: some other settle owns the drain.
        }
        // A terminal state (`Succeed`) is a thread's only natural end: its `Completed` settle completes
        // the thread, the output read off the settled child's own row — the single source of truth —
        // rather than carried on the call. A thread already past `Running` is mid-sweep by its own
        // chain and needs no second completion.
        let Some(act) = ctx
            .storage
            .get_activity(&child.clone().typed::<ActivityKind>())
            .await
            .ok()
            .flatten()
        else {
            return; // the settled child is already gone — nothing to read an output from.
        };
        let ActivityStatus::Completed = act.value.status else {
            return; // not a success terminal (e.g. a `Terminated` child): nothing to complete with.
        };
        // A `Completed` child under a running thread is the scope's success, so it completes the
        // thread with the child's output. A terminal `Succeed` that projects no value carries a
        // `Null` result, which storage folds as no `output` at all — hence the `Null` fallback, so a
        // null-returning terminal still completes its thread (a none-event would strand it).
        let output = act.value.output.clone().unwrap_or(Value::Null);
        out.append_command(Command::CompleteThread(CompleteThread {
            thread: self.thread.clone(),
            output,
        }));
    }

    /// A state activity under this thread reached an abnormal terminal (a leaf `Fail`). The thread is
    /// the scope that activity ran in, so it is taken down with the same reason the state failed
    /// with — read off the settled child's own row, the single source of truth, rather than carried
    /// on the call. A thread already past `Running` is mid-sweep by its own chain and needs no second
    /// termination.
    async fn after_child_terminated(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) {
        let Some(thread) = ctx.storage.get_thread(&self.thread).await.ok().flatten() else {
            return; // gone already — nothing to terminate.
        };
        if self.advance_if_drained(out, &thread) {
            return;
        }
        if !thread.value.status.is_running() {
            return; // finishing with children still attached: some other settle owns the drain.
        }
        // A thread's only child kind is `Activity` (see `TerminateThreadHandler`), and only an
        // *abnormal* terminal — `Terminated` — propagates up as a thread failure; a normal `Completed`
        // settle is the thread's own advance, not its end.
        let Some(act) = ctx
            .storage
            .get_activity(&child.clone().typed::<ActivityKind>())
            .await
            .ok()
            .flatten()
        else {
            return; // the settled child is already gone — nothing to read a reason from.
        };
        let ActivityStatus::Terminated(reason) = &act.value.status else {
            return; // not an abnormal terminal (e.g. a `Completed` child): nothing to propagate.
        };
        out.append_command(Command::TerminateThread(TerminateThread {
            thread: self.thread.clone(),
            reason: reason.clone(),
        }));
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
    use crate::StatePath;
    use crate::StorageError;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, OverlaySink};
    use crate::handlers::dispatch::build_state_handlers;
    use crate::handlers::fixtures::{at, object_ref};
    use crate::storage::{ActivityRecord, Storage, ThreadRecord};
    use crate::types::activity::{ActivityKind, ActivityStatus};
    use crate::types::command::{Command, CompleteThread, TerminateThread, TerminationReason};
    use crate::types::execution::ExecutionKind;
    use crate::types::id::EntryId;
    use crate::types::meta::{HasRawObjectRef, ObjectMeta, ObjectRef, ThreadOwner};
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
            .expect("the store reads cleanly")
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
                    .await;
            } else {
                container
                    .after_child_completed(&mut ctx, &mut out, &child_ref().into_raw_object_ref())
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
            ThreadContainer::open(&work, thread_ref())
                .await
                .expect("the store reads cleanly")
                .is_none(),
            "a missing thread row must not yield a container"
        );
    }

    /// A read that **faults** is `Err`, never `None` (see `ActivityContainer`'s sibling test).
    #[tokio::test]
    async fn a_faulted_owner_read_is_not_a_missing_owner() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_thread()
            .times(1)
            .return_once(|_| Err(StorageError::Backend("injected storage fault".to_string())));
        assert!(
            ThreadContainer::open(&store, thread_ref()).await.is_err(),
            "a fault must surface, not read as a missing row"
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

    /// A thread already finishing is mid-teardown by its own chain — handing it a second termination
    /// would only be refused, so the container holds back.
    #[tokio::test]
    async fn a_terminated_child_leaves_a_finishing_thread_alone() {
        let chain = drive(
            seeded_thread(ThreadStatus::Terminating(TerminationReason::Cancelled), 1),
            Some(seeded_child(ActivityStatus::Terminated(
                TerminationReason::Cancelled,
            ))),
            true,
        )
        .await;
        assert!(
            chain.is_empty(),
            "a finishing thread has no second termination to take: {chain:?}"
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

    /// A `Completing` thread whose last child just drained advances its own finish.
    #[tokio::test]
    async fn a_settled_child_advances_a_drained_completing_thread() {
        let chain = drive(
            seeded_thread(ThreadStatus::Completing, 0),
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

    /// A `Completing` thread with a child still in flight emits nothing: the last settle to land is
    /// the one that advances it.
    #[tokio::test]
    async fn a_settled_child_leaves_an_undrained_completing_thread_alone() {
        let chain = drive(
            seeded_thread(ThreadStatus::Completing, 1),
            Some(seeded_child(ActivityStatus::Completed)),
            false,
        )
        .await;
        assert!(
            chain.is_empty(),
            "undrained Completing thread must wait: {chain:?}"
        );
    }

    /// A terminated child drains a `Terminating` owner into its ContinueTerminate.
    #[tokio::test]
    async fn a_terminated_child_advances_a_drained_terminating_thread() {
        let chain = drive(
            seeded_thread(ThreadStatus::Terminating(TerminationReason::Cancelled), 0),
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
}
