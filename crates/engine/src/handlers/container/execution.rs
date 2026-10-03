use super::Container;
use crate::handler::{Collector, HandlerContext};
use crate::storage::{ExecutionRecord, ReadonlyStorageTxn};
use crate::types::command::Command;
use crate::types::execution::ExecutionKind;
use crate::types::meta::{HasRawObjectRef, ObjectRef, RawObjectRef};
use crate::{ExecutionStatus, StorageError};

/// The container for an `Execution`'s children.
///
/// A run owns one kind of child: its `ExecutionTimeout` timer (see `create_execution`). There is no
/// state-specific half — a run has no container state to replenish — so both hooks route to the same
/// scope drain, and the outcome of the settled child is deliberately *not* what the run reacts to:
/// whether it converges is its own `Completing`/`Terminating` to decide.
pub(crate) struct ExecutionContainer {
    execution: ObjectRef<ExecutionKind>,
}

impl ExecutionContainer {
    /// The half both outcomes share: a run that is already finishing and has just drained its last
    /// child advances its own finish. Returns `true` when it emitted the Continue command.
    fn advance_if_drained(&self, out: &mut Collector<'_>, exec: &ExecutionRecord) -> bool {
        if !exec.active_children.is_empty() {
            return false; // more children in flight — the last one to settle advances the finish.
        }
        let owner = self.execution.as_raw_object_ref().clone();
        match &exec.status {
            ExecutionStatus::Completing => {
                out.append_command(Command::ContinueComplete { owner });
                true
            }
            ExecutionStatus::Terminating(_) => {
                out.append_command(Command::ContinueTerminate { owner });
                true
            }
            _ => false,
        }
    }

    /// The reaction both hooks share, since neither the child's kind nor its outcome decides it.
    async fn settled(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) {
        let Some(exec) = ctx
            .storage
            .get_execution(&self.execution)
            .await
            .ok()
            .flatten()
        else {
            return; // gone already — nothing to advance.
        };
        if self.advance_if_drained(out, &exec) {
            return;
        }
        if exec.status.is_running() {
            // TODO(Map/Parallel): a run's own `Running` child settle has no replenish hook — the
            // container states live under a `Thread`, so an Execution has nothing to refill yet.
            tracing::debug!(
                execution = %self.execution,
                child = %child,
                "child settled under a Running execution; no reaction"
            );
        }
    }
}

impl Container for ExecutionContainer {
    type Owner = ExecutionKind;

    async fn open(
        storage: &dyn ReadonlyStorageTxn,
        owner: ObjectRef<Self::Owner>,
    ) -> Result<Option<Self>, StorageError> {
        // See `ActivityContainer::open`: the call site's owner kind is this impl's own parameter type.
        if storage.get_execution(&owner).await?.is_none() {
            return Ok(None);
        }
        Ok(Some(Self { execution: owner }))
    }

    async fn after_child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) {
        self.settled(ctx, out, child).await;
    }

    async fn after_child_terminated(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) {
        self.settled(ctx, out, child).await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use serde_json::json;
    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_storage::InMemoryStorage;
    use spica_testing::MockReadonlyStorageTxn;

    use super::{Container, ExecutionContainer};
    use crate::EntryPayload;
    use crate::StorageError;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, OverlaySink};
    use crate::handlers::dispatch::build_state_handlers;
    use crate::handlers::fixtures::{at, object_ref};
    use crate::storage::{ExecutionRecord, Storage};
    use crate::types::command::{Command, TerminationReason};
    use crate::types::execution::{Execution, ExecutionKind, ExecutionStatus};
    use crate::types::flow_version::FlowVersionKind;
    use crate::types::id::EntryId;
    use crate::types::meta::{HasRawObjectRef, NoOwner, ObjectMeta, ObjectRef};
    use crate::types::timer::TimerKind;
    use crate::working::WorkingState;

    /// The run whose own children settle — its `ExecutionTimeout` deadline.
    fn execution_ref() -> ObjectRef<ExecutionKind> {
        object_ref("execution", 70)
    }

    /// The child a run hands over — its deadline, the only kind it owns.
    fn timer_ref() -> ObjectRef<TimerKind> {
        object_ref("deadline", 200)
    }

    /// A seeded execution row with `children` live children still attached. A run is the root of its
    /// tree, so it fills no owner slot, and its one owned kind is its own deadline timer — the seed
    /// therefore carries only the status the drain decision reads.
    fn seeded_execution(status: ExecutionStatus, children: usize) -> ExecutionRecord {
        let execution = Execution {
            meta: ObjectMeta::builder(execution_ref().uid())
                .name(execution_ref().name().clone())
                .at(at())
                .with_owner(NoOwner::new()),
            flow_version: object_ref::<FlowVersionKind>("flow-1", 60),
            status,
            deadline: None,
            input: json!({ "in": 1 }),
            output: None,
        };
        let live = (0..children)
            .map(|n| object_ref::<TimerKind>("deadline", 200 + n as u64).into_raw_object_ref())
            .collect::<HashSet<_>>();
        let mut row = ExecutionRecord::from_value(execution, live);
        row.born(at());
        row
    }

    /// Drive one hook over a working overlay seeded with a single execution row — the leader's shape,
    /// so the assertion reads the command the container emitted, not merely an intent.
    async fn settle_execution(execution: ExecutionRecord, terminated: bool) -> Vec<EntryPayload> {
        let mut store = InMemoryStorage::new();
        store
            .put_execution(execution)
            .await
            .expect("the in-memory store seeds an execution row");

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
        let container = ExecutionContainer::open(&work, execution_ref())
            .await
            .expect("the store reads cleanly")
            .expect("the seeded execution resolves its container");
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
                    .after_child_terminated(&mut ctx, &mut out, timer_ref().as_raw_object_ref())
                    .await;
            } else {
                container
                    .after_child_completed(&mut ctx, &mut out, timer_ref().as_raw_object_ref())
                    .await;
            }
        }
        out.into_entries()
            .into_iter()
            .map(|entry| entry.payload)
            .collect()
    }

    /// A settle with no live owner has no container at all: the same contract the activity impl
    /// answers, and the reason a handler resolves the run before it writes any terminal event.
    #[tokio::test]
    async fn an_execution_that_does_not_exist_has_no_container() {
        let store = InMemoryStorage::new();
        let work = WorkingState::new(store.begin_txn().expect("the in-memory store begins a txn"));
        assert!(
            ExecutionContainer::open(&work, execution_ref())
                .await
                .expect("the store reads cleanly")
                .is_none(),
            "a missing execution row must not yield a container"
        );
    }

    /// A faulted read is `Err`, never `None`, for the run impl exactly as for the activity's: a store
    /// that hiccupped must not read as a run that is gone.
    #[tokio::test]
    async fn a_faulted_execution_read_is_not_a_missing_owner() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_execution()
            .times(1)
            .return_once(|_| Err(StorageError::Backend("injected storage fault".to_string())));
        assert!(
            ExecutionContainer::open(&store, execution_ref())
                .await
                .is_err(),
            "a fault must surface, not read as a missing row"
        );
    }

    /// A settled child of a `Completing` run that has just drained advances the run's own finish — the
    /// one-hop `ContinueComplete` the generic child-settled reaction issued for it.
    #[tokio::test]
    async fn a_settled_child_advances_a_drained_completing_execution() {
        let chain = settle_execution(seeded_execution(ExecutionStatus::Completing, 0), false).await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueComplete {
                owner: execution_ref().into_raw_object_ref(),
            })]
        );
    }

    /// A terminated child of a `Terminating` run drains it the same way — and its reason is read back
    /// off the run's own status rather than carried by the child.
    #[tokio::test]
    async fn a_terminated_child_advances_a_drained_terminating_execution() {
        let chain = settle_execution(
            seeded_execution(
                ExecutionStatus::Terminating(TerminationReason::Cancelled),
                0,
            ),
            true,
        )
        .await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueTerminate {
                owner: execution_ref().into_raw_object_ref(),
            })]
        );
    }

    /// A run with a child still in flight emits nothing: the last settle to land is the one that
    /// advances it, so an earlier one must not race ahead of the drain.
    #[tokio::test]
    async fn a_settled_child_leaves_an_undrained_execution_alone() {
        let chain = settle_execution(seeded_execution(ExecutionStatus::Completing, 1), false).await;
        assert!(
            chain.is_empty(),
            "undrained Completing execution must wait: {chain:?}"
        );
    }

    /// A run still `Running` has no finish to advance, and no container state of its own to replenish
    /// — a `Running` child settle under it asks for no reaction at all.
    #[tokio::test]
    async fn a_settled_child_leaves_a_running_execution_alone() {
        let chain = settle_execution(seeded_execution(ExecutionStatus::Running, 0), true).await;
        assert!(
            chain.is_empty(),
            "a Running execution must not react: {chain:?}"
        );
    }
}
