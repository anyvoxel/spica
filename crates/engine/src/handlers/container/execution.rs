use super::Container;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::storage::ReadonlyStorageTxn;
use crate::types::command::{Command, CompleteExecution, TerminateExecution, TerminationReason};
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::execution::ExecutionKind;
use crate::types::meta::{HasRawObjectRef, ObjectKind, ObjectRef, RawObjectRef};
use crate::types::thread::ThreadKind;
use crate::{ExecutionStatus, RejectionType};

/// The container for an `Execution`'s children.
///
/// A run owns two kinds of child: its **root thread** (the run's own top-level scope) and its own
/// `TimeoutSeconds` timer (see `create_execution`). There is no state-specific half — a run has no
/// container state to replenish — so the hooks only decide what an individual settle *means* to a
/// still-`Running` run: its scope going down abnormally starts the run's teardown, and its scope
/// finishing successfully closes it. A run already winding down instead converges through its own
/// `Completing`/`Terminating` to decide.
pub(crate) struct ExecutionContainer {
    execution: ObjectRef<ExecutionKind>,
}

impl Container for ExecutionContainer {
    type Owner = ExecutionKind;

    async fn open(
        storage: &dyn ReadonlyStorageTxn,
        owner: ObjectRef<Self::Owner>,
    ) -> Result<Self, ProcessingError> {
        // The run is the seam's own existence check (see `Container::open`): a run that is gone is
        // refused here, uniformly for every caller whose settle would have nothing to land on.
        if storage.get_execution(&owner).await?.is_none() {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("execution_container: execution {owner} is gone"),
            ));
        }
        Ok(Self { execution: owner })
    }

    async fn after_child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        // The run was resolved *before* the settle's own terminal event was written, and this re-read
        // runs in that same batch — so a row that is gone here is the lifecycle disagreeing with the log
        // rather than a settle to ignore. Refused, never no-op'ed: the terminal is already on this batch,
        // and a silent success would leave it unexplained. A fault is the leader's to retry.
        let Some(exec) = ctx.storage.get_execution(&self.execution).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "execution_container: execution {} is gone from under its settled child",
                    self.execution
                ),
            ));
        };

        // Every cell below is one `(child kind, run status)` pair, and the pair is the whole decision: a
        // run owns exactly two kinds of child — its root thread and its own `TimeoutSeconds` timer — and
        // a settle means something different to each status. Whether the run has *drained* is not checked
        // here: a `Continue*` cell names the run this settle is advancing, and that command's own handler
        // takes down anything still attached and stops short of advancing, so the settle that finally
        // drains the run is the one that advances it.
        match (child.kind, &exec.status) {
            // The deadline fired: the run goes down `TimedOut` rather than closing. Only a *fire*
            // reaches this hook; a cancelled timer takes `after_child_terminated`.
            (ObjectKind::Timer, ExecutionStatus::Running) => {
                out.append_command(Command::TerminateExecution(TerminateExecution {
                    name: self.execution.name().clone(),
                    uid: Some(self.execution.uid()),
                    reason: TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::TimedOut {
                            // The run's own row carries the instant the definition's `TimeoutSeconds`
                            // resolved to — the same one its timer was armed from, so the reason names
                            // the deadline without a second read of the timer the fire has just
                            // detached.
                            message: format!(
                                "execution ran past its TimeoutSeconds deadline ({})",
                                exec.value.deadline.map(|d| d.as_millis()).unwrap_or(0)
                            ),
                        }),
                    },
                }));
                Ok(())
            }
            // The run's own top-level scope finished, so the run closes on its output — folded onto the
            // thread's row by the `ThreadCompleted` applier in this same batch.
            (ObjectKind::Thread, ExecutionStatus::Running) => {
                let output = match ctx
                    .storage
                    .get_thread(&child.clone().typed::<ThreadKind>())
                    .await
                {
                    Ok(Some(thread)) => thread
                        .value
                        .output
                        .clone()
                        .unwrap_or(serde_json::Value::Null),
                    _ => serde_json::Value::Null,
                };
                out.append_command(Command::CompleteExecution(CompleteExecution {
                    execution: self.execution.clone(),
                    output,
                }));
                Ok(())
            }
            // Already finishing: this settle is what advances the run, whether it was a `Completing`
            // run's cancelled deadline landing or a `Terminating` run's swept child. A deadline that
            // fires here is late, not decisive: a run already closing does not reopen as a timeout.
            (ObjectKind::Timer, ExecutionStatus::Completing) => {
                out.append_command(Command::ContinueComplete {
                    owner: self.execution.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            (ObjectKind::Timer | ObjectKind::Thread, ExecutionStatus::Terminating(_)) => {
                out.append_command(Command::ContinueTerminate {
                    owner: self.execution.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            // A run enters `Completing` only through `CompleteExecution`, which the root thread's own
            // settle issues — and which detaches that thread — so a second settling root thread would be
            // a second root scope. The row and the lifecycle disagree, and the batch is refused rather
            // than advanced.
            (ObjectKind::Thread, ExecutionStatus::Completing) => Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "execution_container: execution {} is completing with a settling root thread {child}",
                    self.execution
                ),
            )),
            // A terminal run has no child left to settle, and a run owns no third kind: either way the
            // settle has nothing to mean.
            (kind, status) => Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "execution_container: execution {} settled a {kind} child {child} while {status:?}",
                    self.execution
                ),
            )),
        }
    }

    /// The abnormal-terminal reaction. A run's only termination-triggering child is its **root
    /// thread**: that thread *is* the run's own top-level scope, so its abnormal terminal is the
    /// run's, and a run still `Running` has no teardown of its own to advance — this settle is what
    /// must start one. Every other cell is the success hook's, reached through the same pair.
    async fn after_child_terminated(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        let Some(exec) = ctx.storage.get_execution(&self.execution).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "execution_container: execution {} is gone from under its settled child",
                    self.execution
                ),
            ));
        };
        match (child.kind, &exec.status) {
            // The reason is read off the settled thread's own status, exactly as the run's *output* is
            // read off it on the success path: the teardown the run begins then carries the reason its
            // own top-level scope went down with, rather than one the container would have to invent.
            (ObjectKind::Thread, ExecutionStatus::Running) => {
                let reason = match ctx
                    .storage
                    .get_thread(&child.clone().typed::<ThreadKind>())
                    .await
                {
                    Ok(Some(thread)) => thread.value.status.termination_reason().cloned(),
                    _ => None,
                };
                let Some(reason) = reason else {
                    tracing::debug!(
                        execution = %self.execution,
                        child = %child,
                        "settled thread carries no termination reason; no reaction"
                    );
                    return Ok(());
                };
                out.append_command(Command::TerminateExecution(TerminateExecution {
                    name: self.execution.name().clone(),
                    uid: Some(self.execution.uid()),
                    reason,
                }));
                Ok(())
            }
            // A run's deadline is cancelled only by its own finish or by a teardown, so an abnormal
            // settle of it under a still-`Running` run asks for none: a teardown sweeps the deadline,
            // it never starts one.
            (ObjectKind::Timer, ExecutionStatus::Running) => {
                tracing::debug!(
                    execution = %self.execution,
                    child = %child,
                    "child terminated under a Running execution; no reaction"
                );
                Ok(())
            }
            // Already finishing: this settle is what advances the run, whatever state the run was in
            // when it began to finish and whether or not anything else is still attached — see the
            // drain note on `after_child_completed`.
            (ObjectKind::Timer, ExecutionStatus::Completing) => {
                out.append_command(Command::ContinueComplete {
                    owner: self.execution.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            (ObjectKind::Timer | ObjectKind::Thread, ExecutionStatus::Terminating(_)) => {
                out.append_command(Command::ContinueTerminate {
                    owner: self.execution.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            // A run enters `Completing` only through `CompleteExecution`, which the root thread's own
            // settle issues — and which detaches that thread — so a second settling root thread would be
            // a second root scope. The row and the lifecycle disagree, and the batch is refused rather
            // than advanced.
            (ObjectKind::Thread, ExecutionStatus::Completing) => Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "execution_container: execution {} is completing with a settling root thread {child}",
                    self.execution
                ),
            )),
            // A terminal run has no child left to settle, and a run owns no third kind: either way the
            // settle has nothing to mean.
            (kind, status) => Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "execution_container: execution {} settled a {kind} child {child} while {status:?}",
                    self.execution
                ),
            )),
        }
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
    use crate::RejectionType;
    use crate::StatePath;
    use crate::StorageError;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, OverlaySink, ProcessingError};
    use crate::handlers::dispatch::build_state_handlers;
    use crate::handlers::fixtures::{at, object_ref};
    use crate::storage::{ExecutionRecord, ReadonlyStorageTxn, Storage, ThreadRecord};
    use crate::types::command::{Command, CompleteExecution, TerminationReason};
    use crate::types::error::{ExecutionError, RuntimeError};
    use crate::types::execution::{Execution, ExecutionKind, ExecutionStatus};
    use crate::types::flow_version::FlowVersionKind;
    use crate::types::id::EntryId;
    use crate::types::meta::{
        HasRawObjectRef, NoOwner, ObjectMeta, ObjectRef, RawObjectRef, ThreadOwner,
    };
    use crate::types::thread::{Thread, ThreadKind, ThreadStatus};
    use crate::types::timer::TimerKind;
    use crate::working::WorkingState;

    /// The run whose own children settle — its `TimeoutSeconds` deadline.
    fn execution_ref() -> ObjectRef<ExecutionKind> {
        object_ref("execution", 70)
    }

    /// The child a run hands over — its deadline, the only kind it owns.
    fn timer_ref() -> ObjectRef<TimerKind> {
        object_ref("deadline", 200)
    }

    /// The run's *other* child kind — its root thread, whose settle is what closes the run.
    fn thread_ref() -> ObjectRef<ThreadKind> {
        object_ref("execution-1", 80)
    }

    fn state_path() -> StatePath {
        let mut p = jsonptr::PointerBuf::new();
        p.push_back("States");
        p.push_back("P");
        StatePath::from(p)
    }

    /// The run's root thread, finished with `output` — the row the success cell reads the run's own
    /// output back off of.
    fn seeded_thread(output: serde_json::Value) -> ThreadRecord {
        let thread = Thread {
            meta: ObjectMeta::builder(thread_ref().uid())
                .name(thread_ref().name().clone())
                .at(at())
                .with_owner(ThreadOwner::Execution(execution_ref())),
            execution: execution_ref(),
            state_path: state_path(),
            start_at: "P".to_string(),
            index: 0,
            status: ThreadStatus::Completed,
            input: json!({ "in": 1 }),
            output: Some(output),
        };
        let mut row = ThreadRecord::from_value(thread, HashSet::new());
        row.born(at());
        row
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
                    .await
                    .expect("the hook relays the settle");
            } else {
                container
                    .after_child_completed(&mut ctx, &mut out, timer_ref().as_raw_object_ref())
                    .await
                    .expect("the hook relays the settle");
            }
        }
        out.into_entries()
            .into_iter()
            .map(|entry| entry.payload)
            .collect()
    }

    /// Drive the **completed** hook against a mock store answering the reads the cell needs, with the
    /// container built directly: the shapes that need this (a vanished row, a `Thread` child, a
    /// terminal run) are the ones `open` cannot hand a container back for. Returns the hook's own
    /// outcome beside the chain it emitted, so a refusal and a command are asserted the same way.
    async fn hook_over(
        store: &dyn ReadonlyStorageTxn,
        child: &RawObjectRef,
    ) -> (Result<(), ProcessingError>, Vec<EntryPayload>) {
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(at()));
        let ids: Arc<dyn IdGenerator> = Arc::new(CountingIdGenerator::new());
        let mut out = Collector::new(EntryId::new(1), None, clock.clone(), ids.clone());
        let mut env = EvalEnv::new();
        let mut definitions = HashMap::new();
        let state_handlers = build_state_handlers();
        let container = ExecutionContainer {
            execution: execution_ref(),
        };
        let mut ctx = HandlerContext {
            env: &mut env,
            storage: store,
            clock,
            ids,
            definitions: &mut definitions,
            state_handlers: &state_handlers,
        };
        let result = container
            .after_child_completed(&mut ctx, &mut out, child)
            .await;
        let chain = out
            .into_entries()
            .into_iter()
            .map(|entry| entry.payload)
            .collect();
        (result, chain)
    }

    /// A settle with no live owner is refused up front: `open` names the gone owner, so a caller whose
    /// settle would have nothing to land on answers the absence before it writes its terminal.
    #[tokio::test]
    async fn a_missing_owner_has_no_container() {
        let store = InMemoryStorage::new();
        let work = WorkingState::new(store.begin_txn().expect("the in-memory store begins a txn"));
        let Err(ProcessingError::Rejected(ty, reason)) =
            ExecutionContainer::open(&work, execution_ref()).await
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
    async fn a_faulted_execution_read_is_not_a_missing_owner() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_execution()
            .times(1)
            .return_once(|_| Err(StorageError::Backend("injected storage fault".to_string())));
        assert!(
            matches!(
                ExecutionContainer::open(&store, execution_ref()).await,
                Err(ProcessingError::Unexpected(_))
            ),
            "a fault must surface as Unexpected, not read as a missing row"
        );
    }

    /// A run that is gone from under its settled child refuses the settle instead of no-op'ing it: the
    /// terminal is already on this attempt's batch, so a silent success would leave the log saying the
    /// child settled with nothing after it. The container is built directly — `open` refuses the same
    /// missing row before any hook could be reached.
    #[tokio::test]
    async fn a_vanished_execution_refuses_the_settle() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_execution()
            .times(1)
            .return_once(|_| Ok(None));
        let result = hook_over(&store, timer_ref().as_raw_object_ref()).await.0;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a vanished run must refuse the settle: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("is gone from under its settled child"),
            "the refusal names what it could not relay to: {reason}"
        );
    }

    /// A run cannot be `Completing` with its root thread still settling: a run enters `Completing` only
    /// through `CompleteExecution`, which *that* thread's own settle issues — so a second settling thread
    /// would be a second root scope, and a run has the one. The lifecycle disagreeing with the row is a
    /// refusal, not a crash: the command that carried this settle is answered by a `Reject` entry.
    #[tokio::test]
    async fn a_settling_root_thread_under_a_completing_execution_is_refused() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_execution()
            .times(1)
            .return_once(|_| Ok(Some(seeded_execution(ExecutionStatus::Completing, 0))));
        let result = hook_over(&store, thread_ref().as_raw_object_ref()).await.0;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a second settling root thread must be refused: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("settling root thread"),
            "the refusal names the impossible pair: {reason}"
        );
    }

    /// A `Running` run's deadline firing is a timeout, not a close: the run goes down `TimedOut` with a
    /// reason naming its own `TimeoutSeconds` deadline rather than the timer that carried the news.
    #[tokio::test]
    async fn a_fired_deadline_terminates_a_running_execution() {
        let chain = settle_execution(seeded_execution(ExecutionStatus::Running, 0), false).await;
        let [EntryPayload::Command(Command::TerminateExecution(terminate))] = &chain[..] else {
            panic!("a fired deadline must terminate the run: {chain:?}");
        };
        assert_eq!(terminate.uid, Some(execution_ref().uid()));
        assert!(
            matches!(
                &terminate.reason,
                TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::TimedOut { .. })
                }
            ),
            "the run must go down as `TimedOut`: {:?}",
            terminate.reason
        );
    }

    /// A `Running` run's root thread finishing closes the run on that thread's own output, the same
    /// place the run's deadline reason is read from on the failure path.
    #[tokio::test]
    async fn a_finished_root_thread_completes_a_running_execution() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_execution()
            .times(1)
            .return_once(|_| Ok(Some(seeded_execution(ExecutionStatus::Running, 0))));
        store
            .expect_get_thread()
            .times(1)
            .return_once(|_| Ok(Some(seeded_thread(json!({ "answer": 42 })))));
        let (result, chain) = hook_over(&store, thread_ref().as_raw_object_ref()).await;
        result.expect("the hook closes the run on its thread's output");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::CompleteExecution(
                CompleteExecution {
                    execution: execution_ref(),
                    output: json!({ "answer": 42 }),
                }
            ))]
        );
    }

    /// A settle under a run that already reached a terminal is the row disagreeing with the lifecycle —
    /// a terminal run has no child left to settle — so it is refused rather than answered with a no-op
    /// the log would have no way to explain.
    #[tokio::test]
    async fn a_settle_under_a_terminal_execution_is_refused() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_execution()
            .times(1)
            .return_once(|_| Ok(Some(seeded_execution(ExecutionStatus::Completed, 0))));
        let result = hook_over(&store, timer_ref().as_raw_object_ref()).await.0;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a settle under a terminal run must be refused: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("while Completed"),
            "the refusal names the status the settle cannot mean anything to: {reason}"
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

    /// A run that is already finishing advances on **every** settle, drained or not: whether the drain
    /// is complete is the `Continue*` hop's own business — it takes down whatever is still attached and
    /// stops short of advancing — so an early `Continue` costs a hop, never the run's convergence.
    #[tokio::test]
    async fn a_settled_child_advances_a_completing_execution_before_the_drain() {
        let chain = settle_execution(seeded_execution(ExecutionStatus::Completing, 1), false).await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueComplete {
                owner: execution_ref().into_raw_object_ref(),
            })]
        );
    }

    /// The terminated twin of the above — the shape a teardown takes when a run still holds both its
    /// children: the root thread's settle and the deadline's settle each advance the run, and the hop
    /// that lands on the drained run is the one that terminates it.
    #[tokio::test]
    async fn a_settled_child_advances_a_terminating_execution_before_the_drain() {
        let chain = settle_execution(
            seeded_execution(
                ExecutionStatus::Terminating(TerminationReason::Cancelled),
                1,
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
