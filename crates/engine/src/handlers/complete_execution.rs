use crate::RejectionType;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::{Command, CompleteExecution};
use crate::types::event::Event;
use crate::types::meta::ObjectKind;
use crate::types::timer::TimerKind;

/// Handles `CompleteExecution`: begins the success finish of a **top-level** `Execution` (the root
/// run, terminal `Succeed`/`End` reached). Emits `ExecutionCompleting`, which fixes its output on
/// its row, cancels any owned timers, and — once children drain (immediately if none) — emits
/// `ExecutionCompleted` then finishes.
///
/// This handler is deliberately `Execution`-only: a fan-out `Thread`'s success is driven by its own
/// [`CompleteThreadHandler`](super::complete_thread::CompleteThreadHandler), keeping the two verbs
/// (and their addressed kinds) distinct. An `Execution` is never addressed by a `Thread` here.
#[derive(Default)]
pub struct CompleteExecutionHandler;

impl CompleteExecutionHandler {
    pub(crate) async fn handle(
        &self,
        p: &CompleteExecution,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let CompleteExecution { execution, output } = p;
        // Addressed by kind: `CompleteExecution` is only ever dispatched for a top-level `Execution`,
        // so the row is read directly.
        // A fault reading the row is not a decision about this command — it is returned so the leader
        // can retry it. A *missing* row is refused, mirroring `terminate_execution`: this command is
        // the root thread's own relay (`complete_thread`), so a legitimate relay always names a row
        // that exists, and the refusal is the command's precondition failing rather than the engine's.
        // Terminating here would answer a gone row with a `TerminateExecution` for that same gone row,
        // which `terminate_execution` would refuse in turn — a ghost entry, not an outcome.
        let exec = match ctx.storage.get_execution(execution).await? {
            Some(e) => e,
            None => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("complete_execution: execution {execution} not found"),
                ));
            }
        };
        // The transition owns its own precondition: a run already finishing or terminal has no finish
        // to begin, so it is declined and left untouched. The refusal is the one followup entry this
        // command owes, and the only record that explains why it applied nothing — the relay lost a
        // race to the termination cascade (a cancel, a timeout) or an earlier finish, whose outcome
        // stands. Mirrors `terminate_execution`.
        let mut completing_execution = exec.value();
        if let Err(reason) = completing_execution.mark_completing(output.clone(), ctx.now()) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("complete_execution: execution {execution} cannot be completed: {reason}"),
            ));
        }
        // The finish is advanced in place: the terminal is the *same* value carried forward, so
        // nothing the finish fixed can go missing between the two events. The event gets a copy
        // because appending consumes it.
        out.append_event(Event::ExecutionCompleting {
            execution: completing_execution.clone(),
        })
        .await;

        // Nothing left to wait for: the run closes in the same batch that began its finish, advancing
        // the very value the finish produced, so nothing it fixed can go missing between the two events.
        let children = exec.active_children.clone();
        if children.is_empty() {
            let mut completed_execution = completing_execution;
            debug_assert!(
                completed_execution.mark_completed(ctx.now()).is_ok(),
                "the finish this step just began is Completing"
            );
            // Completion is observable durably: `start` returns the execution id and the caller's
            // `wait_for_execution` poll surfaces this terminal `ExecutionCompleted` from Storage. No
            // deferred ack is needed — terminal notification travels through the poll rather than an
            // `execution → request` ack mapping (see `Engine::wait_for_execution`).
            let completed_event = Event::ExecutionCompleted {
                execution: completed_execution,
            };
            out.append_event(completed_event).await;
            // The top-level run has no parent — `Engine::start` observes its `ExecutionCompleted`
            // directly — but a relayed finish (from a scope below) never arrives here, so no owner
            // relay is needed for a root execution.
            return Ok(());
        }

        // Something is still attached. A success finish cancels the deadlines it owns — a
        // `TimeoutSeconds` must not fire into a finished run — and waits for them to drain. It never
        // *terminates* a live child, because a run whose work is done has none: any other kind here is
        // the row's `active_children` disagreeing with the lifecycle rather than a child to wait on.
        // Refused rather than swept or ignored — sweeping turns a success into a failure cascade, and
        // ignoring closes the run with a live child. A refusal strands nothing: the child's own settle
        // drains the run through `child_settled` → `ContinueComplete`.
        for child in &children {
            match child.kind {
                ObjectKind::Timer => out.append_command(Command::CancelTimer {
                    timer: child.clone().typed::<TimerKind>(),
                }),
                kind => {
                    return Err(ProcessingError::Rejected(
                        RejectionType::InvalidState,
                        format!(
                            "complete_execution: execution {execution} still owns a live {kind} child \
                             {child}; completion refused"
                        ),
                    ));
                }
            }
        }
        tracing::debug!(
            execution = %execution,
            pending = children.len(),
            "execution completing deferred: waiting on owned children"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use serde_json::json;
    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_testing::MockReadonlyStorageTxn;

    use super::CompleteExecutionHandler;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, ProcessingError};
    use crate::handlers::dispatch::build_state_handlers;
    use crate::handlers::fixtures::{at, object_ref};
    use crate::storage::ExecutionRecord;
    use crate::types::command::{Command, CompleteExecution};
    use crate::types::event::Event;
    use crate::types::execution::{Execution, ExecutionKind, ExecutionStatus};
    use crate::types::flow_version::FlowVersionKind;
    use crate::types::id::EntryId;
    use crate::types::meta::{HasRawObjectRef, NoOwner, ObjectMeta, ObjectRef, RawObjectRef};
    use crate::types::thread::ThreadKind;
    use crate::types::timer::TimerKind;
    use crate::{EntryPayload, RejectionType};

    /// The run under completion.
    fn execution_ref() -> ObjectRef<ExecutionKind> {
        object_ref("lifecycle_execution", 70)
    }

    /// A run in `status` whose owned `children` are still attached. The output the command carries is
    /// fixed by the finish, so the seed leaves the row's own `output` unset.
    fn seeded_execution(
        status: ExecutionStatus,
        children: HashSet<RawObjectRef>,
    ) -> ExecutionRecord {
        let execution = Execution {
            meta: ObjectMeta::builder(execution_ref().uid())
                .name(execution_ref().name().clone())
                .at(at())
                .with_owner(NoOwner::new()),
            flow_version: object_ref::<FlowVersionKind>("lifecycle_flow-1", 2),
            status,
            deadline: None,
            input: json!({ "n": 1 }),
            output: None,
        };
        let mut row = ExecutionRecord::from_value(execution, children);
        row.born(at());
        row
    }

    /// Drive the handler over a **mock** store answering exactly the one read it makes. Both the
    /// outcome and the entries are returned: for a refusal, the *absence* of a terminal is as much the
    /// point as the refusal itself.
    async fn complete_over(
        row: ExecutionRecord,
    ) -> (Result<(), ProcessingError>, Vec<EntryPayload>) {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_execution()
            .times(1)
            .return_once(move |_| Ok(Some(row)));
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(at()));
        let ids: Arc<dyn IdGenerator> = Arc::new(CountingIdGenerator::new());
        let mut out = Collector::new(EntryId::new(1), None, clock.clone(), ids.clone());
        let mut env = EvalEnv::new();
        let mut definitions = HashMap::new();
        let state_handlers = build_state_handlers();
        let mut ctx = HandlerContext {
            env: &mut env,
            storage: &store,
            clock,
            ids,
            definitions: &mut definitions,
            state_handlers: &state_handlers,
        };
        let result = CompleteExecutionHandler
            .handle(
                &CompleteExecution {
                    execution: execution_ref(),
                    output: json!({ "ok": true }),
                },
                &mut ctx,
                &mut out,
            )
            .await;
        (
            result,
            out.into_entries().into_iter().map(|e| e.payload).collect(),
        )
    }

    /// A run with nothing left to wait for closes in the batch that began its finish: the terminal
    /// advances the very value the finish fixed, so the output the command carried survives.
    #[tokio::test]
    async fn a_run_with_no_children_closes_in_the_same_batch() {
        let (result, entries) =
            complete_over(seeded_execution(ExecutionStatus::Running, HashSet::new())).await;
        assert!(result.is_ok(), "a drained run completes: {result:?}");
        assert_eq!(entries.len(), 2, "the finish and its terminal: {entries:?}");
        let EntryPayload::Event(Event::ExecutionCompleting {
            execution: completing,
        }) = &entries[0]
        else {
            panic!("the finish begins first: {entries:?}");
        };
        assert_eq!(completing.status, ExecutionStatus::Completing);
        assert_eq!(completing.output, Some(json!({ "ok": true })));
        let EntryPayload::Event(Event::ExecutionCompleted {
            execution: completed,
        }) = &entries[1]
        else {
            panic!("a drained run reaches its terminal: {entries:?}");
        };
        assert_eq!(completed.status, ExecutionStatus::Completed);
        assert_eq!(
            completed.output,
            Some(json!({ "ok": true })),
            "the terminal carries the value the finish fixed"
        );
    }

    /// A run still owning its deadline cancels it and **waits**: the deadline must not fire
    /// into a finished run, and no terminal may land before it drains.
    #[tokio::test]
    async fn a_run_waits_for_the_deadlines_it_cancels() {
        let deadline = object_ref::<TimerKind>("deadline", 200);
        let (result, entries) = complete_over(seeded_execution(
            ExecutionStatus::Running,
            HashSet::from([deadline.clone().into_raw_object_ref()]),
        ))
        .await;
        assert!(
            result.is_ok(),
            "a run awaiting its deadline completes later"
        );
        assert_eq!(entries.len(), 2, "the finish and the sweep: {entries:?}");
        assert!(
            matches!(
                &entries[0],
                EntryPayload::Event(Event::ExecutionCompleting { .. })
            ),
            "the finish still begins: {entries:?}"
        );
        assert_eq!(
            entries[1],
            EntryPayload::Command(Command::CancelTimer { timer: deadline })
        );
    }

    /// A live **non-deadline** child is refused, not obeyed: a success finish never terminates a live
    /// child, and `pending == 0` over the swept deadlines alone would have declared this run complete
    /// while a child was still running. Nothing is swept either — the child's own settle will drain the
    /// run instead.
    #[tokio::test]
    async fn a_live_non_deadline_child_refuses_the_completion() {
        let child = object_ref::<ThreadKind>("execution-1", 80);
        let (result, entries) = complete_over(seeded_execution(
            ExecutionStatus::Running,
            HashSet::from([child.clone().into_raw_object_ref()]),
        ))
        .await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a run with a live child must not be closed over it: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains(&child.to_string()),
            "the refusal names the child it will not close over: {reason}"
        );
        assert_eq!(
            entries.len(),
            1,
            "the finish begins and nothing else — no sweep, no terminal: {entries:?}"
        );
    }

    /// A run already past `Running` has no finish to begin, and the refused transition writes nothing —
    /// the `ExecutionCompleting` never reaches the log.
    #[tokio::test]
    async fn a_run_past_running_has_no_finish_to_begin() {
        let (result, entries) =
            complete_over(seeded_execution(ExecutionStatus::Completed, HashSet::new())).await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a completed run must not begin a second finish: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("Completed"),
            "the refusal names the state the run is in: {reason}"
        );
        assert!(
            entries.is_empty(),
            "a refused finish writes nothing: {entries:?}"
        );
    }
}
