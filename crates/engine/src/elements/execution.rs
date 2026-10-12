use super::Container;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::storage::ReadonlyStorageTxn;
use crate::types::command::{
    ActivateState, Command, CompleteExecution, CreateExecution, TerminateExecution,
    TerminateThread, TerminationReason,
};
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::{Event, ExecutionCreated};
use crate::types::execution::ExecutionKind;
use crate::types::meta::{
    HasRawObjectRef, NoOwner, ObjectKind, ObjectMeta, ObjectRef, RawObjectRef,
};
use crate::types::thread::ThreadKind;
use crate::types::timer::TimerKind;
use crate::{ExecutionStatus, RejectionType, StatePath};

/// The behavior of an `Execution` — the run at the root of the object graph.
///
/// Two faces of one element, gathered here from where they were split before: the **container** side
/// (what a run does when one of its own children settles — its root thread finishing, its deadline
/// firing) and the **command** side (the `TerminateExecution`/`CompleteExecution` verbs that open its
/// teardown or its finish, and the `Continue*` hops that advance each). A run owns two kinds of child:
/// its **root thread** (the run's own top-level scope) and its own `TimeoutSeconds` timer (see
/// `create_execution`). There is no state-specific half — a run has no container state to replenish —
/// so the settle hooks only decide what an individual settle *means* to a still-`Running` run: its
/// scope going down abnormally starts the run's teardown, and its scope finishing successfully closes
/// it. A run already winding down instead converges through its own `Completing`/`Terminating` to
/// decide.
pub(crate) struct ExecutionElementBehavior {
    execution: ObjectRef<ExecutionKind>,
}

impl Container for ExecutionElementBehavior {
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
                format!("execution_element: execution {owner} is gone"),
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
                    "execution_element: execution {} is gone from under its settled child",
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
                    "execution_element: execution {} is completing with a settling root thread {child}",
                    self.execution
                ),
            )),
            // A terminal run has no child left to settle, and a run owns no third kind: either way the
            // settle has nothing to mean.
            (kind, status) => Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "execution_element: execution {} settled a {kind} child {child} while {status:?}",
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
                    "execution_element: execution {} is gone from under its settled child",
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
                    "execution_element: execution {} is completing with a settling root thread {child}",
                    self.execution
                ),
            )),
            // A terminal run has no child left to settle, and a run owns no third kind: either way the
            // settle has nothing to mean.
            (kind, status) => Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "execution_element: execution {} settled a {kind} child {child} while {status:?}",
                    self.execution
                ),
            )),
        }
    }
}

impl ExecutionElementBehavior {
    /// Handles `TerminateExecution`: begins the abnormal finish of a running execution with `reason`.
    /// Emits `ExecutionTerminating`, sweeps its two kinds of direct child (`CancelTimer` for its timer,
    /// `TerminateThread` for its root thread — each child recursively terminates its own subtree), and —
    /// once drained — emits `ExecutionTerminated{reason}`.
    pub(crate) async fn terminate(
        p: &TerminateExecution,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let TerminateExecution { name, uid, reason } = p;
        // Storage keys executions by name, so a name-only probe (the uid, when present, doubles as the
        // incarnation guard below) resolves the row regardless of incarnation.
        let probe = ObjectRef::<ExecutionKind>::new(name.clone(), uid.unwrap_or_default());
        let exec = match ctx.storage.get_execution(&probe).await? {
            Some(e) => e,
            None => {
                // Target execution is gone. Refuse the command — the leader records the single
                // followup entry (an `Event` sequence or a `Reject`) every command must yield, so
                // this is a *returned* refusal rather than a silent no-op. Returned rather than
                // recorded in-band because nothing was emitted before the decision: the batch this
                // dispatch would have produced is empty, and the refusal is the whole outcome.
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("terminate_execution: execution {name} not found"),
                ));
            }
        };
        let exec_ref = exec.meta.raw_object_ref();

        // Optional incarnation guard: with a caller-supplied `uid`, only that exact incarnation may be
        // terminated. A mismatch means the name now points at a different execution than the caller
        // started — a stale handle, refused as `InvalidState` rather than terminating the wrong run.
        if let Some(want) = uid
            && want != &exec_ref.uid
        {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "terminate_execution: execution {name} is incarnation {}, not {want}",
                    exec_ref.uid
                ),
            ));
        }

        // The whole precondition — only a `Running` run has a teardown to begin — lives in the
        // transition (`mark_terminating`), so no call site can honor part of it and forget the rest.
        let mut terminating_execution = exec.value();
        if let Err(reason) = terminating_execution.mark_terminating(reason.clone(), ctx.now()) {
            // Not running (already terminal, or Completing/Terminating): the draining pipeline has
            // already decided this execution's outcome — its eventual event wins. Refuse with a
            // durable Reject (the command still gets its followup entry) rather than swallowing.
            tracing::warn!(
                execution = %exec_ref,
                "termination refused: {reason}"
            );
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("terminate_execution: execution {name} cannot be terminated: {reason}"),
            ));
        }
        out.append_event(Event::ExecutionTerminating {
            execution: terminating_execution.clone(),
        })
        .await;

        let children = exec.active_children.clone();
        // Nothing owns the drain: the run lands its teardown in this same batch, with no sweep to issue
        // and no later `ContinueTerminate` to wait for. Checked before the sweep so the terminal is
        // never deferred on a set that was empty all along.
        if children.is_empty() {
            // The terminal advances the very value the teardown produced — that is where the reason was
            // fixed — so it is moved, not re-read. It cannot decline: the value was set `Terminating`
            // one step above, so a failure is an engine regression, and swallowing it would strand the
            // run in `Terminating` with no terminal ever emitted.
            let mut terminated_execution = terminating_execution;
            terminated_execution.mark_terminated(ctx.now()).expect(
                "engine regression: the run was just set Terminating, so its terminal lands",
            );
            // Termination is observable durably: `start` returns the execution id and the caller's
            // `wait_for_execution` poll surfaces this terminal `ExecutionTerminated` from Storage. No
            // deferred ack is needed — terminal notification travels through the poll rather than an
            // `execution → request` ack mapping (see `Engine::wait_for_execution`).
            out.append_event(Event::ExecutionTerminated {
                execution: terminated_execution,
            })
            .await;
            return Ok(());
        }

        for child in children.iter().cloned() {
            match child.kind {
                ObjectKind::Timer => {
                    out.append_command(Command::CancelTimer {
                        timer: child.typed::<TimerKind>(),
                    });
                }
                // The execution's single root Thread (the top-level owner) is swept here too:
                // terminating the run must tear down the root thread's whole subtree, after which the
                // thread relays its settle back (via its `ThreadContainer`) letting this execution drain and
                // emit its own terminal. Swept, not failing — the reason the *run* died already rides
                // this execution's own `Terminating(reason)`, so the thread carries `Cancelled` rather
                // than being recorded as the author of the failure.
                ObjectKind::Thread => {
                    out.append_command(Command::TerminateThread(TerminateThread {
                        thread: child.typed::<ThreadKind>(),
                        reason: TerminationReason::Cancelled,
                    }));
                }
                // A `Task` and a fan-out `Thread` belong to a container *Activity*, never directly to an
                // Execution; and an `Activity`'s own owner is a `Thread` by type (`ActivityKind::OwnedBy`
                // is `ObjectRef<ThreadKind>`), so an Execution's direct children are exactly its timer and
                // its root thread. Any other kind in its `active_children` is therefore an engine
                // invariant broken — a corrupt projection or a forged child edge. Refuse to guess and
                // fail loud, rather than leave the run deferred on a child no sweep can reap.
                other => panic!("engine regression: an Execution owns no {other:?} child"),
            }
        }
        tracing::debug!(
            execution = %exec_ref,
            children = children.len(),
            "execution terminating deferred: waiting on owned children"
        );

        Ok(())
    }

    /// Handles `CompleteExecution`: begins the success finish of a **top-level** `Execution` (the root
    /// run, terminal `Succeed`/`End` reached). Emits `ExecutionCompleting`, which fixes its output on
    /// its row, cancels any owned timers, and — once children drain (immediately if none) — emits
    /// `ExecutionCompleted` then finishes.
    pub(crate) async fn complete(
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
        // drains the run through its `ExecutionElementBehavior` → `ContinueComplete`.
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

    /// The **failure** side of the deferred drain-continuation for a run: settles a run that a settled
    /// child drained, emitting its `ExecutionTerminated`. The reason was fixed when the teardown began
    /// (`mark_terminating`), so this hop only advances the stored value rather than re-deriving one.
    pub(crate) async fn continue_terminate(
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        node: ObjectRef<ExecutionKind>,
    ) -> Result<(), ProcessingError> {
        // A read fault is the dispatch's — returned, so the leader owns retry/refusal. A missing row is
        // a real refusal: the Continue names the run a settled child drained, so a gone row means the
        // command's own precondition failed rather than "already closed".
        let Some(exec) = ctx.storage.get_execution(&node).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("continue_terminate: execution {node} not found"),
            ));
        };
        // The hop lands only on a `Terminating` run; any other status means the row moved under the
        // read above.
        if !matches!(exec.status, ExecutionStatus::Terminating(_)) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "continue_terminate: execution {node} is not terminating (status {:?})",
                    exec.status
                ),
            ));
        }
        if !exec.active_children.is_empty() {
            // A run's container does not wait for its own drain — a teardown takes down both of a run's
            // children at once, so each of their settles advances it — which means an earlier Continue
            // reaches here with the other child still attached. It is absorbed: the sweep below takes
            // that child down again and this hop stops short of advancing, so only the Continue that
            // lands on the drained run terminates it. A run's direct children are exactly its root
            // thread and its own timers, so any other kind here is the row disagreeing with the
            // lifecycle; both sweepable kinds carry `Cancelled`, for the reason the `Terminating`
            // activity's arm gives.
            for child in &exec.active_children {
                match child.kind {
                    ObjectKind::Timer => out.append_command(Command::CancelTimer {
                        timer: child.clone().typed::<TimerKind>(),
                    }),
                    ObjectKind::Thread => {
                        out.append_command(Command::TerminateThread(TerminateThread {
                            thread: child.clone().typed::<ThreadKind>(),
                            reason: TerminationReason::Cancelled,
                        }))
                    }
                    kind => panic!(
                        "continue_terminate: execution {node} owns a live {kind} child {child}; a \
                         run's children are only its root Thread and its own timers"
                    ),
                }
            }
            return Ok(());
        }
        // The reason was written when the teardown began (`mark_terminating`), so the terminal advances
        // the stored value rather than re-deriving one.
        let mut terminated_execution = exec.value();
        // The arm is the transition's own precondition, so this can only decline if the row moved
        // under the read above.
        if let Err(reason) = terminated_execution.mark_terminated(ctx.now()) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("continue_terminate: execution {node} cannot be terminated: {reason}"),
            ));
        }
        out.append_event(Event::ExecutionTerminated {
            execution: terminated_execution,
        })
        .await;
        // Nothing to relay: a run is the root of its object tree, so it has no owner to settle up to.
        Ok(())
    }

    /// The **success** side of the deferred drain-continuation for a run: settles a run that a settled
    /// child drained, emitting its `ExecutionCompleted`. The output was fixed when the finish began
    /// (`mark_completing`), so this hop only advances the stored value.
    pub(crate) async fn continue_complete(
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        node: ObjectRef<ExecutionKind>,
    ) -> Result<(), ProcessingError> {
        // A read fault is the dispatch's — returned, so the leader owns retry/refusal. A missing row is
        // a real refusal: the Continue names the run a settled child drained, so a gone row means the
        // command's own precondition failed rather than "already closed".
        let Some(exec) = ctx.storage.get_execution(&node).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("continue_complete: execution {node} not found"),
            ));
        };
        // The hop lands only on a `Completing` run — one that a settled child drained (see
        // `child_completed`). Any other status means the row moved under the read above.
        if !matches!(exec.status, ExecutionStatus::Completing) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "continue_complete: execution {node} is not completing (status {:?})",
                    exec.status
                ),
            ));
        }
        if !exec.active_children.is_empty() {
            // A run's container does not wait for its own drain — both of its children can settle under
            // one teardown — so an early Continue reaches here with a child still attached. It is
            // absorbed: the sweep re-issues what is still in flight and this hop stops short of
            // advancing, so the Continue that lands on the drained run is the one that completes it. A
            // success finish keeps only the deadlines it owns (`complete_execution` cancels them and
            // waits for the drain), so any other kind here is the row disagreeing with the lifecycle.
            for child in &exec.active_children {
                match child.kind {
                    ObjectKind::Timer => out.append_command(Command::CancelTimer {
                        timer: child.clone().typed::<TimerKind>(),
                    }),
                    kind => panic!(
                        "continue_complete: execution {node} owns a live {kind} child {child}; a \
                         completing run only keeps its own timers"
                    ),
                }
            }
            return Ok(());
        }
        // The row already carries what the finish fixed (`output` was written when it entered
        // `Completing`), so the terminal advances the stored value rather than re-deriving one.
        let mut completed_execution = exec.value();
        // The arm is the transition's own precondition, so this can only decline if the row moved
        // under the read above.
        if let Err(reason) = completed_execution.mark_completed(ctx.now()) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("continue_complete: execution {node} cannot be completed: {reason}"),
            ));
        }
        out.append_event(Event::ExecutionCompleted {
            execution: completed_execution,
        })
        .await;
        // Nothing to relay: a run is the root of its object tree, so it has no owner to settle up to.
        Ok(())
    }
    /// Handles `CreateExecution`: records the execution (via `ExecutionCreated`) and starts it. Also
    /// arms the state-machine `TimeoutSeconds` timer if configured. Immediately enters the start state
    /// via `ActivateState`.
    pub(crate) async fn create(
        p: &CreateExecution,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        // `request_id` is the awaiting caller's correlation key — echoed onto `ExecutionCreated` so
        // the StreamProcessor's request-ack correlator wakes the awaiting `start` operation with the fact
        // that this execution was durably created (see `Event::ExecutionCreated`). The handler
        // otherwise ignores it.
        let CreateExecution {
            request_id,
            name,
            flow_version,
            input,
        } = p;
        // `CreateExecution` creates only new names: the name is the execution's storage primary key
        // (per-scope unique), so a second run under an already-live name must be **rejected** — the
        // authoritative serialized point the StreamProcessor reaches in strict log order — never
        // aliased onto a fresh execution that would silently clobber the first row. Mirror
        // `create_flow.rs`: the `Engine` boundary pre-check may have been racy (two concurrent
        // same-name starts both passed its read), so this in-order check is the enforcement, and the
        // loser surfaces `ALREADY_EXISTS` to its awaiter (and the log) via a `Reject`, not a silent
        // drop. A name-only probe (nil uid) suffices — storage keys executions by name, so the uid is
        // irrelevant to the read.
        let probe = ObjectRef::<ExecutionKind>::new(name.clone(), ulid::Ulid::nil());
        if let Ok(Some(_)) = ctx.storage.get_execution(&probe).await {
            return Err(ProcessingError::Rejected(
                RejectionType::AlreadyExists,
                format!("create_execution: execution {name} already exists"),
            ));
        }

        // Mint the execution's durable identity here: a fresh `uid` plus the caller-supplied `name`.
        // The uid is NOT carried in the command — replay stays deterministic because the produced
        // `ExecutionCreated` lands in the same atomic batch as this command (committed ⇒ conclusive,
        // never re-dispatched), so a re-dispatch mints a fresh consistent uid.
        let uid: ulid::Ulid = ctx.mint();
        let id = ObjectRef::<ExecutionKind>::new(name.clone(), uid);

        // Resolve the machine this execution binds to. This is the first use of the version in a fresh
        // StreamProcessor — it loads the definition (keyed by the version's object reference) from
        // Storage into the cache. A version that is missing (definition GC'd) or whose stored definition
        // no longer parses cannot produce a run, and re-dispatching would not change that, so the
        // command is refused rather than fanned out into a termination: there is no execution row yet to
        // terminate, and a termination carries no `request_id`, so it could never reach the `start`
        // caller awaiting this command.
        let sm = ctx.machine(flow_version).await.map_err(|e| {
            ProcessingError::Rejected(
                RejectionType::InvalidArgument,
                format!(
                    "create_execution: flow version {flow_version} cannot be resolved for execution \
                     {name}: {e}"
                ),
            )
        })?;
        // Normalize the machine's relative `TimeoutSeconds` into an absolute deadline here, before the
        // birth event, so the run's own `deadline` and the timer that enforces it are
        // written from one computation and cannot disagree. A `TimeoutSeconds` the clock cannot add is a
        // malformed definition, so it is refused *before* the run exists: accepting it, birthing the
        // execution, and then failing it immediately would leave a dead row standing for a command the
        // engine had already decided not to apply — and would answer the caller with a creation that
        // succeeded rather than a refusal.
        let timeout = sm.timeout_seconds.filter(|secs| *secs > 0);
        let deadline = timeout.and_then(|secs| {
            ctx.now()
                .checked_add(std::time::Duration::from_secs(secs as u64))
        });
        if timeout.is_some() && deadline.is_none() {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidArgument,
                format!(
                    "create_execution: execution {name} cannot start: TimeoutSeconds overflows the \
                     absolute deadline"
                ),
            ));
        }

        // Build the birth event once and emit it; an injected `Hook` observer wakes the awaiting `start`
        // caller from this same event (the `request_id` it echoes), so no separate ack echo is needed.
        let created_event = Event::ExecutionCreated(ExecutionCreated {
            request_id: *request_id,
            execution: crate::Execution {
                // The version this run executes against — every nested Thread inherits it.
                flow_version: flow_version.clone(),
                // A top-level run is its own root: it carries no `root_execution` and no branch
                // `state_path` (it resolves states against the machine's top-level `States`). Fan-out
                // children are `Thread`s instead, created via `SpawnThread`.
                status: crate::ExecutionStatus::Running,
                deadline,
                input: input.clone(),
                output: None,
                meta: ObjectMeta::builder(uid)
                    .name(name.clone())
                    .at(ctx.now())
                    // A top-level run is its own root: its owner slot is `NoOwner` by type.
                    .with_owner(NoOwner::new()),
            },
        });
        // The birth `ExecutionCreated` echoes the awaiting `start` caller's request id; the `AckHook`
        // observer wakes it once the engine reports this event applied (see `Engine::start_for_revision`,
        // which returns the execution id at that point, leaving the terminal settle to
        // `wait_for_execution`).
        out.append_event(created_event).await;

        if let Some(deadline) = deadline {
            // The run's own deadline timer is generated **here** (inline): mint the
            // timer's durable uid, and derive the timer's name as `{execution.name}-{counter-suffix}`
            // (k8s generateName style, `PlainName::generated_from_key`) — deterministic and
            // replay-safe, since this `TimerActivated` lands in the
            // same atomic batch as the `ExecutionCreated`. The name is decoupled from the timer's own
            // `uid`, and must be carried forward by later timer events, so TimerActivated children
            // stay resolvable (see `TimerTriggered`/`TimerCancelled`, which preserve the row's meta
            // rather than re-deriving the name).
            let uid: ulid::Ulid = ctx.mint();
            // A generated child's base is the execution's own base name. `ObjectName::base` is total —
            // a `Plain` name is its own base and a `Generated` one carries a plain base by construction
            // (nested generated names are unrepresentable) — so both flavors derive the timer's
            // `{base}-{suffix}` handle the same way, as the root thread below already does.
            let base = id.name().base();
            // The suffix is this partition's local generated-name counter (see `Storage::next_generated_seq`),
            // read via the working overlay so a sibling minted earlier in the same batch is visible; the
            // `TimerActivated` applier bumps the counter past it in the same fold.
            let name = base.generated_from_key(out.next_generated_seq().await);
            let timer = crate::Timer {
                meta: crate::types::meta::ObjectMeta::builder(uid)
                    .name(name)
                    .at(ctx.now())
                    // An execution timeout is armed by the run itself — the `Execution` variant of the
                    // timer slot, never an activity's.
                    .with_owner(crate::types::meta::TimerOwner::Execution(id.clone())),
                execution: id.clone(),
                status: crate::TimerStatus::Active,
                deadline,
            };
            out.append_event(Event::TimerActivated { timer }).await;
        }

        // Derive the execution's single root Thread — the top-level owner of every state, standing in
        // for a whole top-level run the way a Process has one main Thread. It is owned by the
        // Execution (so the `ThreadCreated` applier never aggregates its placeholder index 0 into a
        // container fan-out map) and names the machine's own top-level `States` table, so it resolves
        // through the same walk a fan-out thread does. Its completion/termination bridges back to
        // the Execution (see `complete_thread`/`terminate_thread`), so the externally-addressed root
        // still settles through `wait_for_execution`.
        let root_uid: ulid::Ulid = ctx.mint();
        let root_name = id
            .name()
            .base()
            .generated_from_key(out.next_generated_seq().await);
        let root_thread = ObjectRef::<ThreadKind>::new(root_name.clone(), root_uid);
        // The root thread runs the machine's top-level `States` table and enters the machine's own
        // `StartAt` — the pointer and the entry point are the same pair every fan-out thread carries.
        let root_states = StatePath::root();
        let start_at = sm.start_at.clone();
        out.append_event(Event::ThreadCreated {
            // TODO：应该提供一个 Thread::new 函数？
            thread: crate::Thread {
                meta: ObjectMeta::builder(root_uid)
                    .name(root_name)
                    .at(ctx.now())
                    // A root thread's owner is the run it stands in for — the `Execution` variant, the
                    // one a fan-out thread (owned by its container activity) never carries.
                    .with_owner(crate::types::meta::ThreadOwner::Execution(id.clone())),
                execution: id.clone(),
                state_path: root_states.clone(),
                start_at: start_at.clone(),
                index: 0,
                status: crate::ThreadStatus::Running,
                input: input.clone(),
                output: None,
            },
        })
        .await;

        // Enter the start state: the root thread's `state_path` (the top-level `States` table)
        // extended by its own `start_at`, exactly how `SpawnThread` derives a branch's entry path.
        out.append_command(Command::ActivateState(ActivateState {
            // The top-level state is owned by the derived root Thread, not the Execution.
            execution: id.clone(),
            owner: root_thread,
            state_path: root_states.state(&start_at),
            input: input.clone(),
        }));

        Ok(())
    }
}

#[cfg(test)]
mod settle_tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use serde_json::json;
    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_storage::InMemoryStorage;
    use spica_testing::MockReadonlyStorageTxn;

    use super::{Container, ExecutionElementBehavior};
    use crate::EntryPayload;
    use crate::RejectionType;
    use crate::StatePath;
    use crate::StorageError;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, OverlaySink, ProcessingError};
    use crate::handlers::build_state_handlers;
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
        let container = ExecutionElementBehavior::open(&work, execution_ref())
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
        let container = ExecutionElementBehavior {
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
            ExecutionElementBehavior::open(&work, execution_ref()).await
        else {
            panic!("an open that did not read a missing row");
        };
        assert_eq!(ty, RejectionType::NotFound);
        assert!(
            reason.contains("is gone"),
            "the refusal names the gone owner: {reason}"
        );
    }

    /// A read that **faults** stays a fault, never a missing owner (see `ActivityElementBehavior`'s sibling).
    #[tokio::test]
    async fn a_faulted_execution_read_is_not_a_missing_owner() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_execution()
            .times(1)
            .return_once(|_| Err(StorageError::Backend("injected storage fault".to_string())));
        assert!(
            matches!(
                ExecutionElementBehavior::open(&store, execution_ref()).await,
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

#[cfg(test)]
mod command_tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use serde_json::json;
    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_testing::MockReadonlyStorageTxn;

    use super::ExecutionElementBehavior;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, ProcessingError};
    use crate::handlers::build_state_handlers;
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
        let result = ExecutionElementBehavior::complete(
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

#[cfg(test)]
mod create_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use serde_json::json;
    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_testing::MockReadonlyStorageTxn;

    use super::ExecutionElementBehavior;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, ProcessingError};
    use crate::handlers::build_state_handlers;
    use crate::handlers::fixtures::{at, obj_name, object_ref};
    use crate::types::command::CreateExecution;
    use crate::types::id::{EntryId, RequestId};
    use crate::types::meta::{ObjectMeta, ObjectRef};
    use crate::{
        EntryPayload, FlowKind, FlowVersion, FlowVersionKind, RejectionType, StorageError,
    };

    /// The version the run binds to. Its name is load-bearing: `HandlerContext::machine` loads the
    /// definition by exactly this reference, so the seeded row has to answer to it.
    fn flow_version_ref() -> ObjectRef<FlowVersionKind> {
        object_ref("lifecycle_flow-1", 2)
    }

    /// A definition whose `TimeoutSeconds` is the largest `i64` the ASL model admits: normalizing it
    /// into an absolute deadline needs a millisecond count past `u64::MAX`, so the machine parses
    /// cleanly while the deadline cannot be computed. That gap is the only input the guard exists for.
    fn overflowing_definition() -> String {
        json!({
            "StartAt": "P",
            "TimeoutSeconds": i64::MAX,
            "States": { "P": { "Type": "Pass", "End": true } }
        })
        .to_string()
    }

    /// The handler exercised against a **mock** read-only store: the two reads `CreateExecution` makes
    /// — the execution-name probe and the version's definition — are the only ones scripted, so any
    /// further read panics. Both the outcome and the entries are returned, because for this guard the
    /// *absence* of a birth event is as much the point as the refusal itself.
    async fn create_over(
        store: &MockReadonlyStorageTxn,
    ) -> (Result<(), ProcessingError>, Vec<EntryPayload>) {
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
        let result = ExecutionElementBehavior::create(
            &CreateExecution {
                request_id: RequestId::nil(),
                name: obj_name("lifecycle_execution"),
                flow_version: flow_version_ref(),
                input: json!({ "n": 1 }),
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

    /// A store whose name probe finds nothing and whose version publishes `definition`.
    fn store_over(definition: &str) -> MockReadonlyStorageTxn {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_execution()
            .times(1)
            .return_const(Ok::<_, StorageError>(None));
        let version = FlowVersion {
            meta: ObjectMeta::builder(flow_version_ref().uid())
                .name(flow_version_ref().name().clone())
                .at(at())
                .with_owner(object_ref::<FlowKind>("lifecycle_flow", 1)),
            version: 1,
            definition: definition.to_string(),
            checksum: 0,
        };
        store
            .expect_get_flow_version()
            .times(1)
            .return_once(move |_| Ok(Some(version)));
        store
    }

    /// A store whose name probe finds nothing and whose version row does not exist — the "definition
    /// GC'd out from under a command that names it" case.
    fn store_without_version() -> MockReadonlyStorageTxn {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_execution()
            .times(1)
            .return_const(Ok::<_, StorageError>(None));
        store
            .expect_get_flow_version()
            .times(1)
            .return_const(Ok::<_, StorageError>(None));
        store
    }

    /// A definition that cannot be resolved is refused, and refused as the *command's* failure: the run
    /// is never born, and no termination is fanned out — a termination for a row that does not exist
    /// could not answer this command's `request_id` either, so the awaiting `start` caller would be left
    /// with nothing to wake it.
    #[tokio::test]
    async fn an_unresolvable_definition_is_refused_before_the_run_is_born() {
        let store = store_without_version();
        let (result, entries) = create_over(&store).await;
        let Err(err) = result else {
            panic!("a missing flow version must be refused, not accepted");
        };
        let ProcessingError::Rejected(ty, reason) = err else {
            panic!(
                "an unresolvable definition is the command's failure, not the engine's: {err:?}"
            );
        };
        assert_eq!(ty, RejectionType::InvalidArgument);
        assert!(
            reason.contains(&flow_version_ref().to_string()),
            "the refusal names the version it could not resolve: {reason}"
        );
        assert!(
            entries.is_empty(),
            "the refusal fans out no termination for a row that does not exist: {entries:?}"
        );
    }

    /// A `TimeoutSeconds` the clock cannot add is refused as a malformed definition, and refused
    /// **before the run exists**: no `ExecutionCreated` reaches the log, so a caller is never handed a
    /// creation that "succeeded" and then died. The window between the two reads is closed — the
    /// version is read once and the refusal follows from its definition alone.
    #[tokio::test]
    async fn an_unrepresentable_timeout_is_refused_before_the_run_is_born() {
        let store = store_over(&overflowing_definition());
        let (result, entries) = create_over(&store).await;
        let Err(err) = result else {
            panic!("an unaddable TimeoutSeconds must be refused, not accepted");
        };
        let ProcessingError::Rejected(ty, reason) = err else {
            panic!("a malformed definition is the command's failure, not the engine's: {err:?}");
        };
        assert_eq!(ty, RejectionType::InvalidArgument);
        assert!(
            reason.contains("lifecycle_execution"),
            "the refusal names the execution that cannot start: {reason}"
        );
        assert!(
            entries.is_empty(),
            "the refusal leaves no birth event behind it: {entries:?}"
        );
    }
}
