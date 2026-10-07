use crate::RejectionType;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::{Command, TerminateExecution, TerminateThread, TerminationReason};
use crate::types::event::Event;
use crate::types::execution::ExecutionKind;
use crate::types::meta::{ObjectKind, ObjectRef};
use crate::types::thread::ThreadKind;
use crate::types::timer::TimerKind;

/// Handles `TerminateExecution`: begins the abnormal finish of a running execution with `reason`.
/// Emits `ExecutionTerminating`, sweeps its two kinds of direct child (`CancelTimer` for its timer,
/// `TerminateThread` for its root thread — each child recursively terminates its own subtree), and —
/// once drained — emits `ExecutionTerminated{reason}`.
#[derive(Default)]
pub struct TerminateExecutionHandler;

impl TerminateExecutionHandler {
    pub(crate) async fn handle(
        &self,
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
}
