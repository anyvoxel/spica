use crate::RejectionType;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::{Command, TerminateExecution, TerminateState, TerminateThread};
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectReference};

/// Handles `TerminateExecution`: begins the abnormal finish of a running execution with `reason`.
/// Emits `ExecutionTerminating`, sweeps owned children (`CancelTimer` for timers,
/// `TerminateState` for activities — each child recursively terminates its own subtree), and —
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
        let probe =
            ObjectReference::new(ObjectKind::Execution, name.clone(), uid.unwrap_or_default());
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
        let exec_ref = exec.meta.reference();

        // Optional incarnation guard: with a caller-supplied `uid`, only that exact incarnation may be
        // terminated. A mismatch means the name now points at a different execution than the caller
        // started — refuse with a `StateConflict` Reject rather than terminating the wrong run.
        if let Some(want) = uid
            && want != &exec_ref.uid
        {
            return Err(ProcessingError::Rejected(
                RejectionType::StateConflict,
                format!(
                    "terminate_execution: execution {name} is incarnation {}, not {want}",
                    exec_ref.uid
                ),
            ));
        }

        if !exec.status.is_running() {
            // Not running (already terminal, or Completing/Terminating): the draining pipeline has
            // already decided this execution's outcome — its eventual event wins. Refuse with a
            // durable Reject (the command still gets its followup entry) rather than swallowing.
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "terminate_execution: execution {name} is {:?}, not running",
                    exec.status
                ),
            ));
        }

        let mut terminating_execution = exec.value();
        terminating_execution.status = crate::ExecutionStatus::Terminating(reason.clone());
        // Advance the domain `updated_at` at event construction (not Entry metadata); `created_at`
        // carries forward.
        terminating_execution.meta.with_update_at(ctx.now());
        out.append_event(Event::ExecutionTerminating {
            execution: terminating_execution,
        })
        .await;

        let children = exec.active_children.clone();
        let mut pending = 0usize;
        for child in children {
            match child.kind {
                ObjectKind::Timer => {
                    out.append_command(Command::CancelTimer { timer: child });
                    pending += 1;
                }
                ObjectKind::Activity => {
                    out.append_command(Command::TerminateState(TerminateState {
                        activity: child,
                        reason: reason.clone(),
                    }));
                    pending += 1;
                }
                // The execution's single root Thread (the top-level owner) is swept here too:
                // terminating the run must tear down the root thread's whole subtree, after which the
                // thread relays its settle back (via `child_settled`) letting this execution drain and
                // emit its own terminal.
                ObjectKind::Thread => {
                    out.append_command(Command::TerminateThread(TerminateThread {
                        thread: child,
                        reason: reason.clone(),
                    }));
                    pending += 1;
                }
                // A Parallel-branch or Map-item *Thread* and a `Task` are owned by a container
                // *Activity*, never directly by an Execution — so they are reached transitively
                // through the sweeps above, and there is nothing to sweep at this level. (An Execution
                // directly owns only its root thread, its activities, and its timers.)
                _ => {}
            }
        }
        if pending == 0 {
            let mut terminated_execution = exec.value();
            terminated_execution.status = crate::ExecutionStatus::Terminated(reason.clone());
            terminated_execution.meta.with_update_at(ctx.now());
            // Termination is observable durably: `start` returns the execution id and the caller's
            // `wait_for_execution` poll surfaces this terminal `ExecutionTerminated` from Storage. No
            // deferred ack is needed — terminal notification travels through the poll rather than an
            // `execution → request` ack mapping (see `Engine::wait_for_execution`).
            let terminated_event = Event::ExecutionTerminated {
                execution: terminated_execution,
            };
            out.append_event(terminated_event).await;
        } else {
            tracing::debug!(
                execution = %exec_ref,
                pending,
                "execution terminating deferred: waiting on owned children"
            );
        }

        Ok(())
    }
}
