use async_trait::async_trait;

use crate::RejectionType;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::log::Timestamp;
use crate::types::command::Command;
use crate::types::event::Event;
use crate::types::id::RequestId;
use crate::types::meta::{ObjectKind, ObjectName, ObjectReference};

/// Handles `TerminateExecution`: begins the abnormal finish of a running execution with `reason`.
/// Emits `ExecutionTerminating`, sweeps owned children (`CancelTimer` for timers,
/// `TerminateState` for activities — each child recursively terminates its own subtree), and —
/// once drained — emits `ExecutionTerminated{reason}`.
#[derive(Default)]
pub struct TerminateExecutionHandler;

#[async_trait]
impl CommandHandler for TerminateExecutionHandler {
    fn command(&self) -> Command {
        Command::TerminateExecution {
            name: ObjectName::plain("default").expect("static placeholder name is valid"),
            uid: None,
            reason: crate::types::command::TerminationReason::Cancelled,
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector<'_>) {
        let Command::TerminateExecution { name, uid, reason } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        // Storage keys executions by name, so a name-only probe (the uid, when present, doubles as the
        // incarnation guard below) resolves the row regardless of incarnation.
        let probe =
            ObjectReference::new(ObjectKind::Execution, name.clone(), uid.unwrap_or_default());
        let exec = match super::load_execution(ctx.storage, &probe).await {
            Ok(Some(e)) => e,
            Ok(None) => {
                // Target execution is gone. Refuse with a durable `Reject` — every command must yield
                // a followup entry (an event sequence or a Reject), never a silent no-op return.
                out.reject(
                    RequestId::nil(),
                    RejectionType::NotFound,
                    format!("terminate_execution: execution {name} not found"),
                );
                return;
            }
            // An infrastructure (storage) fault is not a command-level refusal — the fold errors out
            // rather than recording a misleading Reject.
            Err(_) => return,
        };
        let exec_ref = exec.reference();

        // Optional incarnation guard: with a caller-supplied `uid`, only that exact incarnation may be
        // terminated. A mismatch means the name now points at a different execution than the caller
        // started — refuse with a `StateConflict` Reject rather than terminating the wrong run.
        if let Some(want) = uid
            && want != &exec_ref.uid
        {
            out.reject(
                RequestId::nil(),
                RejectionType::StateConflict,
                format!(
                    "terminate_execution: execution {name} is incarnation {}, not {want}",
                    exec_ref.uid
                ),
            );
            return;
        }

        if !exec.status.is_running() {
            // Not running (already terminal, or Completing/Terminating): the draining pipeline has
            // already decided this execution's outcome — its eventual event wins. Refuse with a
            // durable Reject (the command still gets its followup entry) rather than swallowing.
            out.reject(
                RequestId::nil(),
                RejectionType::InvalidState,
                format!(
                    "terminate_execution: execution {name} is {:?}, not running",
                    exec.status
                ),
            );
            return;
        }

        let mut terminating_execution = exec.value();
        terminating_execution.status = crate::ExecutionStatus::Terminating(reason.clone());
        // Advance the domain `updated_at` at event construction (not Entry metadata); `created_at`
        // carries forward.
        terminating_execution.meta.with_update_at(Timestamp::now());
        out.emit_event(Event::ExecutionTerminating {
            execution: terminating_execution,
        })
        .await;

        let children = exec.active_children.clone();
        let mut pending = 0usize;
        for child in children {
            match child.kind {
                ObjectKind::Timer => {
                    out.emit_command(Command::CancelTimer { timer: child });
                    pending += 1;
                }
                ObjectKind::Activity => {
                    out.emit_command(Command::TerminateState {
                        activity: child,
                        reason: reason.clone(),
                    });
                    pending += 1;
                }
                // A Parallel-branch child *execution*, a fan-out `Thread`, and a `Task` are all owned
                // by a container *Activity*, never directly by an Execution — so they are reached
                // transitively through the `TerminateState` sweep above, and there is nothing to
                // sweep at this level. (An Execution directly owns only its activities and timers.)
                _ => {}
            }
        }
        if pending == 0 {
            let mut terminated_execution = exec.value();
            terminated_execution.status = crate::ExecutionStatus::Terminated(reason.clone());
            terminated_execution.meta.with_update_at(Timestamp::now());
            // Termination is observable durably: `start` returns the execution id and the caller's
            // `wait_for_execution` poll surfaces this terminal `ExecutionTerminated` from Storage. No
            // deferred ack is needed — terminal notification travels through the poll rather than an
            // `execution → request` ack mapping (see `Engine::wait_for_execution`).
            let terminated_event = Event::ExecutionTerminated {
                execution: terminated_execution,
            };
            out.emit_event(terminated_event).await;
            // A terminating child execution (a Parallel branch that failed) runs the inline reaction
            // to its owning node — the `Parallel` activity — the same way a successful branch does
            // (see `CompleteExecutionHandler`). Without this the failed branch drains `P`'s
            // `active_children` but nobody converges `P`, so a failed Parallel never finishes and the
            // tree wedges. The top-level run (`parent: None`) has no owner and reacts to nothing.
            if let Some(owner) = exec.value.meta.owner.clone() {
                super::child_completed::child_settled(ctx, out, owner, exec_ref.clone()).await;
            }
        } else {
            tracing::debug!(
                execution = %exec_ref,
                pending,
                "execution terminating deferred: waiting on owned children"
            );
        }
    }
}
