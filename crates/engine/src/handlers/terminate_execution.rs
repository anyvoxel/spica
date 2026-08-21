use async_trait::async_trait;

use crate::command::Command;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::id::NodeId;

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
            id: crate::id::ExecutionId::nil(),
            reason: crate::command::TerminationReason::Cancelled,
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::TerminateExecution { id, reason } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        let exec = match super::load_execution(ctx.storage, *id).await {
            Ok(Some(e)) => e,
            Ok(None) => return, // already gone; nothing to terminate.
            Err(_) => return,
        };
        if !exec.status.is_running() {
            // Idempotency: if already Completing, convert the in-flight success into a
            // termination? No — the draining pipeline for a Completing execution has already
            // decided the outcome; a later Terminate is swallowed as a no-op.
            if exec.status.is_terminal() {
                return;
            }
            // Completing/Terminating: already winding down; the eventual ed wins over this
            // later Terminate. Swallow.
            return;
        }

        let mut terminating_execution = exec.value();
        terminating_execution.status = crate::ExecutionStatus::Terminating(reason.clone());
        out.emit_event(Event::ExecutionTerminating {
            execution: terminating_execution,
        });

        let children = exec.active_children.clone();
        let mut pending = 0usize;
        for child in children {
            match child {
                NodeId::Timer(t) => {
                    out.emit_command(Command::CancelTimer { timer: t });
                    pending += 1;
                }
                NodeId::Activity(a) => {
                    out.emit_command(Command::TerminateState {
                        activity: a,
                        reason: reason.clone(),
                    });
                    pending += 1;
                }
                // A Parallel-branch child *execution* is owned by a Parallel activity, not by an
                // Execution directly — its branches are rooted under the *activity*. So an
                // execution's own children are its activities (above), and a nested Parallel's
                // branch executions live under those activities, reached transitively through the
                // TerminateState sweep. Nothing to do at the Execution level; the `TerminateState`
                // sweep of each activity terminates any deeper Parallel-branch executions.
                NodeId::Execution(_) => {}
                // A `Task` (an in-flight external call) is owned by an Activity, not directly by
                // the Execution — terminating the execution terminates each activity (above), whose
                // own sweep cancels its tasks. A direct Execution->Task link never exists in M1/M2,
                // so there is nothing to sweep here.
                NodeId::Task(_) => {}
            }
        }
        if pending == 0 {
            let mut terminated_execution = exec.value();
            terminated_execution.status = crate::ExecutionStatus::Terminated(reason.clone());
            // Termination is observable durably: `start` returns the execution id and the caller's
            // `wait_for_execution` poll surfaces this terminal `ExecutionTerminated` from Storage. No
            // deferred ack is needed — terminal notification travels through the poll rather than an
            // `execution → request` ack mapping (see `Engine::wait_for_execution`).
            let terminated_event = Event::ExecutionTerminated {
                execution: terminated_execution,
            };
            out.emit_event(terminated_event);
            // A terminating child execution (a Parallel branch that failed) relays its settle to its
            // owning node — the `Parallel` activity — the same way a successful branch does (see
            // `CompleteExecutionHandler`). Without this the failed branch drains `P`'s `active_children`
            // but nobody triggers `P`'s `child_completed`, so a failed Parallel never converges and the
            // tree wedges. The top-level run (`parent: None`) has no owner and relays nothing.
            if let Some(parent) = exec.parent {
                out.emit_command(Command::ProcessChildCompleted {
                    parent,
                    child: NodeId::Execution(*id),
                });
            }
        } else {
            tracing::debug!(
                execution = %id,
                pending,
                "execution terminating deferred: waiting on owned children"
            );
        }
    }
}
