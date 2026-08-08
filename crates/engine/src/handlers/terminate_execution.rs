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

        out.emit_event(Event::ExecutionTerminating {
            id: *id,
            reason: reason.clone(),
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
                // Executions and (later) Tasks terminate in M2; M1 only has Timer/Activity children.
                NodeId::Execution(_) => {}
            }
        }
        if pending == 0 {
            out.emit_event(Event::ExecutionTerminated {
                id: *id,
                reason: reason.clone(),
            });
        } else {
            tracing::debug!(
                execution = %id,
                pending,
                "execution terminating deferred: waiting on owned children"
            );
        }
    }
}
