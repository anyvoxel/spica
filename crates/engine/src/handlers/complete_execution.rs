use async_trait::async_trait;

use crate::command::Command;
use crate::error::ExecutionError;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::id::NodeId;

/// Handles `CompleteExecution`: begins the success finish of a running execution. Emits
/// `ExecutionCompleting`, which fixes the execution's output on its row, cancels any owned timers,
/// and—once children drain (immediately if none)—emits `ExecutionCompleted` then finishes via the
/// cascade.
#[derive(Default)]
pub struct CompleteExecutionHandler;

#[async_trait]
impl CommandHandler for CompleteExecutionHandler {
    fn command(&self) -> Command {
        Command::CompleteExecution {
            id: crate::id::ExecutionId::nil(),
            output: Default::default(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::CompleteExecution { id, output } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        let exec = match super::load_execution(ctx.storage, *id).await {
            Ok(Some(e)) => e,
            Ok(None) => {
                out.fail_execution(
                    *id,
                    ExecutionError::StateNotFound(format!("execution {id}")),
                );
                return;
            }
            Err(e) => {
                out.fail_execution(*id, e);
                return;
            }
        };
        if !exec.status.is_running() {
            return; // idempotency: already finishing or terminal.
        }

        out.emit_event(Event::ExecutionCompleting {
            id: *id,
            output: output.clone(),
        });

        let children = exec.active_children.clone();
        let mut pending_children = 0usize;
        for child in children {
            if let NodeId::Timer(t) = child {
                out.emit_command(Command::CancelTimer { timer: t });
                pending_children += 1;
            }
        }
        if pending_children == 0 {
            out.emit_event(Event::ExecutionCompleted {
                id: *id,
                output: output.clone(),
            });
            // A child execution (a Parallel branch) relays its settle to its owning node so the
            // owner (a `Parallel` activity) can react once its last branch drains. The top-level
            // run has no parent — `Engine::start` observes its `ExecutionCompleted` directly.
            if let Some(parent) = exec.parent {
                out.emit_command(Command::ProcessChildCompleted {
                    parent,
                    child: NodeId::Execution(*id),
                });
            }
        } else {
            tracing::debug!(
                execution = %id,
                pending = pending_children,
                "execution completing deferred: waiting on owned children"
            );
        }
    }
}
