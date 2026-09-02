use async_trait::async_trait;

use crate::ExecutionStatus;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::log::Timestamp;
use crate::storage::ScopeRecord;
use crate::types::command::Command;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectReference};

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

#[async_trait]
impl CommandHandler for CompleteExecutionHandler {
    fn command(&self) -> Command {
        Command::CompleteExecution {
            execution: crate::types::meta::ObjectReference::nil(),
            output: Default::default(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::CompleteExecution { execution, output } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        // The addressed run is a **top-level** `Execution`. Resolve its scope and classify it: the
        // scope wrapper is the uniform record for both kinds, but only `Execution` belongs here — a
        // `Thread` addressed to this handler is an internal fault (dispatch routes those to
        // `CompleteThread`), so it is refused rather than mis-completed.
        let scope = match crate::storage::load_scope_ref(ctx.storage, execution).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                out.fail_execution(
                    execution.clone(),
                    ExecutionError::Runtime(RuntimeError::StateNotFound(format!(
                        "execution {execution}"
                    ))),
                );
                return;
            }
            Err(e) => {
                out.fail_execution(execution.clone(), e);
                return;
            }
        };
        let ScopeRecord::Execution(exec) = scope else {
            tracing::debug!(
                target = %execution,
                "complete_execution: addressed node is not a top-level execution; ignition ignored"
            );
            return;
        };
        if !exec.value.status.is_running() {
            return; // idempotency: already finishing or terminal.
        }

        let mut completing_execution = exec.value();
        completing_execution.status = ExecutionStatus::Completing;
        completing_execution.output = Some(output.clone());
        // A new lifecycle transition — advance the domain `updated_at` (stemmed at event
        // construction, not from Entry metadata); `created_at` is carried forward unchanged.
        completing_execution.meta.touch(Timestamp::now());
        out.emit_event(Event::ExecutionCompleting {
            execution: completing_execution,
        });

        let children = exec.active_children.clone();
        let pending_children = cancel_timers(out, children);
        if pending_children == 0 {
            let mut completed_execution = exec.value();
            completed_execution.status = ExecutionStatus::Completed;
            completed_execution.output = Some(output.clone());
            completed_execution.meta.touch(Timestamp::now());
            // Completion is observable durably: `start` returns the execution id and the caller's
            // `wait_for_execution` poll surfaces this terminal `ExecutionCompleted` from Storage. No
            // deferred ack is needed — terminal notification travels through the poll rather than an
            // `execution → request` ack mapping (see `Engine::wait_for_execution`).
            let completed_event = Event::ExecutionCompleted {
                execution: completed_execution,
            };
            out.emit_event(completed_event);
            // The top-level run has no parent — `Engine::start` observes its `ExecutionCompleted`
            // directly — but a relayed finish (from a scope below) never arrives here, so no owner
            // relay is needed for a root execution.
        } else {
            tracing::debug!(
                execution = %execution,
                pending = pending_children,
                "execution completing deferred: waiting on owned children"
            );
        }
    }
}

/// Cancel every `Timer` child of a scope's in-flight set, returning how many are still pending.
/// A scope's non-timer children (its activity) drain through their own terminal cascade, not here.
/// Shared by the `Execution` and `Thread` completion handlers.
pub(super) fn cancel_timers(
    out: &mut Collector,
    children: std::collections::HashSet<ObjectReference>,
) -> usize {
    let mut pending = 0usize;
    for child in children {
        if child.kind == ObjectKind::Timer {
            out.emit_command(Command::CancelTimer { timer: child });
            pending += 1;
        }
    }
    pending
}
