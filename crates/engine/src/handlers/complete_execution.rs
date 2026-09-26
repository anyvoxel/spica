use crate::ExecutionStatus;
use crate::handler::{Collector, HandlerContext};
use crate::types::command::{Command, CompleteExecution};
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

impl CompleteExecutionHandler {
    pub(crate) async fn handle(
        &self,
        p: &CompleteExecution,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) {
        let CompleteExecution { execution, output } = p;
        // Addressed by kind, so read the execution directly rather than through the generic scope
        // reader: `CompleteExecution` is only ever dispatched for a top-level `Execution`.
        let exec = match ctx.storage.get_execution(execution).await {
            Ok(Some(e)) => e,
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
        if !exec.value.status.is_running() {
            return; // idempotency: already finishing or terminal.
        }

        let mut completing_execution = exec.value();
        completing_execution.status = ExecutionStatus::Completing;
        completing_execution.output = Some(output.clone());
        // A new lifecycle transition — advance the domain `updated_at` (stemmed at event
        // construction, not from Entry metadata); `created_at` is carried forward unchanged.
        completing_execution.meta.with_update_at(ctx.now());
        out.append_event(Event::ExecutionCompleting {
            execution: completing_execution,
        })
        .await;

        let children = exec.active_children.clone();
        let pending_children = cancel_timers(out, children);
        if pending_children == 0 {
            let mut completed_execution = exec.value();
            completed_execution.status = ExecutionStatus::Completed;
            completed_execution.output = Some(output.clone());
            completed_execution.meta.with_update_at(ctx.now());
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
    out: &mut Collector<'_>,
    children: std::collections::HashSet<ObjectReference>,
) -> usize {
    let mut pending = 0usize;
    for child in children {
        if child.kind == ObjectKind::Timer {
            out.append_command(Command::CancelTimer { timer: child });
            pending += 1;
        }
    }
    pending
}
