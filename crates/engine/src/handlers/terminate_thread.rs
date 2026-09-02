use async_trait::async_trait;

use crate::ThreadStatus;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::log::Timestamp;
use crate::storage::ScopeRecord;
use crate::types::command::Command;
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectReference};

/// Handles `TerminateThread`: begins the abnormal finish of a fan-out `Thread` with `reason`.
/// Mirrors [`TerminateExecutionHandler`](super::terminate_execution::TerminateExecutionHandler) but
/// resolved against **thread** storage: a `Thread` is only terminated internally by its owning
/// container Activity (a `Parallel`/`Map` sweep tearing down a branch/item), never by the external
/// name-addressed root terminate. Emits `ThreadTerminating`, sweeps owned children (timers, child
/// activities, and nested child threads — each recursively terminating its own subtree), and —
/// once drained — emits `ThreadTerminated{reason}` plus a `ProcessChildCompleted` relay so the
/// owning container converges on its settle.
#[derive(Default)]
pub struct TerminateThreadHandler;

#[async_trait]
impl CommandHandler for TerminateThreadHandler {
    fn command(&self) -> Command {
        Command::TerminateThread {
            thread: ObjectReference::nil(),
            reason: crate::types::command::TerminationReason::Cancelled,
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::TerminateThread { thread, reason } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        // The addressed run is a fan-out `Thread`; resolve it from thread storage. Any other kind is
        // an internal fault and the termination is dropped (nothing to tear down).
        let scope = match crate::storage::load_scope_ref(ctx.storage, thread).await {
            Ok(Some(s)) => s,
            Ok(None) => return, // thread already gone — nothing to terminate.
            Err(_) => return,   // storage fault — the fold errors out; no fabricated Reject.
        };
        let ScopeRecord::Thread(thread_row) = scope else {
            tracing::debug!(
                target = %thread,
                "terminate_thread: addressed node is not a thread; ignition ignored"
            );
            return;
        };
        let thread_ref = thread_row.reference();
        if !thread_row.value.status.is_running() {
            return; // already finishing or terminal — a later event wins.
        }

        let mut terminating_thread = thread_row.value();
        terminating_thread.status = ThreadStatus::Terminating(reason.clone());
        // Advance the domain `updated_at` at event construction (not Entry metadata); `created_at`
        // carries forward.
        terminating_thread.meta.touch(Timestamp::now());
        out.emit_event(Event::ThreadTerminating {
            thread: terminating_thread,
        });

        let children = thread_row.active_children.clone();
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
                // A nested container within this thread (a `Parallel`/`Map` branch that itself
                // fans out) owns child threads which must themselves be torn down recursively.
                ObjectKind::Thread => {
                    out.emit_command(Command::TerminateThread {
                        thread: child,
                        reason: reason.clone(),
                    });
                    pending += 1;
                }
                // A thread never directly owns an `Execution` child (its container runs threads,
                // not top-level executions), so nothing to sweep here.
                ObjectKind::Execution => {}
                // A `Task` is owned by an activity, not by the thread directly; the `TerminateState`
                // sweep above cancels each activity's tasks. Nothing to do here.
                ObjectKind::Task => {}
                // A thread owns no Flow/FlowVersion child either.
                _ => {}
            }
        }
        if pending == 0 {
            let mut terminated_thread = thread_row.value();
            terminated_thread.status = ThreadStatus::Terminated(reason.clone());
            terminated_thread.meta.touch(Timestamp::now());
            out.emit_event(Event::ThreadTerminated {
                thread: terminated_thread,
            });
            // Relay the settle to its owning container Activity so the `Parallel`/`Map` converges
            // once its last branch/item drains (mirrors the `Execution` completion relay).
            if let Some(owner) = thread_row.value.meta.owner.clone() {
                out.emit_command(Command::ProcessChildCompleted {
                    parent: owner,
                    child: thread_ref.clone(),
                });
            }
        } else {
            tracing::debug!(
                thread = %thread_ref,
                pending,
                "thread terminating deferred: waiting on owned children"
            );
        }
    }
}
