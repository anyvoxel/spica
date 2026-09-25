use crate::ThreadStatus;
use crate::handler::{Collector, HandlerContext};
use crate::storage::ScopeRecord;
use crate::types::command::{Command, TerminateExecution, TerminateState, TerminateThread};
use crate::types::event::Event;
use crate::types::meta::ObjectKind;

/// Handles `TerminateThread`: begins the abnormal finish of a fan-out `Thread` with `reason`.
/// Mirrors [`TerminateExecutionHandler`](super::terminate_execution::TerminateExecutionHandler) but
/// resolved against **thread** storage: a `Thread` is only terminated internally by its owning
/// container Activity (a `Parallel`/`Map` sweep tearing down a branch/item), never by the external
/// name-addressed root terminate. Emits `ThreadTerminating`, sweeps owned children (timers, child
/// activities, and nested child threads — each recursively terminating its own subtree), and —
/// once drained — emits `ThreadTerminated{reason}` plus the inline child-settled reaction so the
/// owning container converges on its settle.
#[derive(Default)]
pub struct TerminateThreadHandler;

impl TerminateThreadHandler {
    pub(crate) async fn handle(
        &self,
        p: &TerminateThread,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) {
        let TerminateThread { thread, reason } = p;
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
        terminating_thread.meta.with_update_at(ctx.now());
        out.append_event(Event::ThreadTerminating {
            thread: terminating_thread,
        })
        .await;

        // A root thread (owner = the Execution) stands in for the whole run: starting its abnormal
        // finish must also start the execution's, or an internal top-level failure would leave the
        // execution Running forever. Only relay while the execution is still Running — if it already
        // went Terminating (an external cancel that swept us here), that terminal already wins.
        if let Some(owner) = thread_row.value.meta.owner.clone()
            && owner.kind == ObjectKind::Execution
            && let Ok(Some(exec)) = ctx.storage.get_execution(&owner).await
            && exec.status.is_running()
        {
            out.append_command(Command::TerminateExecution(TerminateExecution {
                name: owner.name.clone(),
                uid: Some(owner.uid),
                reason: reason.clone(),
            }));
        }

        let children = thread_row.active_children.clone();
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
                // A nested container within this thread (a `Parallel`/`Map` branch that itself
                // fans out) owns child threads which must themselves be torn down recursively.
                ObjectKind::Thread => {
                    out.append_command(Command::TerminateThread(TerminateThread {
                        thread: child,
                        reason: reason.clone(),
                    }));
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
            terminated_thread.meta.with_update_at(ctx.now());
            out.append_event(Event::ThreadTerminated {
                thread: terminated_thread,
            })
            .await;
            // Run the inline child-settled reaction so the owning container converges (mirrors the
            // `Execution` termination reaction).
            if let Some(owner) = thread_row.value.meta.owner.clone() {
                super::child_completed::child_settled(ctx, out, owner, thread_ref.clone()).await;
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
