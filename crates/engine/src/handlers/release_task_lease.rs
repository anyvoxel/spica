use async_trait::async_trait;

use crate::TaskStatus;
use crate::command::Command;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};

/// Handles `ReleaseTaskLease`: a claimed task's `TaskLease` deadline elapsed without a settlement
/// (Zeebe activation timeout / worker unavailability), so the task is re-queued for another worker.
///
/// Idempotent: a no-op unless the task is still `Running` — a task that already settled (or was
/// released) is left alone. This is what makes the at-least-once contract safe: a lease that expired
/// while the worker's close was still in flight re-queues the task, and the eventual settle is then
/// rejected by the lease guard (the re-leased worker owns it), so state advances exactly once. The
/// emit `TaskLeaseExpired` returns the task to `Pending` (worker/lease cleared), available for a fresh
/// pull.
#[derive(Default)]
pub struct ReleaseTaskLeaseHandler;

#[async_trait]
impl CommandHandler for ReleaseTaskLeaseHandler {
    fn command(&self) -> Command {
        Command::ReleaseTaskLease {
            task: crate::id::TaskId::nil(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::ReleaseTaskLease { task } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };

        let act = match ctx.storage.get_task(*task).await {
            Ok(Some(t)) => t,
            Ok(None) | Err(_) => return, // task gone; nothing to release.
        };
        // Only a task still leased can be released. One already settled, or already re-queued, is a
        // no-op — the lease-expiry timer and a settle can race, and the first writer wins.
        if !act.status.is_running() {
            return;
        }

        // Return the task to `Pending`, clearing the worker/lease so a fresh pull can claim it.
        let mut task_value = act.value();
        task_value.status = TaskStatus::Pending;
        task_value.worker_id = None;
        task_value.lease_until = None;
        out.emit_event(Event::TaskLeaseExpired { task: task_value });
    }
}
