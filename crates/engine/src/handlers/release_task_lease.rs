use async_trait::async_trait;

use crate::TaskStatus;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::types::command::Command;
use crate::types::event::Event;

/// Handles `ReleaseTaskLease`: a claimed task's `DeliveryLease` deadline elapsed without a settlement
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
            task: crate::types::meta::ObjectReference::nil(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::ReleaseTaskLease { task } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };

        let act = match ctx.storage.get_task(task).await {
            Ok(Some(t)) => t,
            Ok(None) | Err(_) => return, // task gone; nothing to release.
        };
        // Only a task still leased can be released. One already settled, or already re-queued, is a
        // no-op — the lease-expiry timer and a settle can race, and the first writer wins.
        if !act.status.is_running() {
            return;
        }

        // Return the task to `Pending`, clearing the worker/lease so a fresh pull can claim it.
        // `next_available_at` is cleared too: a lease-expiry re-queue is a *new* eligibility (the
        // prior backoff gate belongs to a claim that is no longer held), so the task is immediately
        // claimable rather than inheriting a stale waiting period.
        let mut task_value = act.value();
        task_value.status = TaskStatus::Pending;
        task_value.worker_id = None;
        task_value.lease_until = None;
        task_value.retry_state.next_available_at = None;
        // Stamp the re-queue moment; `created_at` is already carried on `task_value`.
        task_value.meta.touch(crate::log::Timestamp::now());
        out.emit_event(Event::TaskLeaseExpired { task: task_value });
    }
}
