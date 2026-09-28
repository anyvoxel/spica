use std::time::Duration;

use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::ClaimTasks;
use crate::types::event::{Event, TasksClaimed};
use crate::types::meta::ObjectKind;

/// Handles `ClaimTasks` — the durable claim behind `TaskApi::poll_tasks` (Zeebe `ActivateJobs`).
///
/// Runs in the StreamProcessor's serialized, lock-holding command arm, so discovery and leasing are
/// decided against the same projection snapshot the fold writes. It discovers up to `max_tasks`
/// claimable tasks of `resource` ([`Task::is_claimable_at`] — pending with backoff lapsed, or a
/// lease that has expired) and leases each to `worker_id`, reporting one batched `TasksClaimed`
/// **durable** response. The awaiting `poll_tasks` resolves its grant from that durable event — the
/// discovery-time `Task` snapshot embedded in it — so the exact set is re-derived from the log,
/// never re-read.
///
/// The returned set is the handler's *discovery-time* grant (direct return, not re-read): a narrow
/// race — a concurrent pull that discovers the same task before this one's `TasksClaimed` is applied —
/// can hand a task to two workers. The `TasksClaimed` applier re-decides claimability per entry with
/// the *entry's* timestamp, so the lease stake is exactly-once regardless, and the *state* never
/// advances twice even if the *work* is at-least-once (handlers must be idempotent, Zeebe's contract).
#[derive(Default)]
pub struct ClaimTasksHandler;

impl ClaimTasksHandler {
    pub(crate) async fn handle(
        &self,
        p: &ClaimTasks,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let ClaimTasks {
            request_id,
            worker_id,
            resource,
            max_tasks,
            lease_seconds,
        } = p;
        // Read *now* once, at the moment the lease window is decided: the persisted `lease_expires_at` is
        // what a restarted engine and every later poll reconstruct claimability from. A `None` on
        // overflow means we grant an empty set (a defensively-clamped `now` lease would be expired
        // on arrival — absurd for a pull).
        let now = ctx.now();
        let Some(lease_expires_at) = now.checked_add(Duration::from_secs(*lease_seconds)) else {
            // TODO：发生这种情况的话，应该回复一个 Reject，而不是直接静默掉
            out.append_event(Event::TasksClaimed(TasksClaimed {
                request_id: *request_id,
                tasks: Vec::new(),
            }))
            .await;
            return Ok(());
        };
        // Discover claimable tasks of `resource` under the command arm's storage lock — the same
        // snapshot the fold writes against — so allocation is decided against authoritative state.
        let tasks = match ctx
            .storage
            .activatable_tasks(resource, now, *max_tasks)
            .await
        {
            Ok(t) => t,
            // Discovery is a read; a failure here leaves the pull with nothing granted — the empty
            // debt is still answered, rather than failing the whole worker loop.
            Err(_) => {
                // TODO：应该返回一个 Reject，而不是直接静默掉
                out.append_event(Event::TasksClaimed(TasksClaimed {
                    request_id: *request_id,
                    tasks: Vec::new(),
                }))
                .await;
                return Ok(());
            }
        };
        let mut claimed = Vec::new();
        for t in tasks {
            // A task without an activity owner is an internal fault — the settle paths
            // (`complete_task`/`fail_task`) expect one — so leave it unclaimed rather than hand a
            // worker a task it could never settle.
            if t.meta
                .owner
                .as_ref()
                .is_none_or(|o| o.kind != ObjectKind::Activity)
            {
                continue;
            }
            let mut value = t.value;
            // The claim moment is the very reading the lease window was computed from, so the row's
            // transition stamp and its `lease_expires_at` share one base.
            value.claim(worker_id, lease_expires_at, now);
            claimed.push(value);
        }
        // One batched claim fact for the whole poll — every appended `ClaimTasks` answers its awaiting
        // caller with a durable `TasksClaimed` (all entries share this single causal batch).
        out.append_event(Event::TasksClaimed(TasksClaimed {
            request_id: *request_id,
            tasks: claimed,
        }))
        .await;

        Ok(())
    }
}
