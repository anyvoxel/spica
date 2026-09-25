use std::time::Duration;

use crate::Task;
use crate::TaskStatus;
use crate::handler::{Collector, HandlerContext};
use crate::log::Timestamp;
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
    ) {
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
            out.append_event(Event::TasksClaimed(TasksClaimed {
                request_id: *request_id,
                tasks: Vec::new(),
            }))
            .await;
            return;
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
                out.append_event(Event::TasksClaimed(TasksClaimed {
                    request_id: *request_id,
                    tasks: Vec::new(),
                }))
                .await;
                return;
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
            claimed.push(emit_lease(t.value, worker_id, lease_expires_at, out).await);
        }
        // One batched claim fact for the whole poll — every appended `ClaimTasks` answers its awaiting
        // caller with a durable `TasksClaimed` (all entries share this single causal batch).
        out.append_event(Event::TasksClaimed(TasksClaimed {
            request_id: *request_id,
            tasks: claimed,
        }))
        .await;
    }
}

/// Mark `task_value` claimed (leased to `worker_id` until `lease_expires_at`). The `ClaimTasks` handler
/// uses it so every claim leases identically. No timer is armed for the lease: expiry is decided
/// lazily by whichever poll next observes [`Task::is_claimable_at`], so a claim writes no side-effect
/// child and the only durable trace of the window is `lease_expires_at` on the task itself. Returns the
/// mutated claim value for the handler to collect into its single batched `TasksClaimed`.
async fn emit_lease(
    mut task_value: Task,
    worker_id: &str,
    lease_expires_at: Timestamp,
    out: &mut Collector<'_>,
) -> Task {
    task_value.status = TaskStatus::Running;
    task_value.worker_id = Some(worker_id.to_string());
    task_value.lease_expires_at = Some(lease_expires_at);
    // Stamp the claim moment; `created_at` is already carried on `task_value`.
    task_value.meta.with_update_at(out.now());
    // Claimed — the backoff gate is spent (cleared so a later re-claim after a lapsed lease is
    // immediately claimable rather than inheriting a stale waiting period).
    task_value.retry_state.next_available_at = None;
    task_value
}
