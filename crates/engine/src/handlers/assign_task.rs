use async_trait::async_trait;
use std::time::Duration;

use super::emit_timer;
use crate::Task;
use crate::TaskStatus;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::log::Timestamp;
use crate::task_api::ActivatedTask;
use crate::types::command::{Command, TimerPurpose};
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectReference};

/// Handles `ClaimTasks` — the durable claim behind `TaskApi::poll_tasks` (Zeebe `ActivateJobs`).
///
/// Runs in the StreamProcessor's serialized, lock-holding command arm, so discovery and leasing are
/// decided against the same projection snapshot the fold writes. It discovers up to `max_tasks`
/// `Pending` tasks of `resource`, leases each to `worker_id` (arming its `TaskLease` timer), and
/// returns the granted set to the awaiting `poll_tasks` via the acknowledgment channel
/// (`AckOutcome::Granted`). All claimed tasks ride **one** batched `TasksClaimed` event (they share
/// this single causal batch), with per-task lease timers.
///
/// The returned set is the handler's *discovery-time* grant (direct return, not re-read): a narrow
/// race — a concurrent pull that discovers the same task before this one's `TasksClaimed` is applied —
/// can hand a task to two workers. The `TasksClaimed` applier's conditional per-entry fold
/// (`Pending`→`Running` only) makes the lease stake exactly-once regardless, so the *state* never
/// advances twice even if the *work* is at-least-once (handlers must be idempotent, Zeebe's contract).
#[derive(Default)]
pub struct ClaimTasksHandler;

#[async_trait]
impl CommandHandler for ClaimTasksHandler {
    fn command(&self) -> Command {
        Command::ClaimTasks {
            request_id: crate::types::id::RequestId::nil(),
            worker_id: String::new(),
            resource: String::new(),
            max_tasks: 0,
            lease_seconds: 0,
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::ClaimTasks {
            request_id,
            worker_id,
            resource,
            max_tasks,
            lease_seconds,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        // Lease horizon: `now + lease_seconds`, persisted as the absolute deadline so the paired
        // `TaskLease` timer (and a restarted engine) reconstruct the same window; a `None` on
        // overflow means we grant an empty set (a defensively-clamped `now` lease would fire
        // immediately — absurd for a pull).
        let Some(lease_until) = Timestamp::now().checked_add(Duration::from_secs(*lease_seconds))
        else {
            out.ack_request_tasks(*request_id, Vec::new());
            return;
        };
        // Discover `Pending` tasks of `resource` under the command arm's storage lock — the same
        // snapshot the fold writes against — so allocation is decided against authoritative state.
        let tasks = match ctx.storage.activatable_tasks(resource, *max_tasks).await {
            Ok(t) => t,
            // Discovery is a read; a failure here leaves the pull with nothing granted — report the
            // empty set rather than failing the whole worker loop.
            Err(_) => {
                out.ack_request_tasks(*request_id, Vec::new());
                return;
            }
        };
        let mut granted = Vec::with_capacity(tasks.len());
        let mut claimed = Vec::new();
        for t in tasks {
            // A retrying task is not claimable until its `next_available_at` backoff gate lapses;
            // skip it (it stays `Pending` and is returned by a later poll).
            if t.retry_state
                .next_available_at
                .is_some_and(|at| Timestamp::now() < at)
            {
                continue;
            }
            // The task's own node context is the activity that invoked it; the lease timer is
            // parented there.
            let activity_id = t
                .meta
                .owner
                .clone()
                .expect("a claimable task is always owned by an activity");
            if activity_id.kind != ObjectKind::Activity {
                continue; // a task without an activity owner is an internal fault; skip it.
            }
            // Resolve the owning execution — the `TaskLease` timer belongs to it (its `execution`
            // field and, eventually, its name-prefix). Carry it when the activity is present; when the
            // owner row is absent (raw-seam dispatch) fall back to nil rather than skipping a claimable
            // task.
            let execution = match ctx.storage.get_activity(&activity_id).await {
                Ok(Some(a)) => a.value().execution,
                _ => ObjectReference::nil(),
            };
            granted.push(ActivatedTask {
                task: t.value.meta.name.clone(),
                resource: t.value.resource.clone(),
                arguments: t.value.arguments.clone(),
            });
            claimed.push(emit_lease(
                activity_id,
                execution,
                t.value,
                worker_id,
                lease_until,
                out,
            ));
        }
        // One batched claim fact for the whole poll — all entries share this single causal batch
        // (the per-task lease timers above are their own arming facts).
        if !claimed.is_empty() {
            out.emit_event(Event::TasksClaimed { tasks: claimed });
        }
        // Return the discovery-time grant to the awaiting `poll_tasks`; the conditional `TasksClaimed`
        // applier reconciles any later stale/racing lease to exactly-once state.
        out.ack_request_tasks(*request_id, granted);
    }
}

/// Mark `task_value` claimed (leased to `worker_id` until `lease_until`) and arm its `TaskLease`
/// expiry timer under the owning `activity_id` (belonging to `execution`). The `ClaimTasks` handler
/// uses it so every claim leases + arms identically — the timer, on firing, re-queues the task so a
/// stalled / crashed worker does not hold it forever. The timer is minted inline so the arm lands in
/// the same causal batch as the claimed task. Returns the mutated claim value for the handler to
/// collect into its single batched `TasksClaimed`.
fn emit_lease(
    activity_id: ObjectReference,
    execution: ObjectReference,
    mut task_value: Task,
    worker_id: &str,
    lease_until: Timestamp,
    out: &mut Collector,
) -> Task {
    task_value.status = TaskStatus::Running;
    task_value.worker_id = Some(worker_id.to_string());
    task_value.lease_until = Some(lease_until);
    // Stamp the claim moment; `created_at` is already carried on `task_value`.
    task_value.meta.touch(Timestamp::now());
    // Claimed — the backoff gate is spent (cleared so a later lease-expiry re-queue is immediately
    // claimable rather than inheriting a stale waiting period).
    task_value.retry_state.next_available_at = None;
    // Arm the lease-expiry timer (Zeebe activation timeout) under the owning activity, mirroring how
    // `TaskTimeout` is owned.
    emit_timer(
        out,
        execution,
        activity_id,
        TimerPurpose::TaskLease,
        lease_until,
    );
    task_value
}
