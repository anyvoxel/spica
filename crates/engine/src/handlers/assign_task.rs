use async_trait::async_trait;
use std::time::Duration;

use crate::TaskStatus;
use crate::TaskValue;
use crate::command::{Command, TimerPurpose};
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::id::{ActivityId, NodeId};
use crate::job_api::ActivatedTask;
use crate::log::Timestamp;

/// Handles `AssignTask`: a worker claimed an available (`Pending`) task (Zeebe `ActivateJobs`),
/// leasing it to itself for `lease_seconds`.
///
/// Idempotent: only a task that is still `Pending` (available) can be claimed — one already leased to
/// another worker is a no-op, so racing pulls settle to exactly-one owner. On success it emits
/// `TaskLeased` (status → `Running`, worker/lease recorded) and arms a `TaskLease` timer that
/// re-queues the task if it is not settled within the lease — the Zeebe activation-timeout that
/// stops a stalled/crashed worker from holding a task forever.
#[derive(Default)]
pub struct AssignTaskHandler;

#[async_trait]
impl CommandHandler for AssignTaskHandler {
    fn command(&self) -> Command {
        Command::AssignTask {
            task: crate::id::TaskId::nil(),
            worker_id: String::new(),
            lease_seconds: 0,
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::AssignTask {
            task,
            worker_id,
            lease_seconds,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };

        let act = match ctx.storage.get_task(*task).await {
            Ok(Some(t)) => t,
            Ok(None) | Err(_) => return, // task gone; nothing to claim.
        };
        // Only an *available* task can be claimed. A task already leased to someone else (or already
        // settled) is left alone — this is the exactly-one ownership guard for racing pulls.
        if !act.status.is_pending() {
            return;
        }
        let activity_id = match act.parent {
            NodeId::Activity(a) => a,
            _ => return, // a task without an activity owner is an internal fault.
        };

        // Lease horizon: `now + lease_seconds`. Persisted as the absolute deadline so the paired
        // `TaskLease` timer (and a restarted engine) reconstruct the same window.
        let now = crate::log::Timestamp::now();
        let Some(lease_until) = now.checked_add(Duration::from_secs(*lease_seconds)) else {
            // The lease overflows the timestamp range — clamp to now is absurd; fall back to a
            // bare `now` lease that fires immediately (defensively; real leases are seconds-wide).
            return;
        };

        emit_lease(activity_id, act.value(), worker_id, lease_until, out);
    }
}

/// Handles `PullTasks` — the bulk pull behind `TaskApi::activate` (Zeebe `ActivateJobs`).
///
/// Runs in the StreamProcessor's serialized, lock-holding command arm, so discovery and leasing are
/// decided against the same projection snapshot the fold writes. It discovers up to `max_tasks`
/// `Pending` tasks of `resource`, leases each to `worker_id` (emitting `TaskLeased` + arming its
/// `TaskLease` timer exactly like a single `AssignTask`), and returns the granted set to the awaiting
/// `activate` via the acknowledgment channel (`AckOutcome::Granted`).
///
/// The returned set is the handler's *discovery-time* grant (direct return, not re-read): a narrow
/// race — a concurrent pull that discovers the same task before this one's `TaskLeased` is applied —
/// can hand a task to two workers. The `TaskLeased` applier's conditional fold (`Pending`→`Running`
/// only) makes the lease stake exactly-once regardless, so the *state* never advances twice even if
/// the *work* is at-least-once (handlers must be idempotent, Zeebe's contract).
#[derive(Default)]
pub struct PullTasksHandler;

#[async_trait]
impl CommandHandler for PullTasksHandler {
    fn command(&self) -> Command {
        Command::PullTasks {
            request_id: crate::id::RequestId::nil(),
            worker_id: String::new(),
            resource: String::new(),
            max_tasks: 0,
            lease_seconds: 0,
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::PullTasks {
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
        // immediately — absurd for a pull). This is the same `checked_add` the single `AssignTask`
        // applies.
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
        for t in tasks {
            // The task's own node context is the activity that invoked it; the lease timer is
            // parented there exactly as `AssignTask` parents it.
            let activity_id = match t.parent {
                NodeId::Activity(a) => a,
                _ => continue, // a task without an activity owner is an internal fault; skip it.
            };
            granted.push(ActivatedTask {
                task: t.value.id,
                resource: t.value.resource.clone(),
                arguments: t.value.arguments.clone(),
            });
            let mut task_value = t.value;
            task_value.status = TaskStatus::Running;
            task_value.worker_id = Some(worker_id.clone());
            task_value.lease_until = Some(lease_until);
            emit_lease(activity_id, task_value, worker_id, lease_until, out);
        }
        // Return the discovery-time grant to the awaiting `activate`; the conditional `TaskLeased`
        // applier reconciles any later stale/racing lease to exactly-once state.
        out.ack_request_tasks(*request_id, granted);
    }
}

/// Emit a `TaskLeased` for `task_value` (leased to `worker_id` until `lease_until`) and arm its
/// `TaskLease` expiry timer under the owning `activity_id`. Shared by the single-task `AssignTask`
/// and the bulk `PullTasks` handlers so every claim leases + arms identically — the timer, on
/// firing, re-queues the task so a stalled / crashed worker does not hold it forever.
fn emit_lease(
    activity_id: ActivityId,
    mut task_value: TaskValue,
    worker_id: &str,
    lease_until: Timestamp,
    out: &mut Collector,
) {
    task_value.status = TaskStatus::Running;
    task_value.worker_id = Some(worker_id.to_string());
    task_value.lease_until = Some(lease_until);
    out.emit_event(Event::TaskLeased { task: task_value });
    // Arm the lease-expiry timer (Zeebe activation timeout) under the owning activity, mirroring how
    // `TaskTimeout` is parented.
    let timer = out.next_timer();
    out.emit_command(Command::ActivateTimer {
        parent: NodeId::Activity(activity_id),
        timer,
        purpose: TimerPurpose::TaskLease,
        deadline: lease_until,
    });
}
