use crate::TimerStatus;
use crate::handler::{Collector, HandlerContext};
use crate::types::command::{Command, CompleteState, FailTask, TerminationReason, TimerPurpose};
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectReference};

/// Handles `TriggerTimer`: a timer's deadline elapsed. Idempotent (a no-op if the timer is gone or
/// already terminal). Dispatches by `purpose`: `WaitResume` fires the owning state;
/// `ExecutionTimeout` terminates the owning execution `TimedOut`.
#[derive(Default)]
pub struct TriggerTimerHandler;

impl TriggerTimerHandler {
    pub(crate) async fn handle(
        &self,
        timer: &ObjectReference,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) {
        let act = match ctx.storage.get_timer(timer).await {
            Ok(Some(t)) => t,
            Ok(None) | Err(_) => return, // timer never armed; nothing to do.
        };
        if act.value.status != TimerStatus::Active {
            return; // already completed/cancelled — a duplicate fire is a no-op.
        }

        out.append_event(Event::TimerTriggered {
            timer: crate::Timer {
                execution: act.value.execution.clone(),
                purpose: act.purpose,
                status: crate::TimerStatus::Completed,
                deadline: act.deadline,
                // Carry the timer's full meta (name/uid/created_at/owner) forward. A timer may be
                // custom-named (`{execution.name}-{suffix}`); reconstructing it via
                // `placeholder_with_times` would re-derive `obj-<uid>` and break the child-edge
                // removal (the child was added under its real name). Stamp the fire moment as
                // `updated_at`.
                meta: {
                    let mut m = act.value.meta.clone();
                    m.with_update_at(ctx.now());
                    m
                },
            },
        })
        .await;

        match act.value.purpose {
            TimerPurpose::WaitResume => {
                // Resume the owning state: the activity's `CompleteState` runs its `complete`.
                let activity_id = act
                    .value
                    .meta
                    .owner
                    .clone()
                    .expect("a live timer is always owned");
                if activity_id.kind != ObjectKind::Activity {
                    return; // a Wait timer without an activity owner is an internal fault.
                }
                // Relay the settle now that the fired timer's edge is gone from the activity. A
                // `Terminating` activity parked on this timer — a cancel raced the fire — drains only
                // here: its own `CancelTimer` sweep no-ops on an already-fired timer, and the
                // `CompleteState` below is refused by a non-Running activity.
                super::child_completed::child_settled(ctx, out, activity_id.clone(), timer.clone())
                    .await;
                // A Wait's raw result is its processed input (no distinct raw output). Load it so the
                // `CompleteState` command carries the raw result, keeping the command self-describing.
                let raw_result = match ctx.storage.get_activity(&activity_id).await {
                    Ok(Some(act)) => act.value().input.clone().unwrap_or(serde_json::Value::Null),
                    _ => serde_json::Value::Null, // owner gone — the handler will no-op.
                };
                out.append_command(Command::CompleteState(CompleteState {
                    activity: activity_id,
                    output: raw_result,
                }));
            }
            TimerPurpose::TaskTimeout => {
                // A Task state's `TimeoutSeconds` elapsed before the in-flight task settled. Fail
                // the owning task with `States.Timeout` and route it through the state's
                // `Retry`/`Catch` policy — the same decision a settled failure takes. The timer is
                // parented on the owning activity (like `WaitResume`), so its deadline is enforced
                // by the scheduler and swept when the activity terminates; we discover the in-flight
                // task by asking the activity for its active child task.
                let activity_id = act
                    .value
                    .meta
                    .owner
                    .clone()
                    .expect("a live timer is always owned");
                if activity_id.kind != ObjectKind::Activity {
                    return;
                }
                // Same settle relay as `WaitResume`: it must run even when no in-flight task is found,
                // since that is exactly the case where a cancel already swept the task and only this
                // fired timer is holding the activity open.
                super::child_completed::child_settled(ctx, out, activity_id.clone(), timer.clone())
                    .await;
                let in_flight = ctx
                    .storage
                    .get_children(activity_id)
                    .await
                    .ok()
                    .and_then(|cs| cs.into_iter().find(|c| c.kind == ObjectKind::Task));
                let Some(task) = in_flight else {
                    return; // no in-flight task — the timeout no longer applies.
                };
                // Fail the task with the engine-authoritative timeout: `worker_id` is empty (this is
                // not a worker report, so no lease-match check applies — the deadline is the engine's
                // own backstop). `FailTaskHandler` routes it through Retry/Catch/terminate.
                out.append_command(Command::FailTask(FailTask {
                    task,
                    worker_id: String::new(),
                    error: ExecutionError::Runtime(RuntimeError::TimedOut {
                        message: format!(
                            "task ran past its TimeoutSeconds deadline ({})",
                            act.value.deadline.as_millis()
                        ),
                    }),
                }));
            }
            TimerPurpose::ExecutionTimeout => {
                // The execution ran past its `TimeoutSeconds` deadline. Drive it to a `TimedOut`
                // termination; any in-flight children drain via the cascade started by the
                // terminate command. The timer's owner is the *scope* — a top-level `Execution` (via
                // `TerminateExecution`) or a `Parallel`-branch / `Map`-item `Thread` (via
                // `TerminateThread`, which the name-addressed form would miss).
                let owner = act
                    .value
                    .meta
                    .owner
                    .clone()
                    .expect("a live timer is always owned");
                let reason = TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::TimedOut {
                        message: format!(
                            "execution ran past its TimeoutSeconds deadline ({})",
                            act.value.deadline.as_millis()
                        ),
                    }),
                };
                super::emit_scope_termination(out, &owner, reason);
            }
        }
    }
}
