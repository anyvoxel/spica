use crate::RejectionType;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::{Command, CompleteState, FailTask, TerminationReason, TimerPurpose};
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::{HasRawObjectRef, ObjectKind, ObjectRef, OwnerScope, TimerOwner};
use crate::types::task::TaskKind;
use crate::types::timer::TimerKind;

/// Handles `TriggerTimer`: a timer's deadline elapsed. A fire for a timer that is gone or no longer
/// `Active` is refused, not acted on — the durable record of a fire that lost its race. Dispatches by
/// `purpose`: `WaitResume` fires the owning state; `ExecutionTimeout` terminates the owning execution
/// `TimedOut`.
#[derive(Default)]
pub struct TriggerTimerHandler;

impl TriggerTimerHandler {
    pub(crate) async fn handle(
        &self,
        timer: &ObjectRef<TimerKind>,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        // A timer row is written by the batch that arms it, and the scheduler only learns to fire it
        // once that batch is durable — so a miss is the log and the projection disagreeing (a forged
        // command, or a corrupt store), not a fire that outlived its timer. Nothing ever removes a row,
        // so an already-fired timer keeps its row and lands in the status guard below instead.
        let act = match ctx.storage.get_timer(timer).await? {
            Some(t) => t,
            None => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("trigger_timer: timer {timer} does not exist; fire refused"),
                ));
            }
        };
        // A duplicate fire, or one racing the `CancelTimer` that swept the timer first: the target is
        // in the wrong state, told apart from the miss above by its reason. The fire is recorded rather
        // than swallowed, so the durable log explains why the deadline went unenforced.
        if !act.value.status.is_active() {
            tracing::warn!(
                timer = %timer,
                status = ?act.value.status,
                purpose = ?act.value.purpose,
                "fire arrived for a timer that is no longer Active; refused"
            );
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "trigger_timer: timer {timer} is {:?}, not Active; fire refused",
                    act.value.status
                ),
            ));
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
                // A resume timer is armed by the activity whose deadline it is, so the slot's
                // `Activity` variant is the only one this purpose can carry — a timer owned by
                // anything else has no state to resume, and the fire is dropped as an internal fault.
                let TimerOwner::Activity(activity) = act.value.meta.owner.clone() else {
                    return Ok(());
                };
                // Relay the settle now that the fired timer's edge is gone from the activity. A
                // `Terminating` activity parked on this timer — a cancel raced the fire — drains only
                // here: its own `CancelTimer` sweep no-ops on an already-fired timer, and the
                // `CompleteState` below is refused by a non-Running activity.
                super::child_completed::child_settled(
                    ctx,
                    out,
                    activity.as_raw_object_ref().clone(),
                    timer.as_raw_object_ref().clone(),
                )
                .await;
                // A Wait's raw result is its processed input (no distinct raw output). Load it so the
                // `CompleteState` command carries the raw result, keeping the command self-describing.
                let raw_result = match ctx.storage.get_activity(&activity).await {
                    Ok(Some(act)) => act.value().input.clone().unwrap_or(serde_json::Value::Null),
                    _ => serde_json::Value::Null, // owner gone — the handler will no-op.
                };
                out.append_command(Command::CompleteState(CompleteState {
                    activity,
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
                // Parented on the owning activity, like `WaitResume` — the same single-variant slot.
                let TimerOwner::Activity(activity) = act.value.meta.owner.clone() else {
                    return Ok(());
                };
                // Same settle relay as `WaitResume`: it must run even when no in-flight task is found,
                // since that is exactly the case where a cancel already swept the task and only this
                // fired timer is holding the activity open.
                super::child_completed::child_settled(
                    ctx,
                    out,
                    activity.as_raw_object_ref().clone(),
                    timer.as_raw_object_ref().clone(),
                )
                .await;
                let in_flight = ctx
                    .storage
                    .get_children(activity.as_raw_object_ref().clone())
                    .await
                    .ok()
                    .and_then(|cs| cs.into_iter().find(|c| c.kind == ObjectKind::Task));
                let Some(task) = in_flight else {
                    return Ok(()); // no in-flight task — the timeout no longer applies.
                };
                let task = task.typed::<TaskKind>();
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
                // terminate command. Only `create_execution` arms this purpose, for the run itself,
                // so the slot's `Execution` variant is the only one it can carry — the *scope*
                // termination helper is still the route here because its other callers terminate a
                // branch's `Thread`.
                let TimerOwner::Execution(execution) = act.value.meta.owner.clone() else {
                    return Ok(());
                };
                let reason = TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::TimedOut {
                        message: format!(
                            "execution ran past its TimeoutSeconds deadline ({})",
                            act.value.deadline.as_millis()
                        ),
                    }),
                };
                super::emit_scope_termination(out, &OwnerScope::Execution(execution), reason);
            }
        }

        Ok(())
    }
}
