use async_trait::async_trait;
use spica_asl::State;

use crate::TimerStatus;
use crate::command::{Command, TerminationReason, TimerPurpose};
use crate::error::ExecutionError;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::id::NodeId;

/// Handles `TriggerTimer`: a timer's deadline elapsed. Idempotent (a no-op if the timer is gone or
/// already terminal). Dispatches by `purpose`: `WaitResume` fires the owning state;
/// `ExecutionTimeout` terminates the owning execution `TimedOut`.
#[derive(Default)]
pub struct TriggerTimerHandler;

#[async_trait]
impl CommandHandler for TriggerTimerHandler {
    fn command(&self) -> Command {
        Command::TriggerTimer {
            timer: crate::id::TimerId::nil(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::TriggerTimer { timer } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };

        let act = match ctx.storage.get_timer(*timer).await {
            Ok(Some(t)) => t,
            Ok(None) | Err(_) => return, // timer never armed; nothing to do.
        };
        if act.value.status != TimerStatus::Active {
            return; // already completed/cancelled — a duplicate fire is a no-op.
        }

        out.emit_event(Event::TimerTriggered {
            timer: crate::TimerValue {
                id: act.id,
                parent: act.parent,
                purpose: act.purpose,
                status: crate::TimerStatus::Completed,
                deadline: act.deadline,
            },
        });

        match act.value.purpose {
            TimerPurpose::WaitResume => {
                // Resume the owning state: the activity's `CompleteState` runs its `complete`.
                let activity_id = match act.value.parent {
                    NodeId::Activity(a) => a,
                    _ => return, // a Wait timer without an activity owner is an internal fault.
                };
                out.emit_command(Command::CompleteState {
                    activity: activity_id,
                });
            }
            TimerPurpose::TaskRetryDelay => {
                // A Task state's `Retry` backoff elapsed: re-invoke the owning task. The activity
                // must still be `Running` (it may have since been terminated/cancelled, in which
                // case the re-invocation is a no-op guard), and its owner must be an Execution.
                let activity_id = match act.value.parent {
                    NodeId::Activity(a) => a,
                    _ => return,
                };
                let activity = match ctx.storage.get_activity(activity_id).await {
                    Ok(Some(a)) => a,
                    _ => return, // activity gone — a retry no longer applies.
                };
                if activity.value.status != crate::ActivityStatus::Running {
                    return; // activity no longer running — drop the retry.
                }
                let execution_id = match activity.value.parent {
                    NodeId::Execution(e) => e,
                    _ => return, // internal fault: activity not owned by an execution.
                };
                let exec = match ctx.storage.get_execution(execution_id).await {
                    Ok(Some(e)) => e,
                    _ => return,
                };
                // Resolve the owning Task definition — a Parallel-branch child execution resolves
                // its Task within its branch's `states` (via its `state_path`), so a retry timer
                // re-invokes against the right per-branch definition. Load the bound machine revision
                // (cached by the StreamProcessor) before resolving the state.
                let sm = match ctx.machine(exec.flow_version_id).await {
                    Ok(s) => s,
                    Err(_) => return, // definition gone — nothing to re-invoke.
                };
                let state_def = match super::resolve_state_for(
                    ctx.storage,
                    &sm,
                    execution_id,
                    &crate::handlers::state_name_from_path(activity.value.state_path.as_ptr()),
                )
                .await
                {
                    Ok(s) => s,
                    Err(_) => return, // definition gone — nothing to re-invoke.
                };
                let State::Task(task_state) = state_def else {
                    return; // a retry timer under a non-Task activity is an internal fault.
                };
                // Re-invoke with the fresh projectable `arguments` (deterministic — same payload a
                // fresh `activate` would build), bound to `$states.context.State.RetryCount` so a
                // backoff formula in `Arguments` can react to the attempt.
                let state_name =
                    crate::handlers::state_name_from_path(activity.value.state_path.as_ptr());
                let states = crate::context::build_states(
                    &activity.value.input,
                    None,
                    &state_name,
                    &exec.input,
                    None,
                    activity.value.retry_state.retry_count,
                    None,
                    None, // not a Map item — no `context.Map.Item` binding
                );
                let arguments = match &task_state.arguments {
                    Some(arguments) => match ctx.env.eval_json(arguments, &states, &exec.variables)
                    {
                        Ok(v) => v,
                        Err(_) => return, // arguments no longer evaluable — drop the retry.
                    },
                    None => activity.value.input.clone(),
                };
                let task = out.next_task();
                out.emit_command(Command::ActivateTask {
                    parent: NodeId::Activity(activity_id),
                    task,
                    resource: task_state.resource.clone(),
                    arguments,
                });
            }
            TimerPurpose::TaskTimeout => {
                // A Task state's `TimeoutSeconds` elapsed before the in-flight task settled. Fail
                // the owning task with `States.Timeout` and route it through the state's
                // `Retry`/`Catch` policy — the same decision a settled failure takes. The timer is
                // parented on the owning activity (like `WaitResume`), so its deadline is enforced
                // by the scheduler and swept when the activity terminates; we discover the in-flight
                // task by asking the activity for its active child task.
                let activity_id = match act.value.parent {
                    NodeId::Activity(a) => a,
                    _ => return,
                };
                let in_flight = ctx
                    .storage
                    .get_children(NodeId::Activity(activity_id))
                    .await
                    .ok()
                    .and_then(|cs| {
                        cs.into_iter().find_map(|c| match c {
                            NodeId::Task(t) => Some(t),
                            _ => None,
                        })
                    });
                let Some(task) = in_flight else {
                    return; // no in-flight task — the timeout no longer applies.
                };
                // Fail the task with the engine-authoritative timeout: `worker_id` is empty (this is
                // not a worker report, so no lease-match check applies — the deadline is the engine's
                // own backstop). `FailTaskHandler` routes it through Retry/Catch/terminate.
                out.emit_command(Command::FailTask {
                    task,
                    worker_id: String::new(),
                    error: ExecutionError::TimedOut {
                        message: format!(
                            "task ran past its TimeoutSeconds deadline ({})",
                            act.value.deadline.as_millis()
                        ),
                    },
                });
            }
            TimerPurpose::TaskLease => {
                // A claimed task's lease (Zeebe activation timeout) elapsed without a settle: re-queue
                // it (`Pending`) so a stalled / crashed worker does not hold it forever. Parented on
                // the owning activity like `TaskTimeout`; find the in-flight task child and release it
                // (no-op if it already settled — `ReleaseTaskLeaseHandler` validates status).
                let activity_id = match act.value.parent {
                    NodeId::Activity(a) => a,
                    _ => return,
                };
                let in_flight = ctx
                    .storage
                    .get_children(NodeId::Activity(activity_id))
                    .await
                    .ok()
                    .and_then(|cs| {
                        cs.into_iter().find_map(|c| match c {
                            NodeId::Task(t) => Some(t),
                            _ => None,
                        })
                    });
                let Some(task) = in_flight else {
                    return; // no in-flight task — the lease no longer applies.
                };
                out.emit_command(Command::ReleaseTaskLease { task });
            }
            TimerPurpose::ExecutionTimeout => {
                // The execution ran past its `TimeoutSeconds` deadline. Drive it to a `TimedOut`
                // termination; any in-flight children drain via the cascade started by
                // `TerminateExecution`.
                let execution_id = match act.value.parent {
                    NodeId::Execution(e) => e,
                    _ => return,
                };
                out.emit_command(Command::TerminateExecution {
                    id: execution_id,
                    reason: TerminationReason::Failed {
                        error: ExecutionError::TimedOut {
                            message: format!(
                                "execution ran past its TimeoutSeconds deadline ({})",
                                act.value.deadline.as_millis()
                            ),
                        },
                    },
                });
            }
        }
    }
}
