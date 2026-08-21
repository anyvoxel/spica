use serde_json::Value;
use spica_asl::{State, TaskState};

use super::super::state_handler::StateHandler;
use super::super::{eval_string_or_expr, state_activated_value};
use crate::command::Command;
use crate::context::build_states;
use crate::eval_env::EvalEnv;
use crate::event::Event;
use crate::handler::{ActivityCtx, Collector};
use crate::id::ActivityId;

pub struct TaskStateHandler;

impl StateHandler for TaskStateHandler {
    fn state(&self) -> State {
        State::Task(TaskState::default())
    }

    fn activate(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector,
        activity: ActivityId,
        actx: &ActivityCtx,
        state: &State,
    ) {
        let State::Task(s) = state else {
            unreachable!(
                "activate dispatch guarantees the state handler receives its own variant; got {state:?}"
            );
        };
        activate_task(env, out, activity, actx, s);
    }

    /// Resumed by `CompleteTask`'s `CompleteState` after the external call settles with `Ok`. Runs
    /// the state's `complete` step: projects `Assign`/`Output` against the stored processed input
    /// (`$states.input`) and raw task result (`$states.result`), then routes to `Next`/`End` (the
    /// shared success finish — identical to `Wait`'s `complete`).
    fn complete(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector,
        activity: ActivityId,
        actx: &ActivityCtx,
        state: &State,
    ) {
        let State::Task(s) = state else {
            unreachable!(
                "complete dispatch guarantees the state handler receives its own variant; got {state:?}"
            );
        };
        super::super::complete_activity(
            env,
            out,
            activity,
            actx,
            s.assign.as_ref(),
            s.output.as_ref(),
            s.next.as_deref(),
            s.end,
            actx.activity.retry_state.retry_count,
            None, // success path — no Catch `errorOutput`
        );
    }
}

/// The `Task` state's `activate`: after the framework emits `StateActivating`, this projects
/// `Arguments` (the `Args` / state's `arguments` with JSONata evaluated) and, once projected, emits
/// `StateActivated` and throws `ActivateTask` to invoke the external call. The state then leaves the
/// serial loop (like `Wait` arming its timer): it is resumed later via `CompleteTask` →
/// `CompleteState` → `complete`.
///
/// `resource` is static text (per ASL, the `Resource` URI); `arguments` may embed JSONata. The
/// `timeout_seconds`/`heartbeat_seconds`/`retry`/`catch` fields are not implemented yet — a task
/// that specifies them structurally is out of scope until their milestone.
fn activate_task(
    env: &mut EvalEnv,
    out: &mut Collector,
    activity: ActivityId,
    actx: &ActivityCtx,
    state: &TaskState,
) {
    // `$states` for the activate step: `result` is the task's output, which is unknown until the
    // external call settles, so `result` is `None`/null here (it becomes the stored `input` the
    // complete step later projects against). `assign_ctx = None`: the state's own `Assign` has not
    // yet been applied.
    let states = build_states(
        &actx.activity.input,
        None,
        &actx.state_name(),
        &actx.exec_input,
        None,
        actx.activity.retry_state.retry_count,
        None,
        None, // not a Map item — no `context.Map.Item` binding
    );

    // Project `Arguments`: it may be any JSON value, with strings inside `{% %}` evaluated as
    // JSONata against the current scope. If absent, the task receives the state's input by default
    // (matching ASL, where a Task's `Arguments` defaults to `$states.input`).
    let arguments = match &state.arguments {
        Some(arguments) => fail_or!(
            out,
            Some(activity),
            actx.activity.execution,
            env.eval_json(arguments, &states, &actx.variables)
        ),
        None => actx.activity.input.clone(),
    };

    // TODO(M2): the remaining Task field not yet fully acted on is `heartbeat_seconds`
    // (`States.HeartbeatTimeout`): a heartbeat deadline driven by client keepalives, which the
    // current `TaskHandler::run` interface cannot express. `timeout_seconds` and `retry`/`catch`
    // are handled here (`resolve_task_deadline`) and in `complete_task.rs`'s `route_failure`.
    // A retry re-invocation currently arms a fresh call but does **not** re-arm a fresh
    // `TimeoutSeconds` deadline — TODO(M2): on `TaskRetryDelay` firing, re-arm the `TaskTimeout`
    // timer so the retried attempt is also bounded (the first attempt's timeout has already fired
    // or been swept by then).
    //  `resource` here is passed verbatim.

    let task = out.next_task();
    // The activation work (projecting `Arguments`) is done: emit the activation-complete ed, then
    // throw the invocation as the transition's side effect. The `parent` links the task to the
    // owning activity so a later termination sweeps it.
    out.emit_event(Event::StateActivated {
        activity: state_activated_value(actx, arguments.clone(), None),
    });
    out.emit_command(Command::ActivateTask {
        parent: crate::id::NodeId::Activity(activity),
        task,
        resource: state.resource.clone(),
        arguments,
    });

    // Arm the Task's `TimeoutSeconds` deadline (a `TaskTimeout` timer parented on the activity, so
    // it is swept when the activity terminates). On firing it fails the in-flight task with
    // `States.Timeout`, which then flows through the same `Retry`/`Catch` policy as any settle.
    // A Task with no `TimeoutSeconds` is left to the external handler to settle.
    if let Some(timeout) = &state.timeout_seconds {
        let deadline = resolve_task_deadline(env, out, activity, actx, &states, timeout);
        let Some(deadline) = deadline else {
            return; // timeout was invalid — `resolve_task_deadline` already emitted the failure.
        };
        let timer = out.next_timer();
        out.emit_command(Command::ActivateTimer {
            parent: crate::id::NodeId::Activity(activity),
            timer,
            purpose: crate::command::TimerPurpose::TaskTimeout,
            deadline,
        });
    }
}

/// Emit a `TerminateState` failure for the activity — the shared tail for a `TimeoutSeconds`
/// definition error (invalid value, eval failure, or overflow). The activity's terminate path
/// drives the failure up through the cascade.
fn emit_timeout_definition_failure(
    out: &mut Collector,
    activity: ActivityId,
    execution: crate::id::ExecutionId,
    error: crate::error::ExecutionError,
) {
    use crate::command::TerminationReason;
    out.emit_command(Command::TerminateState {
        activity,
        reason: TerminationReason::Failed { error },
    });
    // Unused in some callers (the execution id is currently only for clarity); keep the activity as
    // the failure's site and let the terminate path decide the execution.
    let _ = execution;
}

/// Resolve a Task state's `TimeoutSeconds` (`Int` or JSONata `Expr`) into an absolute deadline.
/// On an invalid/out-of-range value, emits the failure and returns `None`.
fn resolve_task_deadline(
    env: &mut EvalEnv,
    out: &mut Collector,
    activity: ActivityId,
    actx: &ActivityCtx,
    states: &Value,
    timeout: &spica_asl::IntOrExpr,
) -> Option<crate::log::Timestamp> {
    use crate::command::TerminationReason;
    use crate::error::ExecutionError;
    let seconds = match timeout {
        // Literal `TimeoutSeconds`: a positive integer.
        spica_asl::IntOrExpr::Int(n) => Some(*n),
        // JSONata `TimeoutSeconds`: evaluate, require a non-negative integer result.
        spica_asl::IntOrExpr::Expr(expr) => {
            // Evaluate the JSONata `TimeoutSeconds`; on eval failure emit the failure and `None`.
            let value = match eval_string_or_expr(env, expr.as_str(), states, &actx.variables) {
                Ok(v) => v,
                Err(e) => {
                    emit_timeout_definition_failure(out, activity, actx.activity.execution, e);
                    return None;
                }
            };
            match value {
                Value::Number(num) => num.as_f64().and_then(|f| {
                    if f.fract() == 0.0 && f.is_finite() && f >= 0.0 {
                        Some(f as i64)
                    } else {
                        None
                    }
                }),
                _ => None,
            }
        }
    };
    let seconds = match seconds {
        Some(n) if n > 0 => n,
        _ => {
            out.emit_command(Command::TerminateState {
                activity,
                reason: TerminationReason::Failed {
                    error: ExecutionError::InvalidDefinition(
                        "Task TimeoutSeconds must be a positive integer".into(),
                    ),
                },
            });
            return None;
        }
    };
    let deadline =
        crate::log::Timestamp::now().checked_add(std::time::Duration::from_secs(seconds as u64));
    match deadline {
        Some(d) => Some(d),
        None => {
            out.emit_command(Command::TerminateState {
                activity,
                reason: TerminationReason::Failed {
                    error: ExecutionError::InvalidDefinition(
                        "Task TimeoutSeconds overflows the absolute deadline".into(),
                    ),
                },
            });
            None
        }
    }
}
