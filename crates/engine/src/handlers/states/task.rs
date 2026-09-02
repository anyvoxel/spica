use serde_json::Value;
use spica_asl::{State, TaskState};

use super::super::state_handler::StateHandler;
use super::super::{emit_timer, eval_string_or_expr, state_activated_value};
use crate::eval_env::EvalEnv;
use crate::handler::{ActivityCtx, Collector};
use crate::types::command::Command;
use crate::types::context::build_states;
use crate::types::event::Event;
use crate::types::meta::ObjectReference;

pub struct TaskStateHandler;

impl StateHandler for TaskStateHandler {
    fn state(&self) -> State {
        State::Task(TaskState::default())
    }

    fn activate(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector,
        activity: ObjectReference,
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
        activity: ObjectReference,
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
            actx.activity.retry_state.attempts,
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
    activity: ObjectReference,
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
        actx.activity.retry_state.attempts,
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
            actx.activity
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            env.eval_json(arguments, &states, &actx.variables)
        ),
        None => actx.activity.input.clone(),
    };

    // TODO(M2): the remaining Task field not yet fully acted on is `heartbeat_seconds`
    // (`States.HeartbeatTimeout`): a heartbeat deadline driven by client keepalives, which the
    // current `TaskHandler::run` interface cannot express. `timeout_seconds` and `retry`/`catch`
    // are handled here (`resolve_task_deadline`) and in `fail_task.rs`'s task self-decision.
    // A retry re-queues the same task entity back to `Pending` with a `next_available_at` gate (no
    // re-invocation), so it does **not** re-arm a fresh `TimeoutSeconds` deadline — TODO(M2): on the
    // re-claimed attempt, re-arm `TaskTimeout` so the retried attempt is also bounded (the first
    // attempt's timeout has already fired or been swept by then).
    // `resource` here is passed verbatim; `retry` is resolved once into the frozen `retry_plan` so
    // the reused task decides its own retries without revisiting this definition.

    // Resolve the state's `Retry` array once into a frozen per-retrier plan baked onto the task, so
    // the reused task decides its own retries (matching, budget, backoff) without revisiting this
    // definition — the self-containment a per-`resource` task partition needs. Empty = no retry.
    let retry_plan = state
        .retry
        .as_deref()
        .map(|retriers| retriers.iter().map(crate::RetryPolicy::resolve).collect())
        .unwrap_or_default();

    let task_uid = out.next_task();
    // The activation work (projecting `Arguments`) is done: emit the activation-complete ed, then
    // throw the invocation as the transition's side effect. The `parent` links the task to the
    // owning activity so a later termination sweeps it.
    //
    // The task's reference is minted with a name derived from the owning execution's plain base
    // (finding #13), exactly like the activity (#3) and timer (#11) names, not the opaque
    // `child-<uid>` handle. `execution` is the tree anchor (`actx.activity.execution`), so a branch
    // task still names its root run; the suffix (random, `PlainName::to_generated`) is decoupled
    // from the task's own `uid`. Minted once and reused for both the command's reference and the
    // serialized `meta.name` (storage lookups are keyed by the reference's name).
    let task_name = actx.activity.execution.name.base().to_generated();
    let task_ref = ObjectReference::new(crate::types::meta::ObjectKind::Task, task_name, task_uid);
    out.emit_event(Event::StateActivated {
        activity: state_activated_value(actx, arguments.clone(), None),
    });
    out.emit_command(Command::ActivateTask {
        execution: actx.activity.execution.clone(),
        owner: activity.clone(),
        task: task_ref,
        resource: state.resource.clone(),
        arguments,
        retry_plan,
    });

    // Arm the Task's `TimeoutSeconds` deadline (a `TaskTimeout` timer parented on the activity, so
    // it is swept when the activity terminates). On firing it fails the in-flight task with
    // `States.Timeout`, which then flows through the same `Retry`/`Catch` policy as any settle.
    // A Task with no `TimeoutSeconds` is left to the external handler to settle.
    if let Some(timeout) = &state.timeout_seconds {
        let deadline = resolve_task_deadline(env, out, activity.clone(), actx, &states, timeout);
        let Some(deadline) = deadline else {
            return; // timeout was invalid — `resolve_task_deadline` already emitted the failure.
        };
        emit_timer(
            out,
            // The timer's `execution` anchor is the flat top-level run (`activity.execution`), not
            // the immediate owner scope — it drives `Timer::execution` and the timer's
            // `{execution.name}-{suffix}` generated name, so a branch task's timeout still names its
            // root run.
            actx.activity.execution.clone(),
            activity,
            crate::types::command::TimerPurpose::TaskTimeout,
            deadline,
        );
    }
}

/// Emit a `TerminateState` failure for the activity — the shared tail for a `TimeoutSeconds`
/// definition error (invalid value, eval failure, or overflow). The activity's terminate path
/// drives the failure up through the cascade.
fn emit_timeout_definition_failure(
    out: &mut Collector,
    activity: ObjectReference,
    execution: crate::types::meta::ObjectReference,
    error: crate::types::error::ExecutionError,
) {
    use crate::types::command::TerminationReason;
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
    activity: ObjectReference,
    actx: &ActivityCtx,
    states: &Value,
    timeout: &spica_asl::IntOrExpr,
) -> Option<crate::log::Timestamp> {
    use crate::types::command::TerminationReason;
    use crate::types::error::{ExecutionError, RuntimeError};
    let seconds = match timeout {
        // Literal `TimeoutSeconds`: a positive integer.
        spica_asl::IntOrExpr::Int(n) => Some(*n),
        // JSONata `TimeoutSeconds`: evaluate, require a non-negative integer result.
        spica_asl::IntOrExpr::Expr(expr) => {
            // Evaluate the JSONata `TimeoutSeconds`; on eval failure emit the failure and `None`.
            let value = match eval_string_or_expr(env, expr.as_str(), states, &actx.variables) {
                Ok(v) => v,
                Err(e) => {
                    emit_timeout_definition_failure(
                        out,
                        activity,
                        actx.activity
                            .meta
                            .owner
                            .clone()
                            .expect("an owned activity has an owner"),
                        e,
                    );
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
                    error: ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Task TimeoutSeconds must be a positive integer".into(),
                    )),
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
                    error: ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Task TimeoutSeconds overflows the absolute deadline".into(),
                    )),
                },
            });
            None
        }
    }
}
