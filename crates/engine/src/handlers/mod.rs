/// Evaluate `$expr` (a `Result`); on `Ok` yield the value, on `Err` emit the failure to `$out`
/// (`TerminateState` for `$activity` if `Some`, plus `TerminateExecution` for `$execution`) and
/// `return`. The failure path always goes through `Collector::terminate` so a failing site records
/// its own outcome cohesively before the lifecycle cascade unwinds.
macro_rules! fail_or {
    ($out:expr, $activity:expr, $execution:expr, $expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                $out.terminate($activity, $execution, e);
                return;
            }
        }
    };
}

mod activate_state;
mod activate_timer;
mod cancel_timer;
mod child_finalized;
mod complete_execution;
mod complete_state;
mod complete_timer;
mod create_execution;
mod dispatch;
mod state_handler;
mod states;
mod terminate_execution;
mod terminate_state;

pub use activate_state::ActivateStateHandler;
pub use activate_timer::ActivateTimerHandler;
pub use cancel_timer::CancelTimerHandler;
pub use child_finalized::ChildFinalizedHandler;
pub use complete_execution::CompleteExecutionHandler;
pub use complete_state::CompleteStateHandler;
pub use complete_timer::CompleteTimerHandler;
pub use create_execution::CreateExecutionHandler;
pub use terminate_execution::TerminateExecutionHandler;
pub use terminate_state::TerminateStateHandler;

use serde_json::Value;
use spica_asl::{AssignObject, StateMachine};

use crate::command::Command;
use crate::context::build_states;
use crate::error::ExecutionError;
use crate::eval_env::EvalEnv;
use crate::event::Event;
use crate::handler::{ActivityCtx, Collector};
use crate::id::ActivityId;

// ── Shared helpers ───────────────────────────────────────────────────────────

/// Resolves a state definition by name from the state machine.
pub(super) fn resolve_state<'a>(
    sm: &'a StateMachine,
    state_name: &str,
) -> Result<&'a spica_asl::State, ExecutionError> {
    sm.states
        .get(state_name)
        .ok_or_else(|| ExecutionError::StateNotFound(state_name.to_string()))
}

/// Loads the owning [`crate::storage::Execution`] for a state-ish command. Returns `Ok(None)` when
/// the owning node is gone (already terminal) — the caller treats that as an idempotent no-op rather
/// than a failure.
pub(super) async fn load_execution(
    storage: &dyn crate::storage::Storage,
    execution: crate::id::ExecutionId,
) -> Result<Option<crate::storage::Execution>, ExecutionError> {
    storage.get_execution(execution).await
}

/// Evaluates a string that may be a literal or a `{% ... %}` JSONata expression.
pub(super) fn eval_string_or_expr(
    env: &mut EvalEnv,
    s: &str,
    states: &Value,
    scope: &crate::scope::Scope,
) -> Result<Value, ExecutionError> {
    match crate::eval_env::extract_jsonata(s) {
        Some(inner) => env.eval_expr(inner, states, scope),
        None => Ok(Value::String(s.to_string())),
    }
}

/// Emits the [`crate::Event::StateActivating`] event for the activity being entered.
pub(super) fn state_activating(actx: &ActivityCtx, activity: ActivityId) -> Event {
    Event::StateActivating {
        execution: actx.execution,
        activity,
        state: actx.state_name.clone(),
        input: actx.input.clone(),
    }
}

/// Records the successful state finish's routing — emitting the `StateTransitioned` marker that
/// names the resolved target `next` — then throws the transition [`Command`] that actually performs
/// the hop. The marker is only emitted for a real State→State hop (`Command::ActivateState`): a
/// terminal `End` routes to `CompleteExecution` with no next state, so it carries no marker. Kept
/// separate from the pure `transition_command` resolver so the routing decision is visible on the
/// stream ahead of the command that carries it (`Command::ActivateState` allocates the successor's
/// activity id internally, so the marker can only name the state, not the new activity). On
/// `NoTerminal` the failure is recorded via `out`.
pub(super) fn emit_transition(
    out: &mut Collector,
    execution: crate::id::ExecutionId,
    activity: ActivityId,
    output: &Value,
    next: Option<&str>,
    end: Option<bool>,
) {
    if end == Some(true) {
        // Terminal hop: no next state to route to, so there's no `StateTransitioned` marker — just
        // fold the top-level output and complete the execution.
        out.emit_command(Command::CompleteExecution {
            id: execution,
            output: output.clone(),
        });
    } else if let Some(next) = next {
        // Allocate the successor id before `emit_command` to avoid a double mutable borrow of `out`.
        let next_activity = out.next_activity();
        out.emit_event(crate::event::Event::StateTransitioned {
            activity,
            next: next.to_string(),
            output: output.clone(),
        });
        out.emit_command(Command::ActivateState {
            execution,
            activity: next_activity,
            state: next.to_string(),
            input: output.clone(),
        });
    } else {
        out.terminate(Some(activity), execution, ExecutionError::NoTerminal);
    }
}

/// Shared tail of a successful state completion (Wait resume; Pass/Succeed/Choice now carry their
/// own because their finish differs): evaluates `Assign` (emitting `VariablesAssigned`), evaluates
/// `Output` (defaults to input), emits `StateCompleted`, then the routing via [`emit_transition`].
///
/// `StateCompleting` is **not** emitted here — the `CompleteStateHandler` framework emits it when it
/// opens the complete step (analogous to `StateActivating` opening activate), so any state's success
/// finish emits the ing uniformly regardless of its own output handling.
///
/// This runs only from the `complete` step (see [`state_handler::StateHandler::complete`]) — never
/// from `activate`. Reads scope mutation from `Assign` into the local scope used for the output
/// projection, then drains via a `ChildFinalized` notice to its parent (once drained).
#[allow(clippy::too_many_arguments)]
pub(super) fn complete_activity(
    env: &mut EvalEnv,
    out: &mut Collector,
    activity: ActivityId,
    actx: &ActivityCtx,
    assign: Option<&AssignObject>,
    output: Option<&Value>,
    next: Option<&str>,
    end: Option<bool>,
) {
    // Activate-phase Assign was already applied (mutating scope); the output projection runs with
    // that updated scope so it can reference the Just-assigned variables.
    let states = build_states(
        &actx.input,
        Some(&actx.input),
        &actx.state_name,
        &actx.exec_input,
        Some(&actx.input),
    );
    let mut local_scope = actx.scope.clone();

    if let Some(assign_obj) = assign {
        let assign_value = Value::Object(assign_obj.0.clone());
        let evaluated = fail_or!(
            out,
            Some(activity),
            actx.execution,
            env.eval_json(&assign_value, &states, &local_scope)
        );
        match evaluated {
            Value::Object(map) => {
                if !map.is_empty() {
                    out.emit_event(Event::VariablesAssigned {
                        execution: actx.execution,
                        assignments: map.clone(),
                    });
                    for (k, v) in map {
                        local_scope.insert(k, v);
                    }
                }
            }
            _ => {
                out.terminate(
                    Some(activity),
                    actx.execution,
                    ExecutionError::InvalidDefinition(
                        "Assign must evaluate to a JSON object".to_string(),
                    ),
                );
                return;
            }
        }
    }

    let output_value = match output {
        Some(o) => fail_or!(
            out,
            Some(activity),
            actx.execution,
            env.eval_json(o, &states, &local_scope)
        ),
        None => actx.input.clone(),
    };

    out.emit_event(Event::StateCompleted {
        activity,
        output: output_value.clone(),
    });

    emit_transition(out, actx.execution, activity, &output_value, next, end);
}
