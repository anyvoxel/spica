use serde_json::Value;
use spica_asl::{PassState, State};

use super::super::emit_transition;
use super::super::state_handler::StateHandler;
use crate::context::build_states;
use crate::error::ExecutionError;
use crate::eval_env::EvalEnv;
use crate::event::Event;
use crate::handler::{ActivityCtx, Collector};
use crate::id::ActivityId;

pub struct PassStateHandler;

impl StateHandler for PassStateHandler {
    fn state(&self) -> State {
        State::Pass(PassState::default())
    }

    fn activate(
        &self,
        _env: &mut EvalEnv,
        out: &mut Collector,
        activity: ActivityId,
        _actx: &ActivityCtx,
        _state: &State,
    ) {
        // Pass is fully synchronous: no side effect to arm, so its activate simply moves it on to
        // the complete step in the very next Command. Emit the activation-complete ed first, then
        // the transition command.
        out.emit_event(crate::event::Event::StateActivated { activity });
        out.emit_command(crate::command::Command::CompleteState { activity });
    }

    fn complete(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector,
        activity: ActivityId,
        actx: &ActivityCtx,
        state: &State,
    ) {
        // A Pass state is always completed via `CompleteState` with the `Pass` variant — the
        // dispatch table matches this handler only to `State::Pass`, so any other variant is a
        // programming error (a table/dispatch mismatch), not a runtime condition.
        let State::Pass(s) = state else {
            unreachable!(
                "complete dispatch guarantees the state handler receives its own variant; got {state:?}"
            );
        };
        complete_pass(env, out, activity, actx, s);
    }
}

// A Pass self-implements its success finish (rather than reuse the shared `complete_activity`):
// `StateCompleting` is emitted by the `CompleteStateHandler` framework, so this only has to project
// the output, emit `StateCompleted`, and route. Pass's projection — `Assign` (a delta on the
// execution scope, emitted as `VariablesAssigned`) then `Output` (defaults to the input) — is the
// canonical ASL success projection, kept here as the reference implementation the other complete
// paths mirror.
fn complete_pass(
    env: &mut EvalEnv,
    out: &mut Collector,
    activity: ActivityId,
    actx: &ActivityCtx,
    state: &PassState,
) {
    let states = build_states(
        &actx.input,
        Some(&actx.input),
        &actx.state_name,
        &actx.exec_input,
        Some(&actx.input),
    );
    let mut local_scope = actx.scope.clone();

    if let Some(assign_obj) = &state.assign {
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

    let output_value = match &state.output {
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
    emit_transition(
        out,
        actx.execution,
        activity,
        &output_value,
        state.next.as_deref(),
        state.end,
    );
}
