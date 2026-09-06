use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{PassState, State};

use super::super::state_handler::StateHandler;
use super::super::{emit_transition, state_activated_value, state_completed_value};
use crate::eval_env::EvalEnv;
use crate::handler::{ActivityCtx, Collector};
use crate::types::context::build_states;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::ObjectReference;

pub struct PassStateHandler;

#[async_trait]
impl StateHandler for PassStateHandler {
    fn state(&self) -> State {
        State::Pass(PassState::default())
    }

    async fn activate(
        &self,
        _env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity: ObjectReference,
        actx: &ActivityCtx,
        _state: &State,
    ) {
        // Pass is fully synchronous: no side effect to arm, so its activate simply moves it on to
        // the complete step in the very next Command. Emit the activation-complete ed first, then
        // the transition command.
        out.emit_event(crate::types::event::Event::StateActivated {
            activity: state_activated_value(actx, actx.activity.raw_input.clone(), None),
        })
        .await;
        out.emit_command(crate::types::command::Command::CompleteState {
            activity,
            // A Pass's raw result is its processed input (no distinct raw output); the complete step
            // projects any `Output` template from it.
            output: actx.activity.raw_input.clone(),
        });
    }

    async fn complete(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity: ObjectReference,
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
        complete_pass(env, out, activity, actx, s).await;
    }
}

// A Pass self-implements its success finish (rather than reuse the shared `complete_activity`):
// `StateCompleting` is emitted by the `CompleteStateHandler` framework, so this only has to project
// the output, emit `StateCompleted`, and route. Pass's projection — `Assign` (a delta on the
// execution scope, emitted as `VariablesAssigned`) then `Output` (defaults to the input) — is the
// canonical ASL success projection, kept here as the reference implementation the other complete
// paths mirror.
async fn complete_pass(
    env: &mut EvalEnv,
    out: &mut Collector<'_>,
    activity: ObjectReference,
    actx: &ActivityCtx,
    state: &PassState,
) {
    let states = build_states(
        &actx.activity.raw_input,
        Some(&actx.activity.raw_input),
        &actx.state_name(),
        &actx.exec_input,
        Some(&actx.activity.raw_input),
        actx.activity.retry_count(),
        None, // no Catch `errorOutput` in the success path
        None, // not a Map item — no `context.Map.Item` binding
    );
    let mut local_scope = actx.variables.clone();

    if let Some(assign_obj) = &state.assign {
        let assign_value = Value::Object(assign_obj.0.clone());
        let evaluated = fail_or!(
            out,
            Some(activity),
            actx.activity
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            env.eval_json(&assign_value, &states, &local_scope)
        );
        match evaluated {
            Value::Object(map) => {
                if !map.is_empty() {
                    for (k, v) in map {
                        local_scope.insert(k, v);
                    }
                    out.emit_event(Event::VariablesAssigned {
                        scope: actx
                            .activity
                            .meta
                            .owner
                            .clone()
                            .expect("an owned activity has an owner"),
                        variables: local_scope.clone(),
                    })
                    .await;
                }
            }
            _ => {
                out.terminate(
                    Some(activity),
                    actx.activity
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Assign must evaluate to a JSON object".to_string(),
                    )),
                );
                return;
            }
        }
    }

    let output_value = match &state.output {
        Some(o) => fail_or!(
            out,
            Some(activity),
            actx.activity
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            env.eval_json(o, &states, &local_scope)
        ),
        None => actx.activity.raw_input.clone(),
    };

    out.emit_event(Event::StateCompleted {
        activity: state_completed_value(actx, output_value.clone()),
    })
    .await;
    emit_transition(
        out,
        actx.activity.execution.clone(),
        actx.activity
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner"),
        activity,
        actx.state_path(),
        &output_value,
        state.next.as_deref(),
        state.end,
    )
    .await;
}
