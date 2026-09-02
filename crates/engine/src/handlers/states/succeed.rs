use spica_asl::{State, SucceedState};

use super::super::state_handler::StateHandler;
use super::super::{emit_transition, state_activated_value, state_completed_value};
use crate::eval_env::EvalEnv;
use crate::handler::{ActivityCtx, Collector};
use crate::types::meta::ObjectReference;

pub struct SucceedStateHandler;

impl StateHandler for SucceedStateHandler {
    fn state(&self) -> State {
        State::Succeed(SucceedState::default())
    }

    fn activate(
        &self,
        _env: &mut EvalEnv,
        out: &mut Collector,
        activity: ObjectReference,
        actx: &ActivityCtx,
        _state: &State,
    ) {
        // No side effect: a Succeed state's success is resolved in the complete step. Emit the
        // activation-complete ed, then hand off to the complete step via `CompleteState`.
        out.emit_event(crate::types::event::Event::StateActivated {
            activity: state_activated_value(actx, actx.activity.input.clone(), None),
        });
        out.emit_command(crate::types::command::Command::CompleteState {
            activity,
            // A Succeed's raw result is its processed input (no distinct raw output); the complete
            // step projects any `Output` template from it.
            output: actx.activity.input.clone(),
        });
    }

    fn complete(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector,
        activity: ObjectReference,
        actx: &ActivityCtx,
        state: &State,
    ) {
        // A Succeed state is always completed via `CompleteState` with the `Succeed` variant — the
        // dispatch table matches this handler only to `State::Succeed`, so any other variant is a
        // programming error (a table/dispatch mismatch), not a runtime condition.
        let State::Succeed(s) = state else {
            unreachable!(
                "complete dispatch guarantees the state handler receives its own variant; got {state:?}"
            );
        };
        complete_succeed(env, out, activity, actx, s);
    }
}

// Terminal: complete the state with its (evaluated) output, then complete the whole execution with
// the same output. `StateCompleting` is emitted by the `CompleteStateHandler` framework; this
// projects the output, emits `StateCompleted`, and routes to `End` (`CompleteExecution`).
fn complete_succeed(
    env: &mut EvalEnv,
    out: &mut Collector,
    activity: ObjectReference,
    actx: &ActivityCtx,
    state: &SucceedState,
) {
    let states = crate::types::context::build_states(
        &actx.activity.input,
        Some(&actx.activity.input),
        &actx.state_name(),
        &actx.exec_input,
        Some(&actx.activity.input),
        actx.activity.retry_state.attempts,
        None, // no Catch `errorOutput` in the succeed path
        None, // not a Map item — no `context.Map.Item` binding
    );
    let output = match &state.output {
        Some(o) => fail_or!(
            out,
            Some(activity),
            actx.activity
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            env.eval_json(o, &states, &actx.variables)
        ),
        None => actx.activity.input.clone(),
    };
    out.emit_event(crate::types::event::Event::StateCompleted {
        activity: state_completed_value(actx, output.clone()),
    });
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
        &output,
        None,
        Some(true),
    );
}
