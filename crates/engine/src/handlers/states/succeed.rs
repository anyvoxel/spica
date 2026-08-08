use spica_asl::{State, SucceedState};

use super::super::emit_transition;
use super::super::state_handler::StateHandler;
use crate::eval_env::EvalEnv;
use crate::handler::{ActivityCtx, Collector};
use crate::id::ActivityId;

pub struct SucceedStateHandler;

impl StateHandler for SucceedStateHandler {
    fn state(&self) -> State {
        State::Succeed(SucceedState::default())
    }

    fn activate(
        &self,
        _env: &mut EvalEnv,
        out: &mut Collector,
        activity: ActivityId,
        _actx: &ActivityCtx,
        _state: &State,
    ) {
        // No side effect: a Succeed state's success is resolved in the complete step. Emit the
        // activation-complete ed, then hand off to the complete step via `CompleteState`.
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
    activity: ActivityId,
    actx: &ActivityCtx,
    state: &SucceedState,
) {
    let states = crate::context::build_states(
        &actx.input,
        Some(&actx.input),
        &actx.state_name,
        &actx.exec_input,
        Some(&actx.input),
    );
    let output = match &state.output {
        Some(o) => fail_or!(
            out,
            Some(activity),
            actx.execution,
            env.eval_json(o, &states, &actx.scope)
        ),
        None => actx.input.clone(),
    };
    out.emit_event(crate::event::Event::StateCompleted {
        activity,
        output: output.clone(),
    });
    emit_transition(out, actx.execution, activity, &output, None, Some(true));
}
