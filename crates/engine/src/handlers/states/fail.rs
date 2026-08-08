use serde_json::{Map, Value};
use spica_asl::{FailState, State};

use super::super::eval_string_or_expr;
use super::super::state_handler::StateHandler;
use crate::command::{Command, TerminationReason};
use crate::context::build_states;
use crate::error::ExecutionError;
use crate::eval_env::EvalEnv;
use crate::handler::{ActivityCtx, Collector};
use crate::id::ActivityId;

pub struct FailStateHandler;

impl StateHandler for FailStateHandler {
    fn state(&self) -> State {
        State::Fail(FailState::default())
    }

    fn activate(
        &self,
        _env: &mut EvalEnv,
        out: &mut Collector,
        activity: ActivityId,
        _actx: &ActivityCtx,
        _state: &State,
    ) {
        // Fail is fully synchronous — same shape as Pass: no side effect to arm, so activate is a
        // thin step that moves straight on to `CompleteState`. The failure projection (evaluating
        // `Error`/`Cause` and terminating) happens in `complete`, mirroring how Pass defers its
        // `Assign`/`Output` projection to `complete`. Follows the user's chosen lifecycle: Fail
        // routes through the success-finish framework (`CompleteState` → `StateCompleting` →
        // `complete`), which is a structural-symmetry trade-off against the failure semantics the
        // framework's `StateCompleting` marker implies.
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
        let State::Fail(s) = state else {
            unreachable!(
                "complete dispatch guarantees the state handler receives its own variant; got {state:?}"
            );
        };
        complete_fail(env, out, activity, actx, s);
    }
}

// Fail's `complete` both terminates itself (emitting the activity's failure ed) and, being terminal,
// terminates the execution (throwing `TerminateExecution`). Both are issued from the same complete
// step so the state's terminal ed and the execution's terminating ed are produced in the same causal
// chain. This mirrors Pass: Pass emits `StateCompleted` and routes to `CompleteExecution` / the
// successor; Fail emits the failure ed and routes to `TerminateExecution` / the successor.
fn complete_fail(
    env: &mut EvalEnv,
    out: &mut Collector,
    activity: ActivityId,
    actx: &ActivityCtx,
    state: &FailState,
) {
    // `$states` for the complete step: `assign_ctx = Some` (matching Pass/Succeed) — however late an
    // `Assign` is applied, derived values read consistently with the scope already folded.
    let states = build_states(
        &actx.input,
        None,
        &actx.state_name,
        &actx.exec_input,
        Some(&actx.input),
    );

    let mut err_out = Map::new();
    let mut error_name = "States.Fail".to_string();
    if let Some(error) = &state.error {
        let value = fail_or!(
            out,
            Some(activity),
            actx.execution,
            eval_string_or_expr(env, error, &states, &actx.scope)
        );
        if let Some(s) = value.as_str() {
            error_name = s.to_string();
        }
        err_out.insert("Error".to_string(), value);
    }
    if let Some(cause) = &state.cause {
        let value = fail_or!(
            out,
            Some(activity),
            actx.execution,
            eval_string_or_expr(env, cause, &states, &actx.scope)
        );
        err_out.insert("Cause".to_string(), value);
    }
    let error = ExecutionError::StateFailed {
        state: actx.state_name.clone(),
        error: error_name,
        output: Value::Object(err_out),
    };
    let reason = TerminationReason::Failed { error };

    // Emit the activity's failure ed. `StateTerminating` + `StateTerminated` replace the
    // `StateCompleted` a successful complete would emit; `TerminateExecution` then folds the
    // execution's termination (`ExecutionTerminating` → `ExecutionTerminated`) rather than the
    // `CompleteExecution` Pass would throw.
    out.emit_event(crate::event::Event::StateTerminating { activity });
    out.emit_event(crate::event::Event::StateTerminated {
        activity,
        reason: reason.clone(),
    });
    out.emit_command(Command::TerminateExecution {
        id: actx.execution,
        reason,
    });
}
