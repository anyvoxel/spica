use serde_json::{Map, Value};
use spica_asl::{FailState, State};

use super::super::state_handler::StateHandler;
use super::super::{
    eval_string_or_expr, state_activated_value, state_terminated_value, state_terminating_value,
};
use crate::eval_env::EvalEnv;
use crate::handler::{ActivityCtx, Collector};
use crate::types::command::TerminationReason;
use crate::types::context::build_states;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::meta::ObjectReference;

pub struct FailStateHandler;

impl StateHandler for FailStateHandler {
    fn state(&self) -> State {
        State::Fail(FailState::default())
    }

    fn activate(
        &self,
        _env: &mut EvalEnv,
        out: &mut Collector,
        activity: ObjectReference,
        actx: &ActivityCtx,
        _state: &State,
    ) {
        // Fail is fully synchronous — same shape as Pass: no side effect to arm, so activate is a
        // thin step that moves straight on to `CompleteState`. The failure projection (evaluating
        // `Error`/`Cause` and terminating) happens in `complete`, mirroring how Pass defers its
        // `Assign`/`Output` projection to `complete`. Follows the user's chosen lifecycle: Fail
        // routes through the success-finish framework (`CompleteState` → `StateCompleting` →
        // `complete`), which is a structural-symmetry trade-off against the failure semantics the
        // framework's `StateCompleting` marker implies.
        out.emit_event(crate::types::event::Event::StateActivated {
            activity: state_activated_value(actx, actx.activity.input.clone(), None),
        });
        out.emit_command(crate::types::command::Command::CompleteState {
            activity,
            // A Fail routes through the success-finish framework; its raw result is the processed
            // input (the actual `Error`/`Cause` failure projection happens in `complete`).
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
    activity: ObjectReference,
    actx: &ActivityCtx,
    state: &FailState,
) {
    // `$states` for the complete step: `assign_ctx = Some` (matching Pass/Succeed) — however late an
    // `Assign` is applied, derived values read consistently with the scope already folded.
    let states = build_states(
        &actx.activity.input,
        None,
        &actx.state_name(),
        &actx.exec_input,
        Some(&actx.activity.input),
        actx.activity.retry_state.attempts,
        None, // no Catch `errorOutput` in the fail path
        None, // not a Map item — no `context.Map.Item` binding
    );

    let mut err_out = Map::new();
    let mut error_name = "States.Fail".to_string();
    if let Some(error) = &state.error {
        let value = fail_or!(
            out,
            Some(activity),
            actx.activity
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            eval_string_or_expr(env, error, &states, &actx.variables)
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
            actx.activity
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            eval_string_or_expr(env, cause, &states, &actx.variables)
        );
        err_out.insert("Cause".to_string(), value);
    }
    let error = ExecutionError::Runtime(RuntimeError::StateFailed {
        state: actx.state_name(),
        error: error_name,
        output: Box::new(Value::Object(err_out)),
    });
    let reason = TerminationReason::Failed { error };

    // Emit the activity's failure ed. `StateTerminating` + `StateTerminated` replace the
    // `StateCompleted` a successful complete would emit; `TerminateExecution` then folds the
    // execution's termination (`ExecutionTerminating` → `ExecutionTerminated`) rather than the
    // `CompleteExecution` Pass would throw.
    out.emit_event(crate::types::event::Event::StateTerminating {
        activity: state_terminating_value(actx, reason.clone()),
    });
    out.emit_event(crate::types::event::Event::StateTerminated {
        activity: state_terminated_value(actx, reason.clone()),
    });
    // Route the terminal failure to the *owning scope*. A top-level run is an `Execution` (reached
    // via name-addressed `TerminateExecution`); a `Fail` inside a `Parallel` branch / `Map` item is
    // owned by a `Thread`, which lives in *thread* storage and is only reachable via the
    // reference-addressed `TerminateThread` — a bare `TerminateExecution` here would silently miss
    // it and leave the branch Running, wedging its container.
    super::super::emit_scope_termination(
        out,
        actx.activity
            .meta
            .owner
            .as_ref()
            .expect("an owned activity has an owner"),
        reason,
    );
}
