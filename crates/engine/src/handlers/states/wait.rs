use serde_json::Value;
use spica_asl::{IntOrExpr, State, WaitState, WaitTimestamp};

use super::super::state_handler::StateHandler;
use super::super::{complete_activity, emit_timer, eval_string_or_expr, state_activated_value};
use crate::eval_env::EvalEnv;
use crate::handler::{ActivityCtx, Collector};
use crate::log::Timestamp;
use crate::types::command::TimerPurpose;
use crate::types::context::build_states;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::meta::ObjectReference;

/// The inclusive upper bound of a `Wait` `Seconds` value, per the ASL spec.
const MAX_WAIT_SECONDS: i64 = 99_999_999;

pub struct WaitStateHandler;

impl StateHandler for WaitStateHandler {
    fn state(&self) -> State {
        State::Wait(WaitState::default())
    }

    fn activate(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector,
        activity: ObjectReference,
        actx: &ActivityCtx,
        state: &State,
    ) {
        let State::Wait(s) = state else {
            unreachable!(
                "activate dispatch guarantees the state handler receives its own variant; got {state:?}"
            );
        };
        activate_wait(env, out, activity, actx, s);
    }

    /// Resumed by `CompleteState` after the Wait's timer (`WaitResume`) has fired. Projects
    /// `Assign`/`Output` against the stored input and routes to `Next`/`End`.
    fn complete(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector,
        activity: ObjectReference,
        actx: &ActivityCtx,
        state: &State,
    ) {
        let State::Wait(s) = state else {
            unreachable!(
                "complete dispatch guarantees the state handler receives its own variant; got {state:?}"
            );
        };
        complete_activity(
            env,
            out,
            activity,
            actx,
            s.assign.as_ref(),
            s.output.as_ref(),
            s.next.as_deref(),
            s.end,
            actx.activity.retry_state.attempts,
            None, // Wait has no Catch `errorOutput`
        );
    }
}

fn activate_wait(
    env: &mut EvalEnv,
    out: &mut Collector,
    activity: ObjectReference,
    actx: &ActivityCtx,
    state: &WaitState,
) {
    // `$states` for the activate step, mirroring every other state's activate: `result` is null (a
    // Wait produces no result) and `assign_ctx = None` (the state's own `Assign` has not yet been
    // applied). A JSONata `Seconds`/`Timestamp` expression may reference `$states.input` and any
    // in-scope variables.
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

    // Compute the absolute deadline the Wait holds until. `Seconds` is relative — normalized to an
    // absolute moment at activation; `Timestamp` is already absolute (parsed from RFC3339). Exactly
    // one of the two is present by the well-formedness assumption (validated on submission); both
    // accept a JSONata expression (evaluated against `states`/`scope`).
    let deadline = match (&state.seconds, &state.timestamp) {
        // Literal `Seconds`: a non-negative integer, normalized to an absolute deadline.
        (Some(IntOrExpr::Int(n)), None) => {
            if !(0..=MAX_WAIT_SECONDS).contains(n) {
                out.terminate(
                    Some(activity),
                    actx.activity
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Wait Seconds must be an integer in the range 0..99999999".into(),
                    )),
                );
                return;
            }
            let deadline = Timestamp::now().checked_add(std::time::Duration::from_secs(*n as u64));
            let Some(deadline) = deadline else {
                out.terminate(
                    Some(activity),
                    actx.activity
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Wait Seconds overflows the absolute deadline".into(),
                    )),
                );
                return;
            };
            deadline
        }
        // `Seconds` as a JSONata expression: evaluate it, then require the result to be a
        // non-negative integer in the ASL range before normalizing to a deadline.
        (Some(IntOrExpr::Expr(expr)), None) => {
            let value = fail_or!(
                out,
                Some(activity),
                actx.activity
                    .meta
                    .owner
                    .clone()
                    .expect("an owned activity has an owner"),
                eval_string_or_expr(env, expr.as_str(), &states, &actx.variables)
            );
            // The evaluated result must be a JSON number that is an integer in range (ASL: a JSONata
            // `Seconds` expression must evaluate to a non-negative integer, 0..99,999,999). Anything
            // else — a string, a negative value — is an invalid definition, so we fail loudly at
            // activation rather than arming a wrong timer.
            //
            // `jsonata-core` yields every number as `f64` (see the crate's "Known limitation"), so a
            // JSONata `Seconds` expression like `{% 0 %}` produces `0.0`, not an integral `Number`.
            // We therefore accept any number whose value is an integer (no fractional part), rather
            // than requiring `as_i64` (which only succeeds for a true integral-typed number).
            let n = match value {
                Value::Number(num) => num.as_f64().and_then(|f| {
                    if f.fract() == 0.0 && f.is_finite() {
                        Some(f as i64)
                    } else {
                        None
                    }
                }),
                _ => None,
            };
            let Some(n) = n else {
                out.terminate(
                    Some(activity),
                    actx.activity
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Wait Seconds expression must evaluate to an integer".into(),
                    )),
                );
                return;
            };
            if !(0..=MAX_WAIT_SECONDS).contains(&n) {
                out.terminate(
                    Some(activity),
                    actx.activity
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Wait Seconds expression must evaluate to an integer in the range \
                         0..99999999"
                            .into(),
                    )),
                );
                return;
            }
            let deadline = Timestamp::now().checked_add(std::time::Duration::from_secs(n as u64));
            let Some(deadline) = deadline else {
                out.terminate(
                    Some(activity),
                    actx.activity
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Wait Seconds overflows the absolute deadline".into(),
                    )),
                );
                return;
            };
            deadline
        }
        // Literal `Timestamp`: an RFC3339 string parsed into an absolute moment.
        (None, Some(WaitTimestamp::Literal(s))) => match Timestamp::from_rfc3339(s) {
            Some(t) => t,
            None => {
                out.terminate(
                    Some(activity),
                    actx.activity
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                        "Wait Timestamp is not a valid RFC3339 timestamp: {s}"
                    ))),
                );
                return;
            }
        },
        // `Timestamp` as a JSONata expression: evaluate it, then require the result to be a string
        // that parses as RFC3339 before using it as the absolute deadline.
        (None, Some(WaitTimestamp::Expr(expr))) => {
            let value = fail_or!(
                out,
                Some(activity),
                actx.activity
                    .meta
                    .owner
                    .clone()
                    .expect("an owned activity has an owner"),
                eval_string_or_expr(env, expr.as_str(), &states, &actx.variables)
            );
            let s = match value {
                Value::String(s) => s,
                _ => {
                    out.terminate(
                        Some(activity),
                        actx.activity
                            .meta
                            .owner
                            .clone()
                            .expect("an owned activity has an owner"),
                        ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                            "Wait Timestamp expression must evaluate to a string".into(),
                        )),
                    );
                    return;
                }
            };
            match Timestamp::from_rfc3339(&s) {
                Some(t) => t,
                None => {
                    out.terminate(
                        Some(activity),
                        actx.activity
                            .meta
                            .owner
                            .clone()
                            .expect("an owned activity has an owner"),
                        ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                            "Wait Timestamp expression must evaluate to a valid RFC3339 \
                             timestamp: {s}"
                        ))),
                    );
                    return;
                }
            }
        }
        // Well-formedness assumption: the engine is given a `StateMachine` that has already been
        // validated on submission (see `spica_asl::StateMachine::validate()`, TODO). Per ASL a Wait
        // state specifies *exactly one* of `Seconds` or `Timestamp`. So this arm — `Seconds` and
        // `Timestamp` both absent, or both present — is unreachable at activation: an invalid
        // definition is rejected before it is ever dispatched. Rather than duplicate that
        // cross-field check at runtime (the engine treats the definition as canonical), we
        // `unreachable!` to make the assumption explicit.
        _ => unreachable!(
            "Wait state must specify exactly one of Seconds or Timestamp \
             (definition is validated on submission)"
        ),
    };
    // The activation work (computing the absolute deadline from Seconds/Timestamp) is done: emit the
    // activation-complete ed, then arm the resume timer as the transition's side effect, inlined into
    // this batch so no separate command round-trips the arm.
    out.emit_event(crate::types::event::Event::StateActivated {
        activity: state_activated_value(actx, actx.activity.input.clone(), None),
    });
    emit_timer(
        out,
        // The timer's `execution` anchor is the flat top-level run (`activity.execution`), not the
        // immediate owner scope — it drives `Timer::execution` and the timer's
        // `{execution.name}-{suffix}` generated name, so a branch Wait still names its root run.
        actx.activity.execution.clone(),
        activity,
        TimerPurpose::WaitResume,
        deadline,
    );
}
