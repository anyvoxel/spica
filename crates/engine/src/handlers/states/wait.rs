use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{AssignObject, IntOrExpr, State, WaitState, WaitTimestamp};

use super::super::state_handler::{StateHandler, StateHandlerFactory};
use super::super::{emit_timer, eval_string_or_expr};
use crate::eval_env::EvalEnv;
use crate::handler::Collector;
use crate::log::Timestamp;
use crate::types::command::TimerPurpose;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::{Activity, ActivityState, Variables, WaitActivityState};

/// The inclusive upper bound of a `Wait` `Seconds` value, per the ASL spec.
const MAX_WAIT_SECONDS: i64 = 99_999_999;

pub struct WaitStateHandlerFactory;

#[async_trait]
impl StateHandlerFactory for WaitStateHandlerFactory {
    fn state(&self) -> State {
        State::Wait(WaitState::default())
    }

    fn create<'a>(&self, state: &'a State) -> Box<dyn StateHandler + 'a> {
        let State::Wait(s) = state else {
            unreachable!(
                "create dispatch guarantees the factory receives its own variant; got {state:?}"
            );
        };
        Box::new(WaitStateHandler { state: s })
    }
}

struct WaitStateHandler<'a> {
    state: &'a WaitState,
}

#[async_trait]
impl StateHandler for WaitStateHandler<'_> {
    // Wait's processed input is its raw input; the activate work is resolving the resume instant,
    // which is the activity's activation product.
    async fn process_input(
        &self,
        env: &mut EvalEnv,
        activity: &mut Activity,
        variables: &Variables,
        states: &Value,
        now: Timestamp,
    ) -> Result<Value, ExecutionError> {
        // Resolve the deadline here rather than when the timer is armed, so the instant is a property
        // of the entering activity (carried by `StateActivated` and every later event) and *one*
        // computation feeds both carriers: this repository field and the `WaitResume` timer
        // `after_activated` arms from it.
        let resume_at = self.resolve_wait_deadline(env, variables, states, now)?;
        activity.activity_state = Some(ActivityState::Wait(WaitActivityState { resume_at }));
        Ok(activity.raw_input.clone())
    }

    // The only post-activation work is arming the resume timer, from the instant resolved above — the
    // timer is what actually resumes the state; the field only records when that will be.
    async fn after_activated(
        &self,
        _env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        _variables: &Variables,
        _states: &Value,
    ) -> Result<(), ExecutionError> {
        let Some(ActivityState::Wait(wait)) = activity_value.activity_state.as_ref() else {
            return Ok(()); // deadline not recorded — nothing to arm.
        };
        emit_timer(
            out,
            // The timer's `execution` anchor is the flat top-level run (`activity.execution`), not
            // the immediate owner scope — it drives `Timer::execution` and the timer's
            // `{execution.name}-{suffix}` generated name, so a branch Wait still names its root run.
            activity_value.execution.clone(),
            activity_value.reference(),
            TimerPurpose::WaitResume,
            wait.resume_at,
        )
        .await;
        Ok(())
    }

    fn complete_directly(&self, _activity: &Activity) -> bool {
        false
    }

    // A `Wait` owns no result of its own: the timer that resumes it carries the raw result as the
    // `CompleteState` output, so the base's default `finish` (raw result = `raw_output`, falling back
    // to the processed input) is exactly this state's projection — only the routing is its own.
    fn assign(&self) -> Option<&AssignObject> {
        self.state.assign.as_ref()
    }

    fn output(&self) -> Option<&Value> {
        self.state.output.as_ref()
    }

    fn next(&self) -> Option<&str> {
        self.state.next.as_deref()
    }

    fn end(&self) -> Option<bool> {
        self.state.end
    }
}

impl WaitStateHandler<'_> {
    /// Compute the absolute deadline the Wait holds until. `Seconds` is relative — normalized to an
    /// absolute moment at activation **against the caller's `now`** (the injected clock's reading, so
    /// the expiry is as controllable as the definition); `Timestamp` is already absolute (parsed from
    /// RFC3339). Exactly one of the two is present by the well-formedness assumption (validated on
    /// submission); both may be a JSONata expression (evaluated against the activate-step
    /// `$states`). Any invalid/out-of-range value is a definition error that terminates the activity
    /// via the base hook.
    fn resolve_wait_deadline(
        &self,
        env: &mut EvalEnv,
        variables: &Variables,
        states: &Value,
        now: Timestamp,
    ) -> Result<Timestamp, ExecutionError> {
        match (&self.state.seconds, &self.state.timestamp) {
            // Literal `Seconds`: a non-negative integer, normalized to an absolute deadline.
            (Some(IntOrExpr::Int(n)), None) => {
                if !(0..=MAX_WAIT_SECONDS).contains(n) {
                    return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Wait Seconds must be an integer in the range 0..99999999".into(),
                    )));
                }
                now.checked_add(std::time::Duration::from_secs(*n as u64))
                    .ok_or_else(|| {
                        ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                            "Wait Seconds overflows the absolute deadline".into(),
                        ))
                    })
            }
            // `Seconds` as a JSONata expression: evaluate it, then require an in-range integer result.
            (Some(IntOrExpr::Expr(expr)), None) => {
                let value = eval_string_or_expr(env, expr.as_str(), states, variables)?;
                // `jsonata-core` yields every number as `f64`, so accept any unit-fraction value, not
                // just a true integral-typed `Number`.
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
                    return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Wait Seconds expression must evaluate to an integer".into(),
                    )));
                };
                if !(0..=MAX_WAIT_SECONDS).contains(&n) {
                    return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Wait Seconds expression must evaluate to an integer in the range 0..99999999"
                            .into(),
                    )));
                }
                now.checked_add(std::time::Duration::from_secs(n as u64))
                    .ok_or_else(|| {
                        ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                            "Wait Seconds overflows the absolute deadline".into(),
                        ))
                    })
            }
            // Literal `Timestamp`: an RFC3339 string parsed into an absolute moment.
            (None, Some(WaitTimestamp::Literal(s))) => {
                Timestamp::from_rfc3339(s).ok_or_else(|| {
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                        "Wait Timestamp is not a valid RFC3339 timestamp: {s}"
                    )))
                })
            }
            // `Timestamp` as a JSONata expression: evaluate it, then require a parseable RFC3339 string.
            (None, Some(WaitTimestamp::Expr(expr))) => {
                let value = eval_string_or_expr(env, expr.as_str(), states, variables)?;
                let s = match value {
                    Value::String(s) => s,
                    _ => {
                        return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                            "Wait Timestamp expression must evaluate to a string".to_string(),
                        )));
                    }
                };
                Timestamp::from_rfc3339(&s).ok_or_else(|| {
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                        "Wait Timestamp expression must evaluate to a valid RFC3339 timestamp: {s}"
                    )))
                })
            }
            // Well-formedness assumption: the engine is given a `StateMachine` that has already been
            // validated on submission (see `spica_asl::StateMachine::validate()`, TODO). Per ASL a Wait
            // state specifies *exactly one* of `Seconds` or `Timestamp`, so this arm — both absent or
            // both present — is unreachable at activation.
            _ => unreachable!(
                "Wait state must specify exactly one of Seconds or Timestamp \
                 (definition is validated on submission)"
            ),
        }
    }
}
