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

#[cfg(test)]
mod tests {
    use spica_asl::WaitState;

    use super::super::harness::*;
    use super::*;
    use crate::types::command::{
        ActivateState, Command, TerminateState, TerminateThread, TerminationReason,
    };
    use crate::types::event::{Event, StateTransitioned};
    use crate::types::meta::{ObjectKind, ObjectMeta};
    use crate::{ActivityStatus, EntryPayload, ThreadStatus, Timer, TimerStatus};

    // A `Wait`'s activation product: the deadline is resolved in `process_input` (so it is a property
    // of the entering activity) and the resume timer is armed from that same instant in
    // `after_activated`. Its `complete_directly() == false` is what makes the timer — not an inline
    // `CompleteState` — the thing that resumes the state.

    /// The absolute instant `seconds` after [`at`].
    fn deadline(seconds: i64) -> Timestamp {
        at().checked_add(std::time::Duration::from_secs(seconds as u64))
            .expect("the fixture deadline is representable")
    }

    fn wait_state(seconds: Option<i64>, next: Option<&str>) -> State {
        State::Wait(WaitState {
            seconds: seconds.map(spica_asl::IntOrExpr::Int),
            next: next.map(str::to_string),
            ..Default::default()
        })
    }

    /// The activity value a `Wait` carries once activated: `Wait` seeds no `activity_state` at birth
    /// (it overrides no `initialize`), so the resume instant lands only when `process_input` resolves
    /// it — carried by `StateActivated` and every later event.
    fn activated_activity(resume_at: Timestamp) -> Activity {
        let mut activated = minted_activity(path("/States/P"), seeded_input());
        activated.input = Some(seeded_input());
        activated.activity_state = Some(ActivityState::Wait(WaitActivityState { resume_at }));
        activated
    }

    /// The resume timer `after_activated` arms: its own minted uid/name, anchored on the *root run*
    /// rather than the immediate owner — so a `Wait` inside a branch still names the execution it
    /// belongs to — and owned by the waiting activity, which is what makes it that activity's child.
    fn resume_timer(resume_at: Timestamp) -> Timer {
        Timer {
            execution: execution_ref(),
            purpose: TimerPurpose::WaitResume,
            status: TimerStatus::Active,
            deadline: resume_at,
            meta: ObjectMeta::builder(ObjectKind::Timer, uid(2))
                .name(obj_name("execution-1"))
                .at(at())
                .build()
                .with_owner(minted_activity_ref()),
        }
    }

    /// `activate` resolves the resume instant onto the activity, then arms the timer for that same
    /// instant and stops: a `Wait` completes when the timer fires, so there is no inline
    /// `CompleteState` — the state's whole activation is the pair (recorded deadline, armed timer).
    #[tokio::test]
    async fn activate_records_the_resume_instant_and_arms_the_resume_timer() {
        let resume_at = deadline(30);
        let activated = activate(
            &wait_state(Some(30), Some("P2")),
            &activate_cmd(path("/States/P"), seeded_input()),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;

        assert_eq!(
            activated.chain(),
            vec![
                EntryPayload::Event(Event::StateActivating {
                    activity: minted_activity(path("/States/P"), seeded_input()),
                }),
                EntryPayload::Event(Event::StateActivated {
                    activity: activated_activity(resume_at),
                }),
                EntryPayload::Event(Event::TimerActivated {
                    timer: resume_timer(resume_at),
                }),
            ]
        );

        // The timer is folded as the waiting activity's child — the edge the complete step reads to
        // decide whether the state may finish yet.
        let timer = resume_timer(resume_at).reference();
        assert!(
            activated
                .children(&minted_activity_ref())
                .await
                .contains(&timer),
            "TimerActivated folds the owner's child edge"
        );
    }

    /// A `Wait` whose timer is still live must not finish: the complete step opens the finish —
    /// `StateCompleting` is emitted, so the decision is durable — and then defers, leaving the
    /// activity `Completing` for the timer's settle to drain.
    #[tokio::test]
    async fn complete_defers_the_finish_while_the_resume_timer_lives() {
        let resume_at = deadline(30);
        let state = wait_state(Some(30), Some("P2"));
        let activated = activate(
            &state,
            &activate_cmd(path("/States/P"), seeded_input()),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;
        let Dispatch { store, .. } = activated;

        let completed = complete(&state, store, &complete_cmd(seeded_input())).await;

        let mut completing = activated_activity(resume_at);
        completing.raw_output = Some(seeded_input());
        completing.status = ActivityStatus::Completing;
        assert_eq!(
            completed.chain(),
            vec![EntryPayload::Event(Event::StateCompleting {
                activity: completing
            })],
            "only the durable `ing` lands while the resume timer lives"
        );
        assert_eq!(
            completed
                .activity(&minted_activity_ref())
                .await
                .expect("the row is still there")
                .value
                .status,
            ActivityStatus::Completing,
            "a deferred finish leaves the activity Completing, not Running"
        );
    }

    /// Once the resume timer has drained (its settle detached it), the same complete step finishes:
    /// the projection runs and the state routes to its successor. The timer's own settle drives that
    /// drain, so nothing here re-arms or re-issues anything.
    #[tokio::test]
    async fn complete_finishes_once_the_resume_timer_has_drained() {
        let state = wait_state(Some(30), Some("P2"));
        let completed = complete(
            &state,
            complete_store(seeded_input(), []).await,
            &complete_cmd(seeded_input()),
        )
        .await;

        let mut completing = minted_activity(path("/States/P"), seeded_input());
        completing.input = Some(seeded_input());
        completing.raw_output = Some(seeded_input());
        completing.status = ActivityStatus::Completing;
        let mut done = completing.clone();
        done.status = ActivityStatus::Completed;
        done.output = Some(seeded_input());

        assert_eq!(
            completed.chain(),
            vec![
                EntryPayload::Event(Event::StateCompleting {
                    activity: completing
                }),
                EntryPayload::Event(Event::StateCompleted { activity: done }),
                EntryPayload::Event(Event::StateTransitioned(StateTransitioned {
                    activity: minted_activity_ref(),
                    next: path("/States/P2").as_ptr().to_owned(),
                })),
                EntryPayload::Command(Command::ActivateState(ActivateState {
                    execution: execution_ref(),
                    owner: thread_ref(),
                    state_path: path("/States/P2"),
                    input: seeded_input(),
                })),
            ]
        );
    }

    /// A `Seconds` outside the spec's inclusive range is a definition error, not a clamp: the failure
    /// is routed at the owning scope so the run ends instead of wedging on a deadline that was never
    /// armed.
    #[tokio::test]
    async fn activate_fails_the_state_on_an_out_of_range_seconds() {
        let activated = activate(
            &wait_state(Some(MAX_WAIT_SECONDS + 1), Some("P2")),
            &activate_cmd(path("/States/P"), seeded_input()),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;

        let reason = TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "Wait Seconds must be an integer in the range 0..99999999".into(),
            )),
        };
        assert_eq!(
            activated.chain(),
            vec![
                // The birth was already recorded before the input was processed, so the failure
                // unwinds a state that momentarily existed.
                EntryPayload::Event(Event::StateActivating {
                    activity: minted_activity(path("/States/P"), seeded_input()),
                }),
                EntryPayload::Command(Command::TerminateState(TerminateState {
                    activity: minted_activity_ref(),
                    reason: reason.clone(),
                })),
                EntryPayload::Command(Command::TerminateThread(TerminateThread {
                    thread: thread_ref(),
                    reason,
                })),
            ]
        );
        assert_eq!(
            activated
                .activity(&minted_activity_ref())
                .await
                .expect("the birth event folded a row")
                .value
                .status,
            ActivityStatus::Running,
            "the unwinding terminate is a command, not a fold: the row stays as the birth left it"
        );
    }
}
