use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{IntOrExpr, State, StatePath, WaitState, WaitTimestamp};

use super::super::container::{Container, ThreadContainer};
use super::super::state_handler::{StateHandler, StateHandlerFactory};
use super::super::{emit_timer, eval_string_or_expr};
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::log::Timestamp;
use crate::types::activity::ActivityKind;
use crate::types::command::{
    ActivateState, Command, CompleteState, CompleteThread, TerminateState, TerminationReason,
};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::{Event, StateTransitioned};
use crate::types::meta::{HasRawObjectRef, ObjectKind, ObjectRef, RawObjectRef};
use crate::types::timer::TimerKind;
use crate::{Activity, ActivityState, RejectionType, Variables, WaitActivityState};

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

/// What the complete step's projection routed to, handed back for `after_completing` to turn into the
/// terminal and the transition — the mirror of `Pass`'s `PassFinish`. A `Wait` declares exactly one of
/// `Next`/`End` (per ASL), so the two are an enum, never both present.
enum WaitFinish {
    /// Hop to the resolved sibling successor, activating it with the projected output.
    Next { next: StatePath, output: Value },
    /// `End`: no successor — complete the owner thread with the projected output.
    End { output: Value },
    /// Neither `Next` nor `End` is declared: the definition is malformed, so the activity unwinds
    /// with `States.NoTerminal` and its scope is taken down.
    NoTerminal,
}

#[async_trait]
impl StateHandler for WaitStateHandler<'_> {
    /// (3.4) A `Wait` reaches its complete step only through its resume timer's settle — the timer's
    /// fire is what issues the `CompleteState`, and its settle has already drained the child edge in
    /// the same batch (see `WaitStateHandler::child_completed`), so the state is always childless
    /// here. Its whole complete path — project, then hand the finish's settle to the owning thread's
    /// container or the successor — runs in one go, mirroring `Pass`/`Choice`. A projection failure
    /// is turned into a terminate here rather than returned: the activity is already `Completing`, so
    /// there is no later hop to report it from.
    async fn after_completing(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<(), ProcessingError> {
        let finish = match self
            .process_wait(ctx.env, out, activity_value, variables)
            .await
        {
            Ok(finish) => finish,
            Err(error) => {
                out.append_command(Command::TerminateState(TerminateState {
                    activity: activity_value.meta.object_ref(),
                    reason: TerminationReason::Failed { error },
                }));
                return Ok(());
            }
        };
        match finish {
            WaitFinish::Next { next, output } => {
                let mut completed = activity_value.clone();
                debug_assert!(
                    completed.mark_completed(output.clone(), out.now()).is_ok(),
                    "the complete step that dispatched this after_completing opened the activity as Completing"
                );
                out.append_event(Event::StateCompleted {
                    activity: completed,
                })
                .await;
                out.append_event(Event::StateTransitioned(StateTransitioned {
                    activity: activity_value.meta.object_ref(),
                    next: next.as_ptr().to_owned(),
                }))
                .await;
                out.append_command(Command::ActivateState(ActivateState {
                    execution: activity_value.execution.clone(),
                    owner: activity_value.meta.owner.clone(),
                    state_path: next,
                    input: output,
                }));
            }
            WaitFinish::End { output } => {
                let mut completed = activity_value.clone();
                debug_assert!(
                    completed.mark_completed(output.clone(), out.now()).is_ok(),
                    "the complete step that dispatched this after_completing opened the activity as Completing"
                );
                out.append_event(Event::StateCompleted {
                    activity: completed,
                })
                .await;
                // `End` routes to the owner thread's completion rather than a sibling hop, so it
                // carries no `StateTransitioned` marker.
                out.append_command(Command::CompleteThread(CompleteThread {
                    thread: activity_value.meta.owner.clone(),
                    output,
                }));
            }
            // No route at all: the definition is malformed, so unwind the finishing activity and let
            // its owner thread's container carry the scope down with the failure.
            WaitFinish::NoTerminal => {
                let reason = TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::NoTerminal),
                };
                let owner = activity_value.meta.owner.clone();
                let Some(thread_container) = ThreadContainer::open(ctx.storage, owner).await?
                else {
                    return Err(ProcessingError::Rejected(
                        RejectionType::NotFound,
                        format!(
                            "wait_state: activity {} has no owning thread; termination refused",
                            activity_value.meta.object_ref()
                        ),
                    ));
                };
                let mut terminated = activity_value.clone();
                debug_assert!(
                    terminated
                        .mark_terminating(reason.clone(), out.now())
                        .is_ok(),
                    "the complete that dispatched this after_completing opened the activity as Completing"
                );
                out.append_event(Event::StateTerminating {
                    activity: terminated.clone(),
                })
                .await;
                debug_assert!(
                    terminated.mark_terminated(out.now()).is_ok(),
                    "the activity this after_completing just began terminating is Terminating"
                );
                let activity_ref = terminated.meta.object_ref().into_raw_object_ref();
                out.append_event(Event::StateTerminated {
                    activity: terminated,
                })
                .await;
                thread_container
                    .after_child_terminated(ctx, out, &activity_ref)
                    .await;
            }
        }
        Ok(())
    }

    // A `Wait`'s only child is its resume deadline, swept because the activity is going down: a
    // still-armed timer must not outlive the state it bounds. Cancelling it defers the terminal to the
    // timer's settle; the base already opened `StateTerminating`, so a `Wait` with no timer left closes
    // inline.
    async fn after_terminating(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        _variables: &Variables,
    ) -> Result<(), ProcessingError> {
        let mut pending = 0;
        if let Ok(children) = ctx
            .storage
            .get_children(activity_value.meta.object_ref().into_raw_object_ref())
            .await
        {
            for child in children {
                if child.kind == ObjectKind::Timer {
                    out.append_command(Command::CancelTimer {
                        timer: child.typed::<TimerKind>(),
                    });
                    pending += 1;
                }
            }
        }
        if pending == 0 {
            let mut terminated = activity_value.clone();
            debug_assert!(
                terminated.mark_terminated(out.now()).is_ok(),
                "the terminate step that dispatched this after_terminating opened the activity as Terminating"
            );
            let owner = activity_value.meta.owner.clone().into_raw_object_ref();
            let activity_ref = terminated.meta.object_ref().into_raw_object_ref();
            out.append_event(Event::StateTerminated {
                activity: terminated,
            })
            .await;
            super::super::child_completed::child_settled(ctx, out, owner, activity_ref).await;
        } else {
            tracing::debug!(
                activity = %activity_value.meta.object_ref(),
                pending,
                "wait state terminating deferred: waiting on the deadline"
            );
        }
        Ok(())
    }

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
        // computation feeds both carriers: this repository field and the `Seconds` timer
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
            activity_value,
            wait.resume_at,
        )
        .await;
        Ok(())
    }

    fn complete_directly(&self, _activity: &Activity) -> bool {
        false
    }

    /// A `Wait`'s only child is its resume timer, and that timer's settle *is* its completion trigger:
    /// the state resumes and finishes. The raw result is the activity's processed input — a `Wait`
    /// produces no distinct raw output — carried as the `CompleteState` output, keeping the command
    /// self-describing.
    async fn child_completed(
        &self,
        _ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity: ObjectRef<ActivityKind>,
        activity_value: &Activity,
        _variables: &Variables,
        _child: RawObjectRef,
    ) {
        out.append_command(Command::CompleteState(CompleteState {
            activity,
            output: activity_value
                .input
                .clone()
                .unwrap_or(serde_json::Value::Null),
        }));
    }
}

impl WaitStateHandler<'_> {
    /// The `Wait` complete step: build `$states`, apply `Assign` (emitting `VariablesAssigned` on the
    /// owner scope), project `Output` (defaulting to the raw result), and decide the routing — the
    /// canonical success finish, mirroring `Pass`'s `process_pass`. It emits nothing about the
    /// activity itself; the terminal and the transition belong to `after_completing`.
    async fn process_wait(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<WaitFinish, ExecutionError> {
        let result = activity_value
            .raw_output
            .clone()
            .unwrap_or_else(|| activity_value.raw_input.clone());
        let states = States::new(
            &activity_value.raw_input,
            &activity_value.state_path.state_name(),
            activity_value.retry_count(),
        )
        .with_result(Some(&result))
        .with_assign_ctx(Some(&activity_value.raw_input))
        .build();
        let mut local_scope = variables.clone();
        let owner = activity_value.meta.owner.clone();
        self.apply_assign(
            out,
            env,
            &owner,
            self.state.assign.as_ref(),
            &states,
            &mut local_scope,
        )
        .await?;
        let output = self
            .project_output(
                env,
                self.state.output.as_ref(),
                &states,
                &local_scope,
                result,
            )
            .await?;
        let next = self.state.next.as_deref();
        if self.state.end == Some(true) {
            Ok(WaitFinish::End { output })
        } else if let Some(next) = next {
            Ok(WaitFinish::Next {
                next: activity_value.state_path.sibling(next),
                output,
            })
        } else {
            Ok(WaitFinish::NoTerminal)
        }
    }

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
    use spica_storage::InMemoryStorage;

    use super::super::harness::*;
    use super::*;
    use crate::types::command::{
        ActivateState, Command, CompleteState, CompleteThread, TerminateState, TerminationReason,
    };
    use crate::types::event::{Event, StateTransitioned};
    use crate::types::meta::{HasRawObjectRef, ObjectMeta};
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
            status: TimerStatus::Active,
            deadline: resume_at,
            meta: ObjectMeta::builder(uid(2))
                .name(obj_name("execution-1"))
                .at(at())
                .with_owner(activity_timer_owner(minted_activity_ref())),
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
            seeded_scope(ThreadStatus::Running),
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

        // The timer is folded as the waiting activity's child — the edge its settle detaches when it
        // fires, which is what lets the state finish.
        let timer = resume_timer(resume_at).meta.raw_object_ref();
        assert!(
            activated
                .children(minted_activity_ref().as_raw_object_ref())
                .await
                .contains(&timer),
            "TimerActivated folds the owner's child edge"
        );
    }

    /// Once the resume timer has drained (its settle detached it), the complete step finishes:
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

    /// A `Wait` that declares `End` (no successor) routes its finish to the owning thread's
    /// completion, carrying the projected result — the terminal has no sibling to hop to, so no
    /// `StateTransitioned` is emitted.
    #[tokio::test]
    async fn complete_with_end_completes_the_owner_thread() {
        let state = State::Wait(WaitState {
            seconds: Some(spica_asl::IntOrExpr::Int(30)),
            end: Some(true),
            ..Default::default()
        });
        let completed = complete(
            &state,
            complete_store(seeded_input(), []).await,
            &complete_cmd(seeded_input()),
        )
        .await;

        let mut done = minted_activity(path("/States/P"), seeded_input());
        done.input = Some(seeded_input());
        done.raw_output = Some(seeded_input());
        done.status = ActivityStatus::Completed;
        done.output = Some(seeded_input());

        assert!(
            matches!(
                completed.chain().last(),
                Some(EntryPayload::Command(Command::CompleteThread(
                    CompleteThread {
                        thread: t,
                        output,
                    }
                ))) if *t == thread_ref() && *output == seeded_input()
            ),
            "an `End` Wait completes its owner thread with the result: {:?}",
            completed.chain().last()
        );
        assert!(
            !completed
                .chain()
                .iter()
                .any(|p| matches!(p, EntryPayload::Event(Event::StateTransitioned(_)))),
            "a terminal Wait has no successor to name"
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
            seeded_scope(ThreadStatus::Running),
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

    /// A `Wait`'s resume timer settling resumes and finishes the state: `child_completed` hands the
    /// activity a `CompleteState` whose raw result is the Wait's processed input (a Wait produces no
    /// distinct raw output) — the command the timer handler used to append itself.
    #[tokio::test]
    async fn child_completed_of_the_resume_timer_completes_the_activity() {
        let mut store = InMemoryStorage::new();
        let row = seeded_activity(seeded_input(), []);
        seed_container(&mut store, row, []).await;

        let completed = child_completed(
            &wait_state(Some(30), Some("P2")),
            store,
            minted_activity_ref(),
            resume_timer(deadline(30)).meta.raw_object_ref().clone(),
        )
        .await;

        assert_eq!(
            completed.chain(),
            vec![EntryPayload::Command(Command::CompleteState(
                CompleteState {
                    activity: minted_activity_ref(),
                    output: seeded_input(),
                }
            ))]
        );
    }
}
