use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{IntOrExpr, State, WaitState, WaitTimestamp};

use super::super::state_handler::{StateHandler, StateHandlerFactory};
use super::super::{emit_timer, emit_transition, eval_string_or_expr};
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext};
use crate::log::Timestamp;
use crate::types::command::TimerPurpose;
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectReference};
use crate::{Activity, ActivityStatus, Variables};

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
    // Wait's processed input is its raw input; the only activate work is arming the resume timer.
    async fn after_activated(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        variables: &Variables,
        states: &Value,
    ) -> Result<(), ExecutionError> {
        let deadline = self.resolve_wait_deadline(env, variables, states)?;
        emit_timer(
            out,
            // The timer's `execution` anchor is the flat top-level run (`activity.execution`), not
            // the immediate owner scope — it drives `Timer::execution` and the timer's
            // `{execution.name}-{suffix}` generated name, so a branch Wait still names its root run.
            activity_value.execution.clone(),
            activity_value.reference(),
            TimerPurpose::WaitResume,
            deadline,
        )
        .await;
        Ok(())
    }

    fn complete_directly(&self, _activity: &Activity) -> bool {
        false
    }

    /// Resumed by `CompleteState` after the Wait's `WaitResume` timer fires.
    /// The `Command::CompleteState` finish — the shared orchestration (liveness/Terminating-race
    /// guards, owning-scope resolution, activity and variables reconstruction) and this state's projection,
    /// all inline.
    async fn complete(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity: ObjectReference,
        raw_result: Option<&Value>,
    ) {
        let act = match ctx.storage.get_activity(&activity).await {
            Ok(Some(a)) => a,
            Ok(None) => {
                out.terminate(
                    Some(activity.clone()),
                    crate::types::meta::ObjectReference::nil(),
                    ExecutionError::Runtime(RuntimeError::StateNotFound(format!(
                        "activity {activity}"
                    ))),
                );
                return;
            }
            Err(e) => {
                out.terminate(
                    Some(activity.clone()),
                    crate::types::meta::ObjectReference::nil(),
                    e,
                );
                return;
            }
        };

        // Race fix: a cancel already won on this activity. The drain that would have been emitted by
        // the cancel side may have been missed because the ordering interleaved (e.g. timer-fired +
        // cancel together). Re-emit the deferred termination ed so the parent finishes, reusing the
        // reason embedded in the terminating status itself.
        if act.value.status != ActivityStatus::Running {
            match act.value.status {
                ActivityStatus::Terminating(ref reason) => {
                    // Re-emit the terminal lifecycle event using the canonical activity payload shape,
                    // preserving every previously-folded domain field while only flipping the status
                    // from `Terminating(reason)` to `Terminated(reason)`.
                    let mut activity_value = act.value();
                    activity_value.status = ActivityStatus::Terminated(reason.clone());
                    out.append_event(crate::types::event::Event::StateTerminated {
                        activity: activity_value,
                    })
                    .await;
                }
                _ => return,
            }
            // A synchronous state that owns no children drains its owner Execution as soon as its own
            // terminal lands; run the inline reaction so the owner's own drain walks up.
            let owner = act
                .value
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner");
            if owner.kind == ObjectKind::Activity {
                super::super::child_completed::child_settled(ctx, out, owner, activity.clone())
                    .await;
            }
            return;
        }
        // Defensive: an activity with live children cannot enter success yet; its ed is deferred
        // until drain.
        if !act.active_children.is_empty() {
            return;
        }

        // The activity's owner is its *scope* — resolved through the central Execution/Thread
        // dispatch in storage, which silently ignores non-scope kinds.
        let scope = match crate::storage::load_scope_ref(
            ctx.storage,
            &act.value
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
        )
        .await
        {
            Ok(Some(s)) => s,
            Ok(None) => return, // owning scope already gone (or not a scope) — nothing to complete into.
            Err(_) => return,
        };
        if !scope.is_running() {
            return; // owner is past accepting a new transition; a late CompleteState is a no-op.
        }

        // Rehydrate the same entity-shaped activity value lifecycle events carry, so the complete
        // step observes the canonical domain payload rather than the projection-only row. The
        // command's `output` is the state's raw result; fold it onto the rehydrated activity as
        // `raw_output` so the complete-step events and `complete_activity`'s `$states.result` all
        // record the command-carried result.
        let mut activity_value = act.value();
        if let Some(result) = raw_result {
            activity_value.raw_output = Some(result.clone());
        }
        let variables = scope.variables().clone();
        let env = &mut *ctx.env;

        // Advance the one activity value in place to the completing lifecycle moment — it stays the
        // single source of truth for the rest of the complete step, so the completing status (and its
        // re-stamped update time) carries forward instead of a stale copy held alongside.
        activity_value
            .meta
            .with_update_at(crate::log::Timestamp::now());
        activity_value.status = ActivityStatus::Completing;
        if activity_value.raw_output.is_none() {
            activity_value.raw_output = Some(activity_value.raw_input.clone());
        }
        out.append_event(Event::StateCompleting {
            activity: activity_value.clone(),
        })
        .await;
        let raw_result = activity_value
            .raw_output
            .as_ref()
            .unwrap_or(&activity_value.raw_input);
        let states = States::new(
            &activity_value.raw_input,
            &activity_value.state_path.state_name(),
            activity_value.retry_count(),
        )
        .with_result(Some(raw_result))
        .with_assign_ctx(Some(&activity_value.raw_input))
        .build();
        let mut local_scope = variables.clone();

        let owner = activity_value
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        let assigned = self
            .apply_assign(
                out,
                env,
                &owner,
                self.state.assign.as_ref(),
                &states,
                &mut local_scope,
            )
            .await;
        fail_or!(out, Some(activity), owner.clone(), assigned);

        let output_value = fail_or!(
            out,
            Some(activity),
            owner.clone(),
            self.project_output(
                env,
                self.state.output.as_ref(),
                &states,
                &local_scope,
                raw_result.clone(),
            )
            .await
        );

        // Advance the same value in place to the completed lifecycle moment (mirroring the completing
        // step above): it stays the single source of truth, so the completed status and projected
        // output carry forward into the transition that follows.
        activity_value
            .meta
            .with_update_at(crate::log::Timestamp::now());
        activity_value.status = ActivityStatus::Completed;
        activity_value.output = Some(output_value.clone());
        if activity_value.raw_output.is_none() {
            activity_value.raw_output = Some(activity_value.raw_input.clone());
        }
        out.append_event(Event::StateCompleted {
            activity: activity_value.clone(),
        })
        .await;
        emit_transition(
            out,
            activity_value.execution.clone(),
            activity_value
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            activity,
            &activity_value.state_path,
            &output_value,
            self.state.next.as_deref(),
            self.state.end,
        )
        .await;
    }
}

impl WaitStateHandler<'_> {
    /// Compute the absolute deadline the Wait holds until. `Seconds` is relative — normalized to an
    /// absolute moment at activation; `Timestamp` is already absolute (parsed from RFC3339). Exactly
    /// one of the two is present by the well-formedness assumption (validated on submission); both may
    /// be a JSONata expression (evaluated against the activate-step `$states`). Any invalid/out-of-range
    /// value is a definition error that terminates the activity via the base hook.
    fn resolve_wait_deadline(
        &self,
        env: &mut EvalEnv,
        variables: &Variables,
        states: &Value,
    ) -> Result<Timestamp, ExecutionError> {
        match (&self.state.seconds, &self.state.timestamp) {
            // Literal `Seconds`: a non-negative integer, normalized to an absolute deadline.
            (Some(IntOrExpr::Int(n)), None) => {
                if !(0..=MAX_WAIT_SECONDS).contains(n) {
                    return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Wait Seconds must be an integer in the range 0..99999999".into(),
                    )));
                }
                Timestamp::now()
                    .checked_add(std::time::Duration::from_secs(*n as u64))
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
                Timestamp::now()
                    .checked_add(std::time::Duration::from_secs(n as u64))
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
