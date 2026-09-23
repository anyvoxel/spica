use async_trait::async_trait;
use serde_json::{Map, Value};
use spica_asl::{FailState, State};

use super::super::eval_string_or_expr;
use super::super::state_handler::{StateHandler, StateHandlerFactory};
use crate::Activity;
use crate::ActivityStatus;
use crate::Variables;
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext};
use crate::types::command::TerminationReason;
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::ObjectReference;

pub struct FailStateHandlerFactory;

#[async_trait]
impl StateHandlerFactory for FailStateHandlerFactory {
    fn state(&self) -> State {
        State::Fail(FailState::default())
    }

    fn create<'a>(&self, state: &'a State) -> Box<dyn StateHandler + 'a> {
        let State::Fail(s) = state else {
            unreachable!(
                "create dispatch guarantees the factory receives its own variant; got {state:?}"
            );
        };
        Box::new(FailStateHandler { state: s })
    }
}

struct FailStateHandler<'a> {
    state: &'a FailState,
}

#[async_trait]
impl StateHandler for FailStateHandler<'_> {
    // Fail's `complete` both terminates itself (emitting the activity's failure ed) and, being
    // terminal, terminates the execution (throwing `TerminateExecution`). Both are issued from the
    // same complete step so the state's terminal ed and the execution's terminating ed are produced
    // in the same causal chain. This mirrors Pass: Pass emits `StateCompleted` and routes to
    // `CompleteExecution` / the successor; Fail emits the failure ed and routes to
    // `TerminateExecution` / the successor.
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
        // `$states` for the complete step: `assign_ctx = Some` (matching Pass/Succeed) — however late
        // an `Assign` is applied, derived values read consistently with the scope already folded.
        let states = States::new(
            &activity_value.raw_input,
            &activity_value.state_path.state_name(),
            activity_value.retry_count(),
        )
        .with_assign_ctx(Some(&activity_value.raw_input))
        .build();

        let reason = fail_or!(
            out,
            Some(activity),
            activity_value
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            self.fail_reason(env, &activity_value, &states, &variables)
                .await
        );

        // Emit the activity's failure ed. `StateTerminating` + `StateTerminated` replace the
        // `StateCompleted` a successful complete would emit; `TerminateExecution` then folds the
        // execution's termination (`ExecutionTerminating` → `ExecutionTerminated`) rather than the
        // `CompleteExecution` Pass would throw. Each status is advanced in place on the same value so
        // the terminator carries forward the progressing state.
        activity_value
            .meta
            .with_update_at(crate::log::Timestamp::now());
        activity_value.status = ActivityStatus::Terminating(reason.clone());
        out.append_event(Event::StateTerminating {
            activity: activity_value.clone(),
        })
        .await;
        activity_value
            .meta
            .with_update_at(crate::log::Timestamp::now());
        activity_value.status = ActivityStatus::Terminated(reason.clone());
        out.append_event(Event::StateTerminated {
            activity: activity_value.clone(),
        })
        .await;

        // Route the terminal failure to the *owning scope*. A top-level run is an `Execution`
        // (reached via name-addressed `TerminateExecution`); a `Fail` inside a `Parallel` branch /
        // `Map` item is owned by a `Thread`, which lives in *thread* storage and is only reachable
        // via the reference-addressed `TerminateThread` — a bare `TerminateExecution` here would
        // silently miss it and leave the branch Running, wedging its container.
        super::super::emit_scope_termination(
            out,
            activity_value
                .meta
                .owner
                .as_ref()
                .expect("an owned activity has an owner"),
            reason,
        );
    }
}

impl FailStateHandler<'_> {
    /// Evaluate `Error`/`Cause` (when present) and package them as the `Fail` complete step's
    /// termination reason. An eval failure propagates as `Err`, which the caller's `fail_or!` turns
    /// into the same terminate + return the inline block used to produce.
    async fn fail_reason(
        &self,
        env: &mut EvalEnv,
        activity_value: &Activity,
        states: &Value,
        variables: &Variables,
    ) -> Result<TerminationReason, ExecutionError> {
        let mut err_out = Map::new();
        let mut error_name = "States.Fail".to_string();
        if let Some(error) = &self.state.error {
            let value = eval_string_or_expr(env, error, states, variables)?;
            if let Some(s) = value.as_str() {
                error_name = s.to_string();
            }
            err_out.insert("Error".to_string(), value);
        }
        if let Some(cause) = &self.state.cause {
            let value = eval_string_or_expr(env, cause, states, variables)?;
            err_out.insert("Cause".to_string(), value);
        }
        let error = ExecutionError::Runtime(RuntimeError::StateFailed {
            state: activity_value.state_path.state_name(),
            error: error_name,
            output: Box::new(Value::Object(err_out)),
        });
        Ok(TerminationReason::Failed { error })
    }
}
