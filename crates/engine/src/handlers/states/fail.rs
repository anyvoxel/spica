use async_trait::async_trait;
use serde_json::{Map, Value};
use spica_asl::{FailState, State};

use super::super::eval_string_or_expr;
use super::super::state_handler::{StateHandler, StateHandlerFactory};
use crate::Activity;
use crate::ActivityStatus;
use crate::Variables;
use crate::eval_env::EvalEnv;
use crate::handler::Collector;
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
    // Fail's finish both terminates itself (emitting the activity's failure ed) and, being terminal,
    // terminates the owning scope. Both are issued from the same step so the state's terminal ed and the
    // scope's terminating ed are produced in the same causal chain. This mirrors Pass: Pass emits
    // `StateCompleted` and routes to the successor / a completed owner; Fail replaces that with the
    // failure ed and the scope termination, so it overrides the base's success `finish` wholesale.
    async fn finish(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector<'_>,
        _activity: ObjectReference,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<(), ExecutionError> {
        // `$states` for the failure projection: `assign_ctx = Some` (matching Pass/Succeed) — however
        // late an `Assign` is applied, derived values read consistently with the scope already folded.
        let states = States::new(
            &activity_value.raw_input,
            &activity_value.state_path.state_name(),
            activity_value.retry_count(),
        )
        .with_assign_ctx(Some(&activity_value.raw_input))
        .build();
        let reason = self
            .fail_reason(env, activity_value, &states, variables)
            .await?;

        // Emit the activity's failure ed. `StateTerminating` + `StateTerminated` replace the
        // `StateCompleted` a successful complete would emit. Each status is advanced in place on the
        // same value so the terminator carries forward the progressing state.
        let mut terminated = activity_value.clone();
        terminated.meta.with_update_at(out.now());
        terminated.status = ActivityStatus::Terminating(reason.clone());
        out.append_event(Event::StateTerminating {
            activity: terminated.clone(),
        })
        .await;
        terminated.meta.with_update_at(out.now());
        terminated.status = ActivityStatus::Terminated(reason.clone());
        out.append_event(Event::StateTerminated {
            activity: terminated.clone(),
        })
        .await;

        // Route the terminal failure to the *owning scope*. A top-level run is an `Execution`
        // (reached via name-addressed `TerminateExecution`); a `Fail` inside a `Parallel` branch /
        // `Map` item is owned by a `Thread`, which lives in *thread* storage and is only reachable
        // via the reference-addressed `TerminateThread` — a bare `TerminateExecution` here would
        // silently miss it and leave the branch Running, wedging its container.
        super::super::emit_scope_termination(
            out,
            terminated
                .meta
                .owner
                .as_ref()
                .expect("an owned activity has an owner"),
            reason,
        );
        Ok(())
    }
}

impl FailStateHandler<'_> {
    /// Evaluate `Error`/`Cause` (when present) and package them as the `Fail` complete step's
    /// termination reason. An eval failure propagates as `Err`, which the base turns into the same
    /// terminate + return the inline block used to produce.
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
