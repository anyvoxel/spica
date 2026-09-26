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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use spica_asl::FailState;

    use super::super::harness::*;
    use super::*;
    use crate::ActivityState;
    use crate::types::command::{Command, CompleteState, TerminateThread};
    use crate::types::event::Event;
    use crate::{ActivityStatus, EntryPayload, ThreadStatus};

    // A `Fail` inherits the base's `activate` untouched and overrides only `finish`: the activation
    // chain is therefore the shared synchronous one, while `complete` replaces the success projection
    // with the activity's failure ed followed by the owning scope's termination — the pair that turns
    // "this state failed" into "the run this state belongs to failed".

    fn fail_state(error: Option<&str>, cause: Option<&str>) -> State {
        State::Fail(FailState {
            comment: None,
            cause: cause.map(str::to_string),
            error: error.map(str::to_string),
        })
    }

    /// `activate` runs the shared chain — `Fail` has nothing to scaffold, arm or fan out, and its
    /// terminal routing is decided in the finish.
    #[tokio::test]
    async fn activate_emits_the_synchronous_success_chain() {
        let activated = activate(
            &fail_state(Some("ErrorA"), None),
            &activate_cmd(path("/States/P"), seeded_input()),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;

        let birth = minted_activity(path("/States/P"), seeded_input());
        let mut processed = birth.clone();
        processed.input = Some(seeded_input());

        assert_eq!(
            activated.chain(),
            vec![
                EntryPayload::Event(Event::StateActivating { activity: birth }),
                EntryPayload::Event(Event::StateActivated {
                    activity: processed
                }),
                EntryPayload::Command(Command::CompleteState(CompleteState {
                    activity: minted_activity_ref(),
                    output: seeded_input(),
                })),
            ]
        );
        // No side effect, so nothing beyond the activity row itself was folded as its child.
        assert!(
            activated
                .children(&thread_ref())
                .await
                .contains(&minted_activity_ref()),
            "the failure is still a normal activation until the finish runs"
        );
    }

    /// The finish terminates the activity **and** its owning scope from one step: the activity's
    /// failure ed (`StateTerminating` → `StateTerminated`, replacing the `StateCompleted` a success
    /// would emit), then the scope's termination — here a `TerminateThread`, because the activity's
    /// owner is a thread. A `Fail` with no `Error` names itself with the spec's default.
    #[tokio::test]
    async fn complete_terminates_the_activity_and_its_owner_scope() {
        let completed = complete(
            &fail_state(None, None),
            complete_store(seeded_input(), []).await,
            &complete_cmd(seeded_input()),
        )
        .await;

        let reason = TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "P".to_string(),
                error: "States.Fail".to_string(),
                output: Box::new(json!({})),
            }),
        };
        let mut completing = minted_activity(path("/States/P"), seeded_input());
        completing.input = Some(seeded_input());
        completing.raw_output = Some(seeded_input());
        completing.status = ActivityStatus::Completing;
        let mut terminating = completing.clone();
        terminating.status = ActivityStatus::Terminating(reason.clone());
        let mut terminated = terminating.clone();
        terminated.status = ActivityStatus::Terminated(reason.clone());

        assert_eq!(
            completed.chain(),
            vec![
                EntryPayload::Event(Event::StateCompleting {
                    activity: completing
                }),
                EntryPayload::Event(Event::StateTerminating {
                    activity: terminating
                }),
                EntryPayload::Event(Event::StateTerminated {
                    activity: terminated
                }),
                EntryPayload::Command(Command::TerminateThread(TerminateThread {
                    thread: thread_ref(),
                    reason,
                })),
            ]
        );
        let row = completed
            .activity(&minted_activity_ref())
            .await
            .expect("the failure ed folds the activity row");
        assert!(
            row.value.status.is_terminal(),
            "the activity is left terminal, not Running: {:?}",
            row.value.status
        );
    }

    /// `Error` and `Cause` are projections of the state's input: the evaluated `Error` becomes the
    /// failure's error name (which `Retry`/`Catch` would match on) and both are carried together in
    /// the termination reason's context object.
    #[tokio::test]
    async fn complete_projects_error_and_cause_into_the_failure() {
        let state = fail_state(
            Some("{% $states.input.code %}"),
            Some("{% $states.input.msg %}"),
        );
        let input = json!({ "code": "E-42", "msg": "boom" });
        let completed = complete(
            &state,
            complete_store(input.clone(), []).await,
            &complete_cmd(input.clone()),
        )
        .await;

        let reason = TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "P".to_string(),
                error: "E-42".to_string(),
                output: Box::new(json!({ "Error": "E-42", "Cause": "boom" })),
            }),
        };
        assert!(
            matches!(
                completed.chain().last(),
                Some(EntryPayload::Command(Command::TerminateThread(TerminateThread {
                    reason: actual,
                    ..
                }))) if *actual == reason
            ),
            "the evaluated Error/Cause reach the scope termination: {:?}",
            completed.chain().last()
        );

        // The activity carries no `activity_state` of its own: a leaf `Fail` seeds none, so the
        // failure's context travels entirely on the termination reason.
        assert_eq!(
            completed
                .activity(&minted_activity_ref())
                .await
                .expect("the failure ed folds the activity row")
                .value
                .activity_state,
            None::<ActivityState>
        );
    }
}
