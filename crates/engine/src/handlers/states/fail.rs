use async_trait::async_trait;
use serde_json::{Map, Value};
use spica_asl::{FailState, State};

use super::super::container::{Container, ThreadContainer};
use super::super::eval_string_or_expr;
use super::super::state_handler::{StateHandler, StateHandlerFactory};
use crate::Activity;
use crate::RejectionType;
use crate::Variables;
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::{Command, TerminateState, TerminationReason};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::HasRawObjectRef;

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
    /// (3.4) A `Fail` owns no child at all, so there is nothing to wait on: the whole complete path —
    /// decide the reason, emit the activity's failure ed, then terminate the owning scope — runs here
    /// in one go, with no child count. Nothing reaches this state through the deferred drain either
    /// (see `crate::handlers::continue_`): that hop exists to advance a state whose children settled,
    /// and this one has none to settle.
    ///
    /// A decision failure is turned into a terminate here rather than returned: the activity is
    /// already `Completing`, so there is no later hop to report it from.
    async fn after_completing(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<(), ProcessingError> {
        let reason = match self.process_fail(ctx.env, activity_value, variables).await {
            Ok(reason) => reason,
            Err(error) => {
                out.append_command(Command::TerminateState(TerminateState {
                    activity: activity_value.meta.object_ref(),
                    reason: TerminationReason::Failed { error },
                }));
                return Ok(());
            }
        };

        // Resolve the owning scope's container *before* any terminal event, so an entitled settle is
        // answered while the activity is still intact. An activity's owner slot admits only a
        // `Thread`, so the container is read directly.
        let owner = activity_value.meta.owner.clone();
        let Some(thread_container) = ThreadContainer::open(ctx.storage, owner).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!(
                    "fail_state: activity {} has no owning thread; termination refused",
                    activity_value.meta.object_ref()
                ),
            ));
        };

        // Emit the activity's failure ed. `StateTerminating` + `StateTerminated` replace the
        // `StateCompleted` a successful complete would emit. Each status is advanced in place on the
        // same value so the terminator carries forward the progressing state.
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

        // Hand the settled activity to its owning thread's container; the scope's own termination
        // follows from the state's failure (see `ThreadContainer::after_child_terminated`).
        thread_container
            .after_child_terminated(ctx, out, &activity_ref)
            .await;
        Ok(())
    }

    // A `Fail` owns no children, so its terminate closes inline: mark `Terminated`, emit the
    // terminal, and relay the settle up to the owning thread.
    async fn after_terminating(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        _variables: &Variables,
    ) -> Result<(), ProcessingError> {
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
        Ok(())
    }
}

impl FailStateHandler<'_> {
    /// The `Fail` decision itself: build `$states` and evaluate `Error`/`Cause` (when present) into
    /// the termination reason — the mirror of `Choice`'s `process_choice`, returning the decided
    /// value for `after_completing` to turn into the failure ed, and an `Err` for it to terminate on.
    /// The evaluation error propagates as `Err` for the same reason a `Choice` rule miss does: no
    /// failure can be named until both fields are resolved.
    async fn process_fail(
        &self,
        env: &mut EvalEnv,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<TerminationReason, ExecutionError> {
        // `$states` for the failure projection: `assign_ctx = Some` (matching Pass/Succeed) — however
        // late an `Assign` is applied, derived values read consistently with the scope already folded.
        let states = States::new(
            &activity_value.raw_input,
            &activity_value.state_path.state_name(),
            activity_value.retry_count(),
        )
        .with_assign_ctx(Some(&activity_value.raw_input))
        .build();
        self.fail_reason(env, activity_value, &states, variables)
            .await
    }

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
    use crate::types::meta::HasRawObjectRef;
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
            seeded_scope(ThreadStatus::Running),
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
                .children(thread_ref().as_raw_object_ref())
                .await
                .contains(minted_activity_ref().as_raw_object_ref()),
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
