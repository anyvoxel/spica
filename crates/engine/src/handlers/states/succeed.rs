use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{State, SucceedState};

use super::super::container::{Container, ThreadContainer};
use super::super::state_handler::{StateHandler, StateHandlerFactory};
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::{Command, TerminateState, TerminationReason};
use crate::types::context::States;
use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::meta::HasRawObjectRef;
use crate::{Activity, Variables};

pub struct SucceedStateHandlerFactory;

#[async_trait]
impl StateHandlerFactory for SucceedStateHandlerFactory {
    fn state(&self) -> State {
        State::Succeed(SucceedState::default())
    }

    fn create<'a>(&self, state: &'a State) -> Box<dyn StateHandler + 'a> {
        let State::Succeed(s) = state else {
            unreachable!(
                "create dispatch guarantees the factory receives its own variant; got {state:?}"
            );
        };
        Box::new(SucceedStateHandler { state: s })
    }
}

struct SucceedStateHandler<'a> {
    state: &'a SucceedState,
}

#[async_trait]
impl StateHandler for SucceedStateHandler<'_> {
    /// (3.4) A `Succeed` is terminal and owns no child, so its whole complete path — project `Output`,
    /// open the activity, then hand the completed settle to its owning thread's container, which
    /// completes the thread — runs here in one go, mirroring `Pass`/`Choice`/`Fail`. A projection
    /// failure is turned into a terminate here rather than returned: the activity is already
    /// `Completing`, so there is no later hop to report it from.
    async fn after_completing(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<(), ProcessingError> {
        let output = match self
            .process_succeed(ctx.env, activity_value, variables)
            .await
        {
            Ok(output) => output,
            Err(error) => {
                out.append_command(Command::TerminateState(TerminateState {
                    activity: activity_value.meta.object_ref(),
                    reason: TerminationReason::Failed { error },
                }));
                return Ok(());
            }
        };
        // Resolve the owning thread's container *before* emitting the terminal, so an entitled settle
        // is answered while the activity is still intact (see `FailStateHandler::after_completing`).
        let owner = activity_value.meta.owner.clone();
        let thread_container = ThreadContainer::open(ctx.storage, owner).await?;
        let mut completed = activity_value.clone();
        debug_assert!(
            completed.mark_completed(output, out.now()).is_ok(),
            "the complete step that dispatched this after_completing opened the activity as Completing"
        );
        let activity_ref = completed.meta.object_ref().into_raw_object_ref();
        out.append_event(Event::StateCompleted {
            activity: completed,
        })
        .await;
        // The threaded scope's own completion follows from the terminal's settle (see
        // `ThreadContainer::after_child_completed`): `Succeed` carries no `Next`, so completing the
        // thread with the projected output is the state's one and only route.
        thread_container
            .after_child_completed(ctx, out, &activity_ref)
            .await?;
        Ok(())
    }

    // A `Succeed` owns no children, so its terminate closes inline: mark `Terminated`, emit the
    // terminal, and relay the settle up to the owning thread.
    async fn after_terminating(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        _variables: &Variables,
    ) -> Result<(), ProcessingError> {
        let container =
            ThreadContainer::open(ctx.storage, activity_value.meta.owner.clone()).await?;
        let mut terminated = activity_value.clone();
        debug_assert!(
            terminated.mark_terminated(out.now()).is_ok(),
            "the terminate step that dispatched this after_terminating opened the activity as Terminating"
        );
        let activity_ref = terminated.meta.object_ref().into_raw_object_ref();
        out.append_event(Event::StateTerminated {
            activity: terminated,
        })
        .await;
        container
            .after_child_terminated(ctx, out, &activity_ref)
            .await?;
        Ok(())
    }
}

impl SucceedStateHandler<'_> {
    /// The `Succeed` complete step: build `$states` and project `Output` (defaulting to the raw
    /// result), mirroring `Pass`'s `process_pass`. `Succeed` carries no `Assign` (per ASL), so nothing
    /// is emitted here — no `VariablesAssigned` — and the finish is always the owner's terminal
    /// completion.
    async fn process_succeed(
        &self,
        env: &mut EvalEnv,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<Value, ExecutionError> {
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
        self.project_output(env, self.state.output.as_ref(), &states, variables, result)
            .await
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use spica_asl::SucceedState;

    use super::super::harness::*;
    use super::*;
    use crate::types::command::{Command, CompleteState, CompleteThread};
    use crate::types::event::Event;
    use crate::{ActivityStatus, EntryPayload, ThreadStatus};

    // A `Succeed` overrides nothing on the activate side, so its tests pin two things the base's
    // Template Method inherits verbatim: the state-agnostic activation chain, and — one step later —
    // that its unconditional `end() == Some(true)` turns the finish into an owner-terminal completion
    // rather than a sibling hop.

    fn succeed_state(output: Option<Value>) -> State {
        State::Succeed(SucceedState {
            comment: None,
            output,
        })
    }

    /// `activate` hands into `CompleteState` exactly as a `Pass` does: `Succeed`'s terminal routing is
    /// `finish`'s input, not the activation's, so the activation chain carries nothing of it.
    #[tokio::test]
    async fn activate_emits_the_synchronous_success_chain() {
        let activated = activate(
            &succeed_state(None),
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
    }

    /// The finish projects `Output` and — with `end() == Some(true)` — routes to the owner's
    /// completion: `CompleteThread` carrying the projected result, with no `StateTransitioned` marker,
    /// because a terminal hop has no successor to name.
    #[tokio::test]
    async fn complete_projects_its_output_and_completes_the_owner() {
        let state = succeed_state(Some(json!({ "done": "{% $states.input.n %}" })));
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
        done.output = Some(json!({ "done": 1.0 }));

        assert_eq!(
            completed.chain(),
            vec![
                EntryPayload::Event(Event::StateCompleting {
                    activity: completing
                }),
                EntryPayload::Event(Event::StateCompleted { activity: done }),
                EntryPayload::Command(Command::CompleteThread(CompleteThread {
                    thread: thread_ref(),
                    output: json!({ "done": 1.0 }),
                })),
            ]
        );
        assert_eq!(
            completed
                .activity(&minted_activity_ref())
                .await
                .expect("the finish folds the completed row")
                .value
                .status,
            ActivityStatus::Completed
        );
    }

    /// With no `Output` the raw result passes through untouched — for a `Succeed` reached through
    /// `CompleteState` that is the command's own `output`, not the input it entered with.
    #[tokio::test]
    async fn complete_without_output_passes_the_result_through() {
        let result = json!({ "echo": "from the caller" });
        let completed = complete(
            &succeed_state(None),
            complete_store(seeded_input(), []).await,
            &complete_cmd(result.clone()),
        )
        .await;

        assert!(
            matches!(
                completed.chain().last(),
                Some(EntryPayload::Command(Command::CompleteThread(CompleteThread {
                    output,
                    ..
                }))) if *output == result
            ),
            "the raw result reaches the owner unprojected: {:?}",
            completed.chain().last()
        );
    }
}
