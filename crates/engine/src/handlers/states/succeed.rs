use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{State, SucceedState};

use super::super::state_handler::{StateHandler, StateHandlerFactory};

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
    // Succeed is terminal: it finishes itself with its evaluated output, then completes the whole
    // execution with the same output. Per the ASL spec it carries only an optional `Output` (no
    // `Assign`), so it runs the base's default `finish` and supplies just its output and the terminal
    // routing.
    fn output(&self) -> Option<&Value> {
        self.state.output.as_ref()
    }

    // TODO：终态的路由不应该走 `emit_transition` 的通用分支，而应像 Fail 一样直接完成执行。
    fn end(&self) -> Option<bool> {
        Some(true)
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
