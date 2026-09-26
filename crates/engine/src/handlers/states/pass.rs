use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{AssignObject, PassState, State};

use super::super::state_handler::{StateHandler, StateHandlerFactory};

pub struct PassStateHandlerFactory;

#[async_trait]
impl StateHandlerFactory for PassStateHandlerFactory {
    fn state(&self) -> State {
        State::Pass(PassState::default())
    }

    fn create<'a>(&self, state: &'a State) -> Box<dyn StateHandler + 'a> {
        let State::Pass(s) = state else {
            unreachable!(
                "create dispatch guarantees the factory receives its own variant; got {state:?}"
            );
        };
        Box::new(PassStateHandler { state: s })
    }
}

struct PassStateHandler<'a> {
    state: &'a PassState,
}

#[async_trait]
impl StateHandler for PassStateHandler<'_> {
    // Pass's projection — `Assign` (a delta on the execution scope, emitted as `VariablesAssigned`)
    // then `Output` (defaults to the input) — is the canonical ASL success projection and is exactly
    // the base's default `finish`: Pass supplies only its own `Assign`/`Output` sources and routing.
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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use spica_asl::PassState;

    use super::super::harness::*;
    use super::*;
    use crate::storage::Storage;
    use crate::types::command::{
        ActivateState, Command, CompleteState, TerminateExecution, TerminateState,
        TerminationReason,
    };
    use crate::types::error::{ExecutionError, RuntimeError};
    use crate::types::event::{Event, StateTransitioned, VariablesAssigned};
    use crate::{ActivityStatus, EntryPayload, ThreadStatus, Variables};

    // These exercise the base-owned `StateHandler::activate` — the Template Method a `Pass` inherits
    // rather than implements. A `Pass` has nothing to scaffold, arm or fan out, so its share of the
    // dispatch is exactly the state-agnostic chain (`StateActivating` → `StateActivated` →
    // `CompleteState`), which is what makes it the cheapest place to pin that shared orchestration.
    // The `Assign`/`Output`/`Next`/`End` accessors are the *only* per-state input and are all read one
    // step later, in `complete` — asserted here as the boundary between the two lifecycle steps.

    /// A `Pass` definition with the routing a `Pass` normally carries. Routing is not read by
    /// `activate` — it is `finish`'s input — which the tests below rely on.
    fn pass_state(next: Option<&str>) -> State {
        State::Pass(PassState {
            next: next.map(str::to_string),
            ..Default::default()
        })
    }

    /// `activate` on a live scope is the whole synchronous chain: the activity's birth
    /// (`StateActivating`), its processed input (`StateActivated`), then the inline hand-off into
    /// `CompleteState` — no state-specific step, because a `Pass` has nothing to arm and nothing to
    /// fan out.
    #[tokio::test]
    async fn activate_emits_the_synchronous_success_chain() {
        let activated = activate(
            &pass_state(Some("P2")),
            &activate_cmd(path("/States/P"), json!({"n": 1})),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;

        let birth = minted_activity(path("/States/P"), json!({"n": 1}));
        // The default `process_input` passes `raw_input` through, so the processed input is identical —
        // and the activity is otherwise the birth value re-stamped at the same instant.
        let mut processed = birth.clone();
        processed.input = Some(json!({"n": 1}));

        assert_eq!(
            activated.chain(),
            vec![
                EntryPayload::Event(Event::StateActivating { activity: birth }),
                EntryPayload::Event(Event::StateActivated {
                    activity: processed
                }),
                EntryPayload::Command(Command::CompleteState(CompleteState {
                    activity: minted_activity_ref(),
                    output: json!({"n": 1}),
                })),
            ]
        );

        // The emitted events were folded, not merely enveloped: the activity row exists as its owner's
        // child, and minting its generated name consumed the counter's first free suffix.
        let activity = minted_activity_ref();
        let row = activated
            .activity(&activity)
            .await
            .expect("StateActivating folds the activity row");
        assert_eq!(row.value.status, ActivityStatus::Running);
        assert_eq!(row.created_at, at());
        assert!(
            activated.children(&thread_ref()).await.contains(&activity),
            "StateActivating folds the owner's child edge"
        );
        assert_eq!(
            activated
                .store
                .next_generated_seq()
                .await
                .expect("the store reads"),
            1,
            "the generated name advanced the counter"
        );
    }

    /// A scope that is gone is not the state's problem: the base fails the execution — unwinding the
    /// activity it had already constructed — rather than mislabeling the miss as a flow-definition
    /// error. The failure is routed at the **execution** (not at the owning thread the command named),
    /// because a scope that cannot be read is exactly the case where the tree's own route is what the
    /// caller must fail.
    #[tokio::test]
    async fn activate_on_a_missing_scope_terminates_the_execution() {
        let activated = activate(
            &pass_state(Some("P2")),
            &activate_cmd(path("/States/P"), json!({"n": 1})),
            None,
        )
        .await;

        let reason = TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::StateNotFound(format!(
                "execution {}",
                execution_ref()
            ))),
        };
        assert_eq!(
            activated.chain(),
            vec![
                EntryPayload::Command(Command::TerminateState(TerminateState {
                    activity: minted_activity_ref(),
                    reason: reason.clone(),
                })),
                EntryPayload::Command(Command::TerminateExecution(TerminateExecution {
                    name: execution_ref().name,
                    uid: Some(execution_ref().uid),
                    reason,
                })),
            ]
        );
        // A failure emits commands only: nothing was folded, so the activity has no row.
        assert!(
            activated.activity(&minted_activity_ref()).await.is_none(),
            "a failed activate folds no activity row"
        );
    }

    /// A scope past accepting a transition (a competing terminator won the race) makes a late
    /// `activate` a no-op — no events, no commands, no fold.
    #[tokio::test]
    async fn activate_on_a_non_running_scope_is_a_no_op() {
        let activated = activate(
            &pass_state(Some("P2")),
            &activate_cmd(path("/States/P"), json!({"n": 1})),
            Some(seeded_scope(ThreadStatus::Completed)),
        )
        .await;

        assert!(activated.chain().is_empty());
        assert!(
            activated.activity(&minted_activity_ref()).await.is_none(),
            "a skipped activate folds no activity row"
        );
    }

    /// `activate` never applies the state's own projection: `Assign` and `Output` belong to
    /// `complete`/`finish`, so the processed input reaches `CompleteState` untransformed and no
    /// `VariablesAssigned` is emitted yet. A `Pass` whose `Output` would rewrite the result proves the
    /// boundary — the same definition transforms it one step later.
    #[tokio::test]
    async fn activate_defers_assign_and_output_to_the_finish() {
        let state = State::Pass(PassState {
            assign: Some(AssignObject(
                json!({ "code": "E-42" }).as_object().unwrap().clone(),
            )),
            output: Some(json!({ "echo": "{% $states.input.n %}" })),
            next: Some("P2".to_string()),
            ..Default::default()
        });
        let activated = activate(
            &state,
            &activate_cmd(path("/States/P"), json!({"n": 1})),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;

        let chain = activated.chain();
        assert_eq!(
            chain.len(),
            3,
            "the projections do not change the chain: {chain:?}"
        );
        assert!(
            !chain
                .iter()
                .any(|payload| matches!(payload, EntryPayload::Event(Event::VariablesAssigned(_)))),
            "Assign is applied by the finish, not by activate: {chain:?}"
        );
        assert!(
            matches!(&chain[2], EntryPayload::Command(Command::CompleteState(c))
                if c.output == json!({"n": 1})),
            "the input reaches CompleteState untransformed: {:?}",
            chain[2]
        );
    }

    /// `complete` applies the same `Assign`/`Output` the activation deferred: a `VariablesAssigned`
    /// delta on the owner scope, then the projected result — here `Output` reshaping the input — and
    /// finally the transition to the declared successor.
    #[tokio::test]
    async fn complete_applies_the_projection_and_routes_to_next() {
        let state = State::Pass(PassState {
            assign: Some(AssignObject(
                json!({ "code": "E-42" }).as_object().unwrap().clone(),
            )),
            output: Some(json!({ "echo": "{% $states.input.n %}" })),
            next: Some("P2".to_string()),
            ..Default::default()
        });
        let completed = complete(
            &state,
            complete_store(seeded_input(), []).await,
            &complete_cmd(seeded_input()),
        )
        .await;

        let owner = thread_ref();
        let mut completed_activity = minted_activity(path("/States/P"), seeded_input());
        completed_activity.input = Some(seeded_input());
        let mut completing = completed_activity.clone();
        completing.raw_output = Some(seeded_input());
        completing.status = ActivityStatus::Completing;
        let mut done = completing.clone();
        done.status = ActivityStatus::Completed;
        // JSONata yields every number as `f64`, so the projected `1` comes back as `1.0`.
        done.output = Some(json!({ "echo": 1.0 }));

        assert_eq!(
            completed.chain(),
            vec![
                EntryPayload::Event(Event::StateCompleting {
                    activity: completing
                }),
                EntryPayload::Event(Event::VariablesAssigned(VariablesAssigned {
                    scope: owner.clone(),
                    variables: scope_with_code(),
                })),
                EntryPayload::Event(Event::StateCompleted { activity: done }),
                EntryPayload::Event(Event::StateTransitioned(StateTransitioned {
                    activity: minted_activity_ref(),
                    next: path("/States/P2").as_ptr().to_owned(),
                })),
                EntryPayload::Command(Command::ActivateState(ActivateState {
                    execution: execution_ref(),
                    owner,
                    state_path: path("/States/P2"),
                    input: json!({ "echo": 1.0 }),
                })),
            ]
        );
    }

    /// The variables the seeded scope carries after the `Assign` above folded `code` in: the seeded
    /// thread's scope starts empty, so the assign delta is the whole snapshot.
    fn scope_with_code() -> Variables {
        let mut vars = Variables::default();
        vars.insert("code".to_string(), json!("E-42"));
        vars
    }
}
