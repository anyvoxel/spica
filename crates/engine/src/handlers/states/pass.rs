use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{PassState, State, StatePath};

use super::super::container::{Container, ThreadContainer};
use super::super::state_handler::{StateHandler, StateHandlerFactory};
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::{
    ActivateState, Command, CompleteThread, TerminateState, TerminationReason,
};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::{Event, StateTransitioned};
use crate::types::meta::HasRawObjectRef;
use crate::{Activity, RejectionType, Variables};

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

/// What the complete step's projection routed to, handed back for `after_completing` to turn into the
/// terminal and the transition — the mirror of `Choice`'s `ChoiceBranch`. A `Pass` declares exactly
/// one of `Next`/`End` (per ASL), so the two are an enum, never both present.
enum PassFinish {
    /// Hop to the resolved sibling successor, activating it with the projected output.
    Next { next: StatePath, output: Value },
    /// `End`: no successor — complete the owner thread with the projected output.
    End { output: Value },
    /// Neither `Next` nor `End` is declared: the definition is malformed, so the activity unwinds
    /// with `States.NoTerminal` and its scope is taken down.
    NoTerminal,
}

#[async_trait]
impl StateHandler for PassStateHandler<'_> {
    /// (3.4) The canonical ASL success projection runs here in one go — a `Pass` owns no child to
    /// wait on, so there is nothing to defer, mirroring `Choice`/`Fail`. A projection failure is
    /// turned into a terminate here rather than returned: the activity is already `Completing`, so
    /// there is no later hop to report it from.
    async fn after_completing(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<(), ProcessingError> {
        let finish = match self
            .process_pass(ctx.env, out, activity_value, variables)
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
            PassFinish::Next { next, output } => {
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
            PassFinish::End { output } => {
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
            PassFinish::NoTerminal => {
                let reason = TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::NoTerminal),
                };
                let owner = activity_value.meta.owner.clone();
                let Some(thread_container) = ThreadContainer::open(ctx.storage, owner).await?
                else {
                    return Err(ProcessingError::Rejected(
                        RejectionType::NotFound,
                        format!(
                            "pass_state: activity {} has no owning thread; termination refused",
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

    // A `Pass` owns no children, so its terminate closes inline: the base already opened with
    // `StateTerminating`, so this marks `Terminated`, emits the terminal, and relays the settle up to
    // the owning thread.
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

impl PassStateHandler<'_> {
    /// The `Pass` complete step: build `$states`, apply `Assign` (emitting `VariablesAssigned` on the
    /// owner scope), project `Output` (defaulting to the input), and decide the routing — the
    /// canonical success finish, mirroring `Choice`'s `process_choice`. It emits nothing about the
    /// activity itself; the terminal and the transition belong to `after_completing`.
    async fn process_pass(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<PassFinish, ExecutionError> {
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
            Ok(PassFinish::End { output })
        } else if let Some(next) = next {
            Ok(PassFinish::Next {
                next: activity_value.state_path.sibling(next),
                output,
            })
        } else {
            Ok(PassFinish::NoTerminal)
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use spica_asl::{AssignObject, PassState};

    use super::super::harness::*;
    use super::*;
    use crate::storage::Storage;
    use crate::types::command::{ActivateState, Command, CompleteState};
    use crate::types::event::{Event, StateTransitioned, VariablesAssigned};
    use crate::types::meta::HasRawObjectRef;
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
            seeded_scope(ThreadStatus::Running),
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
            activated
                .children(thread_ref().as_raw_object_ref())
                .await
                .contains(activity.as_raw_object_ref()),
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
            seeded_scope(ThreadStatus::Running),
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
