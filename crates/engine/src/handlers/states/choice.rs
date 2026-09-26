use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{AssignObject, ChoiceCondition, ChoiceState, State};

use super::super::emit_state_completed;
use super::super::state_handler::{StateHandler, StateHandlerFactory};
use crate::Activity;
use crate::eval_env::{EvalEnv, extract_jsonata};
use crate::handler::Collector;
use crate::types::command::{ActivateState, Command};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::{Event, StateTransitioned};
use crate::types::meta::ObjectReference;
use crate::types::variables::Variables;

pub struct ChoiceStateHandlerFactory;

#[async_trait]
impl StateHandlerFactory for ChoiceStateHandlerFactory {
    fn state(&self) -> State {
        State::Choice(ChoiceState::default())
    }

    fn create<'a>(&self, state: &'a State) -> Box<dyn StateHandler + 'a> {
        let State::Choice(s) = state else {
            unreachable!(
                "create dispatch guarantees the factory receives its own variant; got {state:?}"
            );
        };
        Box::new(ChoiceStateHandler { state: s })
    }
}

struct ChoiceStateHandler<'a> {
    state: &'a ChoiceState,
}

impl ChoiceStateHandler<'_> {
    /// Resolve the chosen branch: the first rule whose condition evaluates true, or the state
    /// `Default` when none matches. Returns that branch's `Assign` + `Output` + `next`; a rule with
    /// no condition never matches, and a state with neither a match nor a `Default` yields
    /// `NoChoiceMatched`. Errors propagate so the caller terminates once, instead of terminating
    /// inline on the first failing rule.
    fn resolve_choice(
        &self,
        env: &mut EvalEnv,
        states: &Value,
        variables: &Variables,
        state_name: &str,
    ) -> Result<(Option<AssignObject>, Option<Value>, String), ExecutionError> {
        let mut matched: Option<(Option<AssignObject>, Option<Value>, String)> = None;
        for rule in &self.state.choices {
            let is_match = match &rule.condition {
                Some(ChoiceCondition::Bool(b)) => *b,
                Some(ChoiceCondition::Expr(expr)) => {
                    let inner = extract_jsonata(expr.as_str()).ok_or_else(|| {
                        ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                            "Choice Condition must be a {% %} JSONata expression".to_string(),
                        ))
                    })?;
                    let value = env.eval_expr(inner, states, variables)?;
                    match value {
                        Value::Bool(b) => b,
                        _ => {
                            return Err(ExecutionError::Runtime(RuntimeError::Jsonata {
                                field: expr.as_str().to_string(),
                                message: "Condition must evaluate to a boolean".to_string(),
                            }));
                        }
                    }
                }
                None => false,
            };
            if is_match {
                matched = Some((rule.assign.clone(), rule.output.clone(), rule.next.clone()));
                break;
            }
        }
        match matched {
            Some(m) => Ok(m),
            None => match &self.state.default {
                Some(d) => Ok((None, None, d.clone())),
                None => Err(ExecutionError::Runtime(RuntimeError::NoChoiceMatched {
                    state: state_name.to_string(),
                })),
            },
        }
    }
}

#[async_trait]
impl StateHandler for ChoiceStateHandler<'_> {
    /// Routes the Choice: resolve the matching rule in the finish step, project the `Assign`/`Output`
    /// of the chosen rule (overriding the state level), and emit the transition. `$states` carries no
    /// `.result` — a Choice produces no raw result of its own. No match and no `Default` is a
    /// `NoChoiceMatched` failure, propagated for the base's terminate.
    async fn finish(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity: ObjectReference,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<(), ExecutionError> {
        let state_name = activity_value.state_path.state_name();

        // `$states` for the rule scan: `assign_ctx = Some` (matching Pass/Succeed/Fail) — however late
        // an `Assign` is applied, derived values read consistently with the variables already folded.
        let states = States::new(
            &activity_value.raw_input,
            &state_name,
            activity_value.retry_count(),
        )
        .with_assign_ctx(Some(&activity_value.raw_input))
        .build();

        // The chosen rule's `next` (or the state `Default`) drives the transition, and its
        // `Assign`/`Output` override the state level's.
        let (rule_assign, rule_output, rule_next) =
            self.resolve_choice(env, &states, variables, &state_name)?;
        let assign = rule_assign.as_ref().or(self.state.assign.as_ref());
        let output_src = rule_output.as_ref().or(self.state.output.as_ref());

        // Projection reuses the scan's `$states` — a Choice produces no raw result of its own, so the
        // pass-through fallback is the processed input.
        let mut local_scope = variables.clone();
        let owner = activity_value
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        self.apply_assign(out, env, &owner, assign, &states, &mut local_scope)
            .await?;
        let output_value = self
            .project_output(
                env,
                output_src,
                &states,
                &local_scope,
                activity_value.raw_input.clone(),
            )
            .await?;
        emit_state_completed(out, activity_value, &output_value).await;

        // Choice's `next` is mandatory, so the transition is always a sibling hop — the general
        // `emit_transition` (which also handles the `end`/`NoTerminal` cases) is bypassed.
        let next_path = activity_value.state_path.sibling(&rule_next);
        out.append_event(Event::StateTransitioned(StateTransitioned {
            activity,
            next: next_path.as_ptr().to_owned(),
        }))
        .await;
        out.append_command(Command::ActivateState(ActivateState {
            execution: activity_value.execution.clone(),
            owner: activity_value
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            state_path: next_path,
            input: output_value,
        }));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use spica_asl::{ChoiceCondition, ChoiceRule, ChoiceState};

    use super::super::harness::*;
    use super::*;
    use crate::types::command::{
        Command, CompleteState, TerminateState, TerminateThread, TerminationReason,
    };
    use crate::types::event::{StateTransitioned, VariablesAssigned};
    use crate::{ActivityStatus, EntryPayload, ThreadStatus, Variables};

    // A `Choice` carries no `Next`/`End` of its own: its whole behavior is the rule scan in the
    // finish, which picks a branch and then routes exactly like any other state. The tests below pin
    // the four outcomes of that scan — a matched rule, the `Default` fallback, the `NoChoiceMatched`
    // failure — plus the rule-level `Assign`/`Output` override.

    fn rule(condition: ChoiceCondition, next: &str) -> ChoiceRule {
        ChoiceRule {
            condition: Some(condition),
            next: next.to_string(),
            assign: None,
            output: None,
        }
    }

    fn choice_state(
        choices: Vec<ChoiceRule>,
        default: Option<&str>,
        output: Option<Value>,
    ) -> State {
        State::Choice(ChoiceState {
            comment: None,
            output,
            assign: None,
            default: default.map(str::to_string),
            choices,
        })
    }

    /// A `Choice` built the way a real one arrives — parsed from its ASL document. The condition
    /// forms that only the deserializer can produce (a `{% %}` `Condition` expression) are reachable
    /// this way and no other, since the parsed expression's constructor stays crate-private to
    /// `spica-asl`.
    fn parsed_state(document: &str) -> State {
        serde_json::from_str(document).expect("the fixture is a valid Choice definition")
    }
    /// `activate` runs the shared chain: a `Choice` decides nothing before its finish, so activation
    /// is the same synchronous hand-off every leaf state gets.
    #[tokio::test]
    async fn activate_emits_the_synchronous_success_chain() {
        let activated = activate(
            &choice_state(vec![rule(ChoiceCondition::Bool(true), "P2")], None, None),
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

    /// The matched rule names the successor, so the finish emits the transition marker (the resolved
    /// target *path*) and then the `ActivateState` that performs the hop. A `Choice` produces no
    /// result of its own, so the state's input is what flows to the successor.
    #[tokio::test]
    async fn complete_routes_to_the_matched_rule() {
        let activated = activate(
            &choice_state(vec![rule(ChoiceCondition::Bool(true), "P2")], None, None),
            &activate_cmd(path("/States/P"), seeded_input()),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;
        let Dispatch { store, .. } = activated;
        let completed = complete(
            &choice_state(vec![rule(ChoiceCondition::Bool(true), "P2")], None, None),
            store,
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

    /// A rule condition is a real evaluation, not a literal: the JSONata `Condition` is evaluated
    /// against the state's input, and the first rule that yields `true` wins.
    #[tokio::test]
    async fn complete_routes_by_the_evaluated_condition() {
        let state = parsed_state(
            r#"{
              "Type": "Choice",
              "Choices": [
                { "Condition": "{% $states.input.n = 2 %}", "Next": "P2" },
                { "Condition": "{% $states.input.n = 1 %}", "Next": "P3" }
              ]
            }"#,
        );
        let completed = complete(
            &state,
            complete_store(seeded_input(), []).await,
            &complete_cmd(seeded_input()),
        )
        .await;

        // The first rule (n = 2) fails on this input, so the *second* one decides the hop.
        assert!(
            matches!(
                completed.chain().last(),
                Some(EntryPayload::Command(Command::ActivateState(ActivateState {
                    state_path,
                    ..
                }))) if *state_path == path("/States/P3")
            ),
            "the second rule's successor is entered: {:?}",
            completed.chain().last()
        );
    }

    /// With no rule matching, `Default` is the fallback — and it takes the state-level
    /// `Assign`/`Output`, since there is no rule to override them with.
    #[tokio::test]
    async fn complete_falls_back_to_default_when_no_rule_matches() {
        let state = State::Choice(ChoiceState {
            comment: None,
            output: None,
            assign: Some(AssignObject(
                json!({ "why": "default" }).as_object().unwrap().clone(),
            )),
            default: Some("P3".to_string()),
            choices: vec![rule(ChoiceCondition::Bool(false), "P2")],
        });
        let completed = complete(
            &state,
            complete_store(seeded_input(), []).await,
            &complete_cmd(seeded_input()),
        )
        .await;

        let mut assigned = Variables::default();
        assigned.insert("why".to_string(), json!("default"));
        let chain = completed.chain();
        assert!(
            matches!(&chain[1], EntryPayload::Event(Event::VariablesAssigned(VariablesAssigned {
                variables, ..
            })) if *variables == assigned),
            "the state-level Assign applies on the fallback: {:?}",
            chain[1]
        );
        assert!(
            matches!(
                chain.last(),
                Some(EntryPayload::Command(Command::ActivateState(ActivateState {
                    state_path,
                    ..
                }))) if *state_path == path("/States/P3")
            ),
            "the default successor is entered: {:?}",
            chain.last()
        );
    }

    /// A `Choice` rule may carry its own `Assign`/`Output`, which override the state level's — the
    /// chosen branch reshapes the variables and the result, not the enclosing state.
    #[tokio::test]
    async fn complete_prefers_the_matched_rules_assign_and_output() {
        let state = State::Choice(ChoiceState {
            comment: None,
            output: Some(json!({ "level": "state" })),
            assign: Some(AssignObject(
                json!({ "level": "state" }).as_object().unwrap().clone(),
            )),
            default: None,
            choices: vec![ChoiceRule {
                condition: Some(ChoiceCondition::Bool(true)),
                next: "P2".to_string(),
                assign: Some(AssignObject(
                    json!({ "level": "rule" }).as_object().unwrap().clone(),
                )),
                output: Some(json!({ "level": "rule" })),
            }],
        });
        let completed = complete(
            &state,
            complete_store(seeded_input(), []).await,
            &complete_cmd(seeded_input()),
        )
        .await;

        let mut assigned = Variables::default();
        assigned.insert("level".to_string(), json!("rule"));
        let chain = completed.chain();
        assert!(
            matches!(&chain[1], EntryPayload::Event(Event::VariablesAssigned(VariablesAssigned {
                variables, ..
            })) if *variables == assigned),
            "the rule's Assign wins over the state's: {:?}",
            chain[1]
        );
        assert!(
            matches!(
                chain.last(),
                Some(EntryPayload::Command(Command::ActivateState(ActivateState {
                    input, ..
                }))) if *input == json!({ "level": "rule" })
            ),
            "the rule's Output wins over the state's: {:?}",
            chain.last()
        );
    }

    /// No rule matched and no `Default` is declared: the state cannot route, so it fails the run —
    /// the activity's completion is unwound and the owning scope terminated with
    /// `States.NoChoiceMatched`.
    #[tokio::test]
    async fn complete_terminates_when_no_rule_matches_and_no_default() {
        let state = choice_state(vec![rule(ChoiceCondition::Bool(false), "P2")], None, None);
        let completed = complete(
            &state,
            complete_store(seeded_input(), []).await,
            &complete_cmd(seeded_input()),
        )
        .await;

        let reason = TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::NoChoiceMatched {
                state: "P".to_string(),
            }),
        };
        assert_eq!(
            completed.chain(),
            vec![
                EntryPayload::Event(Event::StateCompleting {
                    activity: {
                        let mut completing = minted_activity(path("/States/P"), seeded_input());
                        completing.input = Some(seeded_input());
                        completing.raw_output = Some(seeded_input());
                        completing.status = ActivityStatus::Completing;
                        completing
                    }
                }),
                EntryPayload::Command(Command::TerminateState(TerminateState {
                    activity: minted_activity_ref(),
                    reason: reason.clone(),
                })),
                EntryPayload::Command(Command::TerminateThread(TerminateThread {
                    thread: thread_ref(),
                    reason,
                })),
            ]
        );
    }
}
