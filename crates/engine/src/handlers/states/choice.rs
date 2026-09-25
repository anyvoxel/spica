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
