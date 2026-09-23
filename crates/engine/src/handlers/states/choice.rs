use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{AssignObject, ChoiceCondition, ChoiceState, State};

use super::super::state_handler::{StateHandler, StateHandlerFactory};
use crate::ActivityStatus;
use crate::RejectionType;
use crate::eval_env::{EvalEnv, extract_jsonata};
use crate::handler::{Collector, HandlerContext};
use crate::types::command::{ActivateState, Command};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::{Event, StateTransitioned};
use crate::types::id::RequestId;
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
    /// Routes the Choice: scan rules in the complete step, evaluate the matching condition, project
    /// the `Assign`/`Output` of the chosen rule (overriding the state level), and emit the
    /// transition. `Assign`/`Output` from the chosen rule override the state level, and the
    /// rule-provided `next` (or `Default`) drives the transition. No `next` and no `Default` is a
    /// `NoChoiceMatched` failure.
    async fn complete(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity: ObjectReference,
        raw_result: Option<&Value>,
    ) {
        // TODO：拿到 activity 之后，如果遇到错误不应该是直接 terminate（例如有些是临时性质的错误，应该重试）
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

        // A late `CompleteState` on an activity that is no longer Running cannot apply — reject it
        // (durably, with the nil id an internal command carries) rather than returning a silent
        // no-op, so every command still yields a followup entry. The old re-emit of `StateTerminated`
        // here was wrong: the cancel side always emits that event itself, and re-emitting it from the
        // complete path would duplicate a terminal event.
        if act.value.status != ActivityStatus::Running {
            out.reject(
                RequestId::nil(),
                RejectionType::InvalidState,
                format!(
                    "activity {activity} is {:?}, not Running; cannot complete",
                    act.value.status
                ),
            );
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
        // Advance the one activity value in place to the completing lifecycle moment — it stays the
        // single source of truth for the rest of the complete step, so the completing status (and its
        // re-stamped update time) carries forward instead of a stale copy being held alongside.
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
        let variables = scope.variables().clone();
        let env = &mut *ctx.env;

        // `$states` for the complete step: `assign_ctx = Some` (matching Pass/Succeed/Fail) —
        // however late an `Assign` is applied, derived values read consistently with the variables
        // already folded.
        let states = States::new(
            &activity_value.raw_input,
            &activity_value.state_path.state_name(),
            activity_value.retry_count(),
        )
        .with_assign_ctx(Some(&activity_value.raw_input))
        .build();

        // The chosen rule's `next` (or the state `Default`) drives the transition; no match and no
        // `Default` is a definitive `NoChoiceMatched` failure.
        let (rule_assign, rule_output, rule_next) = match self.resolve_choice(
            env,
            &states,
            &variables,
            &activity_value.state_path.state_name(),
        ) {
            Ok(res) => res,
            Err(e) => {
                out.terminate(
                    Some(activity.clone()),
                    activity_value
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    e,
                );
                return;
            }
        };

        let assign = rule_assign.as_ref().or(self.state.assign.as_ref());
        let output_src = rule_output.as_ref().or(self.state.output.as_ref());

        // Projection uses the complete-step `$states` plus any `Assign` effect folded in.
        let mut local_variables = variables.clone();
        let owner = activity_value
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        let assigned = self
            .apply_assign(out, env, &owner, assign, &states, &mut local_variables)
            .await;
        fail_or!(out, Some(activity), owner.clone(), assigned);

        let output_value = fail_or!(
            out,
            Some(activity),
            owner.clone(),
            self.project_output(
                env,
                output_src,
                &states,
                &local_variables,
                activity_value.raw_input.clone(),
            )
            .await
        );

        // Advance the same value in place to the completed lifecycle moment (mirroring the completing
        // step above): it stays the single source of truth, so the completed status and projected
        // output carry forward into the transition that follows.
        activity_value
            .meta
            .with_update_at(crate::log::Timestamp::now());
        activity_value.status = ActivityStatus::Completed;
        activity_value.output = Some(output_value.clone());
        if activity_value.raw_output.is_none() {
            activity_value.raw_output = Some(activity_value.raw_input.clone());
        }
        out.append_event(Event::StateCompleted {
            activity: activity_value.clone(),
        })
        .await;

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
            input: output_value.clone(),
        }));
    }
}
