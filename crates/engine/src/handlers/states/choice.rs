use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{AssignObject, ChoiceCondition, ChoiceState, State};

use super::super::state_handler::StateHandler;
use super::super::{emit_transition, state_activated_value, state_completed_value};
use crate::eval_env::{EvalEnv, extract_jsonata};
use crate::handler::{ActivityCtx, Collector};
use crate::types::context::build_states;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::ObjectReference;

pub struct ChoiceStateHandler;

#[allow(clippy::too_many_arguments)]
#[async_trait]
impl StateHandler for ChoiceStateHandler {
    fn state(&self) -> State {
        State::Choice(ChoiceState::default())
    }

    async fn activate(
        &self,
        _env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity: ObjectReference,
        actx: &ActivityCtx,
        _state: &State,
    ) {
        // Choice is fully synchronous — same shape as Pass/Succeed/Fail: no side effect to arm, so
        // activate is a thin step that moves straight on to `CompleteState`. The routing (scanning
        // rules / evaluating conditions) and the Assign/Output projection happen in `complete`,
        // mirroring how the other synchronous states defer their projection to `complete`. Choice
        // therefore routes through the framework's uniform complete step (`CompleteState` →
        // `StateCompleting` → `complete`), like every other M1 state.
        out.emit_event(crate::types::event::Event::StateActivated {
            activity: state_activated_value(actx, actx.activity.raw_input.clone(), None),
        })
        .await;
        out.emit_command(crate::types::command::Command::CompleteState {
            activity,
            // A Choice's raw result is its processed input (no distinct raw output); the chosen
            // branch is resolved in the complete step's routing.
            output: actx.activity.raw_input.clone(),
        });
    }

    async fn complete(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity: ObjectReference,
        actx: &ActivityCtx,
        state: &State,
    ) {
        // A Choice state is always completed via `CompleteState` with the `Choice` variant — the
        // dispatch table matches this handler only to `State::Choice`, so any other variant is a
        // programming error (a table/dispatch mismatch), not a runtime condition.
        let State::Choice(s) = state else {
            unreachable!(
                "complete dispatch guarantees the state handler receives its own variant; got {state:?}"
            );
        };
        complete_choice(env, out, activity, actx, s).await;
    }
}

/// Routes the Choice: scan rules in the complete step, evaluate the matching condition, project the
/// `Assign`/`Output` of the chosen rule (overriding the state level), and emit the transition.
/// `Assign`/`Output` from the chosen rule override the state level, and the rule-provided `next` (or
/// `Default`) drives the transition. No `next` and no `Default` is a `NoChoiceMatched` failure.
#[allow(clippy::too_many_arguments)]
async fn complete_choice(
    env: &mut EvalEnv,
    out: &mut Collector<'_>,
    activity: ObjectReference,
    actx: &ActivityCtx,
    state: &ChoiceState,
) {
    let variables = actx.variables.clone();
    // `$states` for the complete step: `assign_ctx = Some` (matching Pass/Succeed/Fail) — however
    // late an `Assign` is applied, derived values read consistently with the variables already folded.
    let states = build_states(
        &actx.activity.raw_input,
        None,
        &actx.state_name(),
        &actx.exec_input,
        Some(&actx.activity.raw_input),
        actx.activity.retry_count(),
        None, // no Catch `errorOutput` in the choice path
        None, // not a Map item — no `context.Map.Item` binding
    );

    let mut matched: Option<(Option<AssignObject>, Option<Value>, String)> = None;
    for rule in &state.choices {
        let is_match = match &rule.condition {
            Some(ChoiceCondition::Bool(b)) => *b,
            Some(ChoiceCondition::Expr(expr)) => {
                let inner = fail_or!(
                    out,
                    Some(activity),
                    actx.activity
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    extract_jsonata(expr.as_str()).ok_or_else(|| {
                        ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                            "Choice Condition must be a {% %} JSONata expression".to_string(),
                        ))
                    })
                );
                let value = fail_or!(
                    out,
                    Some(activity),
                    actx.activity
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    env.eval_expr(inner, &states, &variables)
                );
                match value {
                    Value::Bool(b) => b,
                    _ => {
                        out.terminate(
                            Some(activity),
                            actx.activity
                                .meta
                                .owner
                                .clone()
                                .expect("an owned activity has an owner"),
                            ExecutionError::Runtime(RuntimeError::Jsonata {
                                field: expr.as_str().to_string(),
                                message: "Condition must evaluate to a boolean".to_string(),
                            }),
                        );
                        return;
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

    let (rule_assign, rule_output, rule_next) = match matched {
        Some(m) => m,
        None => match &state.default {
            Some(d) => (None, None, d.clone()),
            None => {
                out.terminate(
                    Some(activity),
                    actx.activity
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    ExecutionError::Runtime(RuntimeError::NoChoiceMatched {
                        state: actx.state_name(),
                    }),
                );
                return;
            }
        },
    };

    let assign = rule_assign.as_ref().or(state.assign.as_ref());
    let output_src = rule_output.as_ref().or(state.output.as_ref());

    // Projection uses the complete-step `$states` plus any `Assign` effect folded in.
    let mut local_variables = variables.clone();
    if let Some(assign_obj) = assign {
        let assign_value = Value::Object(assign_obj.0.clone());
        let evaluated = fail_or!(
            out,
            Some(activity),
            actx.activity
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            env.eval_json(&assign_value, &states, &local_variables)
        );
        match evaluated {
            Value::Object(map) => {
                if !map.is_empty() {
                    for (k, v) in map {
                        local_variables.insert(k, v);
                    }
                    out.emit_event(Event::VariablesAssigned {
                        scope: actx
                            .activity
                            .meta
                            .owner
                            .clone()
                            .expect("an owned activity has an owner"),
                        variables: local_variables.clone(),
                    })
                    .await;
                }
            }
            _ => {
                out.terminate(
                    Some(activity),
                    actx.activity
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Assign must evaluate to a JSON object".to_string(),
                    )),
                );
                return;
            }
        }
    }

    let output_value = match output_src {
        Some(o) => fail_or!(
            out,
            Some(activity),
            actx.activity
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            env.eval_json(o, &states, &local_variables)
        ),
        None => actx.activity.raw_input.clone(),
    };

    // The routing + projection is done: emit the state's success ed (`StateCompleting` was already
    // emitted by the `CompleteStateHandler` framework when this complete step opened, mirroring the
    // other synchronous states) and throw the transition. Always a State→State hop (`next` is
    // mandatory for Choice via rule/Default), never a terminal `End`.
    out.emit_event(Event::StateCompleted {
        activity: state_completed_value(actx, output_value.clone()),
    })
    .await;
    emit_transition(
        out,
        actx.activity.execution.clone(),
        actx.activity
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner"),
        activity,
        actx.state_path(),
        &output_value,
        Some(&rule_next),
        None,
    )
    .await;
}
