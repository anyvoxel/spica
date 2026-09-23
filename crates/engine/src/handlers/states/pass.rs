use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{PassState, State};

use super::super::emit_transition;
use super::super::state_handler::{StateHandler, StateHandlerFactory};
use crate::ActivityStatus;
use crate::handler::{Collector, HandlerContext};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::ObjectReference;

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
    // then `Output` (defaults to the input) — is the canonical ASL success projection, kept here as
    // the reference the other complete paths mirror.
    /// The `Command::CompleteState` finish — the shared orchestration (liveness/Terminating-race
    /// guards, owning-scope resolution, activity and variables reconstruction) and this state's projection,
    /// all inline.
    async fn complete(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity: ObjectReference,
        raw_result: Option<&Value>,
    ) {
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

        // Race fix: a cancel already won on this activity. The drain that would have been emitted by
        // the cancel side may have been missed because the ordering interleaved (e.g. timer-fired +
        // cancel together). Re-emit the deferred termination ed so the parent finishes, reusing the
        // reason embedded in the terminating status itself.
        if act.value.status != ActivityStatus::Running {
            match act.value.status {
                ActivityStatus::Terminating(ref reason) => {
                    // Re-emit the terminal lifecycle event using the canonical activity payload shape,
                    // preserving every previously-folded domain field while only flipping the status
                    // from `Terminating(reason)` to `Terminated(reason)`.
                    let mut activity_value = act.value();
                    activity_value.status = ActivityStatus::Terminated(reason.clone());
                    out.append_event(crate::types::event::Event::StateTerminated {
                        activity: activity_value,
                    })
                    .await;
                }
                _ => return,
            }
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
        let variables = scope.variables().clone();
        let env = &mut *ctx.env;

        // Advance the one activity value in place to the completing lifecycle moment — it stays the
        // single source of truth for the rest of the complete step, so the completing status (and its
        // re-stamped update time) carries forward instead of a stale copy held alongside.
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
        let states = States::new(
            &activity_value.raw_input,
            &activity_value.state_path.state_name(),
            activity_value.retry_count(),
        )
        .with_result(Some(&activity_value.raw_input))
        .with_assign_ctx(Some(&activity_value.raw_input))
        .build();
        let mut local_scope = variables.clone();

        let owner = activity_value
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        let assigned = self
            .apply_assign(
                out,
                env,
                &owner,
                self.state.assign.as_ref(),
                &states,
                &mut local_scope,
            )
            .await;
        fail_or!(out, Some(activity), owner.clone(), assigned);

        let output_value = fail_or!(
            out,
            Some(activity),
            owner.clone(),
            self.project_output(
                env,
                self.state.output.as_ref(),
                &states,
                &local_scope,
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
        emit_transition(
            out,
            activity_value.execution.clone(),
            activity_value
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            activity,
            &activity_value.state_path,
            &output_value,
            self.state.next.as_deref(),
            self.state.end,
        )
        .await;
    }
}
