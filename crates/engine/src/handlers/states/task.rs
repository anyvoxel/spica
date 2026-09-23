use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{State, TaskState};

use super::super::state_handler::{StateHandler, StateHandlerFactory};
use super::super::{emit_timer, emit_transition, eval_string_or_expr};
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext};
use crate::log::Timestamp;
use crate::types::command::{ActivateTask, Command, TimerPurpose};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectReference};
use crate::{Activity, ActivityStatus, Variables};

pub struct TaskStateHandlerFactory;

#[async_trait]
impl StateHandlerFactory for TaskStateHandlerFactory {
    fn state(&self) -> State {
        State::Task(TaskState::default())
    }

    fn create<'a>(&self, state: &'a State) -> Box<dyn StateHandler + 'a> {
        let State::Task(s) = state else {
            unreachable!(
                "create dispatch guarantees the factory receives its own variant; got {state:?}"
            );
        };
        Box::new(TaskStateHandler { state: s })
    }
}

struct TaskStateHandler<'a> {
    state: &'a TaskState,
}

#[async_trait]
impl StateHandler for TaskStateHandler<'_> {
    // A Task's processed input is its projected `Arguments` (defaults to the state's input), which
    // is what the external call receives.
    async fn process_input(
        &self,
        env: &mut EvalEnv,
        activity: &mut Activity,
        variables: &Variables,
        states: &Value,
    ) -> Result<Value, ExecutionError> {
        match &self.state.arguments {
            Some(arguments) => env.eval_json(arguments, states, variables),
            None => Ok(activity.raw_input.clone()),
        }
    }

    // After `StateActivated` (carrying the projected arguments), throw the invocation: mint the
    // task entity and emit `ActivateTask`, then arm the `TimeoutSeconds` deadline if present.
    async fn after_activated(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        variables: &Variables,
        states: &Value,
    ) -> Result<(), ExecutionError> {
        // Resolve the state's `Retry` array once into a frozen per-retrier plan baked onto the task,
        // so the reused task decides its own retries without revisiting this definition. Empty = no
        // retry. TODO(M2): on a retried attempt, re-arm `TaskTimeout` so it is also bounded.
        let retry_plan = self
            .state
            .retry
            .as_deref()
            .map(|retriers| retriers.iter().map(crate::RetryPolicy::resolve).collect())
            .unwrap_or_default();

        let activity = activity_value.reference();
        let task_uid = ulid::Ulid::new();
        // The task's reference is minted with a name derived from the owning execution's plain base
        // (finding #13), exactly like the activity (#3) and timer (#11) names — not the opaque
        // `child-<uid>` handle. `execution` is the tree anchor, so a branch task still names its
        // root run.
        let task_name = activity_value
            .execution
            .name
            .base()
            .generated_from_key(out.next_generated_seq().await);
        let task_ref = ObjectReference::new(ObjectKind::Task, task_name, task_uid);
        out.append_command(Command::ActivateTask(ActivateTask {
            execution: activity_value.execution.clone(),
            owner: activity.clone(),
            task: task_ref,
            resource: self.state.resource.clone(),
            arguments: activity_value.input.clone().unwrap_or_default(),
            retry_plan,
        }));

        // Arm the Task's `TimeoutSeconds` deadline (a `TaskTimeout` timer parented on the activity,
        // so it is swept when the activity terminates). On firing it fails the in-flight task with
        // `States.Timeout`, then flows through the same `Retry`/`Catch` policy as any settle. A Task
        // with no `TimeoutSeconds` is left to the external handler to settle; an invalid value is a
        // definition error that terminates the activity via the base hook.
        if let Some(timeout) = &self.state.timeout_seconds {
            let deadline = self.resolve_task_deadline(env, variables, states, timeout)?;
            emit_timer(
                out,
                // The timer's `execution` anchor is the flat top-level run (`activity.execution`),
                // not the immediate owner scope — see `emit_timer`.
                activity_value.execution.clone(),
                activity,
                TimerPurpose::TaskTimeout,
                deadline,
            )
            .await;
        }
        Ok(())
    }

    fn complete_directly(&self, _activity: &Activity) -> bool {
        false
    }

    /// Resumed by `CompleteTask`'s `CompleteState` after the external call settles with `Ok` — the
    /// shared success finish, identical to `Wait`'s `complete`.
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
            // A synchronous state that owns no children drains its owner Execution as soon as its own
            // terminal lands; run the inline reaction so the owner's own drain walks up.
            let owner = act
                .value
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner");
            if owner.kind == ObjectKind::Activity {
                super::super::child_completed::child_settled(ctx, out, owner, activity.clone())
                    .await;
            }
            return;
        }
        // Defensive: an activity with live children cannot enter success yet; its ed is deferred
        // until drain.
        if !act.active_children.is_empty() {
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
        let raw_result = activity_value
            .raw_output
            .as_ref()
            .unwrap_or(&activity_value.raw_input);
        let states = States::new(
            &activity_value.raw_input,
            &activity_value.state_path.state_name(),
            activity_value.retry_count(),
        )
        .with_result(Some(raw_result))
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
                raw_result.clone(),
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

impl TaskStateHandler<'_> {
    /// Resolve a Task state's `TimeoutSeconds` (`Int` or JSONata `Expr`) into an absolute deadline. An
    /// invalid/out-of-range value is a definition error that terminates the activity via the base hook.
    fn resolve_task_deadline(
        &self,
        env: &mut EvalEnv,
        variables: &Variables,
        states: &Value,
        timeout: &spica_asl::IntOrExpr,
    ) -> Result<Timestamp, ExecutionError> {
        let seconds = match timeout {
            spica_asl::IntOrExpr::Int(n) if *n > 0 => Some(*n),
            spica_asl::IntOrExpr::Expr(expr) => {
                // `jsonata-core` yields every number as `f64`, so accept any unit-fraction non-negative
                // value.
                let value = eval_string_or_expr(env, expr.as_str(), states, variables)?;
                match value {
                    Value::Number(num) => num.as_f64().and_then(|f| {
                        if f.fract() == 0.0 && f.is_finite() && f >= 0.0 {
                            Some(f as i64)
                        } else {
                            None
                        }
                    }),
                    _ => None,
                }
            }
            // A literal below/equal 0 (or an expression producing 0 / non-integer) is invalid.
            _ => None,
        };
        let Some(seconds) = seconds.filter(|n| *n > 0) else {
            return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "Task TimeoutSeconds must be a positive integer".to_string(),
            )));
        };
        Timestamp::now()
            .checked_add(std::time::Duration::from_secs(seconds as u64))
            .ok_or_else(|| {
                ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                    "Task TimeoutSeconds overflows the absolute deadline".to_string(),
                ))
            })
    }
}
