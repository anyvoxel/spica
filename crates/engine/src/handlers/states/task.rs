use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{AssignObject, State, TaskState};

use super::super::state_handler::{FinishReadiness, StateHandler, StateHandlerFactory};
use super::super::{cancel_activity_timers, emit_timer, eval_string_or_expr};
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext};
use crate::log::Timestamp;
use crate::types::command::{ActivateTask, Command, TimerPurpose};
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::meta::{ObjectKind, ObjectReference};
use crate::{Activity, Variables};

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
        _now: Timestamp,
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

        // Resolve the state's `TimeoutSeconds` (if any) into an absolute deadline **before** the task is
        // minted, so one computation feeds both carriers: the task's own `deadline` (via the command)
        // and the `TaskTimeout` timer armed below.
        //
        // An invalid value is a definition error, but it is raised *after* the command: the chain still
        // names the task the state would have invoked (carrying no deadline, as it did before the task
        // had the field) and the activity then fails through the base hook.
        let (deadline, timeout_error) = match self.resolve_task_deadline(
            env,
            variables,
            states,
            self.state.timeout_seconds.as_ref(),
            out.now(),
        ) {
            Ok(deadline) => (deadline, None),
            Err(e) => (None, Some(e)),
        };

        let activity = activity_value.reference();
        let task_uid = out.mint();
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
            deadline,
        }));

        // A task with no `TimeoutSeconds` is left to the external handler to settle — and so is one
        // whose invalid `TimeoutSeconds` failed the resolution above.
        if let Some(e) = timeout_error {
            return Err(e);
        }
        // Arm the Task's `TimeoutSeconds` deadline (a `TaskTimeout` timer parented on the activity,
        // so it is swept when the activity terminates). On firing it fails the in-flight task with
        // `States.Timeout`, then flows through the same `Retry`/`Catch` policy as any settle.
        if let Some(deadline) = deadline {
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

    // A `Task` owns two kinds of child and disposes of them differently. The `TimeoutSeconds` timer
    // only *bounds* this state: it is moot once the task has settled, and waiting out a deadline that
    // may be minutes away would be wrong, so it is swept here.
    // TODO：不应该使用这个函数，而是应该对所有的 children，如果都还是在 Running 状态则发送一个 Terminate 命令（不能是 Complete 命令，因为只有对象直接结束才会是 Complete）
    // The in-flight task is the blocking child — its settle *is* this state's completion trigger — so
    // what remains after the sweep is left to the generic child-count answer.
    async fn on_completing(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity: &ObjectReference,
        _activity_value: &Activity,
    ) -> FinishReadiness {
        cancel_activity_timers(ctx, out, activity.clone()).await;
        match self.live_children(ctx, activity).await {
            Some(0) => FinishReadiness::Ready,
            Some(pending) => FinishReadiness::Waiting { pending },
            None => FinishReadiness::Gone,
        }
    }

    // Mirrors `Wait`: the settled task's payload arrives as the `CompleteState` output, so the base's
    // default `finish` is this state's projection — only the routing is its own.
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

impl TaskStateHandler<'_> {
    /// Resolve a Task state's `TimeoutSeconds` (`Int` or JSONata `Expr`) into an absolute deadline,
    /// measured from the caller's `now` (the injected clock's reading); `None` when the state sets no
    /// timeout. An invalid/out-of-range value is a definition error that terminates the activity via
    /// the base hook.
    fn resolve_task_deadline(
        &self,
        env: &mut EvalEnv,
        variables: &Variables,
        states: &Value,
        timeout: Option<&spica_asl::IntOrExpr>,
        now: Timestamp,
    ) -> Result<Option<Timestamp>, ExecutionError> {
        let Some(timeout) = timeout else {
            return Ok(None);
        };
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
        now.checked_add(std::time::Duration::from_secs(seconds as u64))
            .map(Some)
            .ok_or_else(|| {
                ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                    "Task TimeoutSeconds overflows the absolute deadline".to_string(),
                ))
            })
    }
}
