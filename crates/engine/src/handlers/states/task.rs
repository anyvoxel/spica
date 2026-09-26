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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use spica_asl::{IntOrExpr, TaskState};

    use super::super::harness::*;
    use super::*;
    use crate::types::command::{
        ActivateState, TerminateState, TerminateThread, TerminationReason,
    };
    use crate::types::event::{Event, StateTransitioned};
    use crate::types::meta::ObjectMeta;
    use crate::{ActivityStatus, EntryPayload, ThreadStatus, Timer, TimerStatus};

    // A `Task` is the engine's external-call edge: the invocation is thrown in `after_activated` (not
    // from `process_input`), so the state's activation product is a pair of side effects — the
    // `ActivateTask` command naming the invoked entity, and the optional `TaskTimeout` timer bounding
    // it — with no inline `CompleteState`: only the task's own settle resumes the state. The tests
    // cover the two side effects, their invalidation, and the sweep the complete step performs before
    // it may finish.

    const RESOURCE: &str = "arn:aws:states:::lambda:invoke";

    /// The invoked entity's reference: named from the execution's plain base (the same convention the
    /// activity and timer names follow) off the partition counter's second free suffix, and minted
    /// from the injected generator's second id.
    fn invoked_task_ref() -> ObjectReference {
        ObjectReference::new(ObjectKind::Task, obj_name("execution-1"), uid(2))
    }

    /// The `TaskTimeout` timer `after_activated` arms: parented on the invoking activity — which is
    /// what makes it that activity's child, and so what the complete step sweeps.
    fn timeout_timer(deadline: Timestamp) -> Timer {
        Timer {
            execution: execution_ref(),
            purpose: TimerPurpose::TaskTimeout,
            status: TimerStatus::Active,
            deadline,
            meta: ObjectMeta::builder(ObjectKind::Timer, uid(3))
                .name(obj_name("execution-2"))
                .at(at())
                .build()
                .with_owner(minted_activity_ref()),
        }
    }

    fn task_state(
        arguments: Option<Value>,
        timeout_seconds: Option<i64>,
        next: Option<&str>,
    ) -> State {
        State::Task(TaskState {
            next: next.map(str::to_string),
            arguments,
            resource: RESOURCE.to_string(),
            timeout_seconds: timeout_seconds.map(IntOrExpr::Int),
            ..Default::default()
        })
    }

    /// `activate` projects `Arguments` as the task's input, throws the invocation, and arms the
    /// `TimeoutSeconds` deadline — three side effects on one activation. The task and its timer are
    /// both named off the execution's base, so a branch task still names its root run.
    #[tokio::test]
    async fn activate_invokes_the_resource_and_arms_the_timeout() {
        let deadline = at()
            .checked_add(std::time::Duration::from_secs(60))
            .expect("the fixture deadline is representable");
        let activated = activate(
            &task_state(
                Some(json!({ "x": "{% $states.input.n %}" })),
                Some(60),
                Some("P2"),
            ),
            &activate_cmd(path("/States/P"), seeded_input()),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;

        let birth = minted_activity(path("/States/P"), seeded_input());
        let mut processed = birth.clone();
        // The projected `Arguments` — not the raw input — is what the external call receives, and it
        // is what the activity carries forward as its input.
        processed.input = Some(json!({ "x": 1.0 }));

        assert_eq!(
            activated.chain(),
            vec![
                EntryPayload::Event(Event::StateActivating { activity: birth }),
                EntryPayload::Event(Event::StateActivated {
                    activity: processed
                }),
                EntryPayload::Command(Command::ActivateTask(ActivateTask {
                    execution: execution_ref(),
                    owner: minted_activity_ref(),
                    task: invoked_task_ref(),
                    resource: RESOURCE.to_string(),
                    arguments: json!({ "x": 1.0 }),
                    retry_plan: vec![],
                    deadline: Some(deadline),
                })),
                EntryPayload::Event(Event::TimerActivated {
                    timer: timeout_timer(deadline),
                }),
            ]
        );
    }

    /// Without `Arguments` the raw input is what the call receives — the projection has nothing to
    /// reshape, so the task is invoked with exactly the input the state entered with.
    #[tokio::test]
    async fn activate_without_arguments_invokes_with_the_raw_input() {
        let activated = activate(
            &task_state(None, None, Some("P2")),
            &activate_cmd(path("/States/P"), seeded_input()),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;

        assert!(
            matches!(
                activated.chain().last(),
                Some(EntryPayload::Command(Command::ActivateTask(ActivateTask {
                    arguments,
                    deadline: None,
                    ..
                }))) if *arguments == seeded_input()
            ),
            "the raw input reaches the resource: {:?}",
            activated.chain().last()
        );
        assert!(
            !activated
                .chain()
                .iter()
                .any(|p| matches!(p, EntryPayload::Event(Event::TimerActivated { .. }))),
            "a task with no TimeoutSeconds arms no bound: {:?}",
            activated.chain()
        );
    }

    /// A non-positive `TimeoutSeconds` is a definition error, and it is raised *after* the invocation
    /// was thrown: the chain still names the task the state would have invoked (carrying no deadline),
    /// and the activity then fails. Dropping the command would leave an external call unaccounted for.
    #[tokio::test]
    async fn activate_fails_the_state_on_an_invalid_timeout() {
        let activated = activate(
            &task_state(None, Some(0), Some("P2")),
            &activate_cmd(path("/States/P"), seeded_input()),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;

        let reason = TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "Task TimeoutSeconds must be a positive integer".to_string(),
            )),
        };
        let chain = activated.chain();
        assert!(
            matches!(&chain[2], EntryPayload::Command(Command::ActivateTask(t))
                if t.deadline.is_none()),
            "the invocation is still thrown, unbounded: {:?}",
            chain[2]
        );
        assert_eq!(
            &chain[3..],
            vec![
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

    /// The complete step disposes of the deadline before it finishes: the `TaskTimeout` timer only
    /// *bounds* the state, so waiting it out would be wrong — it is swept, and the sweep detaches the
    /// child so the state is free to finish in the same step.
    #[tokio::test]
    async fn complete_sweeps_the_deadline_timer_then_finishes() {
        let deadline = at()
            .checked_add(std::time::Duration::from_secs(60))
            .expect("the fixture deadline is representable");
        let state = task_state(None, Some(60), Some("P2"));
        let activated = activate(
            &state,
            &activate_cmd(path("/States/P"), seeded_input()),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;
        let Dispatch { store, .. } = activated;

        let completed = complete(&state, store, &complete_cmd(seeded_input())).await;

        let mut completing = minted_activity(path("/States/P"), seeded_input());
        completing.input = Some(seeded_input());
        completing.raw_output = Some(seeded_input());
        completing.status = ActivityStatus::Completing;
        let mut done = completing.clone();
        done.status = ActivityStatus::Completed;
        done.output = Some(seeded_input());
        let mut cancelled = timeout_timer(deadline);
        cancelled.status = TimerStatus::Cancelled;

        assert_eq!(
            completed.chain(),
            vec![
                EntryPayload::Event(Event::StateCompleting {
                    activity: completing
                }),
                EntryPayload::Event(Event::TimerCancelled { timer: cancelled }),
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
        let row = completed
            .activity(&minted_activity_ref())
            .await
            .expect("the finish folds the completed row");
        assert_eq!(row.value.status, ActivityStatus::Completed);
        assert!(
            row.active_children.is_empty(),
            "the swept timer is no longer a child: {:?}",
            row.active_children
        );
    }
}
