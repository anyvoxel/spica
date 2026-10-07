use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{State, StatePath, TaskState};

use super::super::container::{Container, ThreadContainer};
use super::super::state_handler::{FailureRouting, StateHandler, StateHandlerFactory};
use super::super::{emit_timer, eval_string_or_expr};
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::log::Timestamp;
use crate::types::activity::ActivityKind;
use crate::types::command::{
    ActivateState, ActivateTask, Command, CompleteState, CompleteThread, TerminateState,
    TerminationReason,
};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::{Event, StateTransitioned, VariablesAssigned};
use crate::types::meta::{HasRawObjectRef, ObjectKind, ObjectRef, RawObjectRef};
use crate::types::task::TaskKind;
use crate::types::timer::{TimerKind, TimerStatus};
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

/// What the complete step's projection routed to, handed back for `after_completing` to turn into the
/// terminal and the transition — the mirror of `Pass`'s `PassFinish`. A `Task` declares exactly one of
/// `Next`/`End` (per ASL), so the two are an enum, never both present.
enum TaskFinish {
    /// Hop to the resolved sibling successor, activating it with the projected output.
    Next { next: StatePath, output: Value },
    /// `End`: no successor — complete the owner thread with the projected output.
    End { output: Value },
    /// Neither `Next` nor `End` is declared: the definition is malformed, so the activity unwinds
    /// with `States.NoTerminal` and its scope is taken down.
    NoTerminal,
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
        // retry. TODO(M2): arm a fresh `TimeoutSeconds` timer per attempt — the single deadline armed
        // at activation (below) bounds the state *across* its retries, so a retry that outlives the
        // whole budget is failed by it rather than getting a window of its own.
        let retry_plan = self
            .state
            .retry
            .as_deref()
            .map(|retriers| retriers.iter().map(crate::RetryPolicy::resolve).collect())
            .unwrap_or_default();

        // Resolve the state's `TimeoutSeconds` (if any) into an absolute deadline **before** the task is
        // minted, so one computation feeds both carriers: the task's own `deadline` (via the command)
        // and the `TimeoutSeconds` timer armed below.
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

        let activity = activity_value.meta.object_ref();
        let task_uid = out.mint();
        // The task's reference is minted with a name derived from the owning execution's plain base
        // (finding #13), exactly like the activity (#3) and timer (#11) names — not the opaque
        // `child-<uid>` handle. `execution` is the tree anchor, so a branch task still names its
        // root run.
        let task_name = activity_value
            .execution
            .name()
            .base()
            .generated_from_key(out.next_generated_seq().await);
        let task_ref = ObjectRef::<TaskKind>::new(task_name, task_uid);
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
        // Arm the Task's `TimeoutSeconds` deadline (a timer parented on the activity, so it is swept
        // when the activity terminates). Its fire terminates the state that armed it — the deadline
        // bounds the state, so the failure is the state's, and the in-flight task goes down with the
        // terminate that follows rather than being reached for here.
        if let Some(deadline) = deadline {
            emit_timer(
                out,
                // The timer's `execution` anchor is the flat top-level run (`activity.execution`),
                // not the immediate owner scope — see `emit_timer`.
                activity_value.execution.clone(),
                activity_value,
                deadline,
            )
            .await;
        }
        Ok(())
    }

    fn complete_directly(&self, _activity: &Activity) -> bool {
        false
    }

    /// A child of this state reached a terminal, and *which* child it was is the whole decision — this
    /// state arms two kinds and they mean opposite things.
    ///
    /// The **in-flight task** settling is the completion trigger: its payload is the `raw_output` the
    /// `TaskCompleted` applier folded onto the row in this same batch, and the base `finish` reads it
    /// via `$states.result`.
    ///
    /// The **`TimeoutSeconds` timer** firing is instead this state's own failure — the deadline bounds
    /// the state, so its elapsing terminates the state `TimedOut` and the terminate path owns
    /// everything past that (the in-flight task is disposed of by that teardown, or by the catch exit's
    /// sweep when a catcher takes the failure). Two guards keep that reading honest, both of which the
    /// fire itself would otherwise get wrong:
    ///
    /// - Only a *fire* counts. A cancel reaches this hook too — `ActivityContainer` routes both
    ///   outcomes through `child_completed`, so a `Parallel`/`Map` sees a failed branch — so the timer's
    ///   own recorded status is the discriminator, and a swept deadline fails nothing.
    /// - Only while the attempt is still in flight. The scheduler's fire is decoupled from the dispatch
    ///   loop, so a deadline can land in the window between the task's own settle and the `CompleteState`
    ///   that follows it; by then the timer has stopped bounding anything. A `Task` state's `raw_output`
    ///   is written by nothing but its task's settle, so its absence *is* "the attempt has not landed".
    async fn child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity: ObjectRef<ActivityKind>,
        activity_value: &Activity,
        _variables: &Variables,
        child: RawObjectRef,
    ) {
        if child.kind == ObjectKind::Timer {
            if activity_value.raw_output.is_some() {
                return; // the attempt already landed; the deadline stopped bounding at that moment.
            }
            // The deadline the fire names, read off the timer's own row: `Completed` is a fire, so a
            // timer a sweep cancelled (or one the read cannot account for) fails nothing.
            let deadline = match ctx
                .storage
                .get_timer(&child.clone().typed::<TimerKind>())
                .await
            {
                Ok(Some(t)) if t.value.status == TimerStatus::Completed => t.value.deadline,
                _ => return,
            };
            out.append_command(Command::TerminateState(TerminateState {
                activity,
                reason: TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::TimedOut {
                        message: format!(
                            "task ran past its TimeoutSeconds deadline ({})",
                            deadline.as_millis()
                        ),
                    }),
                },
            }));
            return;
        }
        if child.kind != ObjectKind::Task {
            return;
        }
        out.append_command(Command::CompleteState(CompleteState {
            activity,
            output: activity_value
                .raw_output
                .clone()
                .unwrap_or(serde_json::Value::Null),
        }));
    }

    /// A `Task`'s complete step runs here in one go, mirroring `Pass`/`Succeed`/`Wait`: the task's
    /// own settle drained the task child in the batch that produced this `CompleteState`, so the only
    /// child left is the `TimeoutSeconds` bound — which stopped applying the moment the task landed.
    /// It is swept first so no live timer child outlives the finish. A projection failure is turned
    /// into a terminate here rather than returned: the activity is already `Completing`, so there is
    /// no later hop to report it from.
    async fn after_completing(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<(), ProcessingError> {
        // Sweep the `TimeoutSeconds` deadline: it only bounds the state, so a finished Task must leave
        // no live timer child behind. The child refs come off the edge query rather than a re-read of
        // the activity value we already hold; a fired/cancelled timer is no longer a live child, and
        // `mark_cancelled` returns `Err` for it, so the sweep keeps no second guard in step with that.
        if let Ok(children) = ctx
            .storage
            .get_children(activity_value.meta.object_ref().into_raw_object_ref())
            .await
        {
            for child in children {
                if child.kind != ObjectKind::Timer {
                    continue; // only the timer child is swept here; the task child follows its own settle.
                }
                let Some(t) = ctx
                    .storage
                    .get_timer(&child.clone().typed::<TimerKind>())
                    .await
                    .ok()
                    .flatten()
                else {
                    continue;
                };
                let mut timer_value = t.value;
                if timer_value.mark_cancelled(out.now()).is_err() {
                    continue;
                }
                out.append_event(Event::TimerCancelled { timer: timer_value })
                    .await;
            }
        }
        let finish = match self
            .process_task(ctx.env, out, activity_value, variables)
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
            TaskFinish::Next { next, output } => {
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
            TaskFinish::End { output } => {
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
            TaskFinish::NoTerminal => {
                let reason = TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::NoTerminal),
                };
                let owner = activity_value.meta.owner.clone();
                let thread_container = ThreadContainer::open(ctx.storage, owner).await?;
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
                    .await?;
            }
        }
        Ok(())
    }

    /// A `Task`'s own terminate: the activity is going down, so its deadline timer and in-flight call
    /// are swept — the physical call is left running, but the `Task` child is cancelled so it drains
    /// and this state is not left waiting on it. A `Task` with nothing left closes inline; the base
    /// already opened `StateTerminating`, so the terminal is `StateTerminated` + the settle relay.
    async fn after_terminating(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        _variables: &Variables,
    ) -> Result<(), ProcessingError> {
        let mut pending = 0;
        if let Ok(children) = ctx
            .storage
            .get_children(activity_value.meta.object_ref().into_raw_object_ref())
            .await
        {
            for child in children {
                match child.kind {
                    ObjectKind::Timer => {
                        out.append_command(Command::CancelTimer {
                            timer: child.typed::<TimerKind>(),
                        });
                        pending += 1;
                    }
                    ObjectKind::Task => {
                        out.append_command(Command::CancelTask {
                            task: child.typed::<TaskKind>(),
                        });
                        pending += 1;
                    }
                    _ => {}
                }
            }
        }
        if pending == 0 {
            let container =
                ThreadContainer::open(ctx.storage, activity_value.meta.owner.clone()).await?;
            let mut terminated = activity_value.clone();
            debug_assert!(
                terminated.mark_terminated(out.now()).is_ok(),
                "the terminate step that dispatched this after_terminating opened the activity as Terminating"
            );
            let activity_ref = terminated.meta.object_ref().into_raw_object_ref();
            out.append_event(Event::StateTerminated {
                activity: terminated,
            })
            .await;
            container
                .after_child_terminated(ctx, out, &activity_ref)
                .await?;
        } else {
            tracing::debug!(
                activity = %activity_value.meta.object_ref(),
                pending,
                "task state terminating deferred: waiting on the call and deadline"
            );
        }
        Ok(())
    }

    /// A `Task` that failed terminally — retry exhausted, or no retrier matched — is routed by this
    /// state's `Catch`: the first catcher whose `ErrorEquals` matches binds `$states.errorOutput` and
    /// sends the activity along its `Next` as a *successful* finish, so the state proceeds instead of
    /// failing. Nothing matching leaves the failure to the teardown that asked.
    ///
    /// Retry is deliberately not consulted here — the reused task already decided it, on the failure
    /// path that produced this error.
    ///
    /// The routed finish is written out here rather than shared with the normal success tail, because
    /// the two differ at the one point that matters: this exit never opened `StateCompleting`
    /// (`terminate` asks the failure policy *before* it touches the row), so the terminal it lands is
    /// the whole of the finish instead of the end of a complete step.
    async fn on_failed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        error: &ExecutionError,
    ) -> FailureRouting {
        let matched = self.state.catch.as_deref().and_then(|catchers| {
            catchers
                .iter()
                .find(|c| error.matches_error_names(&c.error_equals))
        });
        let Some(catcher) = matched else {
            return FailureRouting::Uncaught;
        };
        // The owning thread is the catcher's scope: its variables are what the catcher's
        // `Assign`/`Output` evaluate against. A thread row that is gone leaves the failure with no
        // scope to route in, so it stands rather than being projected against nothing.
        let owner = activity_value.meta.owner.clone();
        let Some(thread) = ctx.storage.get_thread(&owner).await.ok().flatten() else {
            return FailureRouting::Uncaught;
        };

        let activity = activity_value.meta.object_ref();
        // The catcher's projectors read the failure it took: `$states.errorOutput` is the caught
        // error's output, which is what makes a routed state's projection tell the two apart.
        let error_output = error.error_output().unwrap_or(Value::Null);
        // A failed attempt produces no distinct raw result — the call that would have set one never
        // landed — so the projection's `$states.result` falls back to the processed input, exactly as
        // the success tail's own default does.
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
        .with_error_output(Some(&error_output))
        .build();
        let mut local_scope = thread.variables.clone();
        if let Some(assign_obj) = catcher.assign.as_ref() {
            let assign_value = Value::Object(assign_obj.0.clone());
            let evaluated = match ctx.env.eval_json(&assign_value, &states, &local_scope) {
                Ok(evaluated) => evaluated,
                Err(e) => {
                    out.append_command(Command::TerminateState(TerminateState {
                        activity: activity.clone(),
                        reason: TerminationReason::Failed { error: e },
                    }));
                    return FailureRouting::Caught;
                }
            };
            match evaluated {
                Value::Object(map) => {
                    if !map.is_empty() {
                        for (k, v) in map {
                            local_scope.insert(k, v);
                        }
                        out.append_event(Event::VariablesAssigned(VariablesAssigned {
                            scope: owner.clone(),
                            variables: local_scope.clone(),
                        }))
                        .await;
                    }
                }
                _ => {
                    out.append_command(Command::TerminateState(TerminateState {
                        activity: activity.clone(),
                        reason: TerminationReason::Failed {
                            error: ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                                "Assign must evaluate to a JSON object".to_string(),
                            )),
                        },
                    }));
                    return FailureRouting::Caught;
                }
            }
        }
        let output_value = match catcher.output.as_ref() {
            Some(o) => match ctx.env.eval_json(o, &states, &local_scope) {
                Ok(output_value) => output_value,
                Err(e) => {
                    out.append_command(Command::TerminateState(TerminateState {
                        activity: activity.clone(),
                        reason: TerminationReason::Failed { error: e },
                    }));
                    return FailureRouting::Caught;
                }
            },
            None => raw_result.clone(),
        };

        // A completion disposes of the activity's own deadlines: a `TimeoutSeconds` deadline only
        // *bounds* the state, so a state that finished early — this very exit — must leave no live
        // timer child behind. Swept as **events**, so they fold into this batch ahead of the terminal
        // below rather than arriving as a later log entry.
        if let Ok(act) = ctx.storage.get_activity(&activity).await {
            for child in act.map(|a| a.active_children).unwrap_or_default() {
                if child.kind != ObjectKind::Timer {
                    continue; // only timer children are swept here; the task child follows its own settle.
                }
                let Some(t) = ctx
                    .storage
                    .get_timer(&child.clone().typed::<TimerKind>())
                    .await
                    .ok()
                    .flatten()
                else {
                    continue;
                };
                let mut timer_value = t.value;
                // A fired/cancelled timer is no longer a live child, and the transition declines it —
                // so no second guard has to be kept in step with that.
                if timer_value.mark_cancelled(out.now()).is_err() {
                    continue;
                }
                out.append_event(Event::TimerCancelled { timer: timer_value })
                    .await;
            }
        }

        // A catcher takes the failure the attempt produced, so the attempt's in-flight call is
        // abandoned with it: the task has no state left to report to, and the complete step that would
        // have swept it (`after_completing`) never opens on this exit. Cancelled as the activity's own
        // child, exactly as the terminate path sweeps one — the child is a bystander here too, so it
        // carries no reason.
        // TODO(fan-out Catch): a `Parallel`/`Map` catching a failure must dispose of its in-flight child
        // executions/threads the same way; a `Task` is the only kind reachable today.
        if let Ok(children) = ctx
            .storage
            .get_children(activity.as_raw_object_ref().clone())
            .await
        {
            for child in children {
                if child.kind == ObjectKind::Task {
                    out.append_command(Command::CancelTask {
                        task: child.typed::<TaskKind>(),
                    });
                }
            }
        }

        // The activity lands `Completed`: the catcher's `Next` is what this state routes on, so the
        // failure is carried by the catcher's activation and nowhere else.
        let mut completed = activity_value.clone();
        completed.meta.with_update_at(out.now());
        completed.status = ActivityStatus::Completed;
        completed.output = Some(output_value.clone());
        if completed.raw_output.is_none() {
            completed.raw_output = Some(completed.raw_input.clone());
        }
        out.append_event(Event::StateCompleted {
            activity: completed,
        })
        .await;

        // The catcher's `Next` lives as a sibling of this state in the same enclosing `States` table —
        // that table is this activity's own `state_path` minus its leaf. The marker carries the
        // resolved target *path*, which is what `ActivateState` routes on.
        let next_path = activity_value.state_path.sibling(&catcher.next);
        out.append_event(Event::StateTransitioned(StateTransitioned {
            activity,
            next: next_path.as_ptr().to_owned(),
        }))
        .await;
        out.append_command(Command::ActivateState(ActivateState {
            execution: activity_value.execution.clone(),
            owner,
            state_path: next_path,
            input: output_value,
        }));
        FailureRouting::Caught
    }
}

impl TaskStateHandler<'_> {
    /// The `Task` complete step: the settled task's payload is the raw result, so build `$states`,
    /// apply the state's `Assign` (emitting `VariablesAssigned` on the owner scope), project `Output`
    /// (defaulting to the payload), and decide the routing — the canonical success finish, mirroring
    /// `Pass`'s `process_pass`. It emits nothing about the activity itself; the terminal and the
    /// transition belong to `after_completing`.
    async fn process_task(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<TaskFinish, ExecutionError> {
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
            Ok(TaskFinish::End { output })
        } else if let Some(next) = next {
            Ok(TaskFinish::Next {
                next: activity_value.state_path.sibling(next),
                output,
            })
        } else {
            Ok(TaskFinish::NoTerminal)
        }
    }

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
    use spica_storage::InMemoryStorage;

    use super::super::harness::*;
    use super::*;
    use crate::storage::{Storage, TimerRecord};
    use crate::types::command::{ActivateState, CompleteState, TerminateState, TerminationReason};
    use crate::types::event::{Event, StateTransitioned};
    use crate::types::meta::{HasRawObjectRef, ObjectMeta};
    use crate::{ActivityStatus, EntryPayload, ThreadStatus, Timer, TimerStatus};

    // A `Task` is the engine's external-call edge: the invocation is thrown in `after_activated` (not
    // from `process_input`), so the state's activation product is a pair of side effects — the
    // `ActivateTask` command naming the invoked entity, and the optional `TimeoutSeconds` timer
    // bounding it — with no inline `CompleteState`: only the task's own settle resumes the state. The
    // tests
    // cover the two side effects, their invalidation, and the sweep the complete step performs before
    // it may finish.

    const RESOURCE: &str = "arn:aws:states:::lambda:invoke";

    /// The invoked entity's reference: named from the execution's plain base (the same convention the
    /// activity and timer names follow) off the partition counter's second free suffix, and minted
    /// from the injected generator's second id.
    fn invoked_task_ref() -> ObjectRef<TaskKind> {
        ObjectRef::new(obj_name("execution-1"), uid(2))
    }

    /// The `TimeoutSeconds` timer `after_activated` arms: parented on the invoking activity — which is
    /// what makes it that activity's child, and so what the complete step sweeps.
    fn timeout_timer(deadline: Timestamp) -> Timer {
        Timer {
            execution: execution_ref(),
            status: TimerStatus::Active,
            deadline,
            meta: ObjectMeta::builder(uid(3))
                .name(obj_name("execution-2"))
                .at(at())
                .with_owner(activity_timer_owner(minted_activity_ref())),
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
            seeded_scope(ThreadStatus::Running),
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
            seeded_scope(ThreadStatus::Running),
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
            seeded_scope(ThreadStatus::Running),
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
            vec![EntryPayload::Command(Command::TerminateState(
                TerminateState {
                    activity: minted_activity_ref(),
                    reason: reason.clone(),
                }
            )),]
        );
    }

    /// The complete step disposes of the deadline before it finishes: the `TimeoutSeconds` timer only
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
            seeded_scope(ThreadStatus::Running),
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

    /// The in-flight task's settle is the state's completion trigger: `child_completed` hands the
    /// activity a `CompleteState` carrying the payload the `TaskCompleted` applier folded onto its
    /// `raw_output` — the very command the container used to emit on the state's behalf.
    #[tokio::test]
    async fn child_completed_of_the_invoked_task_completes_the_activity() {
        let payload = json!({ "answer": 42 });
        let mut store = InMemoryStorage::new();
        let mut row = seeded_activity(seeded_input(), []);
        row.value.raw_output = Some(payload.clone());
        seed_container(&mut store, row, []).await;

        let completed = child_completed(
            &task_state(None, None, Some("P2")),
            store,
            minted_activity_ref(),
            invoked_task_ref().into_raw_object_ref(),
        )
        .await;

        assert_eq!(
            completed.chain(),
            vec![EntryPayload::Command(Command::CompleteState(
                CompleteState {
                    activity: minted_activity_ref(),
                    output: payload,
                }
            ))]
        );
    }

    /// Seed the deadline row a fire (or a sweep) left behind, owned by the invoking activity exactly
    /// as `after_activated`'s timer is: the handler reads the timer's own recorded status to tell a
    /// fire from a cancel, so the row — not the child reference — is what the decision is made on.
    async fn seed_deadline(store: &mut InMemoryStorage, status: TimerStatus) {
        let mut timer = timeout_timer(at());
        timer.status = status;
        let mut row = TimerRecord::from_value(timer);
        row.born(at());
        store
            .put_timer(row)
            .await
            .expect("the in-memory store seeds a timer row");
    }

    /// A fired `TimeoutSeconds` deadline is this state's own failure: while the attempt is still in
    /// flight the state terminates `TimedOut`, naming the deadline the fired row carries — not a
    /// completion, and not a reason the container had to invent.
    #[tokio::test]
    async fn child_completed_of_the_fired_timeout_terminates_the_activity() {
        let mut store = InMemoryStorage::new();
        let row = seeded_activity(seeded_input(), []);
        seed_container(&mut store, row, []).await;
        seed_deadline(&mut store, TimerStatus::Completed).await;

        let completed = child_completed(
            &task_state(None, Some(60), Some("P2")),
            store,
            minted_activity_ref(),
            timeout_timer(at()).meta.raw_object_ref().clone(),
        )
        .await;

        assert_eq!(
            completed.chain(),
            vec![EntryPayload::Command(Command::TerminateState(
                TerminateState {
                    activity: minted_activity_ref(),
                    reason: TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::TimedOut {
                            message: format!(
                                "task ran past its TimeoutSeconds deadline ({})",
                                at().as_millis()
                            ),
                        }),
                    },
                }
            ))]
        );
    }

    /// A *cancelled* deadline reaches this hook too — `ActivityContainer` routes both outcomes through
    /// `child_completed` — so the timer's own recorded status is the discriminator: a swept deadline
    /// fails nothing, since the sweep is the complete step retiring a bound that has stopped applying.
    #[tokio::test]
    async fn child_completed_of_the_cancelled_timeout_does_nothing() {
        let mut store = InMemoryStorage::new();
        let row = seeded_activity(seeded_input(), []);
        seed_container(&mut store, row, []).await;
        seed_deadline(&mut store, TimerStatus::Cancelled).await;

        let completed = child_completed(
            &task_state(None, Some(60), Some("P2")),
            store,
            minted_activity_ref(),
            timeout_timer(at()).meta.raw_object_ref().clone(),
        )
        .await;

        assert!(
            completed.chain().is_empty(),
            "a cancelled deadline must fail nothing: {:?}",
            completed.chain()
        );
    }

    /// A fire landing after the attempt already settled is moot: the `TaskCompleted` applier wrote
    /// `raw_output` in this same batch, and the state is about to complete on it — so a late deadline
    /// must not terminate a state that is already on its way out.
    #[tokio::test]
    async fn child_completed_of_the_timeout_after_the_task_settled_does_nothing() {
        let mut store = InMemoryStorage::new();
        let mut row = seeded_activity(seeded_input(), []);
        row.value.raw_output = Some(json!({ "answer": 42 }));
        seed_container(&mut store, row, []).await;
        seed_deadline(&mut store, TimerStatus::Completed).await;

        let completed = child_completed(
            &task_state(None, Some(60), Some("P2")),
            store,
            minted_activity_ref(),
            timeout_timer(at()).meta.raw_object_ref().clone(),
        )
        .await;

        assert!(
            completed.chain().is_empty(),
            "a deadline that stopped bounding the state must not fail it: {:?}",
            completed.chain()
        );
    }
}
