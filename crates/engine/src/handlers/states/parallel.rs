use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{ParallelState, State};

use super::super::emit_transition;
use super::super::state_handler::{StateHandler, StateHandlerFactory};
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext};
use crate::types::command::{Command, SpawnThread, TerminationReason};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectReference};
use crate::{Activity, ActivityState, ActivityStatus, Variables};

/// The `Parallel` state: runs several branch sub-state-machines concurrently, waits for all of them
/// to reach a terminal state, then transitions — or fails the whole state if any branch fails.
///
/// Fan-out is **flat, not recursive**: each branch runs as a *child execution* (see
/// [`SpawnThreadHandler`](super::super::spawn_thread::SpawnThreadHandler)) rooted under this
/// activity and carrying a [`state_path`](crate::storage::ExecutionRecord), so a branch resolves its
/// own states within the single shared machine document without copying any definition. The
/// `Parallel` activity stays `Running` owning those children; it completes only through
/// `child_completed` once the last branch settles, or terminates on a branch failure.
pub struct ParallelStateHandlerFactory;

#[async_trait]
impl StateHandlerFactory for ParallelStateHandlerFactory {
    fn state(&self) -> State {
        // Only the discriminant matters for the dispatch-table key; `ParallelState` has no `Default`
        // (its `branches` is mandatory), so construct a minimal stub that no real activity ever sees.
        State::Parallel(ParallelState {
            comment: None,
            output: None,
            assign: None,
            next: None,
            end: None,
            branches: Vec::new(),
            arguments: None,
            retry: None,
            catch: None,
        })
    }

    fn create<'a>(&self, state: &'a State) -> Box<dyn StateHandler + 'a> {
        let State::Parallel(s) = state else {
            unreachable!(
                "create dispatch guarantees the factory receives its own variant; got {state:?}"
            );
        };
        Box::new(ParallelStateHandler { state: s })
    }
}

struct ParallelStateHandler<'a> {
    state: &'a ParallelState,
}

#[async_trait]
impl StateHandler for ParallelStateHandler<'_> {
    // Seed the branch-plan container. The child refs are folded in by the SpawnThread/ThreadCreated
    // applier, not here — only the (initially empty) map is definition-derived, unlike a Map's
    // input-derived item plan which waits for `process_input`.
    async fn initialize(&self, activity: &mut Activity) {
        activity.activity_state = Some(ActivityState::Parallel(Default::default()));
    }

    // A Parallel's processed input is its projected `Arguments` — the input each branch receives
    // (defaults to the state's input).
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

    // Fan out each branch as a child Thread after `StateActivated`. Each child inherits
    // `activity_value.execution` (the flat query anchor), is rooted under this activity (so the
    // Parallel waits on all of them via `active_children`), and carries a state_path locating its
    // branch's `states` table. The activity stays `Running`; it completes only via `child_completed`.
    async fn after_activated(
        &self,
        _env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        _variables: &Variables,
        _states: &Value,
    ) -> Result<(), ExecutionError> {
        let activity = activity_value.reference();
        let owner = activity.clone();
        for (index, branch) in self.state.branches.iter().enumerate() {
            let pointer = activity_value.state_path.branch(index);
            out.append_command(Command::SpawnThread(SpawnThread {
                owner: owner.clone(),
                execution: activity_value.execution.clone(),
                state_path: Some(pointer),
                index,
                start_at: branch.start_at.clone(),
                input: activity_value.input.clone().unwrap_or_default(),
            }));
        }
        Ok(())
    }

    fn complete_directly(&self, _activity: &Activity) -> bool {
        false
    }

    // A `Parallel` never completes through the shared `CompleteState` path: it is an async
    // container that finishes only once every branch settles, so its `complete` is not reached in
    // normal flow (nothing throws `CompleteState` on it). This arm stays as a defensive fallback —
    // if it is ever dispatched, there is nothing meaningful to project, so it routes the current
    // input onward to avoid wedging the state machine.
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

        activity_value
            .meta
            .with_update_at(crate::log::Timestamp::now());
        activity_value.status = ActivityStatus::Completed;
        activity_value.output = Some(activity_value.raw_input.clone());
        if activity_value.raw_output.is_none() {
            activity_value.raw_output = Some(activity_value.raw_input.clone());
        }
        out.append_event(Event::StateCompleted {
            activity: activity_value.clone(),
        })
        .await;

        // TODO：为什么这里的 next 固定是 None?
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
            &activity_value.raw_input,
            None,
            Some(true),
        )
        .await;
    }

    /// The **replenish** hook, dispatched by the inline child-settled reaction's Running arm once the
    /// last branch has settled and drained this activity's `active_children`. With every branch
    /// terminal:
    ///
    /// - if **any** branch failed, fail the whole `Parallel` (mirroring `fail.rs`, whose own
    ///   TerminateState + TerminateExecution sweep cancels the surviving sibling branches);
    /// - otherwise aggregate each branch's output (in `Branches` declaration order) into an array
    ///   and run the `Parallel`'s success finish with that array as its result.
    async fn child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity: ObjectReference,
        activity_value: &Activity,
        variables: &Variables,
        _child: ObjectReference,
    ) {
        let Some(act) = ctx.storage.get_activity(&activity).await.ok().flatten() else {
            return; // activity gone — nothing to converge.
        };
        // The inline child-settled reaction dispatches a `Running` activity's `child_completed` on
        // *every* child settle (so a `Map` can replenish one slot at a time). A `Parallel` fans every
        // branch out up front, so it must not try to converge mid-flight: it only owns its convergence
        // once the *last* branch has drained `active_children`. Any earlier invocation (other siblings
        // still in flight) is a no-op here.
        if !act.active_children.is_empty() {
            return; // sibling branches still in flight — not converged yet.
        }

        // The owning activity's branch fan-out (branch index -> child execution) is the convergence
        // map: the last `ThreadCreated` wrote it (from each thread's own `index`), and every child
        // execution is now terminal (drain emptied `active_children` before calling us). Order by
        // branch index so the aggregated output array matches the declared `Branches` order.
        // A non-`Parallel` repository here is an internal fault (a Parallel always fans out while
        // still `Running`); nothing to aggregate — defer.
        let Some(ActivityState::Parallel(progress)) = act.value.activity_state.as_ref() else {
            return;
        };
        let mut entries: Vec<(usize, crate::types::meta::ObjectReference)> = progress
            .branches
            .iter()
            .map(|(i, e)| (*i, e.clone()))
            .collect();
        entries.sort_by_key(|(i, _)| *i);

        let mut outputs = Vec::with_capacity(entries.len());
        for (_index, child_scope_ref) in entries {
            // Each branch is a `Thread` (post-split), which lives in thread storage — not execution
            // storage. Resolve through the scope abstraction so the aggregation reads a uniform
            // record regardless of kind; a branch that resolves to neither is treated as a failure.
            let Some(child) = crate::storage::load_scope_ref(ctx.storage, &child_scope_ref)
                .await
                .ok()
                .flatten()
            else {
                // A child that vanished without settling is treated as a failure — the branch never
                // produced a result.
                let reason = TerminationReason::Failed {
                    error: ExecutionError::Runtime(RuntimeError::StateNotFound(format!(
                        "branch {child_scope_ref}"
                    ))),
                };
                self.fail_parallel(out, activity_value, reason).await;
                return;
            };
            if let Some(reason) = child.termination_reason() {
                // A failed / cancelled branch fails the whole `Parallel` per the ASL spec ("If any
                // branch fails, the entire Parallel state fails and all branches are stopped"). The
                // recorded `reason` becomes the Parallel's own termination reason; the
                // `TerminateExecution` sweep then stops the surviving sibling branches.
                self.fail_parallel(out, activity_value, reason.clone())
                    .await;
                return;
            }
            if child.is_terminal() {
                outputs.push(child.output().cloned().unwrap_or(Value::Null));
            } else {
                // A non-terminal branch (still Running/Completing yet drained empty) is an internal
                // fault; fail the Parallel rather than aggregate a partial result.
                self.fail_parallel(out, activity_value, TerminationReason::Cancelled)
                    .await;
                return;
            }
        }

        // All branches succeeded: the state's result is the ordered array of branch outputs.
        let aggregated = Value::Array(outputs);
        self.finish_parallel(
            ctx.env,
            out,
            activity,
            activity_value,
            variables,
            aggregated,
        )
        .await;
    }
}

impl ParallelStateHandler<'_> {
    /// Fail the `Parallel` activity — the ASL "any branch fails ⇒ whole Parallel fails" rule. Emits
    /// the activity's failure ed and throws `TerminateExecution` on the owning execution from the same
    /// step, mirroring `fail.rs`; the sweep then stops the surviving sibling branches (once the
    /// `TerminateState` sweep handles its `Execution` children).
    async fn fail_parallel(
        &self,
        out: &mut Collector<'_>,
        activity: &Activity,
        reason: TerminationReason,
    ) {
        // `fail_parallel` only borrows `activity`, so it advances a fresh copy in place through the
        // terminating → terminated lifecycle moments — the reason is decided once here.
        let mut terminated = activity.clone();
        terminated.meta.with_update_at(crate::log::Timestamp::now());
        terminated.status = ActivityStatus::Terminating(reason.clone());
        out.append_event(Event::StateTerminating {
            activity: terminated.clone(),
        })
        .await;
        terminated.meta.with_update_at(crate::log::Timestamp::now());
        terminated.status = ActivityStatus::Terminated(reason.clone());
        out.append_event(Event::StateTerminated {
            activity: terminated,
        })
        .await;
        super::super::emit_scope_termination(
            out,
            activity
                .meta
                .owner
                .as_ref()
                .expect("an owned activity has an owner"),
            reason,
        );
    }

    /// The `Parallel`'s success finish: with all branches converged, project the state result —
    /// `$states` `.result` is the aggregated branch-output array, `Assign` mutates the scope, and
    /// `Output` defaults to that array — then emit the ed and route via the shared transition helper.
    ///
    /// Unlike most states, this is **not** reached through the `CompleteStateHandler` framework (which
    /// emits `StateCompleting`), so `StateCompleting` is emitted here — the `ing` that opens the
    /// success finish — before the projection, keeping the `StateCompleting → StateCompleted` pairing
    /// uniform.
    async fn finish_parallel(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity: ObjectReference,
        activity_value: &Activity,
        variables: &Variables,
        aggregated: Value,
    ) {
        let states = States::new(
            &activity_value.raw_input,
            &activity_value.state_path.state_name(),
            activity_value.retry_count(),
        )
        .with_result(Some(&aggregated)) // `$states.result` = the ordered branch outputs
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

        // `Output`, when present, projects over the converged result (so a Parallel can reshape its
        // branch-output array); when absent the state's result *is* the array.
        let output_value = fail_or!(
            out,
            Some(activity),
            owner.clone(),
            self.project_output(
                env,
                self.state.output.as_ref(),
                &states,
                &local_scope,
                aggregated,
            )
            .await
        );

        // `finish_parallel` only borrows `activity_value`, so it advances a fresh copy in place
        // through the completing → completed lifecycle moments.
        let mut finished = activity_value.clone();
        finished.meta.with_update_at(crate::log::Timestamp::now());
        finished.status = ActivityStatus::Completing;
        if finished.raw_output.is_none() {
            finished.raw_output = Some(finished.raw_input.clone());
        }
        out.append_event(Event::StateCompleting {
            activity: finished.clone(),
        })
        .await;
        finished.meta.with_update_at(crate::log::Timestamp::now());
        finished.status = ActivityStatus::Completed;
        finished.output = Some(output_value.clone());
        if finished.raw_output.is_none() {
            finished.raw_output = Some(finished.raw_input.clone());
        }
        out.append_event(Event::StateCompleted { activity: finished })
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
