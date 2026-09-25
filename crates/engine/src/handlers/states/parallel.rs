use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{AssignObject, ParallelState, State};

use super::super::emit_state_completed;
use super::super::emit_transition;
use super::super::state_handler::{StateHandler, StateHandlerFactory};
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext};
use crate::log::Timestamp;
use crate::types::command::{Command, SpawnThread, TerminateState, TerminationReason};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::ObjectReference;
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
        // (its `Branches` is mandatory), so construct a minimal stub that no real activity ever sees.
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
        _now: Timestamp,
    ) -> Result<Value, ExecutionError> {
        match &self.state.arguments {
            Some(arguments) => env.eval_json(arguments, states, variables),
            None => Ok(activity.raw_input.clone()),
        }
    }

    // Fan out each branch as a child Thread after `StateActivated`. Each child inherits
    // `activity_value.execution` (the flat query anchor), is rooted under this activity (so the
    // Parallel waits on all of them via `active_children`), and carries a state_path locating its
    // branch's `States` table. The activity stays `Running`; it completes only via `child_completed`.
    async fn after_activated(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        variables: &Variables,
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
        // No branch means no child will ever settle, and the settle is the only thing that drives this
        // activity's convergence — so converge here instead of wedging on a child that never comes,
        // the same way a `Map` with no items does.
        if self.state.branches.is_empty() {
            self.finish_parallel(
                env,
                out,
                activity,
                activity_value,
                variables,
                Value::Array(Vec::new()),
            )
            .await;
        }
        Ok(())
    }

    fn complete_directly(&self, _activity: &Activity) -> bool {
        false
    }

    // Read by this state's own `finish_parallel`, which projects the aggregated branch outputs.
    fn assign(&self) -> Option<&AssignObject> {
        self.state.assign.as_ref()
    }

    fn output(&self) -> Option<&Value> {
        self.state.output.as_ref()
    }

    // Read by `finish_parallel`'s transition: the successor declared on the Parallel itself.
    fn next(&self) -> Option<&str> {
        self.state.next.as_deref()
    }

    fn end(&self) -> Option<bool> {
        self.state.end
    }

    // A `Parallel` never completes through the shared success projection: it finishes only once every branch
    // settles, driven by `child_completed` → `finish_parallel`, so the base's `finish` is reached only as a
    // defensive fallback. With no successor to hop to, it completes the activity with its own raw input
    // and routes `emit_transition` down its terminal branch (`next = None` + `end = Some(true)`), so a
    // stray `CompleteState` on the container does not wedge the machine — nothing else is projected.
    async fn finish(
        &self,
        _env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity: ObjectReference,
        activity_value: &Activity,
        _variables: &Variables,
    ) -> Result<(), ExecutionError> {
        emit_state_completed(out, activity_value, &activity_value.raw_input).await;
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
        Ok(())
    }

    /// The **convergence** hook, dispatched by the inline child-settled reaction's Running arm on every
    /// branch settle:
    ///
    /// - if **any** branch has failed, fail the whole `Parallel` on that settle — while the survivors
    ///   may still be in flight (mirroring `fail.rs`, whose own TerminateState + TerminateExecution
    ///   sweep cancels those surviving sibling branches);
    /// - otherwise, once the last branch has settled and drained this activity's `active_children`,
    ///   aggregate each branch's output (in `Branches` declaration order) into an array and run the
    ///   `Parallel`'s success finish with that array as its result.
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
        // The owning activity's branch fan-out (branch index -> child execution) is both the
        // convergence map and the settle's outcome table: the last `ThreadCreated` wrote it (from each
        // thread's own `index`). Order by branch index so the aggregated output array matches the
        // declared `Branches` order.
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

        // Resolve every branch *before* deciding whether this settle converges, because a branch's
        // outcome is what decides it: per the ASL spec ("If any branch fails, the entire Parallel state
        // fails and all branches are stopped") a failed branch fails the whole `Parallel` now, while
        // siblings are still in flight — reaching that decision only once the last branch settles would
        // leave those siblings, and whatever deadline they armed, running behind a failure the run
        // already knows about.
        let mut branches = Vec::with_capacity(entries.len());
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
                // A failed / cancelled branch fails the whole `Parallel`; the recorded `reason` becomes
                // the Parallel's own termination reason, and the activity's own `TerminateState` sweep
                // stops the surviving sibling branches.
                self.fail_parallel(out, activity_value, reason.clone())
                    .await;
                return;
            }
            branches.push(child);
        }

        // Everything settled successfully so far — but the inline child-settled reaction dispatches a
        // `Running` activity's `child_completed` on *every* child settle (so a `Map` can replenish one
        // slot at a time). A `Parallel` fans every branch out up front, so it only owns its convergence
        // once the *last* branch has drained `active_children`; any earlier invocation (other siblings
        // still in flight) is a no-op here.
        if !act.active_children.is_empty() {
            return; // sibling branches still in flight — not converged yet.
        }

        let mut outputs = Vec::with_capacity(branches.len());
        for child in branches {
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
    /// step, mirroring `fail.rs`; the activity's own `TerminateState` sweep is what stops the
    /// surviving sibling branches (see `fail_parallel`'s body).
    async fn fail_parallel(
        &self,
        out: &mut Collector<'_>,
        activity: &Activity,
        reason: TerminationReason,
    ) {
        // Issue the activity's termination instead of writing its terminal records here: a
        // hand-written `StateTerminated` closes the activity before the scope's own sweep reaches it,
        // and the `TerminateState` that sweep issues onto an already-terminal activity is a
        // duplicate — so the surviving branch threads, and the timers they armed, are never stopped
        // and outlive a run that has already ended. The state's own terminate handler sweeps them.
        out.append_command(Command::TerminateState(TerminateState {
            activity: activity.reference(),
            reason: reason.clone(),
        }));
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
        let owner = activity_value
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        // `$states.result` / the default state result is the aggregated branch-output array; `Output`,
        // when present, projects over it (so a Parallel can reshape that array).
        let states = States::new(
            &activity_value.raw_input,
            &activity_value.state_path.state_name(),
            activity_value.retry_count(),
        )
        .with_result(Some(&aggregated))
        .with_assign_ctx(Some(&activity_value.raw_input))
        .build();
        let mut local_scope = variables.clone();
        fail_or!(
            out,
            Some(activity.clone()),
            owner.clone(),
            self.apply_assign(out, env, &owner, self.assign(), &states, &mut local_scope)
                .await
        );
        let output_value = fail_or!(
            out,
            Some(activity),
            owner.clone(),
            self.project_output(env, self.output(), &states, &local_scope, aggregated)
                .await
        );

        // `finish_parallel` only borrows `activity_value`, so it advances a fresh copy in place
        // through the completing → completed lifecycle moments.
        let mut finished = activity_value.clone();
        finished.meta.with_update_at(out.now());
        finished.status = ActivityStatus::Completing;
        if finished.raw_output.is_none() {
            finished.raw_output = Some(finished.raw_input.clone());
        }
        out.append_event(Event::StateCompleting {
            activity: finished.clone(),
        })
        .await;
        emit_state_completed(out, &finished, &output_value).await;
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
            self.next(),
            self.end(),
        )
        .await;
    }
}
