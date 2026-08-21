use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{ParallelState, State};

use super::super::state_handler::StateHandler;
use super::super::{
    emit_transition, state_activated_value, state_completed_value, state_completing_value,
    state_terminated_value, state_terminating_value,
};
use crate::command::{Command, TerminationReason};
use crate::context::build_states;
use crate::error::ExecutionError;
use crate::eval_env::EvalEnv;
use crate::event::Event;
use crate::handler::{ActivityCtx, Collector, HandlerContext};
use crate::id::{ActivityId, NodeId};
use crate::{ActivityState, ExecutionStatus};

/// The `Parallel` state: runs several branch sub-state-machines concurrently, waits for all of them
/// to reach a terminal state, then transitions — or fails the whole state if any branch fails.
///
/// Fan-out is **flat, not recursive**: each branch runs as a *child execution* (see
/// [`SpawnBranchHandler`](super::super::spawn_branch::SpawnBranchHandler)) rooted under this
/// activity and carrying a [`state_path`](crate::storage::Execution), so a branch resolves its
/// own states within the single shared machine document without copying any definition. The
/// `Parallel` activity stays `Running` owning those children; it completes only through
/// `child_completed` once the last branch settles, or terminates on a branch failure.
pub struct ParallelStateHandler;

#[async_trait]
impl StateHandler for ParallelStateHandler {
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

    fn activate(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector,
        activity: ActivityId,
        actx: &ActivityCtx,
        state: &State,
    ) {
        let State::Parallel(s) = state else {
            unreachable!(
                "activate dispatch guarantees the state handler receives its own variant; got {state:?}"
            );
        };
        activate_parallel(env, out, activity, actx, s);
    }

    // A `Parallel` never completes through the shared `CompleteState` path: it is an async
    // container that finishes only once every branch settles, so its `complete` is not reached in
    // normal flow (nothing throws `CompleteState` on it). This arm stays as a defensive fallback —
    // if it is ever dispatched, there is nothing meaningful to project, so it routes the current
    // input onward to avoid wedging the state machine.
    fn complete(
        &self,
        _env: &mut EvalEnv,
        out: &mut Collector,
        activity: ActivityId,
        actx: &ActivityCtx,
        _state: &State,
    ) {
        out.emit_event(Event::StateCompleted {
            activity: state_completed_value(actx, actx.activity.input.clone()),
        });
        emit_transition(
            out,
            actx.activity.execution,
            activity,
            &actx.activity.input,
            None,
            Some(true),
        );
    }

    /// The **replenish** hook, resumed by `ProcessChildCompleted`'s Running arm once the last branch
    /// has settled and drained this activity's `active_children`. With every branch terminal:
    ///
    /// - if **any** branch failed, fail the whole `Parallel` (mirroring `fail.rs`, whose own
    ///   TerminateState + TerminateExecution sweep cancels the surviving sibling branches);
    /// - otherwise aggregate each branch's output (in `Branches` declaration order) into an array
    ///   and run the `Parallel`'s success finish with that array as its result.
    async fn child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector,
        activity: ActivityId,
        actx: Option<&ActivityCtx>,
        state: &State,
        _child: crate::id::NodeId,
    ) {
        let State::Parallel(s) = state else {
            unreachable!(
                "child_completed dispatch guarantees the state handler receives its own variant"
            );
        };
        let Some(actx) = actx else {
            return; // owning execution gone — nothing to converge.
        };
        let Some(act) = ctx.storage.get_activity(activity).await.ok().flatten() else {
            return; // activity gone — nothing to converge.
        };
        // `ProcessChildCompleted` now dispatches a `Running` activity's `child_completed` on *every*
        // child settle (so a `Map` can replenish one slot at a time). A `Parallel` fans every branch
        // out up front, so it must not try to converge mid-flight: it only owns its convergence once
        // the *last* branch has drained `active_children`. Any earlier invocation (other siblings
        // still in flight) is a no-op here.
        if !act.active_children.is_empty() {
            return; // sibling branches still in flight — not converged yet.
        }

        // The owning activity's branch fan-out (branch index -> child execution) is the convergence
        // map: the last `ExecutionCreated`/`ParallelBranchSpawned` wrote it, and every child execution
        // is now terminal (drain emptied `active_children` before calling us). Order by branch index
        // so the aggregated output array matches the declared `Branches` order.
        // A non-`Parallel` repository here is an internal fault (a Parallel always fans out while
        // still `Running`); nothing to aggregate — defer.
        let ActivityState::Parallel(progress) = &act.value.activity_state else {
            return;
        };
        let mut entries: Vec<(usize, crate::id::ExecutionId)> =
            progress.branches.iter().map(|(i, e)| (*i, *e)).collect();
        entries.sort_by_key(|(i, _)| *i);

        let mut outputs = Vec::with_capacity(entries.len());
        for (_index, child_exec) in entries {
            let Some(child) = ctx.storage.get_execution(child_exec).await.ok().flatten() else {
                // A child execution that vanished without settling is treated as a failure — the
                // branch never produced a result.
                let reason = TerminationReason::Failed {
                    error: ExecutionError::StateNotFound(format!("branch {child_exec}")),
                };
                fail_parallel(out, activity, actx, reason);
                return;
            };
            match &child.status {
                ExecutionStatus::Completed => {
                    outputs.push(child.output.clone().unwrap_or(Value::Null))
                }
                // A failed / cancelled branch fails the whole `Parallel` per the ASL spec ("If any
                // branch fails, the entire Parallel state fails and all branches are stopped"). The
                // recorded `reason` becomes the Parallel's own termination reason; the
                // `TerminateExecution` sweep then stops the surviving sibling branches.
                ExecutionStatus::Terminated(reason) => {
                    fail_parallel(out, activity, actx, reason.clone());
                    return;
                }
                ExecutionStatus::Terminating(reason) => {
                    fail_parallel(out, activity, actx, reason.clone());
                    return;
                }
                _ => {
                    fail_parallel(out, activity, actx, TerminationReason::Cancelled);
                    return;
                }
            }
        }

        // All branches succeeded: the state's result is the ordered array of branch outputs.
        let aggregated = Value::Array(outputs);
        finish_parallel(ctx.env, out, activity, actx, s, aggregated);
    }
}

fn activate_parallel(
    env: &mut EvalEnv,
    out: &mut Collector,
    activity: ActivityId,
    actx: &ActivityCtx,
    state: &ParallelState,
) {
    // `$states` for the activate step, mirroring other states: `result` is null (a Parallel has no
    // result until its branches settle) and `assign_ctx = None`. A JSONata `Arguments` expression
    // may reference `$states.input` and in-scope variables.
    let states = build_states(
        &actx.activity.input,
        None,
        &actx.state_name(),
        &actx.exec_input,
        None,
        actx.activity.retry_state.retry_count,
        None,
        None, // not a Map item — no `context.Map.Item` binding
    );

    // Project `Arguments` — the input each branch receives (defaults to the state's input). This is
    // the only per-state projection before fan-out.
    let arguments = match &state.arguments {
        Some(arguments) => fail_or!(
            out,
            Some(activity),
            actx.activity.execution,
            env.eval_json(arguments, &states, &actx.variables)
        ),
        None => actx.activity.input.clone(),
    };

    // Fan out each branch as a child execution. Each child inherits `actx.activity.root_execution` (the flat
    // query anchor), is rooted under this activity (`NodeId::Activity(activity)` — so the Parallel
    // waits on all of them via `active_children`), and carries a `state_path` locating its
    // branch's `states` table: the owning execution's pointer extended by
    // `/states/<parallel>/branches/<index>/states` (the path `resolve_states_map` walks for
    // arbitrary nesting).
    let owner = NodeId::Activity(activity);
    for (index, branch) in state.branches.iter().enumerate() {
        let pointer = child_pointer(actx, index);
        out.emit_command(Command::SpawnBranch {
            parent: owner,
            root_execution: actx.activity.root_execution,
            state_path: Some(pointer),
            branch_index: index,
            state: branch.start_at.clone(),
            input: arguments.clone(),
        });
    }

    // The activation work (projecting `Arguments` + fanning out branches) is done: emit the
    // activation-complete ed. No synchronous finish — the activity stays `Running` owning its child
    // executions; it completes only when they all settle (via `child_completed`).
    out.emit_event(Event::StateActivated {
        activity: state_activated_value(actx, arguments.clone(), None),
    });
}

/// Build a child execution's `state_path` for branch `index` of `parallel_name`: the owning
/// execution's pointer extended by `/states/<parallel>/branches/<index>`. For a top-level `Parallel`
/// the owner's pointer is `None`, so the result is `/states/<parallel>/branches/<index>`; for a
/// `Parallel` nested inside a branch it extends that branch's pointer, so `resolve_states_map`
/// descends the whole path in one walk.
///
/// Note the pointer names the branch's `states` table directly (ending *on* it), matching
/// `resolve_states_map`'s walk — which consumes a `branches/<index>` step and returns that branch's
/// `states` when the pointer ends right there. No trailing `/states` token.
fn child_pointer(actx: &ActivityCtx, index: usize) -> jsonptr::PointerBuf {
    // The owning execution's pointer extended by `/states/<parallel>/branches/<index>`, where
    // `<parallel>` is this state's own name (the leaf of `actx.activity.state_path`). For a top-level
    // `Parallel` the owner's pointer is `None`, so we build `/states/<parallel>/branches/ <index>`
    // from scratch; otherwise we clone and append. `push_back` applies RFC 6901 escaping.
    let mut pointer = match &actx.execution_state_path {
        Some(base) => base.clone(),
        None => jsonptr::PointerBuf::new(),
    };
    if actx.execution_state_path.is_none() {
        pointer.push_back("states");
    }
    pointer.push_back(actx.state_name());
    pointer.push_back("branches");
    pointer.push_back(index);
    pointer
}

/// Fail the `Parallel` activity — the ASL "any branch fails ⇒ whole Parallel fails" rule. Emits the
/// activity's failure ed and throws `TerminateExecution` on the owning execution from the same step,
/// mirroring `fail.rs::complete_fail`; the sweep then stops the surviving sibling branches (once the
/// `TerminateState` sweep handles its `Execution` children).
fn fail_parallel(
    out: &mut Collector,
    _activity: ActivityId,
    actx: &ActivityCtx,
    reason: TerminationReason,
) {
    out.emit_event(Event::StateTerminating {
        activity: state_terminating_value(actx, reason.clone()),
    });
    out.emit_event(Event::StateTerminated {
        activity: state_terminated_value(actx, reason.clone()),
    });
    out.emit_command(Command::TerminateExecution {
        id: actx.activity.execution,
        reason,
    });
}

/// The `Parallel`'s success finish: with all branches converged, project the state result — `$states`
/// `.result` is the aggregated branch-output array, `Assign` mutates the scope, and `Output` defaults
/// to that array — then emit the ed and route via the shared transition helper.
///
/// Unlike most states, this is **not** reached through the `CompleteStateHandler` framework (which
/// emits `StateCompleting`), so `StateCompleting` is emitted here — the `ing` that opens the success
/// finish — before the projection, keeping the `StateCompleting → StateCompleted` pairing uniform.
fn finish_parallel(
    env: &mut EvalEnv,
    out: &mut Collector,
    activity: ActivityId,
    actx: &ActivityCtx,
    state: &ParallelState,
    aggregated: Value,
) {
    let states = build_states(
        &actx.activity.input,
        Some(&aggregated), // `$states.result` = the ordered branch outputs
        &actx.state_name(),
        &actx.exec_input,
        Some(&actx.activity.input),
        actx.activity.retry_state.retry_count,
        None, // success path — no Catch `errorOutput`
        None, // not a Map item — no `context.Map.Item` binding
    );
    let mut local_scope = actx.variables.clone();

    if let Some(assign_obj) = &state.assign {
        let assign_value = Value::Object(assign_obj.0.clone());
        let evaluated = fail_or!(
            out,
            Some(activity),
            actx.activity.execution,
            env.eval_json(&assign_value, &states, &local_scope)
        );
        match evaluated {
            Value::Object(map) => {
                if !map.is_empty() {
                    for (k, v) in map {
                        local_scope.insert(k, v);
                    }
                    out.emit_event(Event::VariablesAssigned {
                        execution: actx.activity.execution,
                        variables: local_scope.clone(),
                    });
                }
            }
            _ => {
                out.terminate(
                    Some(activity),
                    actx.activity.execution,
                    ExecutionError::InvalidDefinition(
                        "Assign must evaluate to a JSON object".to_string(),
                    ),
                );
                return;
            }
        }
    }

    // `Output`, when present, projects over the converged result (so a Parallel can reshape its
    // branch-output array); when absent the state's result *is* the array.
    let output_value = match &state.output {
        Some(o) => fail_or!(
            out,
            Some(activity),
            actx.activity.execution,
            env.eval_json(o, &states, &local_scope)
        ),
        None => aggregated,
    };

    out.emit_event(Event::StateCompleting {
        activity: state_completing_value(actx),
    });
    out.emit_event(Event::StateCompleted {
        activity: state_completed_value(actx, output_value.clone()),
    });
    emit_transition(
        out,
        actx.activity.execution,
        activity,
        &output_value,
        state.next.as_deref(),
        state.end,
    );
}
