use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{IntOrExpr, MapItems, MapState, State};

use super::super::state_handler::StateHandler;
use super::super::{
    emit_transition, eval_string_or_expr, state_activated_value, state_completed_value,
    state_completing_value, state_terminated_value, state_terminating_value,
};
use crate::eval_env::EvalEnv;
use crate::handler::{ActivityCtx, Collector, HandlerContext};
use crate::types::command::{Command, TerminationReason};
use crate::types::context::build_states;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectReference};
use crate::{ActivityState, MapActivityState};

/// The `Map` state: iterates an `Items` array, running the `item_processor` sub-state-machine once
/// per item as a child execution, with bounded concurrency (`MaxConcurrency`, 0 = unlimited). It
/// replenishes slots one at a time as items settle — unlike `Parallel`, which fans every branch out
/// up front — and converges to an array of the per-item outputs once every item has settled, or
/// fails the whole state on the first item failure (the `ToleratedFailureCount`/`Percentage`
/// leeway is a deferred TODO; the current behavior equals the ASL default of tolerating 0 failures).
///
/// Each item runs through the exact same `SpawnThread` fan-out a `Parallel` branch uses (see
/// [`crate::handlers::spawn_thread::SpawnThreadHandler`]): a child execution rooted under this
/// activity and carrying a [`state_path`](crate::storage::ExecutionRecord) of
/// `/states/<map>/item_processor` (locating the processor's `states` table in the single shared
/// machine document). The Map activity stays `Running` owning those children; it replenishes via
/// `child_completed` on every settle and completes only once the last item lands.
pub struct MapStateHandler;

#[async_trait]
impl StateHandler for MapStateHandler {
    fn state(&self) -> State {
        // Only the discriminant matters for the dispatch-table key; `MapState` has no `Default`
        // (its `item_processor` is mandatory), so construct a minimal stub that no real activity
        // ever sees. `State::Map` is boxed, so the stub is `Box::new`.
        State::Map(Box::new(MapState {
            comment: None,
            output: None,
            assign: None,
            next: None,
            end: None,
            item_processor: None,
            items: None,
            item_selector: None,
            max_concurrency: None,
            tolerated_failure_percentage: None,
            tolerated_failure_count: None,
            retry: None,
            catch: None,
        }))
    }

    fn activate(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector,
        activity: ObjectReference,
        actx: &ActivityCtx,
        state: &State,
    ) {
        let State::Map(s) = state else {
            unreachable!(
                "activate dispatch guarantees the state handler receives its own variant; got {state:?}"
            );
        };
        activate_map(env, out, activity, actx, s);
    }

    // A `Map` never completes through the shared `CompleteState` path: it is an async container
    // that finishes only once every item settles (or fails on an item failure), so its `complete` is
    // not reached in normal flow. This arm stays as a defensive fallback — same rationale as
    // `ParallelStateHandler::complete` — routing the current input onward to avoid wedging.
    fn complete(
        &self,
        _env: &mut EvalEnv,
        out: &mut Collector,
        activity: ObjectReference,
        actx: &ActivityCtx,
        _state: &State,
    ) {
        out.emit_event(Event::StateCompleted {
            activity: state_completed_value(actx, actx.activity.input.clone()),
        });
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
            &actx.activity.input,
            None,
            Some(true),
        );
    }

    /// The per-settle **replenish** hook, resumed by `ProcessChildCompleted`'s Running arm on *every*
    /// item settle (not only when `active_children` drains — that is what lets a `Map` refill a
    /// freed `MaxConcurrency` slot while other items are still in flight). This settle already
    /// drained one child; the hook:
    ///
    /// 1. identifies the settled item (by `child` → `children` reverse lookup) and reads its
    ///    outcome live from the child execution's status (no persisted settle tally — each child's own
    ///    terminal event, applied before this hook runs, is the source of truth);
    /// 2. on any failure, fails the whole `Map` (default tolerance 0) — mirroring `fail.rs`;
    /// 3. otherwise refills up to `MaxConcurrency` free slots from the never-spawned tail of the
    ///    items array, then converges (`finish_map`) once `completed == total`.
    async fn child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector,
        activity: ObjectReference,
        actx: Option<&ActivityCtx>,
        state: &State,
        child: ObjectReference,
    ) {
        let State::Map(s) = state else {
            unreachable!(
                "child_completed dispatch guarantees the state handler receives its own variant"
            );
        };
        let Some(actx) = actx else {
            return; // owning execution gone — nothing to converge.
        };
        let Some(act) = ctx.storage.get_activity(&activity).await.ok().flatten() else {
            return; // activity gone — nothing to converge.
        };
        // The iteration plan + item child map live in the `Map` state-specific repository, folded
        // from the `StateActivated` activation product; without it this activity is not (or no
        // longer) a Map's — nothing to drive.
        let ActivityState::Map(progress) = &act.value.activity_state else {
            return;
        };

        // Identify this settled item by its child id. The settle drained one child from
        // `active_children`, and the `children` map (index -> child) records who that was. A Map
        // item runs as a `Thread` (post-split), so its settle arrives as a `Thread` reference.
        let child_ref = match child.kind {
            ObjectKind::Execution | ObjectKind::Thread => child.clone(),
            _ => return, // a Map's children are always threads (or legacy executions).
        };
        let Some(child_exec) = progress.children.values().find(|e| **e == child_ref) else {
            return; // settled node isn't one of our items — not ours to react to.
        };
        // A child that vanished without settling counts as a failure (mirrors `parallel.rs`). A
        // thread resolves from thread storage, so read through the scope abstraction.
        let this_successful = match crate::storage::load_scope_ref(ctx.storage, child_exec)
            .await
            .ok()
            .flatten()
        {
            Some(child) => child.is_terminal() && child.termination_reason().is_none(),
            None => false,
        };

        // No tallies are persisted here: the convergence/replenish decision is derived entirely from
        // each item child's own terminal event (`children` + per-child `status`), so a
        // follower rebuilding from the stream needs no Map-specific bookkeeping event. The only
        // Map-specific state that *is* persisted is the activation product on `StateActivated` (the
        // iteration plan), which is static and not re-derivable.

        // With no failure tolerance implemented, any item failure fails the whole Map immediately —
        // the ASL `ToleratedFailureCount` default of 0. The `TerminateExecution` sweep below stops
        // the still-in-flight sibling items (via `terminate_state.rs`'s `Execution` arm).
        if !this_successful {
            tracing::warn!(activity = %activity, child = %child_exec, "map item child failed");
            let reason = TerminationReason::Failed {
                error: ExecutionError::Runtime(RuntimeError::StateFailed {
                    state: actx.state_name(),
                    error: "Map item failed".into(),
                    output: Box::new(Value::Null),
                }),
            };
            fail_map(out, activity, actx, reason);
            return;
        }

        // The convergence/replenish decision is derived from *currently-terminal* children, counted
        // live here. It must NOT be derived from any persisted settle counter: when several item
        // children settle in the same batch, their terminal events are only applied to storage *after*
        // all of this batch's `child_completed` calls have run, so any folded counter would read the
        // same stale pre-batch value and a Map whose items all settle together would never observe
        // `completed == total`. Counting the `Completed` executions among `progress.children`
        // directly is exact regardless of batch coalescing.
        let spawn_count = progress.children.len();
        let mut completed_now = 0usize;
        for exec in progress.children.values() {
            // Resolve through the scope abstraction: it is a `Completed` item iff terminal and not
            // terminated (a `Terminated`/`Terminating` item failed and fails the Map separately).
            let is_done = crate::storage::load_scope_ref(ctx.storage, exec)
                .await
                .ok()
                .flatten()
                .map(|c| c.is_terminal() && c.termination_reason().is_none())
                .unwrap_or(false);
            if is_done {
                completed_now += 1;
            }
        }
        // All items have settled successfully — the last settle drained the final slot, so
        // `completed_now == total` implies no in-flight children remain.
        if completed_now == progress.total {
            // Aggregate the per-item outputs in item-index order (mirroring a `Parallel`'s
            // branch-order aggregation). Every child is `Completed` by now.
            let mut entries: Vec<(usize, crate::types::meta::ObjectReference)> = progress
                .children
                .iter()
                .map(|(i, e)| (*i, e.clone()))
                .collect();
            entries.sort_by_key(|(i, _)| *i);
            let mut outputs = Vec::with_capacity(entries.len());
            for (_index, child_exec) in entries {
                // A Map item's output lives on its `Thread` — resolve through the scope abstraction.
                let output = crate::storage::load_scope_ref(ctx.storage, &child_exec)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|c| c.output().cloned())
                    .unwrap_or(Value::Null);
                outputs.push(output);
            }
            finish_map(ctx.env, out, activity, actx, s, Value::Array(outputs));
            return;
        }

        // Replenish: refill the freed slots up to `MaxConcurrency` from the never-spawned tail.
        // `in_flight` after this settle is the spawned-but-not-yet-terminal children.
        let in_flight = spawn_count - completed_now;
        let capacity = if progress.max_concurrency == 0 {
            progress.total // unlimited — can fill every remaining slot
        } else {
            progress.max_concurrency
        };
        let free_slots = capacity.saturating_sub(in_flight);
        let to_spawn = free_slots.min(progress.total - spawn_count);
        if to_spawn == 0 {
            // No free slot and not converged — a transient state (a sibling settle will refill us).
            tracing::debug!(activity = %activity, "map replenish: no free slot");
            return;
        }
        let owner = activity.clone();
        let pointer = item_pointer(actx);
        let start_at = s
            .item_processor
            .as_ref()
            .map(|p| p.start_at.clone())
            .unwrap_or_default();
        for k in 0..to_spawn {
            let index = spawn_count + k;
            out.emit_command(Command::SpawnThread {
                owner: owner.clone(),
                execution: actx.activity.execution.clone(),
                state_path: Some(pointer.clone()),
                // `index` is the item's ordinal — the `children` key we aggregate on at
                // convergence (matching the ordering semantics of a `Parallel` branch).
                index,
                start_at: start_at.clone(),
                // Base scope: the per-item input is the item itself (ASL default). `ItemSelector`
                // projection is a deferred TODO.
                input: progress.items.get(index).cloned().unwrap_or(Value::Null),
            });
        }
        tracing::debug!(activity = %activity, to_spawn, "map replenishing items");
    }
}

fn activate_map(
    env: &mut EvalEnv,
    out: &mut Collector,
    activity: ObjectReference,
    actx: &ActivityCtx,
    state: &MapState,
) {
    // `$states` for the activate step: `result` is null (a Map has no result until its items
    // settle) and `assign_ctx = None`. A JSONata `Items`/`MaxConcurrency` expression may reference
    // `$states.input` and in-scope variables. No `Map.Item` binding here — `Items` is the whole
    // array, not a per-item value.
    let states = build_states(
        &actx.activity.input,
        None,
        &actx.state_name(),
        &actx.exec_input,
        None,
        actx.activity.retry_state.attempts,
        None,
        None,
    );

    // Resolve the items array. `Items` is either a literal array or a JSONata string that must
    // evaluate to an array; when absent it defaults to the state's input when that is an array.
    // `ItemSelector` (which would transform each element) is a deferred TODO.
    let items: Vec<Value> = match &state.items {
        Some(MapItems::Array(arr)) => arr.clone(),
        Some(MapItems::Expr(expr)) => {
            let evaluated = fail_or!(
                out,
                Some(activity),
                actx.activity
                    .meta
                    .owner
                    .clone()
                    .expect("an owned activity has an owner"),
                eval_string_or_expr(env, expr.as_str(), &states, &actx.variables)
            );
            match evaluated {
                Value::Array(arr) => arr,
                _ => {
                    fail_or!(
                        out,
                        Some(activity),
                        actx.activity
                            .meta
                            .owner
                            .clone()
                            .expect("an owned activity has an owner"),
                        Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                            format!(
                                "Map '{}' Items expression did not evaluate to an array",
                                actx.state_name()
                            )
                        )))
                    );
                    return;
                }
            }
        }
        None => match &actx.activity.input {
            Value::Array(arr) => arr.clone(),
            _ => {
                fail_or!(
                    out,
                    Some(activity),
                    actx.activity
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        format!(
                            "Map '{}' has no Items and its input is not an array",
                            actx.state_name()
                        )
                    )))
                );
                return;
            }
        },
    };

    // Resolve `MaxConcurrency` (default 0 = unlimited). A literal is a non-negative integer; a
    // JSONata string must evaluate to one.
    let max_concurrency: usize =
        match &state.max_concurrency {
            None => 0,
            Some(IntOrExpr::Int(n)) if *n >= 0 => *n as usize,
            Some(IntOrExpr::Int(_)) => {
                fail_or!(
                    out,
                    Some(activity),
                    actx.activity
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Map MaxConcurrency must be a non-negative integer".into(),
                    )))
                );
                return;
            }
            Some(IntOrExpr::Expr(expr)) => {
                let evaluated = fail_or!(
                    out,
                    Some(activity),
                    actx.activity
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    eval_string_or_expr(env, expr.as_str(), &states, &actx.variables)
                );
                let value = match evaluated {
                    Value::Number(num) => num.as_f64(),
                    _ => None,
                };
                match value {
                    Some(f) if f.fract() == 0.0 && f.is_finite() && f >= 0.0 => f as usize,
                    _ => {
                        fail_or!(
                        out,
                        Some(activity),
                        actx.activity.meta.owner.clone().expect("an owned activity has an owner"),
                        Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                            "Map MaxConcurrency expression must evaluate to a non-negative integer"
                                .into(),
                        )))
                    );
                        return;
                    }
                }
            }
        };

    // Fan out the first batch. With a nonzero cap that is `min(total, max)` items; with the default
    // 0 (unlimited) cap it is every item — mirroring `activate_parallel`'s up-front fan-out for the
    // unbounded case, but bounded for `MaxConcurrency` so the rest come via replenish.
    let initial_batch = if max_concurrency == 0 {
        items.len()
    } else {
        max_concurrency.min(items.len())
    };
    let owner = activity.clone();
    let pointer = item_pointer(actx);
    let start_at = state
        .item_processor
        .as_ref()
        .map(|p| p.start_at.clone())
        .unwrap_or_default();
    for index in 0..initial_batch {
        out.emit_command(Command::SpawnThread {
            owner: owner.clone(),
            execution: actx.activity.execution.clone(),
            state_path: Some(pointer.clone()),
            index,
            start_at: start_at.clone(),
            input: items.get(index).cloned().unwrap_or(Value::Null),
        });
    }

    // The activation work (resolving items/cap + fanning out the first batch) is done: emit the
    // activation-complete ed, folding the iteration plan into it. The plan (items/total/cap) is the
    // Map's *activation product* — a JSONata `Items` expression is evaluated here against a scope
    // later replenish rounds can't re-derive, so it must travel on the event for a follower/recovered
    // leader to rebuild the replenish loop. The activity stays `Running` owning its child
    // executions; it completes/replenishes only as they settle (via `child_completed`).
    out.emit_event(Event::StateActivated {
        activity: state_activated_value(
            actx,
            actx.activity.input.clone(),
            Some(ActivityState::Map(MapActivityState {
                items: items.clone(),
                total: items.len(),
                max_concurrency,
                children: std::collections::HashMap::new(),
            })),
        ),
    });

    // An empty items array spawns no children, so nobody will ever trigger `child_completed` —
    // converge immediately to an empty result rather than wedging. `finish_map` is synchronous and
    // needs no storage for the empty aggregation.
    if initial_batch == 0 {
        tracing::debug!(activity = %activity, "map has no items; converging immediately");
        finish_map(env, out, activity, actx, state, Value::Array(Vec::new()));
    }
}

/// Build a Map item child execution's `state_path`: the owning execution's pointer extended by
/// `/states/<map>/item_processor`. Every item runs the *same* processor, so the pointer is shared by
/// all of a Map's item children. It names the processor's `states` table directly (ending *on* it),
/// matching `resolve_states_map`'s walk — which consumes an `item_processor` step and returns that
/// table when the pointer ends right there. No trailing `/states` token (the same convention as a
/// `Parallel` branch pointer).
fn item_pointer(actx: &ActivityCtx) -> jsonptr::PointerBuf {
    // The owning execution's pointer extended by `/states/<map>/item_processor`, where `<map>` is
    // this state's own name (the leaf of `actx.activity.state_path`). Every item runs the *same* processor, so
    // the pointer is shared by all of a Map's item children. For a top-level Map the owner's pointer
    // is `None`, so we build `/states/<map>/item_processor` from scratch; otherwise we clone and
    // append. `push_back` applies RFC 6901 escaping.
    let mut pointer = match &actx.execution_state_path {
        Some(base) => base.clone(),
        None => jsonptr::PointerBuf::new(),
    };
    if actx.execution_state_path.is_none() {
        pointer.push_back("states");
    }
    pointer.push_back(actx.state_name());
    pointer.push_back("item_processor");
    pointer
}

/// Fail the `Map` activity — the ASL "any item failure (beyond tolerance) ⇒ whole Map fails" rule
/// (with the default 0 tolerance, that is *any* failure). Emits the activity's failure ed and throws
/// `TerminateExecution` on the owning execution from the same step, mirroring
/// `parallel.rs::fail_parallel`; the sweep then stops the still-in-flight sibling items.
fn fail_map(
    out: &mut Collector,
    _activity: ObjectReference,
    actx: &ActivityCtx,
    reason: TerminationReason,
) {
    out.emit_event(Event::StateTerminating {
        activity: state_terminating_value(actx, reason.clone()),
    });
    out.emit_event(Event::StateTerminated {
        activity: state_terminated_value(actx, reason.clone()),
    });
    super::super::emit_scope_termination(
        out,
        actx.activity
            .meta
            .owner
            .as_ref()
            .expect("an owned activity has an owner"),
        reason,
    );
}

/// The `Map`'s success finish: with all items converged, project the state result — `$states.result`
/// is the ordered `aggregated` array of per-item outputs, `Assign` mutates the scope, and `Output`
/// defaults to that array — then emit the ed and route via the shared transition helper. Mirrors
/// `finish_parallel` (the aggregation work already happened in the async caller).
///
/// Unlike most states, this is **not** reached through the `CompleteStateHandler` framework (which
/// emits `StateCompleting`), so `StateCompleting` is emitted here — the `ing` that opens the success
/// finish — before the projection, keeping the `StateCompleting → StateCompleted` pairing uniform.
fn finish_map(
    env: &mut EvalEnv,
    out: &mut Collector,
    activity: ObjectReference,
    actx: &ActivityCtx,
    state: &MapState,
    aggregated: Value,
) {
    let states = build_states(
        &actx.activity.input,
        Some(&aggregated), // `$states.result` = the ordered per-item outputs
        &actx.state_name(),
        &actx.exec_input,
        Some(&actx.activity.input),
        actx.activity.retry_state.attempts,
        None, // success path — no Catch `errorOutput`
        None, // not projecting a Map item — no `context.Map.Item` binding
    );
    let mut local_scope = actx.variables.clone();

    if let Some(assign_obj) = &state.assign {
        let assign_value = Value::Object(assign_obj.0.clone());
        let evaluated = fail_or!(
            out,
            Some(activity),
            actx.activity
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            env.eval_json(&assign_value, &states, &local_scope)
        );
        match evaluated {
            Value::Object(map) => {
                if !map.is_empty() {
                    for (k, v) in map {
                        local_scope.insert(k, v);
                    }
                    out.emit_event(Event::VariablesAssigned {
                        scope: actx
                            .activity
                            .meta
                            .owner
                            .clone()
                            .expect("an owned activity has an owner"),
                        variables: local_scope.clone(),
                    });
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

    // `Output`, when present, projects over the converged result (so a Map can reshape its item
    // output array); when absent the state's result *is* the array.
    let output_value = match &state.output {
        Some(o) => fail_or!(
            out,
            Some(activity),
            actx.activity
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
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
        actx.activity.execution.clone(),
        actx.activity
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner"),
        activity,
        actx.state_path(),
        &output_value,
        state.next.as_deref(),
        state.end,
    );
}
