use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{IntOrExpr, MapItems, MapState, State};

use super::super::state_handler::{StateHandler, StateHandlerFactory};
use super::super::{emit_transition, eval_string_or_expr};
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext};
use crate::types::command::{Command, SpawnThread, TerminationReason};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectReference};
use crate::{Activity, ActivityState, ActivityStatus, MapActivityState, Variables};

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
pub struct MapStateHandlerFactory;

#[async_trait]
impl StateHandlerFactory for MapStateHandlerFactory {
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

    fn create<'a>(&self, state: &'a State) -> Box<dyn StateHandler + 'a> {
        let State::Map(s) = state else {
            unreachable!(
                "create dispatch guarantees the factory receives its own variant; got {state:?}"
            );
        };
        Box::new(MapStateHandler { state: s })
    }
}

struct MapStateHandler<'a> {
    state: &'a MapState,
}

#[async_trait]
impl StateHandler for MapStateHandler<'_> {
    // Seed the definition-derived scaffold so `StateActivating` carries the same `Some(activity_state)`
    // a Parallel does — the empty item plan is mirrored, not the item-derived items/total/cap, which
    // are input-derived and thus harvested in `process_input` (replacing this default).
    async fn initialize(&self, activity: &mut Activity) {
        activity.activity_state = Some(ActivityState::Map(Default::default()));
    }

    // Build the iteration plan from the input and fold it onto `activity_state` as the Map's
    // activation product: items / total / max_concurrency are evaluated here against a scope later
    // replenish rounds can't re-derive, so they travel on the StateActivated event.
    async fn process_input(
        &self,
        env: &mut EvalEnv,
        activity: &mut Activity,
        variables: &Variables,
        states: &Value,
    ) -> Result<Value, ExecutionError> {
        let items = self.resolve_map_items(env, activity, variables, states)?;
        let max_concurrency = self.resolve_max_concurrency(env, variables, states)?;
        activity.activity_state = Some(ActivityState::Map(MapActivityState {
            items: items.clone(),
            total: items.len(),
            max_concurrency,
            children: std::collections::HashMap::new(),
        }));
        Ok(activity.raw_input.clone())
    }

    // Fan out the first batch after `StateActivated`: `min(total, max)` items with a cap, or every
    // item when the cap is 0 (unlimited). Further items arrive via replenish in `child_completed`.
    async fn after_activated(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        variables: &Variables,
        _states: &Value,
    ) -> Result<(), ExecutionError> {
        let Some(ActivityState::Map(progress)) = activity_value.activity_state.as_ref() else {
            return Ok(()); // plan not built — nothing to fan out.
        };
        let initial_batch = if progress.max_concurrency == 0 {
            progress.total // unlimited — can fill every remaining slot
        } else {
            progress.max_concurrency.min(progress.total)
        };
        let activity = activity_value.reference();
        let owner = activity.clone();
        let pointer = activity_value.state_path.item_processor();
        let start_at = self
            .state
            .item_processor
            .as_ref()
            .map(|p| p.start_at.clone())
            .unwrap_or_default();
        for index in 0..initial_batch {
            out.append_command(Command::SpawnThread(SpawnThread {
                owner: owner.clone(),
                execution: activity_value.execution.clone(),
                state_path: Some(pointer.clone()),
                index,
                start_at: start_at.clone(),
                input: progress.items.get(index).cloned().unwrap_or(Value::Null),
            }));
        }
        // Empty items spawn no children, so no `child_completed` will ever drive this Map; converge
        // immediately to an empty array (otherwise the side wedges waiting on a settle that never
        // comes).
        if progress.total == 0 {
            self.finish_map(
                env,
                out,
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

    // A `Map` never completes through the shared `CompleteState` path: it is an async container
    // that finishes only once every item settles (or fails on an item failure), so its `complete`
    // is not reached in normal flow. This arm stays as a defensive fallback — same rationale as
    // `ParallelStateHandler::complete` — routing the current input onward to avoid wedging.
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

        // Defensive `complete` never routes to the state's real successor: a Map's genuine routing
        // (its `next`/`end`) happens in `finish_map`, and this arm is reached only if a stray
        // `CompleteState` lands on the container. With no successor to hop to, `next = None` +
        // `end = Some(true)` makes `emit_transition` take its terminal-hop branch and complete the
        // owning scope, so the machine doesn't wedge.
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

    /// The per-settle **replenish** hook, dispatched by the inline child-settled reaction's Running
    /// arm on *every* item settle (not only when `active_children` drains — that is what lets a `Map`
    /// refill a freed `MaxConcurrency` slot while other items are still in flight). This settle already
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
        out: &mut Collector<'_>,
        activity: ObjectReference,
        activity_value: &Activity,
        variables: &Variables,
        child: ObjectReference,
    ) {
        let Some(act) = ctx.storage.get_activity(&activity).await.ok().flatten() else {
            return; // activity gone — nothing to converge.
        };
        // The iteration plan + item child map live in the `Map` state-specific repository, folded
        // from the `StateActivated` activation product; without it this activity is not (or no
        // longer) a Map's — nothing to drive.
        let Some(ActivityState::Map(progress)) = act.value.activity_state.as_ref() else {
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
                    state: activity_value.state_path.state_name(),
                    error: "Map item failed".into(),
                    output: Box::new(Value::Null),
                }),
            };
            self.fail_map(out, activity_value, reason).await;
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
            self.finish_map(
                ctx.env,
                out,
                activity_value,
                variables,
                Value::Array(outputs),
            )
            .await;
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
        let pointer = activity_value.state_path.item_processor();
        let start_at = self
            .state
            .item_processor
            .as_ref()
            .map(|p| p.start_at.clone())
            .unwrap_or_default();
        for k in 0..to_spawn {
            let index = spawn_count + k;
            out.append_command(Command::SpawnThread(SpawnThread {
                owner: owner.clone(),
                execution: activity_value.execution.clone(),
                state_path: Some(pointer.clone()),
                // `index` is the item's ordinal — the `children` key we aggregate on at
                // convergence (matching the ordering semantics of a `Parallel` branch).
                index,
                start_at: start_at.clone(),
                // Base scope: the per-item input is the item itself (ASL default). `ItemSelector`
                // projection is a deferred TODO.
                input: progress.items.get(index).cloned().unwrap_or(Value::Null),
            }));
        }
        tracing::debug!(activity = %activity, to_spawn, "map replenishing items");
    }
}

impl MapStateHandler<'_> {
    /// Resolve the items array the Map iterates. `Items` is either a literal array or a JSONata
    /// string that must evaluate to an array; when absent it defaults to the state's input when that
    /// is an array. `ItemSelector` (which transforms each element) is a deferred TODO.
    fn resolve_map_items(
        &self,
        env: &mut EvalEnv,
        activity: &Activity,
        variables: &Variables,
        states: &Value,
    ) -> Result<Vec<Value>, ExecutionError> {
        let state_name = activity.state_path.state_name();
        match &self.state.items {
            Some(MapItems::Array(arr)) => Ok(arr.clone()),
            Some(MapItems::Expr(expr)) => {
                let evaluated = eval_string_or_expr(env, expr.as_str(), states, variables)?;
                match evaluated {
                    Value::Array(arr) => Ok(arr),
                    _ => Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        format!("Map '{state_name}' Items expression did not evaluate to an array"),
                    ))),
                }
            }
            None => match &activity.raw_input {
                Value::Array(arr) => Ok(arr.clone()),
                _ => Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                    format!("Map '{state_name}' has no Items and its input is not an array"),
                ))),
            },
        }
    }

    /// Resolve `MaxConcurrency` (default 0 = unlimited). The model's accessor supplies the spec
    /// default for an omitted field; a literal is a non-negative integer, a JSONata string one.
    /// `jsonata-core` yields every number as `f64`, so any unit-fraction non-negative value is
    /// accepted.
    fn resolve_max_concurrency(
        &self,
        env: &mut EvalEnv,
        variables: &Variables,
        states: &Value,
    ) -> Result<usize, ExecutionError> {
        match self.state.max_concurrency() {
            IntOrExpr::Int(n) if n >= 0 => Ok(n as usize),
            IntOrExpr::Int(_) => Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "Map MaxConcurrency must be a non-negative integer".into(),
            ))),
            IntOrExpr::Expr(expr) => {
                let evaluated = eval_string_or_expr(env, expr.as_str(), states, variables)?;
                match evaluated {
                    Value::Number(num) => num.as_f64(),
                    _ => None,
                }
                .and_then(|f| {
                    if f.fract() == 0.0 && f.is_finite() && f >= 0.0 {
                        Some(f as usize)
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "Map MaxConcurrency expression must evaluate to a non-negative integer"
                            .into(),
                    ))
                })
            }
        }
    }

    /// Fail the `Map` activity — the ASL "any item failure (beyond tolerance) ⇒ whole Map fails" rule
    /// (with the default 0 tolerance, that is *any* failure). Emits the activity's failure ed and throws
    /// `TerminateExecution` on the owning execution from the same step, mirroring
    /// `parallel.rs::fail_parallel`; the sweep then stops the still-in-flight sibling items.
    async fn fail_map(
        &self,
        out: &mut Collector<'_>,
        activity: &Activity,
        reason: TerminationReason,
    ) {
        // `fail_map` only borrows `activity`, so it advances a fresh copy in place through the
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

    /// The `Map`'s success finish: with all items converged, project the state result — `$states.result`
    /// is the ordered `aggregated` array of per-item outputs, `Assign` mutates the scope, and `Output`
    /// defaults to that array — then emit the ed and route via the shared transition helper. Mirrors
    /// `finish_parallel` (the aggregation work already happened in the async caller).
    ///
    /// Unlike most states, this is **not** reached through the `CompleteStateHandler` framework (which
    /// emits `StateCompleting`), so `StateCompleting` is emitted here — the `ing` that opens the success
    /// finish — before the projection, keeping the `StateCompleting → StateCompleted` pairing uniform.
    async fn finish_map(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity: &Activity,
        variables: &Variables,
        aggregated: Value,
    ) {
        let state = self.state;
        let activity_ref = activity.reference();
        let states = States::new(
            &activity.raw_input,
            &activity.state_path.state_name(),
            activity.retry_count(),
        )
        .with_result(Some(&aggregated)) // `$states.result` = the ordered per-item outputs
        .with_assign_ctx(Some(&activity.raw_input))
        .build();
        let mut local_scope = variables.clone();

        let owner = activity
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        let assigned = self
            .apply_assign(
                out,
                env,
                &owner,
                state.assign.as_ref(),
                &states,
                &mut local_scope,
            )
            .await;
        fail_or!(out, Some(activity_ref), owner.clone(), assigned);

        // `Output`, when present, projects over the converged result (so a Map can reshape its item
        // output array); when absent the state's result *is* the array.
        let output_value = fail_or!(
            out,
            Some(activity_ref),
            owner.clone(),
            self.project_output(
                env,
                state.output.as_ref(),
                &states,
                &local_scope,
                aggregated,
            )
            .await
        );

        // `finish_map` only borrows `activity`, so it advances a fresh copy in place through the
        // completing → completed lifecycle moments.
        let mut activity_value = activity.clone();
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
        activity_value
            .meta
            .with_update_at(crate::log::Timestamp::now());
        activity_value.status = ActivityStatus::Completed;
        activity_value.output = Some(output_value.clone());
        if activity_value.raw_output.is_none() {
            activity_value.raw_output = Some(activity_value.raw_input.clone());
        }
        out.append_event(Event::StateCompleted {
            activity: activity_value,
        })
        .await;
        emit_transition(
            out,
            activity.execution.clone(),
            activity
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            activity_ref,
            &activity.state_path,
            &output_value,
            state.next.as_deref(),
            state.end,
        )
        .await;
    }
}
