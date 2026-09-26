use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{AssignObject, IntOrExpr, MapItems, MapState, State};

use super::super::emit_state_completed;
use super::super::state_handler::{StateHandler, StateHandlerFactory};
use super::super::{emit_transition, eval_string_or_expr};
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext};
use crate::log::Timestamp;
use crate::types::command::{Command, SpawnThread, TerminateState, TerminationReason};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::ObjectReference;
use crate::{Activity, ActivityState, ActivityStatus, MapActivityState, Variables};

/// The `Map` state: iterates an `Items` array, running the `ItemProcessor` sub-state-machine once
/// per item as a child execution, with bounded concurrency (`MaxConcurrency`, 0 = unlimited). It
/// replenishes slots one at a time as items settle — unlike `Parallel`, which fans every branch out
/// up front — and converges to an array of the per-item outputs once every item has settled, or
/// fails the whole state on the first item failure (the `ToleratedFailureCount`/`Percentage`
/// leeway is a deferred TODO; the current behavior equals the ASL default of tolerating 0 failures).
///
/// Each item runs through the exact same `SpawnThread` fan-out a `Parallel` branch uses (see
/// [`crate::handlers::spawn_thread::SpawnThreadHandler`]): a child execution rooted under this
/// activity and carrying a [`state_path`](crate::storage::ExecutionRecord) of
/// `/States/<map>/ItemProcessor/States` (the processor's own `States` table in the single shared
/// machine document — the same shape of pointer a root thread carries). The Map activity stays
/// `Running` owning those children; it replenishes via `child_completed` on every settle and
/// completes only once the last item lands.
pub struct MapStateHandlerFactory;

#[async_trait]
impl StateHandlerFactory for MapStateHandlerFactory {
    fn state(&self) -> State {
        // Only the discriminant matters for the dispatch-table key; `MapState` has no `Default`
        // (its `ItemProcessor` is mandatory), so construct a minimal stub that no real activity
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
        _now: Timestamp,
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

    // Read by this state's own `finish_map`, which projects the aggregated per-item outputs.
    fn assign(&self) -> Option<&AssignObject> {
        self.state.assign.as_ref()
    }

    fn output(&self) -> Option<&Value> {
        self.state.output.as_ref()
    }

    // Read by `finish_map`'s transition: the successor declared on the Map itself.
    fn next(&self) -> Option<&str> {
        self.state.next.as_deref()
    }

    fn end(&self) -> Option<bool> {
        self.state.end
    }

    // A `Map` never completes through the shared success projection: it finishes only once every item
    // settles, driven by `child_completed` → `finish_map`, so the base's `finish` is reached only as a
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
        // `active_children`, and the `children` map (index -> child) records who that was — so
        // membership in it is the whole test, and a node outside it is not ours to react to. Every
        // entry was folded from a `ThreadCreated`, so an item is always a `Thread`.
        let Some(child_exec) = progress.children.values().find(|e| **e == child) else {
            return;
        };
        // A child that vanished without settling counts as a failure (mirrors `parallel.rs`).
        let this_successful = match ctx.storage.get_thread(child_exec).await.ok().flatten() {
            Some(child) => child.is_terminal() && child.value.status.termination_reason().is_none(),
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
            // It is a `Completed` item iff terminal and not terminated (a `Terminated`/`Terminating`
            // item failed and fails the Map separately).
            let is_done = ctx
                .storage
                .get_thread(exec)
                .await
                .ok()
                .flatten()
                .map(|c| c.is_terminal() && c.value.status.termination_reason().is_none())
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
                // A Map item's output lives on its `Thread`.
                let output = ctx
                    .storage
                    .get_thread(&child_exec)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|c| c.value.output.clone())
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
    ///
    /// The activity is terminated by *issuing* its termination rather than by writing its terminal
    /// records here. A hand-written `StateTerminated` marks the activity terminal before the owning
    /// scope's own sweep reaches it, and a `TerminateState` on an already-terminal activity is a
    /// duplicate — so the sweep that should stop this Map's item children would have nothing left to
    /// sweep, and an in-flight item keeps running (with any timer it armed) behind a run that has
    /// already ended. Letting the state's own terminate handler open the close instead means the same
    /// sweep every other teardown uses also cancels the items, recursively.
    async fn fail_map(
        &self,
        out: &mut Collector<'_>,
        activity: &Activity,
        reason: TerminationReason,
    ) {
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
        let activity_ref = activity.reference();
        let owner = activity
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        // `$states.result` / the default state result is the aggregated per-item output array;
        // `Output`, when present, projects over it (so a Map can reshape that array).
        let states = States::new(
            &activity.raw_input,
            &activity.state_path.state_name(),
            activity.retry_count(),
        )
        .with_result(Some(&aggregated))
        .with_assign_ctx(Some(&activity.raw_input))
        .build();
        let mut local_scope = variables.clone();
        fail_or!(
            out,
            Some(activity_ref.clone()),
            owner.clone(),
            self.apply_assign(out, env, &owner, self.assign(), &states, &mut local_scope)
                .await
        );
        let output_value = fail_or!(
            out,
            Some(activity_ref),
            owner.clone(),
            self.project_output(env, self.output(), &states, &local_scope, aggregated)
                .await
        );

        // `finish_map` only borrows `activity`, so it advances a fresh copy in place through the
        // completing → completed lifecycle moments.
        let mut activity_value = activity.clone();
        activity_value.meta.with_update_at(out.now());
        activity_value.status = ActivityStatus::Completing;
        if activity_value.raw_output.is_none() {
            activity_value.raw_output = Some(activity_value.raw_input.clone());
        }
        out.append_event(Event::StateCompleting {
            activity: activity_value.clone(),
        })
        .await;
        emit_state_completed(out, &activity_value, &output_value).await;
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
            self.next(),
            self.end(),
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;
    use spica_asl::{ItemProcessor, MapItems, MapState};

    use super::super::harness::*;
    use super::*;
    use crate::storage::{ActivityRecord, ThreadRecord};
    use crate::types::command::{CompleteThread, TerminateThread};
    use crate::{EntryPayload, MapActivityState, ThreadStatus};
    use spica_storage::InMemoryStorage;

    // A `Map` is a container like `Parallel`, but its fan-out is *input-derived*: the iteration plan
    // (items / total / cap) is built in `process_input` and travels on `StateActivated`, then
    // `after_activated` fans out only the first `min(total, cap)` items — the rest arrive by
    // replenish. These tests pin the plan, the first batch, and the two degenerate outcomes; the
    // per-settle half (`child_completed`) is driven once per item settle and decides between
    // "refill a freed slot", "not my child", "that item failed" and "aggregate and finish". The base
    // `finish` is only a defensive fallback a stray `CompleteState` lands on.

    fn map_state(
        items: Option<MapItems>,
        max_concurrency: Option<i64>,
        end: Option<bool>,
    ) -> State {
        State::Map(Box::new(MapState {
            comment: None,
            output: None,
            assign: None,
            next: None,
            end,
            item_processor: Some(ItemProcessor {
                start_at: "I0".to_string(),
                states: HashMap::new(),
            }),
            items,
            item_selector: None,
            max_concurrency: max_concurrency.map(IntOrExpr::Int),
            tolerated_failure_percentage: None,
            tolerated_failure_count: None,
            retry: None,
            catch: None,
        }))
    }

    /// The plan `process_input` harvests onto the activity: the items verbatim, their count, and the
    /// resolved cap. The `children` map stays empty — it is folded later, from the fan-out.
    fn planned(items: Vec<Value>, max_concurrency: usize) -> ActivityState {
        ActivityState::Map(MapActivityState {
            total: items.len(),
            items,
            max_concurrency,
            children: HashMap::new(),
        })
    }

    /// The activity value once activated, with `plan` as its Map repository.
    fn activated_activity(plan: ActivityState) -> Activity {
        let mut activated = minted_activity(path("/States/P"), seeded_input());
        activated.input = Some(seeded_input());
        activated.activity_state = Some(plan);
        activated
    }

    fn item_spawn(index: usize, input: Value) -> Command {
        Command::SpawnThread(SpawnThread {
            owner: minted_activity_ref(),
            execution: execution_ref(),
            state_path: Some(path("/States/P/ItemProcessor/States")),
            index,
            start_at: "I0".to_string(),
            input,
        })
    }

    /// `activate` folds the iteration plan onto the activity and fans out only the first batch — the
    /// cap's worth, not the whole array. The plan itself rides `StateActivated`, since a later
    /// replenish round can no longer re-derive it from the input.
    #[tokio::test]
    async fn activate_builds_the_iteration_plan_and_fans_the_first_batch_out() {
        let activated = activate(
            &map_state(
                Some(MapItems::Array(vec![json!(1), json!(2), json!(3)])),
                Some(2),
                None,
            ),
            &activate_cmd(path("/States/P"), seeded_input()),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;

        let birth = minted_activity(path("/States/P"), seeded_input());
        let mut sealed = birth.clone();
        // `initialize` seeds the empty scaffold the plan then replaces.
        sealed.activity_state = Some(ActivityState::Map(MapActivityState::default()));

        assert_eq!(
            activated.chain(),
            vec![
                EntryPayload::Event(Event::StateActivating { activity: sealed }),
                EntryPayload::Event(Event::StateActivated {
                    activity: activated_activity(planned(vec![json!(1), json!(2), json!(3)], 2))
                }),
                // Two items, because the cap bounds the first batch; the third arrives by replenish.
                EntryPayload::Command(item_spawn(0, json!(1))),
                EntryPayload::Command(item_spawn(1, json!(2))),
            ]
        );
    }

    /// With no `Items` the state's own input is the array to iterate — the ASL default — and an
    /// unlimited cap (the field's default 0) fans the whole thing out at once.
    #[tokio::test]
    async fn activate_by_defaults_to_the_input_array() {
        let items = json!([1, 2]);
        let activated = activate(
            &map_state(None, None, None),
            &activate_cmd(path("/States/P"), items.clone()),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;

        let mut sealed = minted_activity(path("/States/P"), items.clone());
        sealed.activity_state = Some(ActivityState::Map(MapActivityState::default()));
        let mut with_plan = sealed.clone();
        with_plan.input = Some(items.clone());
        with_plan.activity_state = Some(planned(vec![json!(1), json!(2)], 0));

        assert_eq!(
            activated.chain(),
            vec![
                EntryPayload::Event(Event::StateActivating { activity: sealed }),
                EntryPayload::Event(Event::StateActivated {
                    activity: with_plan
                }),
                EntryPayload::Command(item_spawn(0, json!(1))),
                EntryPayload::Command(item_spawn(1, json!(2))),
            ]
        );
    }

    /// No `Items` and an input that is not an array is a definition error: there is nothing to
    /// iterate, so the activity is unwound and the failure routed at the owning scope.
    #[tokio::test]
    async fn activate_fails_when_items_do_not_resolve_to_an_array() {
        let activated = activate(
            &map_state(None, None, None),
            &activate_cmd(path("/States/P"), seeded_input()),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;

        let reason = TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "Map 'P' has no Items and its input is not an array".to_string(),
            )),
        };
        assert_eq!(
            activated.chain(),
            vec![
                EntryPayload::Event(Event::StateActivating {
                    activity: {
                        let mut sealed = minted_activity(path("/States/P"), seeded_input());
                        sealed.activity_state =
                            Some(ActivityState::Map(MapActivityState::default()));
                        sealed
                    }
                }),
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

    /// An empty `Items` spawns no children, so no settle will ever drive convergence — the container
    /// converges on the spot to the empty aggregated array rather than wedging.
    #[tokio::test]
    async fn activate_converges_a_map_with_no_items() {
        let activated = activate(
            &map_state(Some(MapItems::Array(vec![])), Some(2), Some(true)),
            &activate_cmd(path("/States/P"), seeded_input()),
            Some(seeded_scope(ThreadStatus::Running)),
        )
        .await;

        let mut completing = activated_activity(planned(vec![], 2));
        // The finish falls back to the raw input as the raw result when the activity carries none.
        completing.raw_output = Some(seeded_input());
        completing.status = ActivityStatus::Completing;
        let mut done = completing.clone();
        done.status = ActivityStatus::Completed;
        done.output = Some(json!([]));

        assert_eq!(
            activated.chain(),
            vec![
                EntryPayload::Event(Event::StateActivating {
                    activity: {
                        let mut sealed = minted_activity(path("/States/P"), seeded_input());
                        sealed.activity_state =
                            Some(ActivityState::Map(MapActivityState::default()));
                        sealed
                    }
                }),
                EntryPayload::Event(Event::StateActivated {
                    activity: activated_activity(planned(vec![], 2))
                }),
                EntryPayload::Event(Event::StateCompleting {
                    activity: completing
                }),
                EntryPayload::Event(Event::StateCompleted { activity: done }),
                EntryPayload::Command(Command::CompleteThread(CompleteThread {
                    thread: thread_ref(),
                    output: json!([]),
                })),
            ]
        );
    }

    /// The base `finish` is a defensive fallback: reached only by a stray `CompleteState` on the
    /// container (a real convergence goes through `finish_map`), it ignores the command's raw result
    /// and completes the activity with its own input, routing down the terminal branch.
    #[tokio::test]
    async fn complete_completes_the_container_terminal() {
        let completed = complete(
            &map_state(Some(MapItems::Array(vec![])), Some(2), Some(true)),
            complete_store(seeded_input(), []).await,
            &complete_cmd(json!({ "ignored": true })),
        )
        .await;

        let mut completing = minted_activity(path("/States/P"), seeded_input());
        completing.input = Some(seeded_input());
        completing.raw_output = Some(json!({ "ignored": true }));
        completing.status = ActivityStatus::Completing;
        let mut done = completing.clone();
        done.status = ActivityStatus::Completed;
        done.output = Some(seeded_input());

        assert_eq!(
            completed.chain(),
            vec![
                EntryPayload::Event(Event::StateCompleting {
                    activity: completing
                }),
                EntryPayload::Event(Event::StateCompleted { activity: done }),
                EntryPayload::Command(Command::CompleteThread(CompleteThread {
                    thread: thread_ref(),
                    output: seeded_input(),
                })),
            ]
        );
    }

    /// The item index → child thread map a fanned-out `Map` activity carries. Written by the
    /// `ThreadCreated` fold in production; a test writes it down directly, and writes it *out of
    /// order* so the aggregation's index sort is what the assertion actually pins.
    fn item_children(indices: &[usize]) -> HashMap<usize, ObjectReference> {
        indices.iter().rev().map(|i| (*i, child_ref(*i))).collect()
    }

    fn item_child(index: usize, output: Value, status: ThreadStatus) -> ThreadRecord {
        seeded_child_thread(
            path("/States/P/ItemProcessor/States"),
            index,
            output,
            status,
        )
    }

    /// A `Running` container activity carrying the iteration plan the input produced.
    fn planned_container(
        items: Vec<Value>,
        max_concurrency: usize,
        children: HashMap<usize, ObjectReference>,
        active_children: impl IntoIterator<Item = ObjectReference>,
    ) -> ActivityRecord {
        let total = items.len();
        seeded_activity_with(
            path("/States/P"),
            seeded_input(),
            ActivityState::Map(MapActivityState {
                items,
                total,
                max_concurrency,
                children,
            }),
            ActivityStatus::Running,
            active_children,
        )
    }

    /// The last item's settle converges: every item's output aggregated in *item* order (not the order
    /// the children happen to live in the map), then the state's success finish with that array.
    #[tokio::test]
    async fn child_convergence_aggregates_items_in_index_order() {
        let items = vec![json!(1), json!(2)];
        let mut store = InMemoryStorage::new();
        seed_container(
            &mut store,
            planned_container(items.clone(), 0, item_children(&[0, 1]), []),
            [
                item_child(1, json!({ "i": 1 }), ThreadStatus::Completed),
                item_child(0, json!({ "i": 0 }), ThreadStatus::Completed),
            ],
        )
        .await;

        let aggregated = json!([{ "i": 0 }, { "i": 1 }]);
        let completed = child_completed(
            &map_state(Some(MapItems::Array(items)), None, Some(true)),
            store,
            minted_activity_ref(),
            child_ref(1),
        )
        .await;

        let mut finishing = minted_activity(path("/States/P"), seeded_input());
        finishing.input = Some(seeded_input());
        finishing.activity_state = Some(ActivityState::Map(MapActivityState {
            items: vec![json!(1), json!(2)],
            total: 2,
            max_concurrency: 0,
            children: item_children(&[0, 1]),
        }));
        // A container activity carries no raw result of its own, so the finish falls back to its input.
        finishing.raw_output = Some(seeded_input());
        finishing.status = ActivityStatus::Completing;
        let mut done = finishing.clone();
        done.status = ActivityStatus::Completed;
        done.output = Some(aggregated.clone());

        assert_eq!(
            completed.chain(),
            vec![
                EntryPayload::Event(Event::StateCompleting {
                    activity: finishing
                }),
                EntryPayload::Event(Event::StateCompleted { activity: done }),
                EntryPayload::Command(Command::CompleteThread(CompleteThread {
                    thread: thread_ref(),
                    output: aggregated,
                })),
            ]
        );
    }

    /// A settle that is not the last one refills the slot it freed instead of converging: with a cap of
    /// 1 and 3 items, item 0's settle pulls in item 1 — the next never-spawned index, not the next
    /// free ordinal.
    #[tokio::test]
    async fn child_settle_replenishes_a_freed_slot() {
        let items = vec![json!(1), json!(2), json!(3)];
        let mut store = InMemoryStorage::new();
        seed_container(
            &mut store,
            planned_container(items.clone(), 1, item_children(&[0]), []),
            [item_child(0, json!(null), ThreadStatus::Completed)],
        )
        .await;

        let completed = child_completed(
            &map_state(Some(MapItems::Array(items)), Some(1), Some(true)),
            store,
            minted_activity_ref(),
            child_ref(0),
        )
        .await;

        assert_eq!(
            completed.chain(),
            vec![EntryPayload::Command(Command::SpawnThread(SpawnThread {
                owner: minted_activity_ref(),
                execution: execution_ref(),
                state_path: Some(path("/States/P/ItemProcessor/States")),
                index: 1,
                start_at: "I0".to_string(),
                input: json!(2),
            }))]
        );
    }

    /// A settle that frees no slot — the cap is still occupied by an in-flight item — has nothing to
    /// replenish and has not converged either: a transient state whose sibling settle will move it on.
    #[tokio::test]
    async fn child_settle_with_no_free_slot_replenishes_nothing() {
        let items = vec![json!(1), json!(2), json!(3)];
        let mut store = InMemoryStorage::new();
        // Item 0 is still running *and* still attached, so the cap-1 window is full; item 1 is the
        // settle being driven.
        seed_container(
            &mut store,
            planned_container(items.clone(), 1, item_children(&[0, 1]), [child_ref(0)]),
            [
                item_child(0, json!(null), ThreadStatus::Running),
                item_child(1, json!(null), ThreadStatus::Completed),
            ],
        )
        .await;

        let completed = child_completed(
            &map_state(Some(MapItems::Array(items)), Some(1), Some(true)),
            store,
            minted_activity_ref(),
            child_ref(1),
        )
        .await;

        assert!(
            completed.chain().is_empty(),
            "a settle with no free slot emits nothing: {:?}",
            completed.chain()
        );
    }

    /// An item that settled by failing fails the whole `Map` — with the default tolerance of 0, one
    /// failure is one too many — and the activity's termination is what stops the in-flight siblings.
    #[tokio::test]
    async fn child_failure_fails_the_container() {
        let items = vec![json!(1), json!(2)];
        let failure = TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "I0".to_string(),
                error: "boom".to_string(),
                output: Box::new(json!(null)),
            }),
        };
        let mut store = InMemoryStorage::new();
        seed_container(
            &mut store,
            planned_container(items.clone(), 0, item_children(&[0, 1]), [child_ref(0)]),
            [
                item_child(0, json!(null), ThreadStatus::Running),
                item_child(1, json!(null), ThreadStatus::Terminated(failure)),
            ],
        )
        .await;

        let completed = child_completed(
            &map_state(Some(MapItems::Array(items)), None, None),
            store,
            minted_activity_ref(),
            child_ref(1),
        )
        .await;

        // The container reports the failure in its own terms — the item's own reason is not propagated,
        // because an item's failure is a Map-level fact about *which* item failed, which the Map names.
        let reason = TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "P".to_string(),
                error: "Map item failed".to_string(),
                output: Box::new(json!(null)),
            }),
        };
        assert_eq!(
            completed.chain(),
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

    /// A child the plan does not know is not this Map's to react to — a settle under the same activity
    /// from some other fan-out leaves the plan untouched and emits nothing.
    #[tokio::test]
    async fn child_settle_ignores_an_unknown_child() {
        let items = vec![json!(1), json!(2)];
        let mut store = InMemoryStorage::new();
        seed_container(
            &mut store,
            planned_container(items.clone(), 0, item_children(&[0, 1]), []),
            [item_child(0, json!(null), ThreadStatus::Completed)],
        )
        .await;

        let completed = child_completed(
            &map_state(Some(MapItems::Array(items)), None, None),
            store,
            minted_activity_ref(),
            // Not one of the plan's children.
            child_ref(9),
        )
        .await;

        assert!(
            completed.chain().is_empty(),
            "a settle from outside the plan emits nothing: {:?}",
            completed.chain()
        );
    }
}
