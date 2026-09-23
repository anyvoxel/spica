use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{AssignObject, State};
use std::collections::HashMap;
use std::mem::Discriminant;

use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext};
use crate::log::Timestamp;
use crate::types::command::{ActivateState, Command, CompleteState};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::{Event, VariablesAssigned};
use crate::types::meta::{ObjectKind, ObjectMeta, ObjectReference};
use crate::{Activity, ActivityStatus, Variables};

/// The registered, stateless `State` → factory entry: identifies the [`State`] variant it serves
/// (via [`Self::state`], the single source of truth for the dispatch-table key) and builds a
/// short-lived, typed [`StateHandler`] for a single lifecycle dispatch. The factory holds no
/// per-activity state — the mutable execution resources (`EvalEnv`, `HandlerContext`, `Collector`)
/// travel as method parameters, while the immutable resolved [`State`] definition is
/// bound into the created handler.
#[async_trait]
pub trait StateHandlerFactory: Send + Sync {
    /// The [`State`] variant this factory serves, identified by a `Default` instance of the wrapped
    /// definition standing in only to read its discriminant — the real activity instance the table
    /// later dispatches on is built by the framework. Takes `&self` (rather than being a
    /// `Self: Sized` associated function) so the trait stays object-safe for the
    /// `Box<dyn StateHandlerFactory>` dispatch table.
    fn state(&self) -> State;

    /// Create a handler bound to the resolved `state`. Matches the `State` enum once here, returning
    /// an adapter that holds the already-`unreachable!`-checked, typed definition (`&PassState`,
    /// `&TaskState`, …) so the lifecycle hooks no longer downcast or thread `&State` around.
    ///
    /// The bound handler borrows `state` (which borrows a cached `Arc<StateMachine>`), so it must
    /// be created and consumed within the same lexical scope and never stored, returned beyond it,
    /// or sent to a spawned task — the borrow checker enforces this.
    fn create<'a>(&self, state: &'a State) -> Box<dyn StateHandler + 'a>;
}

/// The shared `State` → [`StateHandlerFactory`] dispatch table, keyed by
/// [`Discriminant<State>`](std::mem::Discriminant) and owned once per [`crate::StreamProcessor`].
/// It is threaded into every [`HandlerContext`](crate::handler::HandlerContext) so both the
/// `ActivateState` and `CompleteState` command handlers (and the inline child-settled reaction) go
/// through the same lookup. [`Self::create`] collapses the lookup + factory dispatch into one step:
/// resolve the definition's variant, find its factory, create a handler bound to the definition.
pub struct StateHandlerRegistry {
    map: HashMap<Discriminant<State>, Box<dyn StateHandlerFactory>>,
}

impl StateHandlerRegistry {
    /// An empty registry; entries are registered by
    /// [`build_state_handlers`](crate::handlers::build_state_handlers) before the processor runs.
    pub(crate) fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Register `factory`, keyed by the [`State`] variant its [`StateHandlerFactory::state`] serves.
    /// The factory is the single source of truth for its own table key — no hand-written key to keep
    /// in sync.
    pub(crate) fn insert(&mut self, factory: Box<dyn StateHandlerFactory>) {
        let key = std::mem::discriminant(&factory.state());
        self.map.insert(key, factory);
    }

    /// Look up the factory serving `state`'s variant and create a handler bound to `state`. `None`
    /// when no factory is registered for that variant (an unsupported state type in M1).
    pub fn create<'a>(&self, state: &'a State) -> Option<Box<dyn StateHandler + 'a>> {
        self.map
            .get(&std::mem::discriminant(state))
            .map(|factory| factory.create(state))
    }

    /// The number of registered factories (all supported `State` variants).
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Iterate the registered factories (test helper for exercising the binding contract).
    #[cfg(test)]
    pub fn values(&self) -> impl Iterator<Item = &Box<dyn StateHandlerFactory>> {
        self.map.values()
    }
}

/// A short-lived handler bound to one resolved [`State`] definition. Owned for a single lifecycle
/// dispatch (activate / complete / child-settled) and dropped once it returns. A concrete adapter
/// holds a typed definition reference (e.g. `&TaskState`), so the per-variant hooks read it directly
/// instead of receiving `&State` and re-matching.
///
/// The base [`activate`](Self::activate) orchestration (a Template Method) constructs the activity,
/// runs the four activate hooks (`initialize` / `process_input` / `after_activated` /
/// `complete_directly`) in a fixed order, and hands into `CompleteState`, so synchronous states
/// (Pass/Succeed/Fail/Choice) share one code path instead of each re-emitting `StateActivated` +
/// `CompleteState`. The activate causal chain is
/// `StateActivating → StateActivated → (CompleteState | side effect)`.
#[async_trait]
pub trait StateHandler: Send + Sync {
    // ── activate hooks — the only per-state variation ────────────────────────

    /// (2.2) Scaffold the freshly-constructed activity *before* its input is known. Only fills what
    /// is derivable from the definition alone (a `Parallel`'s branch map, a `Map`'s empty plan);
    /// anything input-derived (a `Map`'s item plan) must wait for `process_input`.
    async fn initialize(&self, _activity: &mut Activity) {}

    /// (2.4) Turn the activity's `raw_input` into its processed input. Default: pass it through. A
    /// `Task`/`Parallel` project `Arguments`; a `Map` harvests its item plan onto `activity_state`.
    /// An error is returned for the base to terminate on. `activity` is the state being processed
    /// (read `raw_input`, write `activity_state`); `variables` is its scope's bindings, which the
    /// state-specific projections evaluate against — that reads live in the hook because the
    /// activity does not carry scope state. `states` is the activate-step `$states`, built once in
    /// the base (uniform across every state) and shared by all hooks.
    async fn process_input(
        &self,
        _env: &mut EvalEnv,
        activity: &mut Activity,
        _variables: &Variables,
        _states: &Value,
    ) -> Result<Value, ExecutionError> {
        Ok(activity.raw_input.clone())
    }

    /// (2.6) Post-activation side effects: arm a timer (`Wait`, `Task` timeout), throw an external
    /// call (`Task`), or fan out children (`Parallel`/`Map`). An error terminates the activity via
    /// the base.
    async fn after_activated(
        &self,
        _env: &mut EvalEnv,
        _out: &mut Collector<'_>,
        _activity_value: &Activity,
        _variables: &Variables,
        _states: &Value,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    /// (2.7) Whether to complete directly (hand straight into a `CompleteState` command) after this
    /// activation — `true` for synchronous states. States that armed a side effect (`Wait`/`Task`)
    /// or fanned out (`Parallel`/`Map`) return `false` and wait for their own completion.
    fn complete_directly(&self, _activity: &Activity) -> bool {
        true
    }

    /// A child node of this state reached a terminal state while the state is `Running` — the
    /// **replenish** half of the child-settled reaction (see `child_completed`'s docs for the
    /// drain-vs-replenish split). Only a container state (`Parallel`/`Map`) implements this; default
    /// stays a safe no-op for leaf states.
    async fn child_completed(
        &self,
        _ctx: &mut HandlerContext<'_>,
        _out: &mut Collector<'_>,
        _activity: ObjectReference,
        _activity_value: &Activity,
        _variables: &Variables,
        _child: ObjectReference,
    ) {
    }

    /// Apply a state's `Assign` delta (when present) onto the scope variables in place, emitting
    /// `VariablesAssigned` for the owning scope when the eval yields a non-empty object — the
    /// complete-step projection every state shares. `Ok(())` when no assign is present or it applies
    /// cleanly; an eval failure or a non-object result returns `Err`, which the caller's `fail_or!`
    /// turns into the same terminate + return the inline block used to produce. Kept on the base so an
    /// `Assign` is handled identically across states rather than copy-pasted.
    async fn apply_assign(
        &self,
        out: &mut Collector<'_>,
        env: &mut EvalEnv,
        owner: &ObjectReference,
        assign: Option<&AssignObject>,
        states: &Value,
        scope: &mut Variables,
    ) -> Result<(), ExecutionError> {
        let Some(assign) = assign else {
            return Ok(());
        };
        let assign_value = Value::Object(assign.0.clone());
        let evaluated = env.eval_json(&assign_value, states, scope)?;
        match evaluated {
            Value::Object(map) => {
                if !map.is_empty() {
                    for (k, v) in map {
                        scope.insert(k, v);
                    }
                    out.append_event(Event::VariablesAssigned(VariablesAssigned {
                        scope: owner.clone(),
                        variables: scope.clone(),
                    }))
                    .await;
                }
                Ok(())
            }
            _ => Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                "Assign must evaluate to a JSON object".to_string(),
            ))),
        }
    }

    /// Evaluate a state's complete-step `Output` (when present) against the folded scope; without
    /// one, the state's raw result passes through. An eval failure returns `Err`, which the caller's
    /// `fail_or!` turns into the terminate + return shared by every state.
    async fn project_output(
        &self,
        env: &mut EvalEnv,
        output: Option<&Value>,
        states: &Value,
        scope: &Variables,
        fallback: Value,
    ) -> Result<Value, ExecutionError> {
        match output {
            Some(output) => env.eval_json(output, states, scope),
            None => Ok(fallback),
        }
    }

    // ── lifecycle operations ────────────────────────────────────────────────

    /// The `Command::ActivateState` flow, owned by the base. Its only input is the typed
    /// [`ActivateState`] payload; the bound definition supplies the typed state and the activity is
    /// constructed here, so the actual fan-out/owner/meta derivation lives once. Receiving the
    /// payload by its own type (not `&Command`) makes the dispatch a compile-time guarantee.
    async fn activate(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        cmd: &ActivateState,
    ) {
        let ActivateState {
            execution,
            owner,
            state_path,
            input,
        } = cmd;

        // (2.1) Construct the activity value in one place: mint its incarnation uid, name it as a
        // child of the owning execution (finding #3), and build the full meta with the command's
        // owner as its scope (`created_at == updated_at` is the entry moment). The canonical
        // ObjectReference is derived from the value, so the uid/name live in exactly one spot.
        let mut activity_value = Activity {
            execution: execution.clone(),
            state_path: state_path.clone(),
            status: ActivityStatus::Running,
            raw_input: input.clone(),
            input: None,
            raw_output: None,
            activity_state: None,
            retry_state: None,
            output: None,
            meta: ObjectMeta::builder(ObjectKind::Activity, ulid::Ulid::new())
                .name(
                    execution
                        .name
                        .base()
                        .generated_from_key(out.next_generated_seq().await),
                )
                .at(Timestamp::now())
                .build()
                .with_owner(owner.clone()),
        };
        let activity = activity_value.reference();

        // The activity lives in a scope (`Execution` or a fan-out `Thread`) addressed by the
        // command's `owner`; load it to derive the scope variables the hooks evaluate against and
        // confirm the scope still accepts transitions.
        let scope = match crate::storage::load_scope_ref(ctx.storage, owner).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                out.terminate(
                    Some(activity.clone()),
                    execution.clone(),
                    ExecutionError::Runtime(RuntimeError::StateNotFound(format!(
                        "execution {execution}"
                    ))),
                );
                return;
            }
            Err(e) => {
                out.terminate(Some(activity.clone()), execution.clone(), e);
                return;
            }
        };
        if !scope.is_running() {
            return; // scope not running — a rescheduled activate is a no-op.
        }

        let variables = scope.variables().clone();
        let state_name = activity_value.state_path.state_name();

        self.initialize(&mut activity_value).await;

        // (2.3)
        out.append_event(Event::StateActivating {
            activity: activity_value.clone(),
        })
        .await;

        // The activate-step `$states`, uniform across every state — `result` is null (no result yet)
        // and `assign_ctx = None` (the state's own `Assign` applies in `complete`). Built once and
        // shared by all hooks, so a state's processing never re-derives it.
        let states = States::new(
            &activity_value.raw_input,
            &state_name,
            activity_value.retry_count(),
        )
        .build();

        // (2.4)
        let input = match self
            .process_input(ctx.env, &mut activity_value, &variables, &states)
            .await
        {
            Ok(input) => input,
            Err(e) => {
                out.terminate(Some(activity), owner.clone(), e);
                return;
            }
        };
        activity_value.input = Some(input.clone());

        // (2.5) `activity_value` already carries the processed input (`input`, set above) and, for a
        // container state, its activation plan written by `process_input` — so `StateActivated`
        // reuses the in-place value with a fresh `updated_at` stamp.
        activity_value.meta.with_update_at(Timestamp::now());
        out.append_event(Event::StateActivated {
            activity: activity_value.clone(),
        })
        .await;

        // (2.6)
        if let Err(e) = self
            .after_activated(ctx.env, out, &activity_value, &variables, &states)
            .await
        {
            out.terminate(Some(activity), owner.clone(), e);
            return;
        }

        // (2.7)
        if self.complete_directly(&activity_value) {
            out.append_command(Command::CompleteState(CompleteState {
                activity,
                output: input.clone(),
            }));
        }
    }

    /// The `Command::CompleteState` flow, implemented fully by each state: load the activity, drive
    /// the liveness/Terminating-race guards, resolve the owning scope, reconstruct the activity and
    /// variables, and run the state's own finish projection (`StateCompleting`/`StateCompleted`, or `Fail`'s
    /// terminate) — all inline, so a single handler is self-contained. Receiving the activity by
    /// reference (not `&Command`) keeps the dispatch a compile-time guarantee.
    async fn complete(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity: ObjectReference,
        raw_result: Option<&Value>,
    );
}
#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::super::dispatch::build_state_handlers;
    use crate::types::meta::{ObjectKind, ObjectReference};
    use crate::types::state_path::StatePath;
    use crate::{Activity, ActivityStatus};

    /// A minimal empty `Activity` sufficient to dispatch an object-safe lifecycle hook — the create
    /// contract only cares that the hook *dispatches*, not what it does.
    fn empty_activity() -> Activity {
        Activity {
            meta: crate::types::meta::ObjectMeta::builder(ObjectKind::Activity, ulid::Ulid::new())
                .build(),
            execution: ObjectReference::nil(),
            state_path: StatePath::from(jsonptr::PointerBuf::new()),
            status: ActivityStatus::Running,
            raw_input: Value::Null,
            input: None,
            raw_output: None,
            activity_state: None,
            retry_state: None,
            output: None,
        }
    }

    /// Every registered factory must create a handler bound to its *own* variant stub produced by
    /// [`StateHandlerFactory::state`] — the round-trip that validates the factory/`create` selection
    /// contract for all 8 supported states. A lifecycle method is invoked to prove the binding is
    /// live (the default `complete_directly` is overridable per state, so its value is not asserted,
    /// only that it dispatches to the per-state override rather than the trait default).
    #[test]
    fn every_registered_handler_binds_its_own_variant() {
        let registry = build_state_handlers();
        // All 8 supported variants are registered — nothing missing from `build_state_handlers`.
        assert_eq!(registry.len(), 8);

        for factory in registry.values() {
            let sample = factory.state();
            let handler = factory.create(&sample);
            // `complete_directly` is the cheapest object-safe lifecycle hook; calling it proves the
            // bound adapter dispatched without panicking on the mismatch-free create.
            let _ = handler.complete_directly(&empty_activity());
        }
    }

    /// Feeding a factory a definition of the *wrong* variant is an internal invariant violation —
    /// dispatch always selects by `Discriminant<State>` before creating, so the mismatch is only
    /// ever reachable from a programming error, and `create` fails loudly via `unreachable!` rather
    /// than silently misbinding.
    #[test]
    #[should_panic(expected = "create dispatch guarantees")]
    fn create_panics_on_variant_mismatch() {
        let registry = build_state_handlers();
        let factories = registry.values().collect::<Vec<_>>();
        // Two distinct variants: the first entry's stub is the wrong definition for the last.
        let wrong_def = factories[0].state();
        let other = factories[factories.len() - 1];
        other.create(&wrong_def);
    }
}
