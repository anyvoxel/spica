use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{AssignObject, State};
use std::collections::HashMap;
use std::mem::Discriminant;

use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::storage::{ActivityRecord, ThreadRecord};
use crate::types::activity::ActivityKind;
use crate::types::command::{
    ActivateState, Command, CompleteState, TerminateState, TerminationReason,
};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::{Event, VariablesAssigned};
use crate::types::meta::{ObjectMeta, ObjectRef, RawObjectRef};
use crate::types::thread::ThreadKind;
use crate::{Activity, ActivityStatus, RejectionType, Timestamp, Variables};

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

/// The state's answer to "is this failure mine to route?", returned by
/// [`StateHandler::on_failed`]. A named value rather than a `bool` because the two answers differ in
/// what the *caller* does next: a caught failure has already been routed, so the teardown that asked
/// must stop; an uncaught one stands and the teardown proceeds.
pub enum FailureRouting {
    /// The state's error policy took the failure — it has already routed the activity along the
    /// catcher's `Next`, so the activity is completing rather than terminating.
    Caught,
    /// No policy of this state matches: the failure stands and the teardown proceeds.
    Uncaught,
}

/// A short-lived handler bound to one resolved [`State`] definition. Owned for a single lifecycle
/// dispatch (activate / complete / child-settled) and dropped once it returns. A concrete adapter
/// holds a typed definition reference (e.g. `&TaskState`), so the per-variant hooks read it directly
/// instead of receiving `&State` and re-matching.
///
/// Both lifecycle operations are Template Methods owned by the base, each ending in a single per-state
/// call. [`activate`](Self::activate) constructs the activity, runs the four activate hooks
/// (`initialize` / `process_input` / `after_activated` / `complete_directly`) in a fixed order, and
/// hands into `CompleteState`, so synchronous states (Pass/Succeed/Fail/Choice) share one code path
/// instead of each re-emitting `StateActivated` + `CompleteState`. [`complete`](Self::complete) is the
/// drawer alone — fold the raw result in, open the activity with `StateCompleting` — and hands the rest
/// to [`after_completing`](Self::after_completing), so a state's whole complete path lives in one
/// method of its own. The activate causal chain is `StateActivating → StateActivated → (CompleteState |
/// side effect)`; the complete chain is `StateCompleting → (StateCompleted | failure ed) → transition`.
#[async_trait]
pub trait StateHandler: Send + Sync {
    // ── activate hooks — the only per-state variation ────────────────────────

    /// (2.2) Scaffold the freshly-constructed activity *before* its input is known. Only fills what
    /// is derivable from the definition alone (a `Parallel`'s branch map, a `Map`'s empty plan);
    /// anything input-derived (a `Map`'s item plan) must wait for `process_input`.
    async fn initialize(&self, _activity: &mut Activity) {}

    /// (2.4) Turn the activity's `raw_input` into its processed input. Default: pass it through. A
    /// `Task`/`Parallel` project `Arguments`; a `Map` harvests its item plan onto `activity_state`,
    /// and a `Wait` its resume instant. An error is returned for the base to terminate on.
    /// `activity` is the state being processed (read `raw_input`, write `activity_state`);
    /// `variables` is its scope's bindings, which the state-specific projections evaluate against —
    /// that reads live in the hook because the activity does not carry scope state. `states` is the
    /// activate-step `$states`, built once in the base (uniform across every state) and shared by all
    /// hooks. `now` is the entry instant, passed explicitly so a repository that records a *deadline*
    /// (a `Wait`'s resume) resolves it against the same clock reading the base stamps the activity
    /// with, rather than re-reading a clock the hook cannot see.
    async fn process_input(
        &self,
        _env: &mut EvalEnv,
        activity: &mut Activity,
        _variables: &Variables,
        _states: &Value,
        _now: Timestamp,
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
    /// **replenish** half of the child-settled reaction, reached from the owning activity's container
    /// (see `ActivityContainer::settle_running`, which routes a settle to the four states that own
    /// children). A state that owns no children keeps the default no-op, since no settle can reach it.
    async fn child_completed(
        &self,
        _ctx: &mut HandlerContext<'_>,
        _out: &mut Collector<'_>,
        _activity: ObjectRef<ActivityKind>,
        _activity_value: &Activity,
        _variables: &Variables,
        _child: RawObjectRef,
    ) {
    }

    // ── complete hooks — the only per-state variation of the complete step ──

    /// (3.4) The state's own complete step, run by the base [`complete`](Self::complete) once the
    /// activity has been opened with `StateCompleting`. This is the *single* per-state entry for the
    /// complete path — every state writes its own, so no state's completion is inherited by accident:
    /// a state disposes of whatever must not outlive its own decision here and then runs the finish.
    async fn after_completing(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<(), ProcessingError>;

    /// The state's own terminate step, run by the base [`terminate`](Self::terminate) once the
    /// activity has been opened with `StateTerminating` and the scope notified. This is the *single*
    /// per-state entry for the terminate path — the failure mirror of
    /// [`after_completing`](Self::after_completing): each state disposes of whatever must not outlive
    /// its own close and then emits `StateTerminated` and relays the settle. A state that still owns
    /// live children (a `Wait`'s deadline, a `Task`'s call, a `Parallel`/`Map`'s fan-out) sweeps them
    /// and defers — the last settle drains via the generic `Terminating` path — while a childless
    /// state closes inline.
    async fn after_terminating(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<(), ProcessingError>;

    /// Apply a state's `Assign` delta (when present) onto the scope variables in place, emitting
    /// `VariablesAssigned` for the owning scope when the eval yields a non-empty object — the
    /// complete-step projection every state shares. `Ok(())` when no assign is present or it applies
    /// cleanly; an eval failure or a non-object result returns `Err`, which the caller turns into a
    /// `TerminateState` for the activity. Kept on the base so an `Assign` is handled identically across
    /// states rather than copy-pasted.
    async fn apply_assign(
        &self,
        out: &mut Collector<'_>,
        env: &mut EvalEnv,
        owner: &ObjectRef<ThreadKind>,
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
    /// one, the state's raw result passes through. An eval failure returns `Err`, which the caller turns
    /// into a `TerminateState` for the activity.
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

    // ── failure hook ────────────────────────────────────────────────────────

    /// This state's own failure has reached its teardown — take it if this state has a policy for it.
    /// The default declines (the failure stands and the teardown proceeds); a state whose definition
    /// carries an ASL `Catch` (a `Task`) overrides it to route the activity to the catcher's `Next`.
    ///
    /// The decision belongs to the state because the policy *is* a field of its definition, and it
    /// arrives through this hook rather than a `match` at the terminate site for the same reason every
    /// other per-state behaviour does: [`StateHandlerFactory`] is the one place the [`State`] enum is
    /// matched, so no lifespan site ever downcasts a definition.
    ///
    /// The caller asks only for a failure the activity **itself** produced. A swept activity — one an
    /// ancestor's teardown reached — is never asked, which is what keeps a sibling from being routed
    /// by a policy that was never about it.
    async fn on_failed(
        &self,
        _ctx: &mut HandlerContext<'_>,
        _out: &mut Collector<'_>,
        _activity_value: &Activity,
        _error: &ExecutionError,
    ) -> FailureRouting {
        FailureRouting::Uncaught
    }

    // ── lifecycle operations ────────────────────────────────────────────────

    /// The `Command::ActivateState` flow, owned by the base. Its only input is the typed
    /// [`ActivateState`] payload; the bound definition supplies the typed state and the activity is
    /// constructed here, so the actual fan-out/owner/meta derivation lives once. Receiving the
    /// payload by its own type (not `&Command`) makes the dispatch a compile-time guarantee, and the
    /// payload carries its owner in the slot's own type, so no owner argument is threaded beside it.
    /// `thread` is the row the dispatcher already read for that owner — it resolves the definition
    /// from the same row and screened it for liveness before admitting the command, so the base
    /// re-reads nothing and every remaining failure of its own is a domain one that settles the run in
    /// place. The `Err` half of the return type is the shape of that contract, kept for a precondition
    /// landing back in this layer; no arm returns one today.
    async fn activate(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        cmd: &ActivateState,
        thread: &ThreadRecord,
    ) -> Result<(), ProcessingError> {
        let ActivateState {
            execution,
            owner,
            state_path,
            input,
        } = cmd;

        // (2.1) Construct the activity value in one place: mint its incarnation uid, name it as a
        // child of the owning execution (finding #3), and build the full meta with the command's
        // owner as its scope (`created_at == updated_at` is the entry moment). The canonical
        // RawObjectRef is derived from the value, so the uid/name live in exactly one spot.
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
            meta: ObjectMeta::builder(ctx.mint())
                .name(
                    execution
                        .name()
                        .base()
                        .generated_from_key(out.next_generated_seq().await),
                )
                .at(ctx.now())
                .with_owner(owner.clone()),
        };
        let activity = ObjectRef::<ActivityKind>::new(
            activity_value.meta.name.clone(),
            activity_value.meta.uid,
        );

        // The activity's owner is always a `Thread` — the derived root thread for a top-level run, a
        // fan-out thread for a branch/item — which the slot's own type guarantees. The row arrives
        // already read and screened, and it is exactly what yields the variables the hooks evaluate
        // against.
        let variables = thread.variables.clone();
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
            .process_input(ctx.env, &mut activity_value, &variables, &states, ctx.now())
            .await
        {
            Ok(input) => input,
            Err(e) => {
                // Past the first emit: the run's death *is* this command's outcome, so it settles here
                // rather than leaving `StateActivating`/`StateActivated` and a Reject on one entry.
                // Only the activity is named — its scope (the row read above) is taken down by the
                // handler that runs this terminate, so nothing here names it a second time.
                out.append_command(Command::TerminateState(TerminateState {
                    activity: activity.clone(),
                    reason: TerminationReason::Failed { error: e },
                }));
                return Ok(());
            }
        };
        activity_value.input = Some(input.clone());

        // (2.5) `activity_value` already carries the processed input (`input`, set above) and, for a
        // container state, its activation plan written by `process_input` — so `StateActivated`
        // reuses the in-place value with a fresh `updated_at` stamp.
        activity_value.meta.with_update_at(ctx.now());
        out.append_event(Event::StateActivated {
            activity: activity_value.clone(),
        })
        .await;

        // (2.6)
        if let Err(e) = self
            .after_activated(ctx.env, out, &activity_value, &variables, &states)
            .await
        {
            out.append_command(Command::TerminateState(TerminateState {
                activity: activity.clone(),
                reason: TerminationReason::Failed { error: e },
            }));
            return Ok(());
        }

        // (2.7)
        if self.complete_directly(&activity_value) {
            out.append_command(Command::CompleteState(CompleteState {
                activity,
                output: input.clone(),
            }));
        }

        Ok(())
    }

    /// The `Command::CompleteState` flow, owned by the base — the mirror of [`Self::activate`]. The
    /// typed [`CompleteState`] payload supplies the activity and the raw result; `act` and `thread`
    /// are the rows the dispatcher already read for them, screened for liveness before it admitted the
    /// command, so the base re-reads nothing. What remains is the *drawer* — fold the command's raw
    /// result in and open the activity with `StateCompleting` — and then the state's own
    /// [`after_completing`](Self::after_completing), so the base holds no per-state decision at all.
    /// The `Err` half of the return type is the same contract as `activate`'s: kept for a precondition
    /// landing back in this layer, with no arm returning one today.
    async fn complete(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        cmd: &CompleteState,
        act: &ActivityRecord,
        thread: &ThreadRecord,
    ) -> Result<(), ProcessingError> {
        let CompleteState { output, .. } = cmd;

        // (3.1) Open the finish now that this command has won the activity. Emitting here — before the
        // state's children below are dealt with — is what makes the decision durable rather than
        // provisional: a state that defers on a live child leaves the activity `Completing` (not
        // `Running`), so the child's eventual settle drives the drain through the generic `Completing`
        // path instead of depending on the state to re-issue a command. The command's raw result is
        // folded in first (`CompleteState` always carries the state's raw result, so this overwrites
        // rather than defaults) so a deferred drain can read it back off the row to project with.
        let mut activity_value = act.value();
        // The transition owns its own precondition even though the dispatcher screened for `Running`
        // before admitting the command: a `Completing`/terminal target means a competing finish or a
        // cancel already won, and refusing here is the one followup entry this command owes. The
        // refusal is the only record that explains why it applied nothing (mirrors
        // `complete_execution`'s treatment of the same transition).
        if let Err(reason) = activity_value.mark_completing(output.clone(), ctx.now()) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "complete_state: activity {} cannot begin completing: {reason}",
                    activity_value.meta.object_ref()
                ),
            ));
        }
        out.append_event(Event::StateCompleting {
            activity: activity_value.clone(),
        })
        .await;

        // (3.4) The state's own complete step. The scope's variables come off the row the dispatcher
        // already read (an activity is always owned by a `Thread`), so nothing here re-derives them.
        self.after_completing(ctx, out, &activity_value, &thread.variables)
            .await
    }

    /// The `Command::TerminateState` flow, owned by the base — the failure mirror of
    /// [`Self::complete`]. `act` and `thread` are the rows the dispatcher already read for them
    /// (screened for liveness before it admitted the command), so nothing here re-reads. Unlike a
    /// completion, the terminate's drawer is uniform, so the base owns it: ask the state's failure
    /// policy first (an uncaught failure then stands as a teardown, a caught one has already been
    /// routed and leaves nothing to tear down), open the activity with `StateTerminating`, and hand
    /// the per-state disposal + terminal to [`Self::after_terminating`] — which relays the terminal
    /// to the owner's container, so the base tells the owning scope nothing.
    async fn terminate(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        cmd: &TerminateState,
        act: &ActivityRecord,
        thread: &ThreadRecord,
    ) -> Result<(), ProcessingError> {
        let TerminateState { reason, .. } = cmd;
        // The policy gets its say only for a failure the activity **itself** produced: a swept
        // activity carries `Cancelled`, which is what keeps one state's failure from being routed by
        // another's policy.
        if let TerminationReason::Failed { error } = reason
            && matches!(
                self.on_failed(ctx, out, &act.value, error).await,
                FailureRouting::Caught
            )
        {
            return Ok(());
        }
        // Nothing here tells the owning scope: the state's terminal *is* that notification, relayed by
        // each `after_terminating` to its owner's container — one reaction point that also covers the
        // drain, where the scope must advance rather than be told again.
        // The drawer opens the failure now that the policy let it stand, mirroring `complete`'s
        // `StateCompleting`: a terminate that defers on a live child leaves the activity `Terminating`
        // (not `Running`), so the child's eventual settle drives the drain through the generic
        // `Terminating` path instead of depending on the state to re-issue a command.
        let mut activity_value = act.value();
        if let Err(e) = activity_value.mark_terminating(reason.clone(), ctx.now()) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "terminate_state: activity {} cannot begin terminating: {e}",
                    activity_value.meta.object_ref()
                ),
            ));
        }
        out.append_event(Event::StateTerminating {
            activity: activity_value.clone(),
        })
        .await;

        // The state's own terminate step; the scope's variables come off the row the dispatcher
        // already read.
        self.after_terminating(ctx, out, &activity_value, &thread.variables)
            .await
    }
}
#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::super::dispatch::build_state_handlers;
    use crate::StatePath;
    use crate::{Activity, ActivityStatus, ExecutionKind};

    /// A minimal empty `Activity` sufficient to dispatch an object-safe lifecycle hook — the create
    /// contract only cares that the hook *dispatches*, not what it does.
    fn empty_activity() -> Activity {
        Activity {
            meta: crate::types::meta::ObjectMeta::builder(ulid::Ulid::new()).with_owner(
                crate::types::meta::ObjectRef::new(
                    crate::types::meta::ObjectName::plain("execution")
                        .expect("a valid object name"),
                    ulid::Ulid::new(),
                ),
            ),
            execution: crate::types::meta::ObjectRef::<ExecutionKind>::nil(),
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
