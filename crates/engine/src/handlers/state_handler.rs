use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{AssignObject, State};
use std::collections::HashMap;
use std::mem::Discriminant;

use super::{emit_state_completed, emit_transition};
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext};
use crate::types::command::{ActivateState, Command, CompleteState};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::{Event, VariablesAssigned};
use crate::types::id::RequestId;
use crate::types::meta::{ObjectKind, ObjectMeta, ObjectReference};
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

/// The state's answer to "may the success finish run now?", returned by
/// [`StateHandler::on_completing`] once it has dealt with its own unfinished children. A named value
/// rather than a `bool` because `Waiting` also carries *how many* children are still live, which the
/// base reports in its deferral log.
pub enum FinishReadiness {
    /// Proceed into the finish: no child of this state outlives the decision.
    Ready,
    /// Defer: `pending` children are still live. The activity is already `Completing`, so the last
    /// one's settle drives the drain (see `crate::handlers::continue_`) — nothing re-drives `complete`.
    Waiting { pending: usize },
    /// The activity's row is gone or unreadable: there is no finish to run.
    Gone,
}

/// A short-lived handler bound to one resolved [`State`] definition. Owned for a single lifecycle
/// dispatch (activate / complete / child-settled) and dropped once it returns. A concrete adapter
/// holds a typed definition reference (e.g. `&TaskState`), so the per-variant hooks read it directly
/// instead of receiving `&State` and re-matching.
///
/// Both lifecycle operations are Template Methods owned by the base. [`activate`](Self::activate)
/// constructs the activity, runs the four activate hooks (`initialize` / `process_input` /
/// `after_activated` / `complete_directly`) in a fixed order, and hands into `CompleteState`, so
/// synchronous states (Pass/Succeed/Fail/Choice) share one code path instead of each re-emitting
/// `StateActivated` + `CompleteState`. [`complete`](Self::complete) runs the shared complete-step
/// orchestration (load, guards, `StateCompleting`, the [`on_completing`](Self::on_completing) child
/// gate, scope resolution) and then the single per-state [`finish`](Self::finish). The activate causal
/// chain is `StateActivating → StateActivated → (CompleteState | side effect)`; the complete chain is
/// `StateCompleting → (StateCompleted | failure ed) → transition`.
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

    // ── complete hooks — the only per-state variation of the complete step ──

    /// (3.4) Deal with this state's unfinished children as the finish opens, reporting whether the
    /// base may continue into [`finish`](Self::finish). The state is the only code that knows *what*
    /// its children are, so the disposition belongs here: it cleans up whatever must not outlive its
    /// own decision (a `Task` cancels the deadline/lease timers that merely bounded it), then reports a
    /// wait for whatever is left. The default waits on the generic child set — a state owning no
    /// children is trivially `Ready`, and a state whose child *is* its completion trigger (`Wait`'s
    /// resume timer, a container's fan-out) needs no cleanup of its own.
    async fn on_completing(
        &self,
        ctx: &mut HandlerContext<'_>,
        _out: &mut Collector<'_>,
        activity: &ObjectReference,
        _activity_value: &Activity,
    ) -> FinishReadiness {
        match self.live_children(ctx, activity).await {
            Some(0) => FinishReadiness::Ready,
            Some(pending) => FinishReadiness::Waiting { pending },
            None => FinishReadiness::Gone,
        }
    }

    /// How many children the activity still has, re-read from storage. Re-read rather than trusting
    /// the complete step's opening snapshot: an [`on_completing`](Self::on_completing) override folds
    /// child edges away (a cancelled timer) before asking, and those folds must be visible to the
    /// count. `None` when the row is gone or unreadable.
    async fn live_children(
        &self,
        ctx: &HandlerContext<'_>,
        activity: &ObjectReference,
    ) -> Option<usize> {
        match ctx.storage.get_activity(activity).await {
            Ok(Some(a)) => Some(a.active_children.len()),
            Ok(None) | Err(_) => None,
        }
    }

    /// The state's `Assign` delta, applied to the completing scope by the default
    /// [`finish`](Self::finish). These sources live on the typed definition, which a trait default
    /// method cannot reach, so a state on the canonical success path (`Pass`/`Succeed`/`Wait`/`Task`)
    /// exposes them here instead of re-implementing the finish. A state that overrides `finish`
    /// (`Fail`, `Choice`, a container) projects its own sources inside that override.
    fn assign(&self) -> Option<&AssignObject> {
        None
    }

    /// The state's `Output` projection over its raw result; absent means the result passes through.
    fn output(&self) -> Option<&Value> {
        None
    }

    /// The successor this state routes to — a sibling hop in the same `States` table. `None` has no
    /// successor, which [`end`](Self::end) turns into an owner-terminal completion.
    fn next(&self) -> Option<&str> {
        None
    }

    /// Whether this state is terminal (`End`), routing to its owner's completion instead of a sibling.
    fn end(&self) -> Option<bool> {
        None
    }

    /// (3.5) The per-state finish, run by the base [`complete`](Self::complete) once the activity has
    /// been loaded, guarded, opened with `StateCompleting` and cleared for the finish by
    /// [`on_completing`](Self::on_completing). The default is the canonical
    /// ASL success projection — `Assign` (a delta on the scope, emitted as `VariablesAssigned`) then
    /// `Output` (defaulting to the state's raw result) — followed by `StateCompleted` and the state's
    /// transition, so every state on that path (`Pass`/`Succeed`/`Wait`/`Task`) shares one
    /// implementation instead of four copies. `Fail` overrides it to terminate instead, `Choice` to
    /// route by its resolved rule, and a container to its own defensive finish. An error is turned
    /// into a terminate by the base.
    async fn finish(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector<'_>,
        activity: ObjectReference,
        activity_value: &Activity,
        variables: &Variables,
    ) -> Result<(), ExecutionError> {
        // `$states.result` is the state's raw result — for a `Task` the worker payload, for
        // synchronous states and `Wait` the processed input (see `CompleteState`'s docs). It is also
        // the fallback the projection passes through when the state declares no `Output`.
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
        let owner = activity_value
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        self.apply_assign(out, env, &owner, self.assign(), &states, &mut local_scope)
            .await?;
        let output_value = self
            .project_output(env, self.output(), &states, &local_scope, result)
            .await?;

        emit_state_completed(out, activity_value, &output_value).await;
        emit_transition(
            out,
            activity_value.execution.clone(),
            owner,
            activity,
            &activity_value.state_path,
            &output_value,
            self.next(),
            self.end(),
        )
        .await;
        Ok(())
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
            meta: ObjectMeta::builder(ObjectKind::Activity, ctx.mint())
                .name(
                    execution
                        .name
                        .base()
                        .generated_from_key(out.next_generated_seq().await),
                )
                .at(ctx.now())
                .build()
                .with_owner(owner.clone()),
        };
        let activity = activity_value.reference();

        // The activity's owner is always a `Thread` — the derived root thread for a top-level run, a
        // fan-out thread for a branch/item (see `emit_transition`, which only ever names a `Thread`).
        // Reading the concrete row is exactly what yields the variables the hooks evaluate against and
        // confirms the thread still accepts transitions.
        let scope = match ctx.storage.get_thread(owner).await {
            Ok(Some(t)) => t,
            Ok(None) => {
                out.terminate(
                    Some(activity.clone()),
                    execution.clone(),
                    ExecutionError::Runtime(RuntimeError::StateNotFound(format!("thread {owner}"))),
                );
                return;
            }
            Err(e) => {
                out.terminate(Some(activity.clone()), execution.clone(), e);
                return;
            }
        };
        if !scope.value.status.is_running() {
            return; // scope not running — a rescheduled activate is a no-op.
        }

        let variables = scope.variables.clone();
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
                out.terminate(Some(activity), owner.clone(), e);
                return;
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

    /// The `Command::CompleteState` flow, owned by the base — the mirror of [`Self::activate`]. The
    /// only input is the typed [`CompleteState`] payload (receiving it by its own type rather than as
    /// loose `activity`/`output` parameters makes the dispatch a compile-time guarantee), and every
    /// step below is identical across states: load the activity, run the liveness / Terminating-race
    /// guards, open the finish with `StateCompleting` (folding the command's raw result in), hand the
    /// unfinished children to [`on_completing`](Self::on_completing) and defer if it says to wait,
    /// resolve the owning scope and its variables, and hand into the per-state [`finish`](Self::finish).
    async fn complete(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        cmd: &CompleteState,
    ) {
        let CompleteState { activity, output } = cmd;

        // (3.1) Load the activity the command names. A missing row means the activity is gone (or was
        // never a state's) — fail the execution, with no activity to attach a state-level terminate to.
        // TODO：拿到 activity 之后，如果遇到错误不应该是直接 terminate（有些临时性质的错误应该重试）。
        let act = match ctx.storage.get_activity(activity).await {
            Ok(Some(a)) => a,
            Ok(None) => {
                out.terminate(
                    Some(activity.clone()),
                    ObjectReference::nil(),
                    ExecutionError::Runtime(RuntimeError::StateNotFound(format!(
                        "activity {activity}"
                    ))),
                );
                return;
            }
            Err(e) => {
                out.terminate(Some(activity.clone()), ObjectReference::nil(), e);
                return;
            }
        };

        // (3.2) A cancel (or a competing terminator) already won on this activity: the command lost the
        // race, so the success finish no longer applies. Refuse it durably with the nil id an internal
        // command carries — a rejection still leaves a trace, unlike a silent no-op. Recovering the
        // activity's *own* terminal is deliberately not this step's job: the child whose settle stranded
        // it drives that drain (see [`crate::handlers::trigger_timer`]), so emitting `StateTerminated`
        // from here would duplicate a terminal event the settle path already owns.
        if act.value.status != ActivityStatus::Running {
            out.reject(
                RequestId::nil(),
                RejectionType::InvalidState,
                format!(
                    "activity {activity} is {:?}, not Running; cannot complete",
                    act.value.status
                ),
            );
            return;
        }

        // (3.3) Open the finish now that this command has won the activity. Emitting here — before the
        // children below are dealt with — is what makes the decision durable rather than provisional:
        // a step that defers on a live child leaves the activity `Completing` (not `Running`), so the
        // child's eventual settle drives the drain through the generic `Completing` path instead of
        // depending on the state to re-issue a command. The command's raw result is folded in first
        // (`CompleteState` always carries the state's raw result, so this overwrites rather than
        // defaults) so a deferred drain can read it back off the row to project with.
        let mut activity_value = act.value();
        activity_value.raw_output = Some(output.clone());
        activity_value.meta.with_update_at(ctx.now());
        activity_value.status = ActivityStatus::Completing;
        out.append_event(Event::StateCompleting {
            activity: activity_value.clone(),
        })
        .await;

        // (3.4) Hand the disposition of this state's unfinished children to the state itself (see
        // `on_completing`): it cleans up whatever must not outlive its own decision, and reports
        // whether the finish may run now. A wait leaves the activity `Completing`, so the last child's
        // settle drives the drain (see `continue_`) and nothing re-drives `complete` — which is why the
        // decision had to be durable before this point (3.3).
        match self
            .on_completing(ctx, out, activity, &activity_value)
            .await
        {
            FinishReadiness::Ready => {}
            FinishReadiness::Waiting { pending } => {
                tracing::debug!(
                    activity = %activity,
                    children = pending,
                    "state is Completing but its children have not all drained; finish deferred"
                );
                return;
            }
            FinishReadiness::Gone => return,
        }

        // The activity's owner is always a `Thread` (see `activate`), so — as there — the concrete row
        // is read directly. The owner is taken from the *persisted row* rather than from the command,
        // so this read is also what supplies the variables `finish` evaluates against.
        let owner = act
            .value
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        let scope = match ctx.storage.get_thread(&owner).await {
            Ok(Some(t)) => t,
            Ok(None) => return, // owning thread already gone — nothing to complete into.
            Err(_) => return,
        };
        if !scope.value.status.is_running() {
            return; // owner is past accepting a new transition; a late CompleteState is a no-op.
        }

        // (3.5) The scope's variables as of the transition, and the per-state finish — the projection
        // (`$states.result` / `Assign` / `Output`) plus the `Next`/`End` routing. `activity_value` is
        // the value (3.3) already opened the finish with, so its `Completing` status and folded raw
        // result carry forward into whichever lifecycle the finish emits.
        let variables = scope.variables.clone();
        let finished = self
            .finish(ctx.env, out, activity.clone(), &activity_value, &variables)
            .await;
        fail_or!(out, Some(activity.clone()), owner, finished);
    }
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
