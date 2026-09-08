/// Evaluate `$expr` (a `Result`); on `Ok` yield the value, on `Err` emit the failure to `$out`
/// (`TerminateState` for `$activity` if `Some`, plus `TerminateExecution` for `$execution`) and
/// `return`. The failure path always goes through `Collector::terminate` so a failing site records
/// its own outcome cohesively before the lifecycle cascade unwinds.
macro_rules! fail_or {
    ($out:expr, $activity:expr, $execution:expr, $expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                $out.terminate($activity, $execution, e);
                return;
            }
        }
    };
}

mod activate_state;
mod activate_task;
mod assign_task;
mod cancel_task;
mod cancel_timer;
mod child_completed;
mod complete_execution;
mod complete_state;
mod complete_task;
mod complete_thread;
mod continue_;
mod create_execution;
mod create_flow;
mod dispatch;
mod fail_task;
mod release_task_lease;
mod spawn_thread;
pub(crate) mod state_handler;
mod states;
mod terminate_execution;
mod terminate_state;
mod terminate_thread;
mod trigger_timer;

pub use activate_state::ActivateStateHandler;
pub use activate_task::ActivateTaskHandler;
pub use assign_task::ClaimTasksHandler;
pub use cancel_task::CancelTaskHandler;
pub use cancel_timer::CancelTimerHandler;
pub use complete_execution::CompleteExecutionHandler;
pub use complete_state::CompleteStateHandler;
pub use complete_task::CompleteTaskHandler;
pub use complete_thread::CompleteThreadHandler;
pub use continue_::{ContinueCompleteHandler, ContinueTerminateHandler};
pub use create_execution::CreateExecutionHandler;
pub use create_flow::CreateFlowHandler;
pub use fail_task::FailTaskHandler;
pub use release_task_lease::ReleaseTaskLeaseHandler;
pub use spawn_thread::SpawnThreadHandler;
pub use terminate_execution::TerminateExecutionHandler;
pub use terminate_state::TerminateStateHandler;
pub use terminate_thread::TerminateThreadHandler;
pub use trigger_timer::TriggerTimerHandler;

pub(crate) use dispatch::build_state_handlers;

use std::collections::HashMap;

use serde_json::Value;
use spica_asl::{AssignObject, StateMachine};

use crate::eval_env::EvalEnv;
use crate::handler::{ActivityCtx, Collector, HandlerContext};
use crate::types::command::{Command, TerminationReason};
use crate::types::context::build_states;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectReference};
use crate::{Activity, ActivityState, ActivityStatus};

// ── Shared helpers ───────────────────────────────────────────────────────────

/// Resolves a state definition by name from the state machine.
pub(super) fn resolve_state<'a>(
    sm: &'a StateMachine,
    state_name: &str,
) -> Result<&'a spica_asl::State, ExecutionError> {
    sm.states
        .get(state_name)
        .ok_or_else(|| ExecutionError::Runtime(RuntimeError::StateNotFound(state_name.to_string())))
}

/// Resolve a `states` table (a `HashMap<String, State>`) within the shared machine document by a
/// JSON Pointer of the form
/// `/states/<P>/branches/<i>/states/<P2>/branches/<j>/states…` for a `Parallel`, or
/// `/states/<M>/item_processor/states/<P>/branches/<i>/…` for a `Map` — the exact pointers a child
/// Parallel-branch / Map-item `Execution` carries. Each step names the container state and the
/// child of it the run descends into (a `Parallel` branch index, or the `Map`'s single
/// `item_processor`); the walk lands on that deepest child's `states` map in one pass (arbitrary
/// nesting depth). Any mix of `Parallel` and `Map` steps composes naturally because every step
/// yields a `states` table of the same type.
///
/// The machine document stays a **single shared instance** — this walk only *locates* a table inside
/// it, never copies child state. That is what makes each child execution self-resolving: it carries
/// a small flat pointer, so resolving its own state never needs to query its parent or the root.
///
/// Iteration runs over the pointer's already-decoded tokens ([`jsonptr::Pointer::tokens`] unescapes
/// `~0`/`~1` for us), so a state name containing `/` or `~` resolves exactly as RFC 6901 intends.
fn resolve_states_map<'a>(
    sm: &'a StateMachine,
    pointer: &jsonptr::Pointer,
) -> Result<&'a HashMap<String, spica_asl::State>, ExecutionError> {
    use spica_asl::State as S;
    let tokens: Vec<jsonptr::Token<'_>> = pointer.tokens().collect();
    let mut states: &HashMap<String, S> = &sm.states;
    let mut i = 0usize;
    loop {
        // If we've consumed the whole pointer, the current `states` map is the target (the pointer
        // always ends *on* a child's states table).
        if i >= tokens.len() {
            return Ok(states);
        }
        // Each descent begins with the `states` opener naming a container state.
        if tokens[i].decoded().as_ref() != "states" {
            return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                format!(
                    "state_path malformed: expected 'states', got '{}'",
                    tokens[i].decoded()
                ),
            )));
        }
        let name = tokens.get(i + 1).ok_or_else(|| {
            ExecutionError::Runtime(RuntimeError::StateNotFound(
                "state_path truncated at state name".into(),
            ))
        })?;
        let name = name.decoded();
        let state = states.get(name.as_ref()).ok_or_else(|| {
            ExecutionError::Runtime(RuntimeError::StateNotFound(name.as_ref().to_string()))
        })?;
        i += 2; // consumed "states" + <name>
        match state {
            // A `Parallel` descent names a branch: `branches/<idx>`, which yields that branch's
            // `states` table. Consumes `branches` + <idx>.
            S::Parallel(p) => {
                // `Cow<str> == &str` compares the decoded token against the literal without building
                // a borrowed reference to a temporary.
                if !tokens.get(i).is_some_and(|t| t.decoded() == "branches") {
                    return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "state_path malformed: expected 'branches'".into(),
                    )));
                }
                let idx: usize = tokens
                    .get(i + 1)
                    .ok_or_else(|| {
                        ExecutionError::Runtime(RuntimeError::StateNotFound(
                            "state_path truncated at branch index".into(),
                        ))
                    })?
                    .decoded()
                    .parse()
                    .map_err(|_| {
                        ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                            "state_path branch index is not an integer".into(),
                        ))
                    })?;
                let branch = p.branches.get(idx).ok_or_else(|| {
                    ExecutionError::Runtime(RuntimeError::StateNotFound(format!(
                        "branch index {idx} of state {name}"
                    )))
                })?;
                i += 2; // consumed "branches" + <idx>
                states = &branch.states;
            }
            // A `Map` descent names its single `item_processor` (no index): every item runs the same
            // processor, so the pointer names the `item_processor` token and lands directly on its
            // `states` table. Consumes `item_processor` only.
            S::Map(m) => {
                if !tokens
                    .get(i)
                    .is_some_and(|t| t.decoded() == "item_processor")
                {
                    return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "state_path malformed: expected 'item_processor'".into(),
                    )));
                }
                let processor = m.item_processor.as_ref().ok_or_else(|| {
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                        "Map state '{name}' has no item_processor"
                    )))
                })?;
                i += 1; // consumed "item_processor"
                states = &processor.states;
            }
            // Any other state type cannot be descended into — the pointer must always name a child
            // of a container state.
            _ => {
                return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                    format!("state_path step '{name}' is not a Parallel or Map state"),
                )));
            }
        }
        // Loop: the next token is either another "states" (a nested container) or the pointer ended —
        // in which case the next iteration returns this child's states.
    }
}

/// Resolve a state definition for the scope owning the current activity, honoring a scope's
/// `state_path`: a `Thread` (Parallel-branch / Map-item child) resolves its state within the shared
/// machine at the pointer location (one flat lookup, no parent/root query); a top-level `Execution`
/// falls back to the machine's top-level `states`. The `scope` is a loaded [`ScopeRecord`], so the
/// caller (which already resolved the owning scope) passes it — no second storage read.
pub(super) async fn resolve_state_for<'a>(
    sm: &'a StateMachine,
    scope: &crate::storage::ScopeRecord,
    state_name: &str,
) -> Result<&'a spica_asl::State, ExecutionError> {
    match scope.state_path() {
        // Thread: resolve within its branch/item_processor's `states` table.
        Some(pointer) => {
            let states = resolve_states_map(sm, pointer.as_ptr())?;
            states.get(state_name).ok_or_else(|| {
                ExecutionError::Runtime(RuntimeError::StateNotFound(state_name.to_string()))
            })
        }
        // Top-level execution: the machine's top-level `states`.
        None => resolve_state(sm, state_name),
    }
}

/// Resolve a state definition by its full `state_path` (a JSON Pointer from the machine root to the
/// state: `/states/<name>` for a top-level state, or `/states/.../branches/<idx>/<name>` for a branch /
/// item). The carried path makes `Command::ActivateState` self-locating — the lookup no longer infers
/// the enclosing `states` table from the owning scope's stored `state_path`. A top-level path is
/// exactly `/states/<leaf>` (two tokens) and maps to the machine root; any deeper path's parent is a
/// container's `states` table, resolved via `resolve_states_map`.
pub(crate) fn resolve_state_from_path<'a>(
    sm: &'a StateMachine,
    state_path: &jsonptr::Pointer,
) -> Result<&'a spica_asl::State, ExecutionError> {
    let leaf = state_name_from_path(state_path);
    if state_path.tokens().count() == 2 {
        // `/states/<leaf>` — a top-level state, resolved against the machine's top-level `states`.
        return resolve_state(sm, &leaf);
    }
    let mut parent = state_path.to_owned();
    parent.pop_back();
    let states = resolve_states_map(sm, parent.as_ptr())?;
    states
        .get(&leaf)
        .ok_or_else(|| ExecutionError::Runtime(RuntimeError::StateNotFound(leaf)))
}

/// Loads the owning [`crate::storage::ExecutionRecord`] for a state-ish command. Returns `Ok(None)` when
/// the owning node is gone (already terminal) — the caller treats that as an idempotent no-op rather
/// than a failure.
pub(super) async fn load_execution<S: crate::storage::ReadonlyStorageTxn + ?Sized>(
    storage: &S,
    execution: &ObjectReference,
) -> Result<Option<crate::storage::ExecutionRecord>, ExecutionError> {
    storage.get_execution(execution).await
}

/// Direct a terminal failure (or abort) at the scope that owns the given activity: a top-level run is
/// an `Execution` (name+uid-addressed `TerminateExecution`), while a `Parallel` branch / `Map` item
/// is owned by a `Thread`, which lives in **thread** storage and is only reachable via the
/// reference-addressed `TerminateThread`. Centralizing this branch keeps every terminal-fail site
/// (state `Fail`, parallel/map converge-fail, task fail, timer abort) from re-discovering that the
/// two store kinds address differently — a bare `TerminateExecution` silently misses a Thread and
/// leaves the branch Running, wedging its container.
pub(super) fn emit_scope_termination(
    out: &mut Collector<'_>,
    scope: &ObjectReference,
    reason: TerminationReason,
) {
    match scope.kind {
        ObjectKind::Execution => out.emit_command(Command::TerminateExecution {
            name: scope.name.clone(),
            uid: Some(scope.uid),
            reason,
        }),
        ObjectKind::Thread => out.emit_command(Command::TerminateThread {
            thread: scope.clone(),
            reason,
        }),
        _ => {
            // An activity is always owned by a scope; terminating into any other owner is an
            // internal fault with nowhere to route — nothing to emit, the failure is dropped.
            tracing::error!(owner = %scope, "terminal fail on a non-scope owner; cannot terminate");
        }
    }
}

/// Cancel every active timer child of `activity`. Used by the task **settlement** handlers
/// (`CompleteTask`, `FailTask`) to sweep the task's parented timers — the `DeliveryLease` armed on
/// assign, and the optional `TaskTimeout` — before the activity completes. Mirrors the M1 terminate
/// sweep's timer arm, but for a *settling* (still `Running`) activity: leaving a live timer child
/// would trip the activity-completion guard's "still has children" refusal, stalling the state.
/// Idempotent: a timer already fired or cancelled is not an active child and is simply skipped.
///
/// Emits the `TimerCancelled` **events** directly (rather than `CancelTimer` commands) so they land
/// in the same batch **before** the caller's `CompleteState` — a `CancelTimer` command would only
/// produce `TimerCancelled` as a *later* log entry, after which `CompleteState` had already read the
/// activity with its child still attached. The applier deschedules the deadline and detaches the
/// child, which is all the settle path needs (the activity itself is about to complete via
/// `CompleteState`, so no parent drain reaction is needed here).
pub(super) async fn cancel_activity_timers(
    ctx: &HandlerContext<'_>,
    out: &mut Collector<'_>,
    activity: ObjectReference,
) {
    let Some(act) = ctx.storage.get_activity(&activity).await.ok().flatten() else {
        return; // activity already gone — nothing to sweep.
    };
    for child in act.active_children {
        if child.kind != ObjectKind::Timer {
            continue; // only timer children matter here (M1 task activities own none other).
        }
        let Some(t) = ctx.storage.get_timer(&child).await.ok().flatten() else {
            continue;
        };
        if t.value.status != crate::TimerStatus::Active {
            continue; // already terminal — a fired/cancelled timer is no longer a live child.
        }
        out.emit_event(crate::types::event::Event::TimerCancelled {
            timer: crate::Timer {
                execution: t.value.execution.clone(),
                purpose: t.value.purpose,
                status: crate::TimerStatus::Cancelled,
                deadline: t.value.deadline,
                // Carry the timer's full meta (name/uid/created_at/owner) forward. A timer may be
                // custom-named (`{execution.name}-{suffix}`); reconstructing it via
                // `placeholder_with_times` would re-derive `obj-<uid>` and break the child-edge
                // removal. Stamp the cancel moment as `updated_at`.
                meta: {
                    let mut m = t.value.meta.clone();
                    m.with_update_at(crate::log::Timestamp::now());
                    m
                },
            },
        })
        .await;
    }
}

/// Evaluates a string that may be a literal or a `{% ... %}` JSONata expression.
pub(super) fn eval_string_or_expr(
    env: &mut EvalEnv,
    s: &str,
    states: &Value,
    variables: &crate::types::variables::Variables,
) -> Result<Value, ExecutionError> {
    match crate::eval_env::extract_jsonata(s) {
        Some(inner) => env.eval_expr(inner, states, variables),
        None => Ok(Value::String(s.to_string())),
    }
}

/// The leaf name of the state a JSON Pointer locates — the pointer's final (decoded) token. For the
/// `state_path` carried on an [`Activity`](crate::storage::ActivityRecord)/`ActivityCtx`, this is the state's
/// name in its enclosing `states` table. Returns the empty string for an empty (document-root) path —
/// a shape that should never reach a state handler, but kept total. `jsonptr`'s [`decoded`] API only
/// yields a `Cow` tied to a transient token borrow, so we resolve the leaf to an owned `String` here —
/// a direct derivation of the state name that used to be stored as its own field.
///
/// [`decoded`]: jsonptr::Token::decoded
pub(crate) fn state_name_from_path(path: &jsonptr::Pointer) -> String {
    path.last()
        .map(|t| t.decoded().into_owned())
        .unwrap_or_default()
}

/// Emits the [`crate::Event::StateActivating`] event for the activity being entered.
///
/// The event carries the full entity-shaped [`Activity`], not only ids/fields for the current
/// step, so a follower can rebuild the same domain activity object from the stream alone. It does
/// **not** carry the processed `input`: at the entering (`ing`) moment the state has only received
/// its **raw** input (kept in `raw_input`), and the state's own activate — which emits
/// The entering `StateActivating` event. The processed input isn't determined until the state
/// finishes activating (`StateActivated` carries it), so `input` is left `None` here — the raw
/// entry input (verbatim in `raw_input`) is all that's real.
pub(super) fn state_activating(actx: &ActivityCtx, _activity: ObjectReference) -> Event {
    Event::StateActivating {
        activity: actx.activity.clone(),
    }
}

/// Build a new [`Activity`] from the current context, replacing only the fields the lifecycle
/// step just changed. This keeps every lifecycle emitter updating the same entity payload shape.
pub(super) fn activity_value_with(
    actx: &ActivityCtx,
    status: Option<ActivityStatus>,
    input: Option<Value>,
    activity_state: Option<ActivityState>,
    raw_output: Option<Option<Value>>,
    output: Option<Option<Value>>,
) -> Activity {
    // Carry the activity's `meta` forward (its `created_at` = birth moment and `uid`) and
    // re-stamp `updated_at` to the transition moment — the meta-level equivalent of the old
    // `created_at` forward + `updated_at` re-stamp.
    let mut meta = actx.activity.meta.clone();
    meta.with_update_at(crate::log::Timestamp::now());
    Activity {
        meta,
        execution: actx.activity.execution.clone(),
        state_path: actx.activity.state_path.clone(),
        status: status.unwrap_or_else(|| actx.activity.status.clone()),
        raw_input: actx.activity.raw_input.clone(),
        input: input.or_else(|| actx.activity.input.clone()),
        raw_output: raw_output.unwrap_or_else(|| actx.activity.raw_output.clone()),
        activity_state: activity_state.or(actx.activity.activity_state.clone()),
        retry_state: actx.activity.retry_state.clone(),
        output: output.unwrap_or_else(|| actx.activity.output.clone()),
    }
}

/// Build the `StateActivated` payload by applying the activate step's processed input and optional
/// `Map` activation plan onto the current activity value.
pub(super) fn state_activated_value(
    actx: &ActivityCtx,
    input: Value,
    activity_state: Option<ActivityState>,
) -> Activity {
    activity_value_with(actx, None, Some(input), activity_state, None, None)
}

/// Build the `StateCompleted` payload by fixing the final projected output on the activity value.
/// The projected `output` travels on this event; `raw_output` (the pre-`Output` raw result) is
/// written alongside it so the complete step carries both views and never drops one to `null`.
pub(super) fn state_completed_value(actx: &ActivityCtx, output: Value) -> Activity {
    activity_value_with(
        actx,
        Some(ActivityStatus::Completed),
        None,
        None,
        Some(Some(state_raw_result(actx))),
        Some(Some(output)),
    )
}

/// Build the `StateTerminating` payload by embedding the final termination reason into the activity
/// status.
pub(super) fn state_terminating_value(
    actx: &ActivityCtx,
    reason: crate::types::command::TerminationReason,
) -> Activity {
    activity_value_with(
        actx,
        Some(ActivityStatus::Terminating(reason)),
        None,
        None,
        None,
        None,
    )
}

/// Build the `StateTerminated` payload by embedding the terminal termination reason into the activity
/// status.
pub(super) fn state_terminated_value(
    actx: &ActivityCtx,
    reason: crate::types::command::TerminationReason,
) -> Activity {
    activity_value_with(
        actx,
        Some(ActivityStatus::Terminated(reason)),
        None,
        None,
        None,
        None,
    )
}

/// The raw result a state produced before any complete-step `Output` projection — the canonical
/// `$states.result`. For a state whose raw result *is* a distinct value (a Task's worker payload,
/// kept in `raw_output`) that value wins; for a synchronous state with no distinct raw result it
/// defaults to the processed input. This is exactly the derivation `complete_activity` uses, hoisted
/// so the complete-step *events* (not the computation) can carry the same value.
pub(crate) fn state_raw_result(actx: &ActivityCtx) -> Value {
    actx.activity
        .raw_output
        .clone()
        .unwrap_or_else(|| actx.activity.raw_input.clone())
}

/// Build the `StateCompleting` payload by flipping only the lifecycle status. Unlike the input side
/// (`StateActivating` pins `input` to `Null` because the *processed* view isn't determined until
/// `StateActivated`), `raw_output` is already known here — the raw result is derivable purely from
/// the activity (`raw_output` else `input`) with no completion-time computation — so the entering
/// event carries it rather than the logging-information hole the design review flagged.
pub(super) fn state_completing_value(actx: &ActivityCtx) -> Activity {
    activity_value_with(
        actx,
        Some(ActivityStatus::Completing),
        None,
        None,
        Some(Some(state_raw_result(actx))),
        None,
    )
}

/// Records the successful state finish's routing — emitting the `StateTransitioned` marker that
/// carries the resolved target **path** — then throws the transition [`Command`] that actually
/// performs the hop. The marker is only emitted for a real State→State hop (`Command::ActivateState`):
/// a terminal `End` routes to `CompleteExecution` with no next state, so it carries no marker. Kept
/// separate from the pure `transition_command` resolver so the routing decision is visible on the
/// stream ahead of the command that carries it (`Command::ActivateState` allocates the successor's
/// activity id internally, so the marker can only name the target path, not the new activity). On
/// `NoTerminal` the failure is recorded via `out`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn emit_transition(
    out: &mut Collector<'_>,
    execution: ObjectReference,
    owner: ObjectReference,
    activity: ObjectReference,
    activity_state_path: &jsonptr::PointerBuf,
    output: &Value,
    next: Option<&str>,
    end: Option<bool>,
) {
    if end == Some(true) {
        // Terminal hop: no next state to route to, so there's no `StateTransitioned` marker — just
        // fold the output and complete the *scope*. The scope kind decides the verb: a **branch's**
        // terminal state completes its fan-out `Thread` (`CompleteThread`, converged by the owning
        // container Activity), while a **top-level** state completes the `Execution`
        // (`CompleteExecution`). Both are scoped, so they take the `owner`, not the top-level anchor.
        if owner.kind == ObjectKind::Thread {
            out.emit_command(Command::CompleteThread {
                thread: owner,
                output: output.clone(),
            });
        } else {
            out.emit_command(Command::CompleteExecution {
                execution: owner,
                output: output.clone(),
            });
        }
    } else if let Some(next) = next {
        // The successor lives as a sibling of the completing state in the same enclosing `states`
        // table — that table is the completing activity's `state_path` minus its own leaf.
        let mut next_path = activity_state_path.clone();
        next_path.pop_back();
        next_path.push_back(next);
        // The marker carries the resolved target *path* (self-locating), not a bare name that would
        // need the completing activity's context to be reconstructed.
        out.emit_event(crate::types::event::Event::StateTransitioned {
            activity,
            next: next_path.clone(),
        })
        .await;
        // The successor's activity id is allocated inside the `ActivateState` handler (see its doc).
        out.emit_command(Command::ActivateState {
            execution,
            owner,
            state_path: next_path,
            input: output.clone(),
        });
    } else {
        out.terminate(
            Some(activity),
            execution,
            ExecutionError::Runtime(RuntimeError::NoTerminal),
        );
    }
}

/// Mint and arm a timer inline: allocate its uid (a raw `ulid::Ulid`) and derive a generated name
/// (`{execution.name}-{8-char-suffix}`) from the owning execution, then emit `Event::TimerActivated`
/// — the fact that both folds the timer row and arms the physical deadline (see
/// `TimerActivatedApplier`). Inlined rather than a `Command` so the arm lands in the same causal
/// batch as the state decision that triggers it (create_execution already does this for its
/// ExecutionTimeout). The name is decoupled from the timer's `uid` and must be carried forward by
/// later timer events (`TimerTriggered`/`TimerCancelled` preserve the row's meta instead of
/// re-deriving it).
pub(super) async fn emit_timer(
    out: &mut Collector<'_>,
    execution: ObjectReference,
    owner: ObjectReference,
    purpose: crate::types::command::TimerPurpose,
    deadline: crate::log::Timestamp,
) {
    let timer_uid: ulid::Ulid = ulid::Ulid::new();
    let timer_name = execution
        .name
        .base()
        .generated_from_key(out.next_generated_seq().await);
    out.emit_event(Event::TimerActivated {
        timer: crate::Timer {
            execution,
            purpose,
            status: crate::TimerStatus::Active,
            deadline,
            meta: crate::types::meta::ObjectMeta::builder(ObjectKind::Timer, timer_uid)
                .name(timer_name)
                .at(crate::log::Timestamp::now())
                .build()
                .with_owner(owner),
        },
    })
    .await;
}

/// Shared tail of a successful state completion (Wait resume; Pass/Succeed/Choice now carry their
/// own because their finish differs): evaluates `Assign` (emitting `VariablesAssigned`), evaluates
/// `Output` (defaults to input), emits `StateCompleted`, then the routing via [`emit_transition`].
///
/// `StateCompleting` is **not** emitted here — the `CompleteStateHandler` framework emits it when it
/// opens the complete step (analogous to `StateActivating` opening activate), so any state's success
/// finish emits the ing uniformly regardless of its own output handling.
///
/// This runs only from the `complete` step (see [`state_handler::StateHandler::complete`]) — never
/// from `activate`. Reads variable mutation from `Assign` into the local variables used for the output
/// projection, then run the inline child-settled reaction that drains the parent (once drained).
#[allow(clippy::too_many_arguments)]
pub(super) async fn complete_activity(
    env: &mut EvalEnv,
    out: &mut Collector<'_>,
    activity: ObjectReference,
    actx: &ActivityCtx,
    assign: Option<&AssignObject>,
    output: Option<&Value>,
    next: Option<&str>,
    end: Option<bool>,
    retry_count: u32,
    error_output: Option<&Value>,
) {
    // The complete step sees two different values: `$states.input` is the processed input the state
    // actually ran on, while `$states.result` is the raw result produced before any complete-step
    // `Output` projection. For states that produce no distinct raw result, the result defaults to the
    // processed input so the shared success semantics stay unchanged.
    let raw_result = actx
        .activity
        .raw_output
        .as_ref()
        .unwrap_or(&actx.activity.raw_input);
    // Activate-phase Assign was already applied (mutating scope); the output projection runs with
    // that updated scope so it can reference the Just-assigned variables.
    let states = build_states(
        &actx.activity.raw_input,
        Some(raw_result),
        &actx.state_name(),
        &actx.exec_input,
        Some(&actx.activity.raw_input),
        retry_count,
        error_output,
        None, // not a Map item — no `context.Map.Item` binding
    );
    let mut local_scope = actx.variables.clone();

    if let Some(assign_obj) = assign {
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
                    })
                    .await;
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

    let output_value = match output {
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
        None => raw_result.clone(),
    };

    out.emit_event(Event::StateCompleted {
        activity: state_completed_value(actx, output_value.clone()),
    })
    .await;

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
        next,
        end,
    )
    .await;
}
