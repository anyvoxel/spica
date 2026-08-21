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
mod activate_timer;
mod assign_task;
mod cancel_task;
mod cancel_timer;
mod complete_execution;
mod complete_state;
mod complete_task;
mod create_execution;
mod create_flow;
mod dispatch;
mod fail_task;
mod process_child_completed;
mod release_task_lease;
mod spawn_branch;
mod state_handler;
mod states;
mod terminate_execution;
mod terminate_state;
mod trigger_timer;

pub use activate_state::ActivateStateHandler;
pub use activate_task::ActivateTaskHandler;
pub use activate_timer::ActivateTimerHandler;
pub use assign_task::{AssignTaskHandler, PullTasksHandler};
pub use cancel_task::CancelTaskHandler;
pub use cancel_timer::CancelTimerHandler;
pub use complete_execution::CompleteExecutionHandler;
pub use complete_state::CompleteStateHandler;
pub use complete_task::CompleteTaskHandler;
pub use create_execution::CreateExecutionHandler;
pub use create_flow::CreateFlowHandler;
pub use fail_task::FailTaskHandler;
pub use process_child_completed::ProcessChildCompletedHandler;
pub use release_task_lease::ReleaseTaskLeaseHandler;
pub use spawn_branch::SpawnBranchHandler;
pub use terminate_execution::TerminateExecutionHandler;
pub use terminate_state::TerminateStateHandler;
pub use trigger_timer::TriggerTimerHandler;

use std::collections::HashMap;

use serde_json::Value;
use spica_asl::{AssignObject, StateMachine};

use crate::command::Command;
use crate::context::build_states;
use crate::error::ExecutionError;
use crate::eval_env::EvalEnv;
use crate::event::Event;
use crate::handler::{ActivityCtx, Collector, HandlerContext};
use crate::id::ActivityId;
use crate::{ActivityState, ActivityStatus, ActivityValue, MapActivityState};

// ── Shared helpers ───────────────────────────────────────────────────────────

/// Resolves a state definition by name from the state machine.
pub(super) fn resolve_state<'a>(
    sm: &'a StateMachine,
    state_name: &str,
) -> Result<&'a spica_asl::State, ExecutionError> {
    sm.states
        .get(state_name)
        .ok_or_else(|| ExecutionError::StateNotFound(state_name.to_string()))
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
            return Err(ExecutionError::InvalidDefinition(format!(
                "state_path malformed: expected 'states', got '{}'",
                tokens[i].decoded()
            )));
        }
        let name = tokens.get(i + 1).ok_or_else(|| {
            ExecutionError::StateNotFound("state_path truncated at state name".into())
        })?;
        let name = name.decoded();
        let state = states
            .get(name.as_ref())
            .ok_or_else(|| ExecutionError::StateNotFound(name.as_ref().to_string()))?;
        i += 2; // consumed "states" + <name>
        match state {
            // A `Parallel` descent names a branch: `branches/<idx>`, which yields that branch's
            // `states` table. Consumes `branches` + <idx>.
            S::Parallel(p) => {
                // `Cow<str> == &str` compares the decoded token against the literal without building
                // a borrowed reference to a temporary.
                if !tokens.get(i).is_some_and(|t| t.decoded() == "branches") {
                    return Err(ExecutionError::InvalidDefinition(
                        "state_path malformed: expected 'branches'".into(),
                    ));
                }
                let idx: usize = tokens
                    .get(i + 1)
                    .ok_or_else(|| {
                        ExecutionError::StateNotFound("state_path truncated at branch index".into())
                    })?
                    .decoded()
                    .parse()
                    .map_err(|_| {
                        ExecutionError::InvalidDefinition(
                            "state_path branch index is not an integer".into(),
                        )
                    })?;
                let branch = p.branches.get(idx).ok_or_else(|| {
                    ExecutionError::StateNotFound(format!("branch index {idx} of state {name}"))
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
                    return Err(ExecutionError::InvalidDefinition(
                        "state_path malformed: expected 'item_processor'".into(),
                    ));
                }
                let processor = m.item_processor.as_ref().ok_or_else(|| {
                    ExecutionError::InvalidDefinition(format!(
                        "Map state '{name}' has no item_processor"
                    ))
                })?;
                i += 1; // consumed "item_processor"
                states = &processor.states;
            }
            // Any other state type cannot be descended into — the pointer must always name a child
            // of a container state.
            _ => {
                return Err(ExecutionError::InvalidDefinition(format!(
                    "state_path step '{name}' is not a Parallel or Map state"
                )));
            }
        }
        // Loop: the next token is either another "states" (a nested container) or the pointer ended —
        // in which case the next iteration returns this child's states.
    }
}

/// Resolve a state definition for the execution owning the current activity, honoring a child
/// execution's `state_path`: a Parallel-branch child resolves its state within the shared
/// machine at the pointer location (one flat lookup, no parent/root query); a top-level execution
/// falls back to the machine's top-level `states`. Asynchronous because it reads the owning
/// execution's row to discover the pointer.
pub(super) async fn resolve_state_for<'a>(
    storage: &dyn crate::storage::Storage,
    sm: &'a StateMachine,
    execution: crate::id::ExecutionId,
    state_name: &str,
) -> Result<&'a spica_asl::State, ExecutionError> {
    let Some(exec) = storage.get_execution(execution).await.ok().flatten() else {
        return Err(ExecutionError::StateNotFound(format!(
            "execution {execution}"
        )));
    };
    match &exec.state_path {
        // Child Parallel-branch execution: resolve within its branch's `states` table.
        Some(pointer) => {
            let states = resolve_states_map(sm, pointer.as_ptr())?;
            states
                .get(state_name)
                .ok_or_else(|| ExecutionError::StateNotFound(state_name.to_string()))
        }
        // Top-level execution: the machine's top-level `states`.
        None => resolve_state(sm, state_name),
    }
}

/// Loads the owning [`crate::storage::Execution`] for a state-ish command. Returns `Ok(None)` when
/// the owning node is gone (already terminal) — the caller treats that as an idempotent no-op rather
/// than a failure.
pub(super) async fn load_execution(
    storage: &dyn crate::storage::Storage,
    execution: crate::id::ExecutionId,
) -> Result<Option<crate::storage::Execution>, ExecutionError> {
    storage.get_execution(execution).await
}

/// Cancel every active timer child of `activity`. Used by the task **settlement** handlers
/// (`CompleteTask`, `FailTask`) to sweep the task's parented timers — the `TaskLease` armed on
/// assign, and the optional `TaskTimeout` — before the activity completes. Mirrors the M1 terminate
/// sweep's timer arm, but for a *settling* (still `Running`) activity: leaving a live timer child
/// would trip the activity-completion guard's "still has children" refusal, stalling the state.
/// Idempotent: a timer already fired or cancelled is not an active child and is simply skipped.
///
/// Emits the `TimerCancelled` **events** directly (rather than `CancelTimer` commands) so they land
/// in the same batch **before** the caller's `CompleteState` — a `CancelTimer` command would only
/// produce `TimerCancelled` as a *later* log entry, after which `CompleteState` had already read the
/// activity with its child still attached. The applier deschedules the deadline and detaches the
/// child, which is all the settle path needs (`ProcessChildCompleted` omitted: the activity itself is
/// about to complete via `CompleteState`).
pub(super) async fn cancel_activity_timers(
    ctx: &HandlerContext<'_>,
    out: &mut Collector,
    activity: ActivityId,
) {
    let Some(act) = ctx.storage.get_activity(activity).await.ok().flatten() else {
        return; // activity already gone — nothing to sweep.
    };
    for child in act.active_children {
        let crate::id::NodeId::Timer(timer) = child else {
            continue; // only timer children matter here (M1 task activities own none other).
        };
        let Some(t) = ctx.storage.get_timer(timer).await.ok().flatten() else {
            continue;
        };
        if t.value.status != crate::TimerStatus::Active {
            continue; // already terminal — a fired/cancelled timer is no longer a live child.
        }
        out.emit_event(crate::event::Event::TimerCancelled {
            timer: crate::TimerValue {
                id: t.value.id,
                parent: t.value.parent,
                purpose: t.value.purpose,
                status: crate::TimerStatus::Cancelled,
                deadline: t.value.deadline,
            },
        });
    }
}

/// Evaluates a string that may be a literal or a `{% ... %}` JSONata expression.
pub(super) fn eval_string_or_expr(
    env: &mut EvalEnv,
    s: &str,
    states: &Value,
    variables: &crate::variables::Variables,
) -> Result<Value, ExecutionError> {
    match crate::eval_env::extract_jsonata(s) {
        Some(inner) => env.eval_expr(inner, states, variables),
        None => Ok(Value::String(s.to_string())),
    }
}

/// The leaf name of the state a JSON Pointer locates — the pointer's final (decoded) token. For the
/// `state_path` carried on an [`Activity`](crate::storage::Activity)/`ActivityCtx`, this is the state's
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
/// The event carries the full entity-shaped [`ActivityValue`], not only ids/fields for the current
/// step, so a follower can rebuild the same domain activity object from the stream alone.
pub(super) fn state_activating(actx: &ActivityCtx, _activity: ActivityId) -> Event {
    Event::StateActivating {
        activity: actx.activity.clone(),
    }
}

/// Build a new [`ActivityValue`] from the current context, replacing only the fields the lifecycle
/// step just changed. This keeps every lifecycle emitter updating the same entity payload shape.
pub(super) fn activity_value_with(
    actx: &ActivityCtx,
    status: Option<ActivityStatus>,
    input: Option<Value>,
    activity_state: Option<ActivityState>,
    raw_output: Option<Option<Value>>,
    output: Option<Option<Value>>,
) -> ActivityValue {
    ActivityValue {
        id: actx.activity.id,
        execution: actx.activity.execution,
        root_execution: actx.activity.root_execution,
        parent: actx.activity.parent,
        state_path: actx.activity.state_path.clone(),
        status: status.unwrap_or_else(|| actx.activity.status.clone()),
        raw_input: actx.activity.raw_input.clone(),
        input: input.unwrap_or_else(|| actx.activity.input.clone()),
        raw_output: raw_output.unwrap_or_else(|| actx.activity.raw_output.clone()),
        activity_state: activity_state.unwrap_or_else(|| actx.activity.activity_state.clone()),
        retry_state: actx.activity.retry_state.clone(),
        output: output.unwrap_or_else(|| actx.activity.output.clone()),
    }
}

/// Build the `StateActivated` payload by applying the activate step's processed input and optional
/// `Map` activation plan onto the current activity value.
pub(super) fn state_activated_value(
    actx: &ActivityCtx,
    input: Value,
    plan: Option<MapActivityState>,
) -> ActivityValue {
    let activity_state = plan.map(ActivityState::Map);
    activity_value_with(actx, None, Some(input), activity_state, None, None)
}

/// Build the `StateCompleted` payload by fixing the final projected output on the activity value.
pub(super) fn state_completed_value(actx: &ActivityCtx, output: Value) -> ActivityValue {
    activity_value_with(
        actx,
        Some(ActivityStatus::Completed),
        None,
        None,
        None,
        Some(Some(output)),
    )
}

/// Build the `StateTerminating` payload by embedding the final termination reason into the activity
/// status.
pub(super) fn state_terminating_value(
    actx: &ActivityCtx,
    reason: crate::command::TerminationReason,
) -> ActivityValue {
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
    reason: crate::command::TerminationReason,
) -> ActivityValue {
    activity_value_with(
        actx,
        Some(ActivityStatus::Terminated(reason)),
        None,
        None,
        None,
        None,
    )
}

/// Build the `StateCompleting` payload by flipping only the lifecycle status.
pub(super) fn state_completing_value(actx: &ActivityCtx) -> ActivityValue {
    activity_value_with(
        actx,
        Some(ActivityStatus::Completing),
        None,
        None,
        None,
        None,
    )
}

/// Records the successful state finish's routing — emitting the `StateTransitioned` marker that
/// names the resolved target `next` — then throws the transition [`Command`] that actually performs
/// the hop. The marker is only emitted for a real State→State hop (`Command::ActivateState`): a
/// terminal `End` routes to `CompleteExecution` with no next state, so it carries no marker. Kept
/// separate from the pure `transition_command` resolver so the routing decision is visible on the
/// stream ahead of the command that carries it (`Command::ActivateState` allocates the successor's
/// activity id internally, so the marker can only name the state, not the new activity). On
/// `NoTerminal` the failure is recorded via `out`.
pub(super) fn emit_transition(
    out: &mut Collector,
    execution: crate::id::ExecutionId,
    activity: ActivityId,
    output: &Value,
    next: Option<&str>,
    end: Option<bool>,
) {
    if end == Some(true) {
        // Terminal hop: no next state to route to, so there's no `StateTransitioned` marker — just
        // fold the top-level output and complete the execution.
        out.emit_command(Command::CompleteExecution {
            id: execution,
            output: output.clone(),
        });
    } else if let Some(next) = next {
        // Allocate the successor id before `emit_command` to avoid a double mutable borrow of `out`.
        let next_activity = out.next_activity();
        out.emit_event(crate::event::Event::StateTransitioned {
            activity,
            next: next.to_string(),
            output: output.clone(),
        });
        out.emit_command(Command::ActivateState {
            execution,
            activity: next_activity,
            state: next.to_string(),
            input: output.clone(),
        });
    } else {
        out.terminate(Some(activity), execution, ExecutionError::NoTerminal);
    }
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
/// projection, then drains via a `ProcessChildCompleted` notice to its parent (once drained).
#[allow(clippy::too_many_arguments)]
pub(super) fn complete_activity(
    env: &mut EvalEnv,
    out: &mut Collector,
    activity: ActivityId,
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
        .unwrap_or(&actx.activity.input);
    // Activate-phase Assign was already applied (mutating scope); the output projection runs with
    // that updated scope so it can reference the Just-assigned variables.
    let states = build_states(
        &actx.activity.input,
        Some(raw_result),
        &actx.state_name(),
        &actx.exec_input,
        Some(&actx.activity.input),
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

    let output_value = match output {
        Some(o) => fail_or!(
            out,
            Some(activity),
            actx.activity.execution,
            env.eval_json(o, &states, &local_scope)
        ),
        None => raw_result.clone(),
    };

    out.emit_event(Event::StateCompleted {
        activity: state_completed_value(actx, output_value.clone()),
    });

    emit_transition(
        out,
        actx.activity.execution,
        activity,
        &output_value,
        next,
        end,
    );
}
