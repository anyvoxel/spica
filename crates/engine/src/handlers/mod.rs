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
pub use spawn_thread::SpawnThreadHandler;
pub use terminate_execution::TerminateExecutionHandler;
pub use terminate_state::TerminateStateHandler;
pub use terminate_thread::TerminateThreadHandler;
pub use trigger_timer::TriggerTimerHandler;

pub(crate) use dispatch::{build_state_handlers, dispatch_command};

use std::collections::HashMap;

use serde_json::Value;
use spica_asl::{AssignObject, StateMachine};

use crate::Variables;
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext};
use crate::types::command::{
    ActivateState, Command, CompleteThread, TerminateExecution, TerminateThread, TerminationReason,
};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::{Event, StateTransitioned, VariablesAssigned};
use crate::types::meta::{ObjectKind, ObjectReference};
use crate::types::state_path::StatePath;
use crate::{Activity, ActivityStatus};

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

/// Resolve a `States` table (a `HashMap<String, State>`) within the shared machine document by a
/// JSON Pointer of the form `/States` (the machine's own top-level table), or
/// `/States/<P>/Branches/<i>/States/<P2>/Branches/<j>/States…` for a `Parallel`, or
/// `/States/<M>/ItemProcessor/States/<P>/Branches/<i>/…` for a `Map` — the exact pointers a thread
/// carries. Each descent names the container state and the child of it the run descends into (a
/// `Parallel` branch index, or the `Map`'s single `ItemProcessor`), then that child's `States`
/// opener; the walk lands on the deepest table in one pass (arbitrary nesting depth). Any mix of
/// `Parallel` and `Map` steps composes naturally because every step yields a `States` table of the
/// same type. The tokens are the document's own keys (the constants on [`StatePath`]), compared
/// case-sensitively: a path recorded against a differently-spelled key is malformed, not a
/// silently different table.
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
        // A pointer with nothing left names the table the walk currently stands on. Two shapes land
        // here: the empty pointer of a thread recorded before this pointer spelled out its `States`
        // opener (still read, so an old log replays), and the tail of every descent below — the
        // opener is what a pointer ends *on*, so the table it names is simply where it stopped.
        if i >= tokens.len() {
            return Ok(states);
        }
        // Each descent begins with the `States` opener naming a container state.
        if tokens[i].decoded().as_ref() != StatePath::STATES {
            return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                format!(
                    "state_path malformed: expected '{}', got '{}'",
                    StatePath::STATES,
                    tokens[i].decoded()
                ),
            )));
        }
        i += 1; // consumed the "States" opener
        // The opener with nothing behind it *is* the target: `/States` is the machine's top-level
        // table, `/States/<P>/Branches/<i>/States` that branch's.
        if i >= tokens.len() {
            return Ok(states);
        }
        let name = tokens[i].decoded();
        let state = states.get(name.as_ref()).ok_or_else(|| {
            ExecutionError::Runtime(RuntimeError::StateNotFound(name.as_ref().to_string()))
        })?;
        i += 1; // consumed <name>
        match state {
            // A `Parallel` descent names a branch: `Branches/<idx>`, whose `States` opener the next
            // iteration consumes. Consumes `Branches` + <idx>.
            S::Parallel(p) => {
                // `Cow<str> == &str` compares the decoded token against the constant without building
                // a borrowed reference to a temporary.
                if !tokens
                    .get(i)
                    .is_some_and(|t| t.decoded() == StatePath::BRANCHES)
                {
                    return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        format!("state_path malformed: expected '{}'", StatePath::BRANCHES),
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
                i += 2; // consumed "Branches" + <idx>
                states = &branch.states;
            }
            // A `Map` descent names its single `ItemProcessor` (no index): every item runs the same
            // processor, so the pointer names the `ItemProcessor` token and its `States` opener the
            // next iteration consumes. Consumes `ItemProcessor` only.
            S::Map(m) => {
                if !tokens
                    .get(i)
                    .is_some_and(|t| t.decoded() == StatePath::ITEM_PROCESSOR)
                {
                    return Err(ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        format!(
                            "state_path malformed: expected '{}'",
                            StatePath::ITEM_PROCESSOR
                        ),
                    )));
                }
                let processor = m.item_processor.as_ref().ok_or_else(|| {
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                        "Map state '{name}' has no ItemProcessor"
                    )))
                })?;
                i += 1; // consumed "ItemProcessor"
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
        // Loop: the next token is either another `States` opener (a nested container) or the pointer
        // ended — in which case the next iteration returns this child's states.
    }
}

/// Resolve a state definition for the scope owning the current activity, honoring a scope's
/// `state_path`: a `Thread` (Parallel-branch / Map-item child) resolves its state within the shared
/// machine at the pointer location (one flat lookup, no parent/root query); a top-level `Execution`
/// falls back to the machine's top-level `States`. The `scope` is a loaded [`ScopeRecord`], so the
/// caller (which already resolved the owning scope) passes it — no second storage read.
pub(super) async fn resolve_state_for<'a>(
    sm: &'a StateMachine,
    scope: &crate::storage::ScopeRecord,
    state_name: &str,
) -> Result<&'a spica_asl::State, ExecutionError> {
    match scope.state_path() {
        // Thread: resolve within its branch/ItemProcessor's `States` table.
        Some(pointer) => {
            let states = resolve_states_map(sm, pointer.as_ptr())?;
            states.get(state_name).ok_or_else(|| {
                ExecutionError::Runtime(RuntimeError::StateNotFound(state_name.to_string()))
            })
        }
        // Top-level execution: the machine's top-level `States`.
        None => resolve_state(sm, state_name),
    }
}

/// Resolve a state definition by its full `state_path` (a JSON Pointer from the machine root to the
/// state: `/States/<name>` for a top-level state, or `/States/.../Branches/<idx>/States/<name>` for
/// a branch / item). The carried path makes `Command::ActivateState` self-locating — the lookup no
/// longer infers the enclosing `States` table from the owning scope's stored `state_path`. A
/// top-level path is exactly `/States/<leaf>` (two tokens) and maps to the machine root; any deeper
/// path's parent is a container's `States` table, resolved via `resolve_states_map`.
pub(crate) fn resolve_state_from_path<'a>(
    sm: &'a StateMachine,
    state_path: &StatePath,
) -> Result<&'a spica_asl::State, ExecutionError> {
    let leaf = state_path.state_name();
    if state_path.tokens().count() == 2 {
        // `/States/<leaf>` — a top-level state, resolved against the machine's top-level `States`.
        return resolve_state(sm, &leaf);
    }
    let mut parent = state_path.as_ptr().to_owned();
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
        ObjectKind::Execution => {
            out.append_command(Command::TerminateExecution(TerminateExecution {
                name: scope.name.clone(),
                uid: Some(scope.uid),
                reason,
            }))
        }
        ObjectKind::Thread => out.append_command(Command::TerminateThread(TerminateThread {
            thread: scope.clone(),
            reason,
        })),
        _ => {
            // An activity is always owned by a scope; terminating into any other owner is an
            // internal fault with nowhere to route — nothing to emit, the failure is dropped.
            tracing::error!(owner = %scope, "terminal fail on a non-scope owner; cannot terminate");
        }
    }
}

/// Cancel every active timer child of `activity`. A `Task` state's
/// [`on_completing`](state_handler::StateHandler::on_completing) calls this for its own timers — a
/// `TaskTimeout` only bounds the state, so it is swept as part of finishing rather
/// than waited out. The task **failure** handlers call it directly too, for the paths that never reach
/// `complete` (a retry re-queues the task, a terminal failure routes to `Catch`/terminate): a settled
/// attempt must leave no live child behind. Idempotent: a timer already fired or cancelled is not an
/// active child and is simply skipped.
///
/// Emits the `TimerCancelled` **events** directly (rather than `CancelTimer` commands) so they fold
/// into the *current* batch, ahead of whatever the caller does next — a `CancelTimer` command would
/// only produce `TimerCancelled` as a later log entry, after which the activity had already been read
/// with the child still attached. The applier deschedules the deadline and detaches the child, which is
/// all these callers need: a completing activity is already past the point of wanting a deadline, and a
/// failing one is deciding its own next move — neither wants a parent drain reaction here.
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
        out.append_event(crate::types::event::Event::TimerCancelled {
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
                    m.with_update_at(ctx.now());
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
    activity_state_path: &StatePath,
    output: &Value,
    next: Option<&str>,
    end: Option<bool>,
) {
    if end == Some(true) {
        // Terminal hop: no next state to route to, so there's no `StateTransitioned` marker — just
        // fold the output and complete the owning Thread. Every state's owner is a Thread (the
        // derived root thread for a top-level run, or a fan-out thread for a branch/item); a root
        // thread's success is bridged to its Execution in `complete_thread`, so no Execution/Thread
        // dialect is needed here.
        out.append_command(Command::CompleteThread(CompleteThread {
            thread: owner,
            output: output.clone(),
        }));
    } else if let Some(next) = next {
        // The successor lives as a sibling of the completing state in the same enclosing `States`
        // table — that table is the completing activity's `state_path` minus its own leaf.
        let next_path = activity_state_path.sibling(next);
        // The marker carries the resolved target *path* (self-locating), not a bare name that would
        // need the completing activity's context to be reconstructed.
        out.append_event(crate::types::event::Event::StateTransitioned(
            StateTransitioned {
                activity,
                next: next_path.as_ptr().to_owned(),
            },
        ))
        .await;
        // The successor's activity id is allocated inside the `ActivateState` handler (see its doc).
        out.append_command(Command::ActivateState(ActivateState {
            execution,
            owner,
            state_path: next_path,
            input: output.clone(),
        }));
    } else {
        out.terminate(
            Some(activity),
            execution,
            ExecutionError::Runtime(RuntimeError::NoTerminal),
        );
    }
}

/// Advance a completing activity value to its completed lifecycle moment and emit `StateCompleted` —
/// the terminator every success finish ends on (the base `StateHandler::finish`, and a container's own
/// `finish_parallel`/`finish_map`), so the event's payload shape stays identical across states. The
/// caller passes the value already advanced to `Completing`, so its `raw_output` is already settled.
pub(super) async fn emit_state_completed(
    out: &mut Collector<'_>,
    activity_value: &Activity,
    output_value: &Value,
) {
    let mut completed = activity_value.clone();
    completed.meta.with_update_at(out.now());
    completed.status = ActivityStatus::Completed;
    completed.output = Some(output_value.clone());
    out.append_event(Event::StateCompleted {
        activity: completed,
    })
    .await;
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
    let timer_uid: ulid::Ulid = out.mint();
    let timer_name = execution
        .name
        .base()
        .generated_from_key(out.next_generated_seq().await);
    out.append_event(Event::TimerActivated {
        timer: crate::Timer {
            execution,
            purpose,
            status: crate::TimerStatus::Active,
            deadline,
            meta: crate::types::meta::ObjectMeta::builder(ObjectKind::Timer, timer_uid)
                .name(timer_name)
                .at(out.now())
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
/// `StateCompleting` (the ing) is **not** emitted here — each state's `complete` opens the finish with
/// it, so this helper only carries the successful projection tail for paths that already opened the
/// complete step (the exiting `Catch` route).
///
/// This runs only from the `complete` step (see [`state_handler::StateHandler::complete`]) — never
/// from `activate`. Reads variable mutation from `Assign` into the local variables used for the output
/// projection, then run the inline child-settled reaction that drains the parent (once drained).
#[allow(clippy::too_many_arguments)]
pub(super) async fn complete_activity(
    env: &mut EvalEnv,
    out: &mut Collector<'_>,
    activity: ObjectReference,
    activity_value: &Activity,
    variables: &Variables,
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
    let raw_result = activity_value
        .raw_output
        .as_ref()
        .unwrap_or(&activity_value.raw_input);
    // Activate-phase Assign was already applied (mutating scope); the output projection runs with
    // that updated scope so it can reference the Just-assigned variables.
    let states = States::new(
        &activity_value.raw_input,
        &activity_value.state_path.state_name(),
        retry_count,
    )
    .with_result(Some(raw_result))
    .with_assign_ctx(Some(&activity_value.raw_input))
    .with_error_output(error_output)
    .build();
    let mut local_scope = variables.clone();

    if let Some(assign_obj) = assign {
        let assign_value = Value::Object(assign_obj.0.clone());
        let evaluated = fail_or!(
            out,
            Some(activity),
            activity_value
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
                    out.append_event(Event::VariablesAssigned(VariablesAssigned {
                        scope: activity_value
                            .meta
                            .owner
                            .clone()
                            .expect("an owned activity has an owner"),
                        variables: local_scope.clone(),
                    }))
                    .await;
                }
            }
            _ => {
                out.terminate(
                    Some(activity),
                    activity_value
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
            activity_value
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner"),
            env.eval_json(o, &states, &local_scope)
        ),
        None => raw_result.clone(),
    };

    // `complete_activity` only borrows `activity_value`, so the completed payload is a fresh copy
    // advanced in place — `state_completed_value` was removed.
    let mut completed = activity_value.clone();
    completed.meta.with_update_at(out.now());
    completed.status = ActivityStatus::Completed;
    completed.output = Some(output_value.clone());
    if completed.raw_output.is_none() {
        completed.raw_output = Some(completed.raw_input.clone());
    }
    out.append_event(Event::StateCompleted {
        activity: completed,
    })
    .await;

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
        next,
        end,
    )
    .await;
}
