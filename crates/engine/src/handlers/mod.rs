/// Evaluate `$expr` (a `Result`); on `Ok` yield the value, on `Err` emit the failure to `$out`
/// (`TerminateState` for `$activity` if `Some`, plus the scope termination of `$scope`) and
/// `return`. The failure path always goes through `Collector::terminate` so a failing site records
/// its own outcome cohesively before the lifecycle cascade unwinds.
///
/// Two forms, because the failure arm has to return the *caller's* type: the plain form belongs to a
/// helper whose caller owns the outcome (the state terminated in-band, so the helper returns `()`),
/// while `result` belongs to a handler's own dispatch body, which ends by telling the leader it
/// produced no outcome.
macro_rules! fail_or {
    (result, $out:expr, $activity:expr, $scope:expr, $expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                $out.terminate($activity, $scope, e);
                return Ok(());
            }
        }
    };
    ($out:expr, $activity:expr, $scope:expr, $expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                $out.terminate($activity, $scope, e);
                return;
            }
        }
    };
}

mod activate_state;
mod activate_task;
mod cancel_task;
mod cancel_timer;
mod child_completed;
mod claim_tasks;
mod complete_execution;
mod complete_state;
mod complete_task;
mod complete_thread;
pub(crate) mod container;
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
pub use cancel_task::CancelTaskHandler;
pub use cancel_timer::CancelTimerHandler;
pub use claim_tasks::ClaimTasksHandler;
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

use serde_json::Value;
use spica_asl::AssignObject;

use crate::StatePath;
use crate::Variables;
use crate::eval_env::EvalEnv;
use crate::handler::{Collector, HandlerContext};
use crate::types::command::{
    ActivateState, Command, CompleteThread, TerminateExecution, TerminateThread, TerminationReason,
};
use crate::types::context::States;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::{Event, StateTransitioned, VariablesAssigned};
use crate::types::meta::{ErasedOwner, ObjectKind, ObjectReference, OwnerScope};
use crate::{Activity, ActivityStatus};

// ── Shared helpers ───────────────────────────────────────────────────────────

/// Direct a terminal failure (or abort) at the scope that owns the given activity: the top-level run
/// is an [`OwnerScope::Execution`] (name+uid-addressed `TerminateExecution`), while a `Parallel`
/// branch / `Map` item's scope is an [`OwnerScope::Thread`], which lives in **thread** storage and is
/// only reachable via the reference-addressed `TerminateThread`. The two roles are a *type*, not a
/// runtime kind test: a caller that only holds "the scope above me" cannot reach for the verb the
/// other store answers to — a bare `TerminateExecution` silently misses a Thread and leaves the
/// branch Running, wedging its container.
pub(super) fn emit_scope_termination(
    out: &mut Collector<'_>,
    scope: &OwnerScope,
    reason: TerminationReason,
) {
    match scope {
        OwnerScope::Execution(execution) => {
            out.append_command(Command::TerminateExecution(TerminateExecution {
                name: execution.name().clone(),
                uid: Some(execution.uid()),
                reason,
            }))
        }
        OwnerScope::Thread(thread) => {
            out.append_command(Command::TerminateThread(TerminateThread {
                thread: thread.erased().clone(),
                reason,
            }))
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
        let mut timer_value = t.value.clone();
        timer_value.cancel(ctx.now());
        out.append_event(crate::types::event::Event::TimerCancelled { timer: timer_value })
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
            OwnerScope::of_reference(&execution),
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
    owner: &Activity,
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
            meta: crate::types::meta::ObjectMeta::builder(timer_uid)
                .name(timer_name)
                .at(out.now())
                // An inline timer is always armed by the activity whose deadline it is, so the slot's
                // `Activity` variant is built here from the activity's own identity — no flat
                // reference is passed in, and a wrong kind cannot reach the slot.
                .with_owner(crate::types::meta::TimerOwner::Activity(
                    crate::types::meta::OwnerRef::new(owner.meta.name.clone(), owner.meta.uid),
                )),
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
    // A thread is the only thing that can own an activity (the slot's own type), so the owners below
    // need no `kind` guard; the emitters take the flat address storage and commands speak, so the
    // erasure happens once here instead of at each of them.
    let owner = activity_value.meta.owner.clone().into_erased();

    if let Some(assign_obj) = assign {
        let assign_value = Value::Object(assign_obj.0.clone());
        let evaluated = fail_or!(
            out,
            Some(activity),
            OwnerScope::of_reference(&owner),
            env.eval_json(&assign_value, &states, &local_scope)
        );
        match evaluated {
            Value::Object(map) => {
                if !map.is_empty() {
                    for (k, v) in map {
                        local_scope.insert(k, v);
                    }
                    out.append_event(Event::VariablesAssigned(VariablesAssigned {
                        scope: owner.clone(),
                        variables: local_scope.clone(),
                    }))
                    .await;
                }
            }
            _ => {
                out.terminate(
                    Some(activity),
                    OwnerScope::of_reference(&owner),
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
            OwnerScope::of_reference(&owner),
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
        owner.clone(),
        activity,
        &activity_value.state_path,
        &output_value,
        next,
        end,
    )
    .await;
}
