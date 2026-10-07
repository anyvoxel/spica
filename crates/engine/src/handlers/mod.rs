mod activate_state;
mod activate_task;
mod cancel_task;
mod cancel_timer;
mod claim_tasks;
mod complete_execution;
mod complete_state;
mod complete_task;
mod complete_thread;
pub(crate) mod container;
mod continue_complete;
mod continue_terminate;
mod create_execution;
mod create_flow;
mod dispatch;
mod fail_task;
#[cfg(test)]
pub(crate) mod fixtures;
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
pub use continue_complete::ContinueCompleteHandler;
pub use continue_terminate::ContinueTerminateHandler;
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

use crate::Activity;
use crate::eval_env::EvalEnv;
use crate::handler::Collector;
use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::execution::ExecutionKind;
use crate::types::meta::ObjectRef;

// ── Shared helpers ───────────────────────────────────────────────────────────

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

/// Mint and arm a timer inline: allocate its uid (a raw `ulid::Ulid`) and derive a generated name
/// (`{execution.name}-{8-char-suffix}`) from the owning execution, then emit `Event::TimerActivated`
/// — the fact that both folds the timer row and arms the physical deadline (see
/// `TimerActivatedApplier`). Inlined rather than a `Command` so the arm lands in the same causal
/// batch as the state decision that triggers it (create_execution already does this for the run's
/// own deadline). The name is decoupled from the timer's `uid` and must be carried forward by
/// later timer events (`TimerTriggered`/`TimerCancelled` preserve the row's meta instead of
/// re-deriving it).
pub(super) async fn emit_timer(
    out: &mut Collector<'_>,
    execution: ObjectRef<ExecutionKind>,
    owner: &Activity,
    deadline: crate::log::Timestamp,
) {
    let timer_uid: ulid::Ulid = out.mint();
    let timer_name = execution
        .name()
        .base()
        .generated_from_key(out.next_generated_seq().await);
    out.append_event(Event::TimerActivated {
        timer: crate::Timer {
            execution,
            status: crate::TimerStatus::Active,
            deadline,
            meta: crate::types::meta::ObjectMeta::builder(timer_uid)
                .name(timer_name)
                .at(out.now())
                // An inline timer is always armed by the activity whose deadline it is, so the slot's
                // `Activity` variant is built here from the activity's own identity — no flat
                // reference is passed in, and a wrong kind cannot reach the slot.
                .with_owner(crate::types::meta::TimerOwner::Activity(
                    crate::types::meta::ObjectRef::new(owner.meta.name.clone(), owner.meta.uid),
                )),
        },
    })
    .await;
}
