//! Per-`Event` applier implementations, one unit-struct per file.
//!
//! Each applier knows its [`Event`](crate::types::event::Event) variant via [`EventApplier::event`] and
//! folds it into Storage (and, for timers, the scheduler). Splitting them one-per-file mirrors how
//! the command handlers live one-per-file under `handlers/`, keeping each fold rule local and
//! greppable. The [`EventDispatcher`](crate::applier::EventDispatcher) registers all of them via the
//! `event_applier_entry!` macro against the variants they self-describe.

mod execution_completed;
mod execution_completing;
mod execution_created;
mod execution_terminated;
mod execution_terminating;
mod flow_created;
mod flow_version_created;
mod state_activated;
mod state_activating;
mod state_completed;
mod state_completing;
mod state_terminated;
mod state_terminating;
mod state_transitioned;
mod task_activated;
mod task_cancelled;
mod task_completed;
mod task_failed;
mod task_lease_expired;
mod tasks_claimed;
mod thread_completed;
mod thread_completing;
mod thread_created;
mod thread_terminated;
mod thread_terminating;
mod timer_activated;
mod timer_cancelled;
mod timer_triggered;
mod variables_assigned;

pub(crate) use execution_completed::ExecutionCompletedApplier;
pub(crate) use execution_completing::ExecutionCompletingApplier;
pub(crate) use execution_created::ExecutionCreatedApplier;
pub(crate) use execution_terminated::ExecutionTerminatedApplier;
pub(crate) use execution_terminating::ExecutionTerminatingApplier;
pub(crate) use flow_created::FlowCreatedApplier;
pub(crate) use flow_version_created::FlowVersionCreatedApplier;
pub(crate) use state_activated::StateActivatedApplier;
pub(crate) use state_activating::StateActivatingApplier;
pub(crate) use state_completed::StateCompletedApplier;
pub(crate) use state_completing::StateCompletingApplier;
pub(crate) use state_terminated::StateTerminatedApplier;
pub(crate) use state_terminating::StateTerminatingApplier;
pub(crate) use state_transitioned::StateTransitionedApplier;
pub(crate) use task_activated::TaskActivatedApplier;
pub(crate) use task_cancelled::TaskCancelledApplier;
pub(crate) use task_completed::TaskCompletedApplier;
pub(crate) use task_failed::TaskFailedApplier;
pub(crate) use task_lease_expired::TaskLeaseExpiredApplier;
pub(crate) use tasks_claimed::TasksClaimedApplier;
pub(crate) use thread_completed::ThreadCompletedApplier;
pub(crate) use thread_completing::ThreadCompletingApplier;
pub(crate) use thread_created::ThreadCreatedApplier;
pub(crate) use thread_terminated::ThreadTerminatedApplier;
pub(crate) use thread_terminating::ThreadTerminatingApplier;
pub(crate) use timer_activated::TimerActivatedApplier;
pub(crate) use timer_cancelled::TimerCancelledApplier;
pub(crate) use timer_triggered::TimerTriggeredApplier;
pub(crate) use variables_assigned::VariablesAssignedApplier;

/// Raise the partition's generated-name counter to `suffix + 1` when a create event folds a
/// generated-name child. At runtime the counter is already advanced when the name was minted
/// (`Collector::next_generated_seq`); this **replays it** so that on recovery — where storage is
/// rebuilt from events and mint never re-runs — the counter is reconstructed from the persisted
/// names and no future mint collides. Idempotent (`max`); a plain (non-counter) name carries no
/// suffix and is skipped.
pub(crate) async fn bump_generated_seq(
    storage: &mut dyn crate::storage::StorageTxn,
    name: &crate::types::meta::ObjectName,
) -> Result<(), crate::types::error::ExecutionError> {
    let Some(suffix) = name.suffix() else {
        return Ok(());
    };
    let current = storage.next_generated_seq().await?;
    storage
        .put_next_generated_seq(current.max(suffix as i64 + 1))
        .await?;
    Ok(())
}
