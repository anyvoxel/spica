//! Event projection, as a per-variant applier dispatch.
//!
//! Storage is a pure fold of the [`Event`] stream; the [`StreamProcessor`](crate::StreamProcessor) rebuilds the
//! execution tree by applying each event. Mirroring the [`Command`](crate::types::command::Command)
//! dispatch, projection is split into per-`Event` applier implementations (each in
//! [`appliers`]), keeping each event's fold rule local.
//!
//! An applier is a **pure function of the fold**: it mutates the store only. External side effects are
//! *derived* elsewhere from the durable event itself — e.g. a `TimerActivated` event carries the
//! absolute `deadline`, so a consumer (via the injected [`Hook`](crate::Hook) observer) re-derives the
//! physical timer arm from the event without the engine knowing a scheduler. Keeping the fold free of
//! any environment (no scheduler handle, no log) makes the same fold safe on both the pre-append work
//! transaction and post-commit recovery replay alike.
//!
//! A storage implementation needs only supply the read/mutate primitives ([`Storage`](crate::Storage));
//! the applier dispatch is a single exhaustive [`dispatch_event`] match.

mod appliers;

use crate::log::Timestamp;
use crate::types::error::ExecutionError;
use crate::types::event::Event;

use appliers::*;

/// Route one `Event` to its applier's fold. Exhaustive: every variant maps to an inherent applier
/// method (`XApplier::apply`), so adding an `Event` variant fails to compile until a match arm
/// exists — the compile-time guarantee that previously came from a `Discriminant` table.
pub async fn dispatch_event(
    ctx: &mut ApplierContext<'_>,
    event: &Event,
) -> Result<(), ExecutionError> {
    match event {
        Event::ExecutionCreated(p) => ExecutionCreatedApplier.apply(ctx, p).await,
        Event::ExecutionCompleting { execution } => {
            ExecutionCompletingApplier.apply(ctx, execution).await
        }
        Event::ExecutionCompleted { execution } => {
            ExecutionCompletedApplier.apply(ctx, execution).await
        }
        Event::ExecutionTerminating { execution } => {
            ExecutionTerminatingApplier.apply(ctx, execution).await
        }
        Event::ExecutionTerminated { execution } => {
            ExecutionTerminatedApplier.apply(ctx, execution).await
        }
        Event::FlowCreated(p) => FlowCreatedApplier.apply(ctx, p).await,
        Event::FlowVersionCreated(p) => FlowVersionCreatedApplier.apply(ctx, p).await,
        Event::StateActivating { activity } => StateActivatingApplier.apply(ctx, activity).await,
        Event::StateActivated { activity } => StateActivatedApplier.apply(ctx, activity).await,
        Event::StateCompleting { activity } => StateCompletingApplier.apply(ctx, activity).await,
        Event::StateCompleted { activity } => StateCompletedApplier.apply(ctx, activity).await,
        Event::StateTerminating { activity } => StateTerminatingApplier.apply(ctx, activity).await,
        Event::StateTerminated { activity } => StateTerminatedApplier.apply(ctx, activity).await,
        Event::TimerActivated { timer } => TimerActivatedApplier.apply(ctx, timer).await,
        Event::TimerTriggered { timer } => TimerTriggeredApplier.apply(ctx, timer).await,
        Event::TimerCancelled { timer } => TimerCancelledApplier.apply(ctx, timer).await,
        Event::VariablesAssigned(p) => VariablesAssignedApplier.apply(ctx, p).await,
        Event::StateTransitioned(p) => StateTransitionedApplier.apply(ctx, p).await,
        Event::TaskActivated { task } => TaskActivatedApplier.apply(ctx, task).await,
        Event::TasksClaimed(p) => TasksClaimedApplier.apply(ctx, p).await,
        Event::TaskLeaseExpired { task } => TaskLeaseExpiredApplier.apply(ctx, task).await,
        Event::TaskCompleted(p) => TaskCompletedApplier.apply(ctx, p).await,
        Event::TaskFailed(p) => TaskFailedApplier.apply(ctx, p).await,
        Event::TaskCancelled { task } => TaskCancelledApplier.apply(ctx, task).await,
        Event::ThreadCreated { thread } => ThreadCreatedApplier.apply(ctx, thread).await,
        Event::ThreadCompleting { thread } => ThreadCompletingApplier.apply(ctx, thread).await,
        Event::ThreadCompleted { thread } => ThreadCompletedApplier.apply(ctx, thread).await,
        Event::ThreadTerminating { thread } => ThreadTerminatingApplier.apply(ctx, thread).await,
        Event::ThreadTerminated { thread } => ThreadTerminatedApplier.apply(ctx, thread).await,
    }
}

/// Context handed to a single event applier `apply` call: mutable access to the store, plus the
/// **`timestamp`** of the entry currently being applied — the single deterministic source for the
/// projection's `created_at`/`updated_at` facts; it is the value frozen in the log record, so every
/// replica replaying the same entries computes identical times (see `storage::*::created_at`).
/// Appliers must use this value and **never** call `Timestamp::now()` locally, which would make the
/// fold non-deterministic across replicas.
///
/// M1→M2 note: there is **no** task service on the context. Applying `TaskActivated` used to invoke
/// the handler in-process as a side effect; now it only makes the task *claimable* — a worker pulls
/// it via the engine's task API (the worker-side contract lives in `spica-client`'s `worker` module).
/// Task settlements are inbound reports the engine validates, not side effects of a fold.
pub struct ApplierContext<'a> {
    /// Write handle into the projection. A [`StorageTxn`](crate::storage::StorageTxn), **not** the
    /// raw [`Storage`](crate::storage::Storage): the applier can fold rows but cannot commit (which
    /// consumes the `Box`) nor move the resume watermark — atomicity is *type-enforced*.
    pub storage: &'a mut dyn crate::storage::StorageTxn,
    pub timestamp: Timestamp,
}
