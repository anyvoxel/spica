//! Event projection, as a per-variant applier table.
//!
//! Storage is a pure fold of the [`Event`] stream; the [`StreamProcessor`](crate::StreamProcessor) rebuilds the
//! execution tree by applying each event. Mirroring the [`CommandHandler`](crate::CommandHandler)
//! design, projection is split into per-`Event` applier implementations (each in
//! [`appliers`]), keeping each event's fold rule local and — because some events carry *side
//! effects* (arming a timer schedules a physical deadline; cancelling one deschedules it) — the
//! applier context hands each impl a [`ApplierContext`] through which it can both mutate the store
//! and drive the timer [`Scheduler`](crate::scheduler::Scheduler).
//!
//! A storage implementation needs only supply the read/mutate primitives ([`Storage`](crate::Storage));
//! the applier table is shared and constructed once per StreamProcessor.

mod appliers;

use std::collections::HashMap;
use std::mem::discriminant;

use crate::event::Event;
use crate::log::Timestamp;

use appliers::*;

/// Registers one or more [`EventApplier`]s into a `Discriminant<Event>` dispatch map.
/// Each applier knows which [`Event`] variant it serves via [`EventApplier::event`], which returns
/// that variant as a `Default` placeholder (used only to read its discriminant — real events are
/// folded by the StreamProcessor). The applier type is therefore the single source of truth for its own
/// key; there is no hand-written placeholder to keep in sync. `$applier` is captured as a `path` so
/// it can serve both as a type (`<… as EventApplier>`) and as a `Default`-constructible value
/// (`<$applier>::default()`).
///
/// Recursive: `event_applier_entry!` handles the first applier and recurses into the
/// `event_applier_entry!`-rest form; the tail emits nothing.
macro_rules! event_applier_entry {
    ($map:expr, $applier:path $(, $rest:path)*) => {{
        let sample = <$applier as EventApplier>::event(&<$applier>::default());
        $map.insert(std::mem::discriminant(&sample), Box::new(<$applier>::default()));
        event_applier_entry!($map $(, $rest)*);
    }};
    ($map:expr) => {};
}

/// An `EventApplier` receives one `Event` plus context and mutates Storage (and optionally the
/// scheduler). Table-driven like [`CommandHandler`](crate::CommandHandler).
#[async_trait::async_trait]
pub trait EventApplier: Send + Sync {
    /// The [`Event`] variant this applier folds, identified by a `Default` placeholder instance
    /// standing in only to read its discriminant — the real event instances are applied by the
    /// StreamProcessor. The [`EventDispatcher`]'s table reads this off the applier to derive its key, so
    /// the applier is the single source of truth for which variant it handles. Takes `&self` (rather
    /// than being a `Self: Sized` associated function) so the trait stays object-safe for the
    /// `Box<dyn EventApplier>` dispatch table.
    fn event(&self) -> Event;

    async fn apply(
        &self,
        context: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), crate::error::ExecutionError>;
}

/// Context handed to a single `EventApplier::apply` call: mutable access to the store and a handle
/// to the timer scheduler (for `TimerActivated` / `TimerCancelled` scheduling), plus the envelope
/// identity `cause_id` and the **`timestamp`** of the entry currently being applied. The scheduler
/// needs `cause_id` to later re-envelope the resumption command it fires (there is no per-execution
/// stream — a LogStream is one stream, so stream identity lives on the log, not the context);
/// `timestamp` is the single deterministic source for the projection's `created_at`/`updated_at`
/// facts — it is the value frozen in the log record, so every replica replaying the same entries
/// computes identical times (see `storage::*::created_at`). Appliers must use this value and
/// **never** call `Timestamp::now()` locally, which would make the fold non-deterministic across
/// replicas.
///
/// M1→M2 note: there is **no** task service on the context. Applying `TaskActivated` used to invoke
/// the handler in-process as a side effect; now it only makes the task *claimable* — a worker pulls
/// it via the engine's `TaskApi` (see `crate::task_service`). Task settlements are inbound reports
/// the engine validates, not side effects of a fold.
pub struct ApplierContext<'a> {
    /// Write handle into the projection. A [`StorageTxn`](crate::storage::StorageTxn), **not** the
    /// raw [`Storage`](crate::storage::Storage): the applier can fold rows but cannot commit (which
    /// consumes the `Box`) nor move the resume watermark — atomicity is *type-enforced*.
    pub storage: &'a mut dyn crate::storage::StorageTxn,
    pub scheduler: &'a dyn crate::scheduler::Scheduler,
    pub cause_id: crate::id::EntryId,
    pub timestamp: Timestamp,
}

/// Consume the collector's accumulated entries and route them to the applier table.
///
/// Returns nothing; each applier mutates Storage/Scheduler directly.
pub struct EventDispatcher {
    handlers: HashMap<std::mem::Discriminant<Event>, Box<dyn EventApplier>>,
}

impl EventDispatcher {
    pub fn new() -> Self {
        let mut map: HashMap<std::mem::Discriminant<Event>, Box<dyn EventApplier>> = HashMap::new();
        event_applier_entry!(
            map,
            ExecutionCreatedApplier,
            ExecutionCompletingApplier,
            ExecutionCompletedApplier,
            ExecutionTerminatingApplier,
            ExecutionTerminatedApplier,
            FlowCreatedApplier,
            FlowVersionCreatedApplier,
            StateActivatingApplier,
            StateActivatedApplier,
            StateCompletingApplier,
            StateCompletedApplier,
            StateTerminatingApplier,
            StateTerminatedApplier,
            TimerActivatedApplier,
            TimerTriggeredApplier,
            TimerCancelledApplier,
            VariablesAssignedApplier,
            StateTransitionedApplier,
            RetryScheduledApplier,
            ParallelBranchSpawnedApplier,
            TaskActivatedApplier,
            TaskLeasedApplier,
            TaskLeaseExpiredApplier,
            TaskCompletedApplier,
            TaskFailedApplier,
            TaskCancelledApplier
        );
        Self { handlers: map }
    }

    pub async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), crate::error::ExecutionError> {
        let handler = self
            .handlers
            .get(&discriminant(event))
            .expect("an applier is registered for every Event variant");
        handler.apply(ctx, event).await
    }
}

impl Default for EventDispatcher {
    fn default() -> Self {
        Self::new()
    }
}
