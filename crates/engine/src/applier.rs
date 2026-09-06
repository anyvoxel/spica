//! Event projection, as a per-variant applier table.
//!
//! Storage is a pure fold of the [`Event`] stream; the [`StreamProcessor`](crate::StreamProcessor) rebuilds the
//! execution tree by applying each event. Mirroring the [`CommandHandler`](crate::CommandHandler)
//! design, projection is split into per-`Event` applier implementations (each in
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
//! the applier table is shared and constructed once per StreamProcessor.

mod appliers;

use std::collections::HashMap;
use std::mem::discriminant;

use crate::log::Timestamp;
use crate::types::event::Event;

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

/// An `EventApplier` receives one `Event` plus context, mutates Storage, and returns nothing — the
/// projection is a pure fold (an applier never performs external side effects itself). Table-driven
/// like [`CommandHandler`](crate::CommandHandler).
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
    ) -> Result<(), crate::types::error::ExecutionError>;
}

/// Context handed to a single `EventApplier::apply` call: mutable access to the store, plus the
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

/// Consume the collector's accumulated entries and route them to the applier table.
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
            TaskActivatedApplier,
            TasksClaimedApplier,
            TaskLeaseExpiredApplier,
            TaskCompletedApplier,
            TaskFailedApplier,
            TaskCancelledApplier,
            ThreadCreatedApplier,
            ThreadCompletingApplier,
            ThreadCompletedApplier,
            ThreadTerminatingApplier,
            ThreadTerminatedApplier
        );
        Self { handlers: map }
    }

    pub async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), crate::types::error::ExecutionError> {
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
