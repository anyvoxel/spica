//! Event projection, as a per-variant applier table.
//!
//! Storage is a pure fold of the [`Event`] stream; the [`Processor`](crate::Processor) rebuilds the
//! execution tree by applying each event. Mirroring the [`CommandHandler`](crate::CommandHandler)
//! design, projection is split into per-`Event` applier implementations (each in
//! [`appliers`]), keeping each event's fold rule local and — because some events carry *side
//! effects* (arming a timer schedules a physical deadline; cancelling one deschedules it) — the
//! applier context hands each impl a [`ApplierContext`] through which it can both mutate the store
//! and drive the timer [`Scheduler`](crate::scheduler::SchedulerHandle).
//!
//! A storage implementation needs only supply the read/mutate primitives ([`Storage`](crate::Storage));
//! the applier table is shared and constructed once per Processor.

mod appliers;

use std::collections::HashMap;
use std::mem::discriminant;

use crate::event::Event;

use appliers::*;

/// Registers one or more [`EventApplier`]s into a `Discriminant<Event>` dispatch map.
/// Each applier knows which [`Event`] variant it serves via [`EventApplier::event`], which returns
/// that variant as a `Default` placeholder (used only to read its discriminant — real events are
/// folded by the Processor). The applier type is therefore the single source of truth for its own
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
    /// Processor. The [`EventDispatcher`]'s table reads this off the applier to derive its key, so
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
/// to the timer scheduler (for `TimerActivated` / `TimerCancelled` scheduling side effects), plus
/// the envelope identity (`stream_id`, `cause_id`) of the entry currently being applied. The
/// scheduler needs these to later re-envelope the `CompleteTimer` it fires; the Processor sets them
/// from the entry it holds before applying.
pub struct ApplierContext<'a> {
    pub storage: &'a mut dyn crate::storage::Storage,
    pub scheduler: &'a crate::scheduler::SchedulerHandle,
    pub stream_id: crate::id::StreamId,
    pub cause_id: crate::id::EntryId,
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
            StateActivatingApplier,
            StateActivatedApplier,
            StateCompletingApplier,
            StateCompletedApplier,
            StateTerminatingApplier,
            StateTerminatedApplier,
            TimerActivatedApplier,
            TimerCompletedApplier,
            TimerCancelledApplier,
            VariablesAssignedApplier,
            StateTransitionedApplier
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
