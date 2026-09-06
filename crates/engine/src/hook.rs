//! The injectable **observation** seam between the engine and the world around it.
//!
//! The engine's job is to drive the ASL state machine through the causal log and the projection; it
//! should not itself own tangential concerns such as acknowledging awaiting callers, publishing an
//! outbox, or emitting external notifications. Those are *observers* of what the engine does, not
//! participants in its processing.
//!
//! [`Hook`] is that seam: a pure-observation trait whose methods report **facts** the engine
//! definitively established — an `Event` was applied (durably), a command was rejected. A task grant
//! is reported only in its durable form (the `TasksClaimed` event), so every fact a hook observes is
//! grounded in the log. Implementations learn those facts but never direct or read back the engine's
//! in-flight processing, and the engine never blocks on a hook. Every method has a default no-op so
//! an implementation picks only the facts it cares about.
//!
//! ACK delivery is the first observer (spica-server's `AckHook` is the first concrete implementation
//! outside the engine): correlation of an applied event to its awaiting request is entirely its business,
//! keyed by the `request_id` an awaited event carries. Future observers (outbox, metrics, external
//! notifications) compose on the same seam.
//!
//! Transaction lifecycle (open / commit / rollback) is a natural extension point of this trait for a
//! future observer that must react to durability boundaries as such; the migration that introduced
//! [`Hook`] deliberately wires only the three facts ACK needs, and leaves those lifecycle methods for
//! the observer that actually needs them.

use async_trait::async_trait;

use crate::types::event::Event;
use crate::types::id::RequestId;
use crate::types::reject::Reject;

/// A fact reported to an injected observer after the engine processes an entry.
///
/// Observation-only: methods are fire-and-forget notifications of what the engine already decided
/// and made durable. Implementations must be cheap and side-effect tolerant (a hook failure is
/// never the engine's failure — it is logged, never propagated), and must not call back into the
/// engine in a way that re-enters the processing loop.
#[async_trait]
pub trait Hook: Send + Sync {
    /// An [`Event`] was applied to Storage and its batch committed — the event is durable. Fired
    /// once per event, strictly after the transaction commits (post-commit), so an observer may
    /// treat the event as durable ground truth. This is the fact ACK correlates to a waiting
    /// request.
    async fn on_event_applied(&self, _event: &Event) {}

    /// A client-originated awaiting command was refused application, carrying the durable [`Reject`]
    /// that records the refusal.
    async fn on_command_rejected(&self, _request_id: RequestId, _reject: &Reject) {}
}
