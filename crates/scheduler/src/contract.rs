//! The timer scheduling **contract**, co-located with its concrete implementations.
//!
//! These traits describe the whole scheduler seam — the scheduler itself plus the write channel by
//! which a fired timer is handed back to the engine. The engine no longer holds a scheduler: it
//! re-derives timer arms/cancels through the injectable [`Hook`](spica_engine::Hook) observer, and
//! the consumer owns both this contract's use and its wiring. They live **here** (not in
//! `spica-engine`) because the engine never references them anymore — the seam is implemented and
//! consumed entirely on this, downstream, side of the graph, and both traits carry only `spica-engine`
//! value types ([`ObjectReference`](spica_engine::ObjectReference), [`Timestamp`](spica_engine::Timestamp)).
//!
//! A scheduler is a **producer** of engine commands — a fired `TriggerTimer` must reach the log.
//! Rather than the engine *pulling* fired commands back, the scheduler *pushes* into an injected
//! [`TimerSink`]: the consumer attaches a sink via [`Scheduler::attach_sink`] and the concrete
//! scheduler calls [`TimerSink::trigger`] on expiry. The sink owns the append (it funnels through the
//! engine's single-writer path), so the scheduler never writes to the log directly. Keeping the loop
//! separate from dispatch means an external `TerminateExecution` (or another timer) flows through the
//! stream while a `Wait`'s timer is pending — the earlier deadlock where an inline `sleep` blocked the
//! whole stream can't recur.

use std::sync::Arc;

use spica_engine::{ObjectReference, Timestamp};

/// The write channel by which a fired timer's resumption reaches the log.
///
/// On expiry the scheduler calls [`TimerSink::trigger`] instead of writing to the log itself. The sink
/// implementation (owned by the consumer) funnels the write through the engine's single-writer
/// append path, which is where the expiry-triggered write is validated before it is appended. The
/// consumer attaches the sink via [`Scheduler::attach_sink`] *after* constructing the scheduler —
/// the scheduler is caller-created first, so the sink cannot be a constructor argument (the push
/// model's wiring gap, closed at assembly).
#[async_trait::async_trait]
pub trait TimerSink: Send + Sync {
    /// A timer's deadline elapsed: append the `TriggerTimer` that resumes the `Wait` state. The
    /// write carries no `cause_id` — causal provenance of the fired trigger is derived from the
    /// entry that armed the timer, not from an explicit cause. The log assigns the append's position
    /// and its own stream id.
    async fn trigger(&self, timer: &ObjectReference);
}

/// Contract for the timer side-effect service.
///
/// A scheduler is a **producer** of engine commands — a fired `TriggerTimer` must reach the log.
/// Rather than the engine pulling the fired command back, the scheduler *pushes* into a
/// caller-injected [`TimerSink`] via [`attach_sink`](Self::attach_sink), and the scheduler calls
/// [`TimerSink::trigger`] on expiry. The scheduler never touches the log directly — the sink keeps the
/// write funneled through the engine. Object safety: `#[async_trait]` erases the one async method (on
/// `TimerSink`), and every method takes `&self` so the contract works behind `&dyn Scheduler` /
/// `Arc<dyn Scheduler>`.
#[async_trait::async_trait]
pub trait Scheduler: Send + Sync {
    /// Inject the [`TimerSink`] this scheduler calls on expiry. Attached once by the consumer before
    /// any `TimerActivated` can arm a timer, so a sink is always present by the time one fires.
    fn attach_sink(&self, sink: Arc<dyn TimerSink>);

    /// Arm `timer` to fire a `TriggerTimer` at the absolute `deadline`. Deliberately carries no
    /// `cause_id`: causal provenance of a fired timer is derived from the entry that armed it, so the
    /// arm needs none and the `TimerSink::trigger` it drives none either. There is no stream to route
    /// to — a log is one stream, so the fired `TriggerTimer` is handed to the attached sink and the
    /// log stamps its own stream id when it is appended.
    fn schedule(&self, timer: &ObjectReference, deadline: Timestamp);

    /// Cancel a previously-armed `timer` (a `TimerCancelled` event was applied).
    fn cancel(&self, timer: &ObjectReference);
}
