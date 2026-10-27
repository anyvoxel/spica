//! The deadline scheduling **contract**, co-located with its concrete implementations.
//!
//! These traits describe the whole scheduler seam — the scheduler itself plus the write channel by
//! which a fired deadline is handed back to the engine. The engine does not hold a scheduler: it
//! exposes the durable facts that record a deadline (an `ExecutionCreated` carrying the run's
//! `TimeoutSeconds`, a `TaskActivated` carrying the task's, a `StateActivated` carrying a `Wait`'s
//! resume instant), and an injected [`Hook`](spica_engine::Hook) observer re-derives the physical
//! arm from them. They live **here** (not in `spica-engine`) because the engine never references
//! them — the seam is implemented and consumed entirely on this, downstream, side of the graph, and
//! both traits carry only `spica-engine` value types
//! ([`DeadlineTarget`](spica_engine::DeadlineTarget), [`Timestamp`](spica_engine::Timestamp)).
//!
//! A scheduler is a **producer** of engine commands — a fired deadline must reach the log. Rather
//! than the engine *pulling* fired commands back, the scheduler *pushes* into an injected
//! [`DeadlineSink`]: the consumer attaches a sink via [`Scheduler::attach_sink`] and the concrete
//! scheduler calls [`DeadlineSink::trigger`] on expiry. The sink owns the append (it funnels through
//! the engine's single-writer path), so the scheduler never writes to the log directly. Keeping the
//! loop separate from dispatch means an external `TerminateExecution` (or another deadline) flows
//! through the stream while a `Wait` is pending — the earlier deadlock where an inline `sleep`
//! blocked the whole stream can't recur.

use std::sync::Arc;

use spica_engine::{DeadlineTarget, Timestamp};

/// The write channel by which a fired deadline's command reaches the log.
///
/// On expiry the scheduler calls [`DeadlineSink::trigger`] instead of writing to the log itself. The
/// sink implementation (owned by the consumer) funnels the write through the engine's single-writer
/// append path, which is where the expiry-triggered write is validated before it is appended. The
/// consumer attaches the sink via [`Scheduler::attach_sink`] *after* constructing the scheduler —
/// the scheduler is caller-created first, so the sink cannot be a constructor argument (the push
/// model's wiring gap, closed at assembly).
#[async_trait::async_trait]
pub trait DeadlineSink: Send + Sync {
    /// `target`'s `deadline` elapsed: append the `TriggerDeadline` that lets the target act on it.
    /// The write carries no `cause_id` — causal provenance of the fire is derived from the entry that
    /// recorded the deadline, not from an explicit cause. The log assigns the append's position and
    /// its own stream id.
    async fn trigger(&self, target: &DeadlineTarget, deadline: Timestamp);
}

/// Contract for the deadline side-effect service.
///
/// A scheduler is a **producer** of engine commands — a fired deadline must reach the log. Rather
/// than the engine pulling the fired command back, the scheduler *pushes* into a caller-injected
/// [`DeadlineSink`] via [`attach_sink`](Self::attach_sink), and the scheduler calls
/// [`DeadlineSink::trigger`] on expiry. The scheduler never touches the log directly — the sink keeps
/// the write funneled through the engine. Object safety: `#[async_trait]` erases the one async method
/// (on `DeadlineSink`), and every method takes `&self` so the contract works behind `&dyn Scheduler` /
/// `Arc<dyn Scheduler>`.
#[async_trait::async_trait]
pub trait Scheduler: Send + Sync {
    /// Inject the [`DeadlineSink`] this scheduler calls on expiry. Attached once by the consumer
    /// before any deadline can be armed, so a sink is always present by the time one fires.
    fn attach_sink(&self, sink: Arc<dyn DeadlineSink>);

    /// Arm `target` to fire at the absolute `deadline`. Deliberately carries no `cause_id`: causal
    /// provenance of a fired deadline is derived from the entry that recorded it, so the arm needs
    /// none and the [`DeadlineSink::trigger`] it drives none either. There is no stream to route to —
    /// a log is one stream, so the fired command is handed to the attached sink and the log stamps
    /// its own stream id when it is appended.
    ///
    /// A re-arm for the same target replaces the prior deadline.
    fn schedule(&self, target: &DeadlineTarget, deadline: Timestamp);

    /// Cancel a previously-armed `target` (its element finished before the deadline).
    fn cancel(&self, target: &DeadlineTarget);

    /// Re-evaluate the armed deadlines against the clock's current reading — "time has moved, look
    /// again". Required **only** of a caller that moves the clock itself (a manual clock substituted
    /// for the wall clock, e.g. in a test): nothing inside the scheduler can observe such an advance,
    /// so without this call a deadline that has passed would stay armed until some other event woke
    /// the loop. With the wall clock the loop's own wait already covers every advance and this is
    /// never needed, which is why the default is a no-op.
    ///
    /// A caller must inject the *same* [`Clock`](spica_machinery::Clock) it advances here and into
    /// the engine (see `EngineBuilder::with_clock`): deadlines are computed against one clock and
    /// awaited against another otherwise.
    fn tick(&self) {}
}
