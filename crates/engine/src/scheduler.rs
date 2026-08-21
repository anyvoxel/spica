//! Timer scheduling, decoupled from the serial dispatch loop.
//!
//! Arming a timer (`TimerActivated`) is recorded in the stream as a durable fact. The actual
//! *physical* timing is a side effect driven by that fact, not by the command dispatcher: instead
//! of the StreamProcessor special-casing `ActivateTimer` (which previously forced an in-dispatch
//! `sleep`/spawn), the run loop feeds `TimerActivated`/`TimerCancelled` events into the
//! [`Scheduler`] contract. The scheduler owns a single long-lived `DelayQueue` and, on expiry,
//! hands the resumption command back to the engine through its injected [`TimerSink`] (see below).
//!
//! Keeping the timer loop separate from dispatch means an external `TerminateExecution` (or another
//! timer) flows through the stream while a `Wait`'s timer is pending — the earlier deadlock where an
//! inline `sleep` blocked the whole stream can't recur.
//!
//! The engine injects the write entry (a [`TimerSink`]) into the scheduler at boot, so the expiry
//! write still funnels through the engine — the scheduler never writes to the log itself, and the
//! log keeps a single writer. The log assigns the appended `TriggerTimer` its `entry_id` and stream
//! id at append time (positions no longer come from a shared `IdSource`), so the re-enveloping write
//! needs no counter bookkeeping.
//!
//! ## Contract vs implementation
//!
//! This module holds only the [`Scheduler`] **contract** — mirroring how `Storage` lives in this
//! crate while its implementations live downstream (see `crate::storage`). The engine holds an
//! `Arc<dyn Scheduler>` and never fabricates a concrete runtime; the M1 in-memory implementation
//! lives in the downstream `spica-scheduler` crate (`crate::scheduler::SchedulerHandle` is gone).
//! That keeps the crate graph acyclic (`spica-scheduler → spica-engine`) and lets a distributed /
//! remote timer implementation swap in behind the same trait later.

use std::sync::Arc;

use crate::id::{EntryId, TimerId};
use crate::log::Timestamp;

/// The engine's controlled write entry for a fired timer, injected into the [`Scheduler`] at boot.
///
/// On expiry the scheduler calls [`TimerSink::trigger`] instead of writing to the log itself. This
/// keeps the log's writer funneled through the engine: the sink implementation (owned by the engine)
/// is where the expiry-triggered write is validated before it is appended (see `EngineTimerSink` in
/// `engine.rs`). The engine injects the sink via [`Scheduler::attach_sink`] *after* constructing the
/// scheduler — the scheduler is caller-created before `start()`, so the sink cannot be a constructor
/// argument (the push model's wiring gap, closed at boot).
#[async_trait::async_trait]
pub trait TimerSink: Send + Sync {
    /// A timer's deadline elapsed: append the `TriggerTimer` that resumes the `Wait` state, causally
    /// linked to the `TimerActivated` entry identified by `cause_id`. Implementations (the engine)
    /// validate the write and append it; the log assigns the entry's position and its own stream id.
    async fn trigger(&self, timer: TimerId, cause_id: EntryId);
}

/// Contract for the engine's timer side-effect service.
///
/// Unlike [`Storage`](crate::storage::Storage) (a passive consumer the engine calls into), a
/// scheduler is a **producer** of engine commands — a fired `TriggerTimer` must reach the log. Rather
/// than the run loop *pulling* the fired command back (which made the loop's `select!` depend on
/// `next_fired` and its internal fired-command queue), the scheduler *pushes* into the engine-owned
/// write entry: the engine injects an `Arc<dyn TimerSink>` via [`Scheduler::attach_sink`] at boot,
/// and the scheduler calls [`TimerSink::trigger`] on expiry. The scheduler never touches the log
/// directly — the engine keeps the validation boundary. Object safety: `#[async_trait]` erases the
/// one async method (on `TimerSink`), and every method takes `&self` so the contract works behind
/// `&dyn Scheduler` / `Arc<dyn Scheduler>`.
#[async_trait::async_trait]
pub trait Scheduler: Send + Sync {
    /// Inject the engine-owned [`TimerSink`] this scheduler calls on expiry. Called once by
    /// [`EngineBuilder::start`](crate::EngineBuilder::start) before the run loop boots, so a sink is
    /// always attached before any `TimerActivated` can arm a timer.
    fn attach_sink(&self, sink: Arc<dyn TimerSink>);

    /// Arm `timer` to fire a `TriggerTimer` at the absolute `deadline`, causally linked to the
    /// `TimerActivated` entry identified by `cause_id`. There is no stream to route to — a log is
    /// one stream, so the fired `TriggerTimer` is handed to the attached sink (via
    /// [`TimerSink::trigger`]) and the log stamps its own stream id when it is appended.
    fn schedule(&self, timer: TimerId, deadline: Timestamp, cause_id: EntryId);

    /// Cancel a previously-armed `timer` (a `TimerCancelled` event was applied).
    fn cancel(&self, timer: TimerId);
}
