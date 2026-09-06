//! Timer scheduling, decoupled from the engine's serial dispatch loop.
//!
//! Arming a timer (`TimerActivated`) is recorded in the stream as a durable fact; the actual
//! *physical* timing is a side effect derived downstream from that fact (via the injectable
//! `Hook` observer) and driven by the [`Scheduler`] implementation in this crate. Both sides of the
//! seam live here: the [`Scheduler`]/[`TimerSink`] **contract** ([`contract`]) and its concrete
//! implementation. Keeping the timer loop separate from dispatch means an external
//! `TerminateExecution` (or another timer) flows through the stream while a `Wait`'s timer is
//! pending — the earlier deadlock where an inline `sleep` blocked the whole stream can't recur.
//!
//! This crate depends on `spica-engine` for the value types the contract carries
//! ([`ObjectReference`](spica_engine::ObjectReference), [`Timestamp`](spica_engine::Timestamp)); the
//! graph stays acyclic (`scheduler → engine`, never the reverse), and the assembly binary
//! (`spica-server`) selects a concrete implementation and injects it as `Arc<dyn Scheduler>`.
//!
//! The one implementation today is [`InMemoryScheduler`], an in-process `DelayQueue` loop. It is the
//! scheduler's analogue of `InMemoryStorage`: sufficient for single-node operation and tests, and a
//! drop-in shape a distributed / remote timer implementation can later match behind the same traits.

mod contract;
mod in_memory;

pub use contract::{Scheduler, TimerSink};
pub use in_memory::InMemoryScheduler;
