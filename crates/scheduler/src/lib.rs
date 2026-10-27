//! Deadline scheduling, decoupled from the engine's serial dispatch loop.
//!
//! A deadline is recorded on the element that acts on it (a run's `TimeoutSeconds` on its row, a
//! `Task` state's on the task, a `Wait`'s resume instant on the activity), so the *physical* timing
//! is a side effect derived downstream from those durable facts (via the injectable `Hook` observer)
//! and driven by the [`Scheduler`] implementation in this crate. Both sides of the seam live here:
//! the [`Scheduler`]/[`DeadlineSink`] **contract** ([`contract`]) and its concrete implementation.
//! Keeping the deadline loop separate from dispatch means an external `TerminateExecution` (or
//! another deadline) flows through the stream while a `Wait` is pending — the earlier deadlock where
//! an inline `sleep` blocked the whole stream can't recur.
//!
//! This crate depends on `spica-engine` for the value types the contract carries
//! ([`DeadlineTarget`](spica_engine::DeadlineTarget), [`Timestamp`](spica_engine::Timestamp)) and
//! on `spica-machinery` for the [`Clock`](spica_machinery::Clock) its expiry decisions read; the
//! graph stays acyclic (`scheduler → engine`/`machinery`, never the reverse), and the assembly binary
//! (`spica-server`) selects a concrete implementation and injects it as `Arc<dyn Scheduler>`.
//!
//! The one implementation today is [`InMemoryScheduler`], an in-process loop that fires each
//! deadline once its injected clock reaches it. It is the scheduler's analogue of
//! `InMemoryStorage`: sufficient for single-node operation and tests, and a drop-in shape a
//! distributed / remote timer implementation can later match behind the same traits.

mod contract;
mod in_memory;

pub use contract::{DeadlineSink, Scheduler};
pub use in_memory::InMemoryScheduler;
