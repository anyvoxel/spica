//! Timer scheduling for the engine's [`Scheduler`](spica_engine::Scheduler) contract.
//!
//! This crate holds the **implementation** half of the timer split, the mirror image of how
//! `spica-storage` implements `spica_engine::Storage`. The contract
//! ([`spica_engine::Scheduler`]) lives upstream in `spica-engine`; this crate depends on
//! `spica-engine` to implement it, and the assembly binary (`spica-server`) selects the concrete
//! implementation and injects it as `Arc<dyn Scheduler>` — keeping the crate graph acyclic
//! (`scheduler → engine`, never the reverse).
//!
//! The one implementation today is [`InMemoryScheduler`], an in-process `DelayQueue` loop. It is the
//! scheduler's analogue of `InMemoryStorage`: sufficient for single-node operation and tests, and a
//! drop-in shape a distributed / remote timer implementation can later match behind the same trait.

mod in_memory;

pub use in_memory::InMemoryScheduler;
