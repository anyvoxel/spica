//! External-resource task execution for the engine's [`TaskService`](spica_engine::TaskService)
//! contract.
//!
//! This crate holds the **implementation** half of the task split, the mirror image of how
//! `spica-scheduler` implements `spica_engine::Scheduler` (and how `spica-storage` implements
//! `spica_engine::Storage`). The contract ([`spica_engine::TaskService`]) lives upstream in
//! `spica-engine`; this crate depends on `spica-engine` to implement it, and the assembly binary
//! (`spica-server`) selects the concrete implementation and injects it as `Arc<dyn TaskService>` —
//! keeping the crate graph acyclic (`task-service → engine`, never the reverse).
//!
//! The one implementation today is [`InMemoryTaskService`], an in-process dispatcher that routes
//! each invoked `TaskState` to a caller-registered [`TaskHandler`](spica_engine::TaskHandler) keyed
//! by `resource`. It is the task service's analogue of `InMemoryStorage`/`InMemoryScheduler`:
//! sufficient for single-node operation and tests, and a drop-in shape a distributed / remote task
//! executor can later match behind the same trait.

mod in_memory;

pub use in_memory::InMemoryTaskService;
