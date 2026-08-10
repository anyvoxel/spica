//! The Spica ASL execution engine — Causal Command Event Sourcing (CCES).
//!
//! The engine drives an [`spica_asl::StateMachine`] from `StartAt` through `Next`/`End` to a
//! terminal state, evaluating every `{% expr %}` JSONata expression via `jsonata-core`.
//!
//! ## Architecture
//!
//! - [`Command`]s and [`Event`](crate::Event)s are appended to a durable, ordered
//!   [`LogStream`]; each entry is causally linked (`cause_id`) to the Command that produced it,
//!   and the Events + subsequent Commands produced by one Command are appended atomically.
//! - A [`Processor`] reads entries in order: [`Command`]s are dispatched to [`CommandHandler`]s
//!   (e.g. [`StateProcessor`](crate::handlers)s), which produce more entries;
//!   [`Event`](crate::Event)s are applied to [`Storage`] to materialize
//!   [`Execution`]/[`Activity`] state.
//! - [`Storage`] is a projection (fold) of the [`Event`](crate::Event) stream — any worker can
//!   rebuild it by replaying. Recovery re-applies Events; Commands are not re-run.
//!
//! ## Scope and milestone roadmap
//!
//! Implemented: `Pass`, `Succeed`, `Fail`, `Choice`, `Wait` (literal or JSONata `Seconds`/
//! `Timestamp`), a `Task` dispatched to user-registered [`TaskHandler`]s keyed by `Resource`,
//! `Map`/`Parallel` container fan-out (bounded-concurrency `Map` with per-settle slot replenish;
//! `Parallel` with branch aggregation), the state-machine `TimeoutSeconds`, `$states.context`
//! (`Execution`/`State`/`Map.Item`) binding, and execution cancellation (`Engine::terminate`).
//!
//! Not yet implemented (each carries a `TODO` marker at its site; see in particular
//! `handlers/states/task.rs`, `handlers/complete_task.rs`, `task_service.rs`,
//! `handlers/states/map.rs`, and `handlers/states/mod.rs`):
//!
//! - **M2**: Task `HeartbeatSeconds` (`States.HeartbeatTimeout`) — the current
//!   [`TaskHandler`] interface cannot express a client-keepalive-driven heartbeat deadline; and
//!   re-arming a fresh `TimeoutSeconds` when a retried `Task` attempt re-invokes (the first
//!   attempt's deadline is not reset). Task `Retry`/`Catch` matching, `TimeoutSeconds`, and
//!   `$states.errorOutput` binding *are* implemented.
//! - **M3 `Map`** (fan-out, bounded `MaxConcurrency` replenish, aggregate/fail are implemented;
//!   deferred): `ItemSelector` (per-item input transform), `ToleratedFailureCount` /
//!   `ToleratedFailurePercentage` (current behavior equals the default tolerance of 0), and
//!   threading `$states.context.Map.Item` down into the item-processor child states.
//! - **Cross-cutting**: submission-time `spica_asl::StateMachine::validate()`; a fully-designed
//!   recovery path for `Command::ProcessChildCompleted` over container (Map/Parallel) states;
//!   `$states.context.StateMachine` stats.
//!
//! The CCES seams (LogStream/Storage/Processor/CommandHandler) are the foundation a distributed
//! deployment implements.
//!
//! ## Known limitation
//!
//! `jsonata-core` represents all JSON numbers as `f64`, so an integer assigned or output via
//! JSONata (e.g. `5`) round-trips as `5.0`. Comparisons inside JSONata are unaffected.

mod applier;
mod command;
mod context;
mod engine;
mod error;
mod eval_env;
mod event;
mod handler;
mod handlers;
mod id;
mod log;
mod processor;
mod result;
mod scheduler;
mod scope;
mod storage;
mod task_service;

pub use applier::{ApplierContext, EventApplier, EventDispatcher};
pub use command::{Command, TerminationReason, TimerPurpose};
pub use engine::Engine;
pub use error::ExecutionError;
pub use event::Event;
pub use handler::{ActivityCtx, Collector, CommandHandler, CtxKind, HandlerContext};
pub use id::{ActivityId, EntryId, ExecutionId, IdSource, NodeId, StreamId, TaskId, TimerId};
pub use log::{Entry, EntryPayload, InMemoryLogStream, LogStream, Timestamp};
pub use processor::Processor;
pub use result::ExecutionResult;
pub use scheduler::SchedulerHandle;
pub use scope::Scope;
pub use storage::{
    Activity, ActivityState, ActivityStatus, Execution, ExecutionStatus, InMemoryStorage,
    MapActivityState, ParallelActivityState, RetrierAttemptState, RetryState, Storage, Task,
    TaskStatus, Timer, TimerStatus,
};
pub use task_service::{TaskHandler, TaskServiceHandle};

// Re-export the ASL state types the public API references (so callers need only depend on
// `spica-engine`).
pub use spica_asl::StateMachine;
