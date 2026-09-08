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
//! - A [`StreamProcessor`] reads entries in order: [`Command`]s are dispatched to [`CommandHandler`]s
//!   (e.g. [`StateStreamProcessor`](crate::handlers)s), which produce more entries;
//!   [`Event`](crate::Event)s are applied to [`Storage`] to materialize
//!   [`Execution`]/[`Activity`] state.
//! - [`Storage`] is a projection (fold) of the [`Event`](crate::Event) stream — any worker can
//!   rebuild it by replaying. Recovery re-applies Events; Commands are not re-run.
//!
//! ## Scope and milestone roadmap
//!
//! Implemented: `Pass`, `Succeed`, `Fail`, `Choice`, `Wait` (literal or JSONata `Seconds`/
//! `Timestamp`), a `Task` made claimable over the engine-hosted [`TaskApi`] (a worker executes its
//! `Resource` and reports `complete`/`fail`),
//! `Map`/`Parallel` container fan-out (bounded-concurrency `Map` with per-settle slot replenish;
//! `Parallel` with branch aggregation), the state-machine `TimeoutSeconds`, `$states.context`
//! (`Execution`/`State`/`Map.Item`) binding, and execution cancellation (`Engine::cancel_execution`).
//!
//! Not yet implemented (each carries a `TODO` marker at its site; see in particular
//! `handlers/states/task.rs`, `handlers/complete_task.rs`,
//! `handlers/states/map.rs`, and `handlers/states/mod.rs`):
//!
//! - **M2**: Task `HeartbeatSeconds` (`States.HeartbeatTimeout`) — the current worker/handler
//!   contract (in `spica-client`'s `worker` module) cannot express a client-keepalive-driven
//!   heartbeat deadline; and
//!   re-arming a fresh `TimeoutSeconds` when a retried `Task` attempt re-invokes (the first
//!   attempt's deadline is not reset). Task `Retry`/`Catch` matching, `TimeoutSeconds`, and
//!   `$states.errorOutput` binding *are* implemented.
//! - **M3 `Map`** (fan-out, bounded `MaxConcurrency` replenish, aggregate/fail are implemented;
//!   deferred): `ItemSelector` (per-item input transform), `ToleratedFailureCount` /
//!   `ToleratedFailurePercentage` (current behavior equals the default tolerance of 0), and
//!   threading `$states.context.Map.Item` down into the item-processor child states.
//! - **Cross-cutting**: submission-time `spica_asl::StateMachine::validate()`; a fully-designed
//!   recovery path for the inline child-settled reaction over container (Map/Parallel) states;
//!   `$states.context.StateMachine` stats.
//!
//! The CCES seams (LogStream/Storage/StreamProcessor/CommandHandler) are the foundation a distributed
//! deployment implements.
//!
//! ## Known limitation
//!
//! `jsonata-core` represents all JSON numbers as `f64`, so an integer assigned or output via
//! JSONata (e.g. `5`) round-trips as `5.0`. Comparisons inside JSONata are unaffected.

mod applier;
mod engine;
mod eval_env;
mod follower;
mod handler;
mod handlers;
mod hook;
mod leader;
mod log;
mod processing;
mod query;
mod storage;
mod stream_processor;
mod task_api;
mod types;
mod working;

pub use applier::{ApplierContext, EventApplier, EventDispatcher};
pub use engine::{Engine, EngineBuilder};
pub use handler::{ActivityCtx, Collector, CommandHandler, CtxKind, HandlerContext};
pub use hook::Hook;
pub use log::{Entry, EntryPayload, InMemoryLogStream, LogStream, RocksLogStream, Timestamp};
pub use query::{QueryListPage, QueryObject};
pub use storage::{
    ActivityRecord, ExecutionRecord, Storage, StorageTxn, TaskRecord, ThreadRecord, TimerRecord,
};
pub use stream_processor::StreamProcessor;
pub use task_api::{ActivatedTask, TaskApi};
pub use types::activity::{
    Activity, ActivityState, ActivityStatus, MapActivityState, ParallelActivityState,
};
pub use types::command::{Command, TerminationReason, TimerPurpose};
pub use types::error::{ExecutionError, InfraError, RuntimeError};
pub use types::event::Event;
pub use types::execution::{Execution, ExecutionStatus};
pub use types::flow::{Flow, FlowStatus};
pub use types::flow_version::FlowVersion;
pub use types::id::{EntryId, FlowName, RequestId, StreamId};
pub use types::meta::{
    ObjectKind, ObjectMeta, ObjectName, ObjectReference, OwnerReference, PlainName, ScopeName,
};
pub use types::reject::{Reject, RejectionType};
pub use types::task::{RetrierAttemptState, RetryPolicy, RetryState, Task, TaskStatus};
pub use types::thread::{Thread, ThreadStatus};
pub use types::timer::{Timer, TimerStatus};
pub use types::variables::Variables;

// Re-export the ASL state types the public API references (so callers need only depend on
// `spica-engine`).
pub use spica_asl::StateMachine;
