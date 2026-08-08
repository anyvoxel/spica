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
//! ## M1 scope
//!
//! The no-I/O subset: `Pass`, `Succeed`, `Fail`, `Choice`, and `Wait` (literal `Seconds` only).
//! `Task`, `Map`, `Parallel`, and `Retry`/`Catch` are rejected as unsupported — but the CCES
//! seams (LogStream/Storage/Processor/CommandHandler) are the foundation a distributed
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

pub use applier::{ApplierContext, EventApplier, EventDispatcher};
pub use command::{Command, TerminationReason, TimerPurpose};
pub use engine::Engine;
pub use error::ExecutionError;
pub use event::Event;
pub use handler::{ActivityCtx, Collector, CommandHandler, CtxKind, HandlerContext};
pub use id::{ActivityId, EntryId, ExecutionId, IdSource, NodeId, StreamId, TimerId};
pub use log::{Entry, EntryPayload, InMemoryLogStream, LogStream, Timestamp};
pub use processor::Processor;
pub use result::ExecutionResult;
pub use scheduler::SchedulerHandle;
pub use scope::Scope;
pub use storage::{Activity, Execution, InMemoryStorage, Status, Storage, Timer, TimerStatus};

// Re-export the ASL state types the public API references (so callers need only depend on
// `spica-engine`).
pub use spica_asl::StateMachine;
