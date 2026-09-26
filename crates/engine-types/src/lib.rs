//! The engine's type vocabulary — the values that cross every seam (commands, events, rejections,
//! the projected entity types) — together with the storage contract they are persisted through.
//!
//! Nothing here executes; the engine proper consumes and produces these values. The vocabulary is a
//! crate of its own so the storage contract is a **single type** everywhere it is implemented or
//! consumed. A trait defined inside the engine cannot offer that: `spica-storage` would have to
//! depend on the engine in order to implement it, and the engine's own test build would then be
//! handed a second, unrelated copy of the trait — every `Storage` implementation would stop
//! satisfying the engine's bounds.

pub mod storage;
pub mod types;

// The flat vocabulary below is this crate's public face — the same names `spica-engine` re-exports,
// so a consumer (or a storage implementation) names one path regardless of which crate it is
// talking to. The two modules stay public for callers that prefer the explicit split.
pub use spica_machinery::Timestamp;
pub use storage::{
    ActivityRecord, ExecutionRecord, Storage, StorageTxn, TaskRecord, ThreadRecord, TimerRecord,
};
pub use types::activity::{
    Activity, ActivityState, ActivityStatus, MapActivityState, ParallelActivityState,
    WaitActivityState,
};
pub use types::command::{
    ActivateState, ActivateTask, ClaimTasks, Command, CompleteExecution, CompleteState,
    CompleteTask, CompleteThread, CreateExecution, CreateFlow, FailTask, SpawnThread,
    TerminateExecution, TerminateState, TerminateThread, TerminationReason, TimerPurpose,
};
pub use types::error::{ExecutionError, InfraError, RuntimeError};
pub use types::event::{
    Event, ExecutionCreated, FlowCreated, FlowVersionCreated, StateTransitioned, TaskCompleted,
    TaskFailed, TasksClaimed, VariablesAssigned,
};
pub use types::execution::{Execution, ExecutionStatus};
pub use types::flow::{Flow, FlowStatus};
pub use types::flow_version::FlowVersion;
pub use types::id::{EntryId, FlowName, RequestId, StreamId};
pub use types::meta::{
    ObjectKind, ObjectMeta, ObjectName, ObjectReference, OwnerReference, PlainName, ScopeName,
};
pub use types::reject::{Reject, RejectionType};
pub use types::state_path::StatePath;
pub use types::task::{RetrierAttemptState, RetryPolicy, RetryState, Task, TaskStatus};
pub use types::thread::{Thread, ThreadStatus};
pub use types::timer::{Timer, TimerStatus};
pub use types::variables::Variables;
