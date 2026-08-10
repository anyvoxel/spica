use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::command::TerminationReason;
use crate::error::ExecutionError;
use crate::id::{ActivityId, ExecutionId, NodeId, TaskId, TimerId};
use crate::log::Timestamp;

/// The result of executing a [`Command`](crate::Command). Events are appended to the
/// [`LogStream`](crate::LogStream) alongside Commands; the [`Processor`](crate::Processor) applies
/// each to [`Storage`](crate::Storage) to materialize the execution tree.
///
/// Lifecycle verbs split into **before/after (`ing`/`ed`)** pairs so each phase records both what it
/// consumed (input / scheduling info, kept on the `ing` event) and what it produced (output /
/// outcome, on the `ed` event). For synchronous states both events land in one batch; for a node
/// that owns children the `ed` is **deferred** until the children have drained (each child
/// terminates, the projection removes it from the parent's `active_children`, and the shared
/// cascade in the lifecycle handlers emits the ed). `ExecutionCreated` is the single creation
/// record (its ed form would carry nothing).
///
/// Storage is a projection (fold) of the event stream and can be rebuilt by replaying it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Event {
    /// Result of `Command::CreateExecution` (a top-level run) or `Command::SpawnBranch` (a child
    /// Parallel branch) — the execution's single creation record. For a child execution `parent`
    /// links it into the owning tree and `state_path` locates its branch `states` table within
    /// the shared machine document (see [`crate::storage::Execution::state_path`]);
    /// `root_execution` is always the top-level run's id (the flat query anchor), carried verbatim
    /// through every nesting level.
    ExecutionCreated {
        id: ExecutionId,
        /// The id of the top-level run this execution belongs to (self for the top-level run).
        root_execution: ExecutionId,
        /// `Some(owner)` for a child execution (M3 Parallel branch); `None` for the top-level run.
        parent: Option<NodeId>,
        /// JSON Pointer to this run's branch `states` table within the shared machine; `None` for
        /// the top-level run.
        state_path: Option<jsonptr::PointerBuf>,
        input: Value,
    },

    /// Result of `Command::SpawnBranch` — a `Parallel` activity fanned out one branch as a child
    /// execution. Records `execution` under `index` (the branch's position in the `Parallel`'s
    /// `Branches` array) so the `Parallel` can aggregate branch outputs in order at convergence.
    ParallelBranchSpawned {
        /// The `Parallel` state's activity that owns the fanned-out branches.
        activity: ActivityId,
        /// The branch index within the `Parallel`'s `Branches` array.
        index: usize,
        /// The child execution created for this branch.
        execution: ExecutionId,
    },

    /// Success path began on the execution (records the computed output).
    ExecutionCompleting { id: ExecutionId, output: Value },
    /// Execution succeeded after any owned children drained (records `output`, the result).
    ExecutionCompleted { id: ExecutionId, output: Value },

    /// Termination began on the execution (records the reason).
    ExecutionTerminating {
        id: ExecutionId,
        reason: TerminationReason,
    },
    /// Execution terminated after any owned children drained (records the terminal reason).
    ExecutionTerminated {
        id: ExecutionId,
        reason: TerminationReason,
    },

    /// Result of `Command::ActivateState` — the state was entered with `input`.
    ///
    /// `state_path` is a JSON Pointer (RFC 6901) locating this state's definition within the shared
    /// [`StateMachine`](spica_asl::StateMachine) document, e.g. `/states/P2` for a top-level state or
    /// `/states/P1/branches/0/states/P2` for a state inside a Parallel branch. It is the **complete,
    /// self-contained pointer to the state** (the owning execution's `state_path` names the
    /// enclosing `states` table; `state_path` extends it by the state's name). The leaf state name is
    /// **derived** as the pointer's last token, so it is not redundantly carried here. The event is
    /// carried so a follower / recovered leader can record exactly where the activity lives without
    /// re-deriving it from the machine + parent chain.
    StateActivating {
        execution: ExecutionId,
        activity: ActivityId,
        /// JSON Pointer to this state's definition in the machine document.
        state_path: jsonptr::PointerBuf,
        input: Value,
    },
    /// The state finished activating — emitted by the `StateHandler::activate` **only after** it has
    /// processed the state's input. It is the ed of `StateActivating` and precedes the state's own
    /// follow-up: a `CompleteState`/`TerminateState` sequence or an armed side-effect (e.g. a Wait
    /// resume timer). Full per-entry chain: `StateActivating → StateActivated → …`.
    ///
    /// Carries the state's **activation product** — state that the activate step computed and that a
    /// follower / recovered leader must be able to rebuild from the event stream alone (it is not
    /// re-derivable from the static machine definition). For a `Map` state this is the iteration
    /// plan it projected from `Items`/`MaxConcurrency` (a JSONata `Items` expression is evaluated at
    /// activate time against a scope that later replenish rounds can't re-derive, so it must travel
    /// here). For every non-container state the plan is `None` — its activation consumed no state that
    /// isn't already on the `Activity` row.
    ///
    /// `input` is the state's **processed input** — the result of its activation-time input
    /// preprocessing (e.g. projecting a `Task`'s/`Parallel`'s `Arguments`) applied to the raw input
    /// carried on `StateActivating`. A state that consumes its raw input verbatim carries a copy of it
    /// (never `None`). Folded onto the activity's `input` so the processed view is inspectable without
    /// re-running the projection.
    StateActivated {
        activity: ActivityId,
        /// The state's processed input (raw input after activation-time preprocessing).
        input: Value,
        /// The activation product: the `Map` iteration plan, or `None` for non-container states.
        plan: Option<crate::storage::MapActivityState>,
    },

    /// The state began its success finish (the complete step started; children, if any, may still
    /// be draining).
    StateCompleting { activity: ActivityId },
    /// The state finished successfully, producing `output` (the next state's input, or the
    /// execution output if terminal).
    StateCompleted { activity: ActivityId, output: Value },

    /// The state began terminating with `reason` (children may still be draining).
    StateTerminating {
        activity: ActivityId,
        reason: TerminationReason,
    },
    /// The state terminated after its owner/children drained; reason flows from the cascade.
    StateTerminated {
        activity: ActivityId,
        reason: TerminationReason,
    },

    /// A timer was armed for `purpose` (records the absolute `deadline` and the owning `parent`).
    TimerActivated {
        parent: NodeId,
        timer: TimerId,
        purpose: crate::command::TimerPurpose,
        deadline: Timestamp,
    },
    /// A timer's deadline passed.
    TimerCompleted { timer: TimerId },
    /// A timer was cancelled before firing.
    TimerCancelled { timer: TimerId },

    /// Variables assigned by an Activity's `Assign`. Applied as a delta to the Execution's scope.
    VariablesAssigned {
        execution: ExecutionId,
        assignments: serde_json::Map<String, Value>,
    },

    /// The state finished successfully and routed to its successor — `next` is the resolved
    /// transition target; `output` is the projection result that becomes the successor's input. It
    /// is the pure "routing resolved" marker: the actual hop (the successor's `ActivateState`) is
    /// carried by the following `Command`. Emitted only for a real State→State hop — a terminal
    /// `End` routes to `CompleteExecution` instead and carries no marker. Emitted between
    /// `StateCompleted` and the transition command so the transition decision is recorded in the
    /// stream independent of the throwing code (`Command::ActivateState` allocates the successor's
    /// id internally, so routing names the target but not the new activity). The foldable data
    /// (`output`, `next`) lives on the following command's bookkeeping; the applier is a no-op,
    /// mirroring `StateActivated`.
    StateTransitioned {
        activity: ActivityId,
        next: String,
        output: Value,
    },

    // ── Task (external-resource call, M2 lifecycle) ──────────────────────────────
    /// A `Task` state invoked its `Resource` — the logical equivalent of arming a timer: the
    /// physical invocation is delegated to the [`TaskService`](crate::task_service::TaskService),
    /// which drives the external call and routes the outcome back as `Command::CompleteTask`. The
    /// single creation record (a task never owns children, so it has no deferred ing/ed — mirroring
    /// `TimerActivated`). `parent` is the invoking `NodeId::Activity`; `resource` is the URI the
    /// service dispatches on; `arguments` are the projected call payload.
    TaskActivated {
        parent: NodeId,
        task: TaskId,
        resource: String,
        arguments: Value,
    },
    /// The task settled successfully: `output` becomes the state's result (the input to the
    /// successor's `Assign`/`Output` projection via `$states.result`).
    TaskCompleted { task: TaskId, output: Value },
    /// The task's registered handler failed: `error` drives the owning state to a
    /// `TerminateState{reason: Failed{error}}` (unless a `Retry`/`Catch` intercepts it in the
    /// complete-task decision). Kept as a distinct variant from `TaskCompleted`
    /// (rather than folding the `Result` into one event) so the projection can mark the task
    /// `Failed` and the state's termination path is explicit — mirroring how `State-Terminated`
    /// carries a `TerminationReason`.
    TaskFailed { task: TaskId, error: ExecutionError },
    /// A `Retry` was scheduled for `activity`: the matching retrier at `retrier_index` consumed its
    /// next attempt (`retrier_attempt`), the activity's overall retry count became `retry_count`, and
    /// `scheduled_at` records when this retry decision was made. The concrete **when** of the retry
    /// will fire still lives on the paired `TimerActivated { purpose: TaskRetryDelay, deadline }`;
    /// this event carries the retry bookkeeping facts needed to replay the decision without
    /// re-matching the retrier.
    RetryScheduled {
        activity: ActivityId,
        retrier_index: usize,
        retrier_attempt: u32,
        retry_count: u32,
        scheduled_at: Timestamp,
    },
    /// The task was cancelled before settling (e.g. the owning activity/execution was terminated
    /// while the call was in flight) — the sweep counterpart of `TaskCompleted`/`TaskFailed`.
    TaskCancelled { task: TaskId },
}
