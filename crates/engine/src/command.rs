use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ExecutionError;
use crate::id::{ActivityId, ExecutionId, NodeId, TaskId, TimerId};
use crate::log::Timestamp;

/// Why an entity (execution or activity) terminated without succeeding.
///
/// Failure is **data carried on `Terminate*`, not a separate command family**: cancel and timeout
/// share the same termination path (ing + cleanup + deferred ed) and differ from failure only in
/// this payload, so there is a single `Terminate*` verb per entity and a single cascade.
///
/// `error_name` mirrors [`ExecutionError::error_name`] / [`ExecutionError::error_output`] so a
/// later milestone's `Retry`/`Catch` can match and intercept before termination propagates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TerminationReason {
    /// A definitive runtime failure (a `Fail` state, a JSONata error prefix, `NoChoiceMatched`).
    Failed { error: ExecutionError },
    /// The state-machine `TimeoutSeconds` timer fired before the execution completed.
    TimedOut,
    /// An external request aborted the execution.
    Cancelled,
}

impl TerminationReason {
    /// The ASL reserved error name, used by `Retry`/`Catch` matching in a later milestone.
    pub fn error_name(&self) -> &str {
        match self {
            TerminationReason::Failed { error } => error.error_name(),
            TerminationReason::TimedOut => "States.Timeout",
            TerminationReason::Cancelled => "States.Cancelled",
        }
    }

    /// The error-output object bound to `$states.errorOutput` by `Catch` in a later milestone.
    pub fn error_output(&self) -> Option<Value> {
        match self {
            TerminationReason::Failed { error } => error.error_output(),
            TerminationReason::TimedOut => Some(serde_json::json!({
                "Error": "States.Timeout",
                "Cause": "The execution's TimeoutSeconds elapsed",
            })),
            TerminationReason::Cancelled => None,
        }
    }

    /// Maps to the terminal [`ExecutionError`]. Only meaningful at the execution level, where
    /// cancel maps to a catchable `States.Cancelled` failure; activity termination reason is
    /// flowing data, not an error surfaced from the engine's entry path.
    pub fn to_execution_error(&self) -> ExecutionError {
        match self {
            TerminationReason::Failed { error } => error.clone(),
            TerminationReason::TimedOut => ExecutionError::TimedOut {
                message: "The execution's TimeoutSeconds elapsed".to_string(),
            },
            TerminationReason::Cancelled => ExecutionError::Cancelled {
                message: "The execution was cancelled".to_string(),
            },
        }
    }
}

/// An operation to perform against an [`Execution`](crate::Execution), [`Activity`](crate::Activity),
/// or [`Timer`](crate::Timer). Commands are appended to the [`LogStream`](crate::LogStream) and
/// consumed by the [`Processor`](crate::Processor), which dispatches each to the matching handler.
///
/// The model is **Command-driven lifecycle** per entity — three verbs: `Activate` (enter),
/// `Complete` (finish successfully), `Terminate` (finish abnormally with a
/// [`TerminationReason`]). A handler emits one or more before/after ([`Event`](crate::Event)) pairs
/// plus **cleanup** Commands for the node's active children, and **defers the after (ed) event**
/// when the node still owns children, because teardown of those children takes real time (and is
/// itself driven by their own Commands). The ed is emitted as soon as the node's own work finishes
/// and, when it had children, cascaded by a shared helper as each child's terminal event drains the
/// owning parent.
///
/// The state machine definition is provided to the Processor out-of-band (by the
/// [`Engine`](crate::Engine)); a distributed deployment would register state machines by id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    /// Begin executing a state machine. Produces `ExecutionCreated` + `ActivateState`(start state) +
    /// (if `TimeoutSeconds` is set) `ActivateTimer`(`ExecutionTimeout`).
    CreateExecution { id: ExecutionId, input: Value },

    /// Fan out one branch of a `Parallel` state as a **child execution** (M3). Produces an
    /// `ExecutionCreated{parent, root_execution, state_path}` (rooting the child in the owning
    /// tree, inheriting the top-level run's id as its flat query anchor, and pointing its
    /// `state_path` at the branch's `states` table within the shared machine) followed by an
    /// `ActivateState` entering the branch's `StartAt` state. The child runs as a self-contained
    /// sub-state-machine; its terminal hop cascades back to `parent` via `ProcessChildCompleted`.
    ///
    /// `parent` is the `Parallel` activity that owns the branch, `root_execution` the top-level run
    /// (carried verbatim through every nesting level), `state_path` the resolved JSON Pointer to
    /// this branch's `states` table (computed by the fan-out from the owning execution's pointer +
    /// the `Parallel` state name + branch index), and `state` the branch's `StartAt` to enter first.
    SpawnBranch {
        /// The owning node — the `Parallel` state's `ActivityId` (wrapped as `NodeId::Activity`)
        /// whose `active_children` must drain before the `Parallel` can finish.
        parent: NodeId,
        /// The id of the top-level run this branch belongs to (the child inherits it as
        /// `root_execution`, carried verbatim through every nesting level).
        root_execution: ExecutionId,
        /// Resolved JSON Pointer to this branch's `states` table within the shared machine.
        state_path: Option<jsonptr::PointerBuf>,
        /// This branch's index within the parent `Parallel`'s `Branches` array. Carried so the
        /// created child execution can be recorded under this index (for ordered output
        /// aggregation when the `Parallel` converges).
        branch_index: usize,
        /// The state name entered first within the branch (the branch's `StartAt`).
        state: String,
        /// The input the branch receives (the `Parallel` state's projected `Arguments`, or the
        /// state's input by default).
        input: Value,
    },

    /// Drive an execution to a successful finish (terminal `Succeed`/`End` reached). Carries the
    /// output (the terminal state's result). Produces `ExecutionCompleting`, cancels the sm-timer,
    /// and `ExecutionCompleted` once drained. The output itself is already fixed at
    /// `ExecutionCompleting` and is stored directly on the execution row.
    CompleteExecution { id: ExecutionId, output: Value },

    /// Drive an execution to an abnormal finish with `reason`. Produces `ExecutionTerminating`,
    /// terminates active children (states / timers), and `ExecutionTerminated{reason}` once drained.
    TerminateExecution {
        id: ExecutionId,
        reason: TerminationReason,
    },

    /// Enter a single state (create its `Activity`). Emits `StateActivating` and runs the state's
    /// `activate` step; the state's handler then emits `StateActivated` once it has processed the
    /// input, followed by its command: synchronous states immediately self-emit
    /// `CompleteState`/`TerminateState`; asynchronous `Wait` emits `ActivateTimer` to arm its
    /// resume timer.
    ActivateState {
        execution: ExecutionId,
        activity: ActivityId,
        state: String,
        input: Value,
    },

    /// Successfully finish the state bound to `activity` (the activity is `Running`). Runs the
    /// state's `complete` step: emits `StateCompleting`/`StateCompleted` (recording output via
    /// Assign/Output eval), then the transition (`ActivateState` next / `CompleteExecution` if
    /// terminal).
    CompleteState { activity: ActivityId },

    /// Abnormally finish the state bound to `activity` with `reason`. Emits `StateTerminating`,
    /// terminates active children, then `StateTerminated{reason}` (deferred/cascaded), which drains
    /// the parent execution.
    TerminateState {
        activity: ActivityId,
        reason: TerminationReason,
    },

    /// Arm a timer under owner `parent` (`NodeId::Execution` for the state-machine
    /// `ExecutionTimeout`, `NodeId::Activity` for a `Wait`'s `WaitResume`). The side-effect handler
    /// sleeps until `deadline` and emits `TimerActivated` + `CompleteTimer`.
    ActivateTimer {
        /// Owner of the timer — the node whose deadline it enforces (execution or activity).
        parent: NodeId,
        timer: TimerId,
        purpose: TimerPurpose,
        /// Absolute wall-clock time at which the timer fires. A relative ASL `Seconds`/`TimeoutSeconds`
        /// is normalized to an absolute deadline at activation; a `Wait.Timestamp` is used as-is.
        /// Persisting the absolute moment (rather than a relative duration) makes `TimerActivated`
        /// a self-contained, replayable fact: how long is left is derivable from `deadline - now`.
        deadline: Timestamp,
    },

    /// Signal that an armed timer has fired (its deadline passed). Dispatched by a `WaitResume`
    /// fires the owning state's resume; by an `ExecutionTimeout` triggers `TerminateExecution` with
    /// `TimedOut`. Idempotent if the owner already moved past.
    CompleteTimer { timer: TimerId },

    /// Cancel a pending timer (e.g. the execution's `TimeoutSeconds` once it finishes). Idempotent —
    /// a no-op if the timer already completed/cancelled.
    CancelTimer { timer: TimerId },

    /// A child reached a terminal state; notify `parent` so *it* can decide what to do next.
    ///
    /// The child never knows whether its settle drains the parent (was it the last child?) nor
    /// whether the parent should replenish a work slot (Map/Parallel concurrency) — those are the
    /// parent's own decisions. This command hands that decision to [`ProcessChildCompletedHandler`]
    /// (crate::handlers::ProcessChildCompletedHandler), which runs on the *next* dispatch round after the
    /// child's terminal event was applied, so it sees `parent`'s real post-drain `active_children`.
    ///
    /// `parent` is carried in the command (rather than re-derived from storage) because by the time
    /// this command is dispatched, the child may have been removed from `parent`'s snapshot or,
    /// in the re-entry path, swept from the tree entirely — the child's own terminal path reads the
    /// parent link before that. `child` is retained for logging / debugging and for the M2/M3
    /// Map/Parallel replenish logic.
    ///
    /// TODO(recovery + design): this command's handling is unfinished for the container states and
    /// for cause-based recovery. The following design work is deferred:
    ///
    /// - **Cause-based recovery must stay decidable.** On restart, a command is re-dispatched iff it
    ///   has no causal follow-up event. Currently this command's silent branches (parent not draining,
    ///   parent already terminal, Timer/Task targets) produce **zero events** — so those outcomes are
    ///   indistinguishable from "never processed" and would be re-run. Worse, any branch that emits a
    ///   parent terminal event would be re-emitted on a spurious re-run. Once durable recovery lands,
    ///   every outcome must have a deterministic, logged, idempotent causal product — either the high-
    ///   value parent terminal event, or an idempotent "handled/rejected" confirmation event (mirroring
    ///   Zeebe's `COMMAND_REJECTED`, which is always written, never silent).
    ///
    /// - **`StateHandler` needs a `child_completed` hook** (default `unreachable!` only *while* no
    ///   container state exists): dispatched from here when `parent` is a `Running` Activity, it lets a
    ///   Map/Parallel container decide replenish-vs-drain. But `unreachable!` is **unsafe under
    ///   recovery replay**, where a panic would keep the restarted service from booting — so the
    ///   unreachable default is only valid while there are no container states (M1/M2), and must be
    ///   replaced (by the idempotent confirmation path above) before Map/Parallel + recovery coexist.
    ProcessChildCompleted { parent: NodeId, child: NodeId },

    // ── Task (external-resource call) ───────────────────────────────────────────
    /// Invoke an external `Task` — the Task-state analogue of [`Command::ActivateTimer`]. The
    /// side-effect handler emits only `Event::TaskActivated`; the physical call is performed by the
    /// [`TaskService`](crate::task_service::TaskService), which dispatches on `resource` and routes
    /// the outcome back as [`Command::CompleteTask`] (mirroring the scheduler's `CompleteTimer`).
    /// `parent` is the invoking activity's `NodeId::Activity`; `resource` is the URI the service
    /// dispatches on; `arguments` are the projected call payload.
    ActivateTask {
        parent: NodeId,
        task: TaskId,
        resource: String,
        arguments: Value,
    },

    /// A foreign `Task` settled: its registered handler returned `Ok(output)` (drive the owning
    /// state's `complete` via `CompleteState`) or `Err(error)` (drive it to a
    /// `TerminateState{Failed{error}}`). Routed back to the run loop's single writer by the
    /// [`TaskService`](crate::task_service::TaskService) when the external call finishes — the
    /// stream stays strictly ordered while the task runs in the background.
    CompleteTask {
        task: TaskId,
        result: Result<Value, ExecutionError>,
    },

    /// Cancel a pending `Task` (e.g. the owning activity/execution is terminated while the call is
    /// in flight). Idempotent — a no-op if the task already settled.
    CancelTask { task: TaskId },
}

/// The arm of a [`Command::ActivateTimer`]: why the timer exists. Drives `CompleteTimer`'s split
/// (resume vs. timeout) and is a placeholder for later per-state `TimeoutSeconds` (M2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimerPurpose {
    /// The state-machine `TimeoutSeconds` deadline; its firing terminates the execution `TimedOut`.
    ExecutionTimeout,
    /// A `Wait` state's `Seconds` delay; its firing completes the owning state.
    WaitResume,
    /// A Task state's `Retry` backoff delay; its firing re-invokes the retried task.
    TaskRetryDelay,
    /// A Task state's `TimeoutSeconds` deadline; its firing fails the in-flight task with
    /// `States.Timeout` (routed back into the owning state's `Retry`/`Catch` policy).
    TaskTimeout,
    // M2: StateTimeout
}
