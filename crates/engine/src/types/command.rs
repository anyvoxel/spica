use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::id::{FlowName, RequestId};
use crate::types::meta::{ObjectName, ObjectReference};
use crate::types::task::RetryPolicy;

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
            TerminationReason::TimedOut => ExecutionError::Runtime(RuntimeError::TimedOut {
                message: "The execution's TimeoutSeconds elapsed".to_string(),
            }),
            TerminationReason::Cancelled => ExecutionError::Runtime(RuntimeError::Cancelled {
                message: "The execution was cancelled".to_string(),
            }),
        }
    }
}

/// An operation to perform against an [`Execution`](crate::Execution), [`Activity`](crate::Activity),
/// or [`Timer`](crate::Timer). Commands are appended to the [`LogStream`](crate::LogStream) and
/// consumed by the [`StreamProcessor`](crate::StreamProcessor), which dispatches each to the matching handler.
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
/// The state machine definition is **created in Storage** (by id) rather than threaded through
/// commands: [`Command::CreateFlow`] is the single transport for a definition, and every execution
/// command after that references a version's `ObjectReference`. This keeps execution commands small and lets
/// a recovered/restarted Engine resolve machines by id from storage without re-supplying them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    /// Create a new version of a flow definition. The **only** command that carries a full
    /// definition (mirroring Zeebe's Deployment→Process record), and it carries it as the **raw
    /// ASL string** — the JSON-encoded form of a [`StateMachine`](spica_asl::StateMachine) — not a
    /// pre-parsed struct: the parsed model is a derived, transient value (validated at the create
    /// boundary, parsed on demand at execution). The handler (running in the StreamProcessor) is what
    /// assigns identity: it get-or-creates the [`Flow`](crate::Flow) by `name` (the name *is* the
    /// flow's identity — no generation id), computes the next `version` ordinal (1 for a new name,
    /// `max+1` for an existing one), mints the version's `ObjectReference` the new version is bound to, and
    /// emits [`Event::FlowCreated`](crate::Event), which folds the definition into Storage. Every
    /// other command carries only ids — an execution references a version's `ObjectReference`, never the
    /// machine — keeping execution commands small and fully serializable.
    CreateFlow {
        /// Correlation key for the awaiting caller (Zeebe's `requestId`): an opaque, never-reused
        /// id minted by the authoring Engine operation, carried onto the `FlowCreated` event so the
        /// acknowledgement is routed back to exactly this request — never keyed by the entity.
        request_id: RequestId,
        name: FlowName,
        /// The state machine definition as its raw ASL (JSON) string.
        definition: String,
    },

    // ── Execution (lifecycle: spawn → complete → terminate) ─────────────────
    /// Begin executing a state machine. `flow_version` is the [`ObjectReference`] of the immutable
    /// flow version the execution binds to (its definition); the machine is resolved from Storage at
    /// dispatch time, never carried in the command. Produces `ExecutionCreated` +
    /// `ActivateState`(start state) + (if `TimeoutSeconds` is set) a `TimerActivated`
    /// (`ExecutionTimeout`, emitted inline).
    CreateExecution {
        /// Correlation key for the awaiting caller (Zeebe's `requestId`). The acknowledgement for a
        /// `CreateExecution` is the execution's birth `ExecutionCreated`, which echoes this id; the
        /// `AckHook` observer wakes the caller with it once the event is applied (see the engine's injected Hook consumer).
        request_id: RequestId,
        /// The execution's **user-supplied addressing name** (validated at the public boundary via
        /// `ObjectName::plain`). The execution's durable `uid` is not carried here: the handler mints
        /// it at dispatch, which stays replay-deterministic because `CreateExecution`'s produced
        /// `ExecutionCreated` lands in the same atomic batch (a committed command has a causal event
        /// and is never re-dispatched; an uncommitted one is re-dispatched fresh).
        name: ObjectName,
        flow_version: ObjectReference,
        input: Value,
    },

    /// Fan out one branch of a `Parallel` state as a **child execution** (M3). Produces an
    /// `ExecutionCreated{parent, execution, state_path}` (rooting the child in the owning
    /// tree, inheriting the top-level run's id as its flat query anchor, and pointing its
    /// `state_path` at the branch's `states` table within the shared machine) followed by an
    /// `ActivateState` entering the branch's `StartAt` state. The child runs as a self-contained
    /// sub-state-machine; its terminal hop runs the inline child-settled reaction back to `owner`.
    ///
    /// `owner` is the `Parallel` activity that owns the branch, `execution` the top-level run
    /// (carried verbatim through every nesting level), `state_path` the resolved JSON Pointer to
    /// this branch's `states` table (computed by the fan-out from the owning execution's pointer +
    /// the `Parallel` state name + branch index), and `state` the branch's `StartAt` to enter first.
    ///
    /// TODO(command-design): `SpawnThread` is reused by both `Parallel` branches and `Map` items
    /// (the rename from `SpawnBranch` resolved the naming concern — it now matches the `Thread`
    /// entity it creates), but its payload is still too thin: `index/start_at/input/state_path`
    /// is just enough to enter the child, not enough to preserve the higher-level source semantics
    /// (container kind, whether the index is a branch or item index, and any future fan-out
    /// metadata). Redesign this command as a more general child-thread spawn command before adding
    /// richer container types.
    SpawnThread {
        /// The owning node — the `Parallel`/`Map` activity's reference (kind `Activity`) whose
        /// `active_children` must drain before the container can finish.
        owner: ObjectReference,
        /// The reference of the top-level run this branch belongs to (the child inherits it as
        /// its `execution` anchor, carried verbatim through every nesting level).
        execution: ObjectReference,
        /// Resolved JSON Pointer to this branch's `states` table within the shared machine.
        state_path: Option<jsonptr::PointerBuf>,
        /// This child's ordinal within its container's fan-out source — the `Branches` array index
        /// for a `Parallel`, or the `Items` array index for a `Map`. Carried so the created child
        /// (`Thread.index`) is recorded under this index for ordered output aggregation when the
        /// container converges. `SpawnThread` is shared by both, hence the container-neutral name.
        index: usize,
        /// The **entry-point state name** the child enters first — the branch's `StartAt` for a
        /// `Parallel`, the item-processor's `StartAt` for a `Map`. Distinct from `state_path` (which
        /// names only the sub-machine's `states` table): it is the one carrier of *where within that
        /// table* the child starts, needed to build the sibling `ActivateState`'s path and
        /// unrecoverable from `state_path` alone without re-resolving the definition.
        start_at: String,
        /// The input the branch receives (the `Parallel` state's projected `Arguments`, or the
        /// state's input by default).
        input: Value,
    },

    /// Drive an execution to a successful finish (terminal `Succeed`/`End` reached). Carries the
    /// output (the terminal state's result). Produces `ExecutionCompleting`, cancels the sm-timer,
    /// and `ExecutionCompleted` once drained. The output itself is already fixed at
    /// `ExecutionCompleting` and is stored directly on the execution row.
    CompleteExecution {
        execution: ObjectReference,
        output: Value,
    },

    /// Drive a fan-out [`Thread`](crate::Thread) to a successful finish (its sub-run's terminal
    /// `Succeed`/`End` reached). Distinct from [`CompleteExecution`](Command::CompleteExecution):
    /// that verb is reserved for the **top-level** run, while a `Thread`'s terminal hop — the end of
    /// a `Parallel` branch or a `Map` item — completes only the branch, converged by the owning
    /// container Activity. Address by the thread's `ObjectReference` (its generated single-use uid),
    /// never a user name.
    ///
    /// Produces `ThreadCompleting`, cancels the branch's sm-timers, and `ThreadCompleted` once
    /// drained — mirroring `CompleteExecution`'s cascade but resolved against thread storage and
    /// running the inline child-settled reaction back to the owning container.
    CompleteThread {
        thread: ObjectReference,
        output: Value,
    },

    /// Drive an execution to an abnormal finish with `reason`. Addressed by `name` (the execution's
    /// per-scope-unique primary key, matching `CreateExecution`); the optional `uid` is an
    /// **incarnation guard** — when set, only the named execution with exactly this `uid` is
    /// terminated (a stale/wrong incarnation is refused with a `StateConflict` `Reject`), while
    /// `None` addresses by name alone. Produces `ExecutionTerminating`, terminates active children
    /// (states / timers), and `ExecutionTerminated{reason}` once drained.
    TerminateExecution {
        name: ObjectName,
        uid: Option<ulid::Ulid>,
        reason: TerminationReason,
    },

    /// Drive a fan-out [`Thread`](crate::Thread) to an abnormal finish with `reason`. Distinct from
    /// [`TerminateExecution`](Command::TerminateExecution): that verb is **name-addressed** (the
    /// external root-termination path, keyed in execution storage), while a `Thread` is only ever
    /// terminated **internally** by its owning container Activity — a `Parallel`/`Map` sweep issues
    /// this when a branch/item must be torn down (e.g. an ancestor cancellation draining the tree).
    /// Address by the thread's `ObjectReference` (its generated single-use uid), not a user name.
    ///
    /// Produces `ThreadTerminating`, sweeps owned children, and `ThreadTerminated{reason}` once
    /// drained — mirroring `TerminateExecution`'s cascade, but resolved against thread storage and
    /// running the inline child-settled reaction back to the owning container.
    TerminateThread {
        thread: ObjectReference,
        reason: TerminationReason,
    },

    // ── Activity / State (single-state lifecycle) ────────────────────────────
    /// Enter a single state (create its `Activity`). Emits `StateActivating` and runs the state's
    /// `activate` step; the state's handler then emits `StateActivated` once it has processed the
    /// input, followed by its command: synchronous states immediately self-emit
    /// `CompleteState`/`TerminateState`; asynchronous `Wait` arms its resume timer inline (a
    /// `TimerActivated`).
    ///
    /// `execution` and `owner` are distinct: `execution` is the **top-level anchor** of the tree
    /// this state lives in (carried verbatim through every nesting level, so every Activity can
    /// address its owning run directly); `owner` is the **immediate scope** the new Activity enters
    /// into — the top-level `Execution` for a top-level state, or the fan-out `Thread` for a
    /// `Parallel` branch / `Map` item. They coincide for a top-level execution and diverge inside a
    /// thread; `owner` becomes the new Activity's `meta.owner`, while `execution` becomes its
    /// `Activity::execution`.
    ///
    /// `state_path` is the exact JSON Pointer to the state being entered (from the machine root:
    /// `/states/<name>` for a top-level state, `/states/.../branches/<idx>/<name>` for a branch /
    /// item). Carrying the full path makes `ActivateState` self-locating — the definition lookup no
    /// longer infers the enclosing `states` table from the owning scope's stored `state_path` — and
    /// keeps the log self-describing for replay/audit. The state's leaf name is
    /// [`state_name_from_path`](crate::handlers::state_name_from_path).
    ActivateState {
        execution: ObjectReference,
        owner: ObjectReference,
        state_path: jsonptr::PointerBuf,
        input: Value,
    },

    /// Successfully finish the state bound to `activity` (the activity is `Running`). Runs the
    /// state's `complete` step: emits `StateCompleting`/`StateCompleted` (recording output via
    /// Assign/Output eval), then the transition (`ActivateState` next / `CompleteExecution` if
    /// terminal).
    ///
    /// `output` is the state's **raw result** — the value recorded as `Activity::raw_output` /
    /// `$states.result` before any complete-step `Output` projection. For a `Task` it is the
    /// `CompleteTask` worker payload; for synchronous states and `Wait` it defaults to the
    /// processed input. Carrying it makes the command self-describing (the log alone says what the
    /// completing state produced) and lets the complete step record it without re-reading storage.
    CompleteState {
        activity: ObjectReference,
        output: Value,
    },

    /// Abnormally finish the state bound to `activity` with `reason`. Emits `StateTerminating`,
    /// terminates active children, then `StateTerminated{reason}` (deferred/cascaded), which drains
    /// the parent execution.
    TerminateState {
        activity: ObjectReference,
        reason: TerminationReason,
    },

    // ── Timer (fire → cancel) ────────────────────────────────────────────────
    /// Signal that an armed timer has fired (its deadline passed). Dispatched by a `WaitResume`
    /// fires the owning state's resume; by an `ExecutionTimeout` triggers `TerminateExecution` with
    /// `TimedOut`. Idempotent if the owner already moved past.
    TriggerTimer { timer: ObjectReference },

    /// Cancel a pending timer (e.g. the execution's `TimeoutSeconds` once it finishes). Idempotent —
    /// a no-op if the timer already completed/cancelled.
    CancelTimer { timer: ObjectReference },

    // ── Task (external-resource call, Zeebe-style lease lifecycle) ─────────────
    /// Invoke an external `Task` — the Task-state analogue of inline timer arming. The
    /// side-effect handler emits only `Event::TaskActivated`, which makes the task **available**
    /// (`Pending`) for a worker to claim; the physical call is performed by the worker (see
    /// `spica-client`'s `worker` module), not by the engine. `owner` is the invoking activity's
    /// reference (kind `Activity`); `resource` is the URI workers claim on (the job type); `arguments` are
    /// the projected call payload — **frozen once here** (Zeebe job payload) and reused verbatim on
    /// every retry of the same task entity, never re-projected.
    ///
    /// `retry_plan` is the state's `Retry` array **resolved at activation** into a list of
    /// [`RetryPolicy`] and baked onto the task, so the reused task decides its own retries
    /// (per-retrier budget + backoff) without revisiting the owning state's definition — the
    /// self-containment a per-`resource` task partition needs. Empty = no retry.
    ///
    /// `execution` is the owning top-level run (the flat anchor, carried through branches); it feeds
    /// `Task::execution` and the task's `{execution.name}-{suffix}` generated name (finding #13).
    ActivateTask {
        execution: ObjectReference,
        owner: ObjectReference,
        task: ObjectReference,
        resource: String,
        arguments: Value,
        retry_plan: Vec<RetryPolicy>,
    },

    /// A worker's claim of up to `max_tasks` available (`Pending`) tasks of `resource` (Zeebe
    /// `ActivateJobs`). The engine's `poll_tasks` API writes this **only when a read-first gate found
    /// claimable work** — an idle poll is a pure query and never reaches the log. Dispatch leases each
    /// discovered task to `worker_id` for `lease_seconds`, emitting one batched `TasksClaimed` (+ a
    /// per-task `DeliveryLease` timer), and returns the granted set to the awaiting caller via the
    /// acknowledgment channel (`AckOutcome::Granted`). Allocation stays in the StreamProcessor's
    /// serialized, lock-holding dispatch, so the grant is decided where the projection is read.
    /// `request_id` correlates the caller's `poll_tasks` with the returned task list (the
    /// `TasksClaimed` event echoes it back).
    ClaimTasks {
        request_id: RequestId,
        worker_id: String,
        resource: String,
        max_tasks: usize,
        lease_seconds: u64,
    },

    /// A worker reported its claimed task completed successfully (Zeebe `CompleteJob`). Validated by
    /// the engine: the task must be `Running` (leased) to this `worker_id`. Drives the owning
    /// state's `complete` via `CompleteState`.
    ///
    /// `request_id` is the worker-supplied correlation key (mirroring [`CreateFlow`](Command::CreateFlow)
    /// / [`CreateExecution`](Command::CreateExecution)): the completing worker mints it per call and it is
    /// echoed back on the outcome so the engine can route the *actual processing result* — the applied
    /// `TaskCompleted` on success, or a `Reject` if the settlement guard refuses — to that exact caller.
    /// Without it a `CompleteTask` would be fire-and-forget; with it, `TaskApi::complete` awaits the
    /// result and reports whether the settlement was accepted or rejected.
    CompleteTask {
        request_id: RequestId,
        task: ObjectReference,
        worker_id: String,
        output: Value,
    },

    /// A worker reported its claimed task failed (Zeebe `FailJob`), or the engine's `TimeoutSeconds`
    /// backstop failed it (`worker_id` empty). Validated by the engine (leasing worker must match);
    /// drives the owning state's `Retry`/`Catch`/terminate policy.
    FailTask {
        task: ObjectReference,
        worker_id: String,
        error: ExecutionError,
    },

    /// A task's lease elapsed without a settle: return it to `Pending` (re-claimable by any worker).
    /// Produces `Event::TaskLeaseExpired`. Idempotent — a no-op if the task already settled.
    ReleaseTaskLease { task: ObjectReference },

    /// Cancel a pending `Task` (e.g. the owning activity/execution is terminated while the call is
    /// in flight). Idempotent — a no-op if the task already settled.
    CancelTask { task: ObjectReference },

    /// Continue a drained-and-finishing `owner`'s **success** drain on a later round. Issued by the
    /// one-hop child-settled reactor (see `handlers::child_completed`) the moment it observes the
    /// `owner` is `Completing` with no remaining children; the [`ContinueCompleteHandler`] emits the
    /// owner's terminal next round and issues a follow-up Continue for *its* owner. Replaces the old
    /// inline recursive cascade with one hop per round (Zeebe's `COMPLETE_ELEMENT` decoupling), so
    /// convergence no longer recurses up the owner chain on the call stack.
    ContinueComplete { owner: ObjectReference },

    /// Continue a drained-and-finishing `owner`'s **failure** drain on a later round — the
    /// `Terminating` analogue of [`ContinueComplete`](Command::ContinueComplete). The `reason` is
    /// recovered from the `owner`'s `Terminating(reason)` status at drain time (single source of
    /// truth), so it is deliberately not carried.
    ContinueTerminate { owner: ObjectReference },
}

/// Why an armed timer exists — its lifecycle role. Drives `TriggerTimer`'s dispatch and is a
/// placeholder for later per-state `TimeoutSeconds` (M2).
///
/// The first three variants are **state-machine semantic** timers: they arise from the ASL
/// definition (`Seconds`/`TimeoutSeconds`) and drive a state transition when they fire. The last
/// (`DeliveryLease`) is an **infra/delivery** guard, not workflow-defined — it shares the durable
/// timer path only because a claimed-but-unsettled task must be re-queued after a restart, and its
/// firing is engine-internal coordination rather than a state-machine behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimerPurpose {
    /// The state-machine `TimeoutSeconds` deadline; its firing terminates the execution `TimedOut`.
    ExecutionTimeout,
    /// A `Wait` state's `Seconds` delay; its firing completes the owning state.
    WaitResume,
    /// A Task state's `TimeoutSeconds` deadline; its firing fails the in-flight task with
    /// `States.Timeout` (routed back into the owning state's `Retry`/`Catch` policy).
    TaskTimeout,
    /// The engine's **delivery lease** on a claimed Task (Zeebe activation timeout); its firing
    /// re-queues the task (`Pending`) so a stalled / crashed worker does not hold it forever. An
    /// infra/delivery timer, unlike the state-machine semantic timers above.
    DeliveryLease,
    // M2: StateTimeout
}
