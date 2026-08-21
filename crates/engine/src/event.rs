use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::activity::ActivityValue;
use crate::error::ExecutionError;
use crate::execution::ExecutionValue;
use crate::flow::Flow;
use crate::flow_version::FlowVersion;
use crate::id::{ActivityId, ExecutionId, RequestId};
use crate::log::Timestamp;
use crate::task::TaskValue;
use crate::timer::TimerValue;
use crate::variables::Variables;

/// The result of executing a [`Command`](crate::Command). Events are appended to the
/// [`LogStream`](crate::LogStream) alongside Commands; the [`StreamProcessor`](crate::StreamProcessor) applies
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
    /// `FlowCreated` — a **brand-new flow** (a `name` appearing for the first time) was created.
    /// This is the durable record of a flow aggregate's birth, carrying the authoritative
    /// [`Flow`](crate::Flow) value (its minted audit `flow_id`, `name`, creation stamp, `Active`
    /// status, and — since a flow is born with its first version — the initial
    /// `latest_flow_version_id`). It fires **only** on a new name: creating a *new version* of an
    /// existing flow is a distinct operation that emits [`FlowVersionCreated`](Event::FlowVersionCreated)
    /// alone (see the split rationale there).
    ///
    /// `CreateFlow` requires the name to **not already exist** (the `Engine` boundary pre-checks
    /// before appending), so this event and the co-emitted `FlowVersionCreated` together make up one
    /// atomic creation in a single command batch. `request_id` is the [`RequestId`] the `CreateFlow`
    /// command carried — the awaiting operation's correlation key. The StreamProcessor routes the create
    /// ack on `FlowVersionCreated` (which always fires), so this event's `request_id` is carried for
    /// correlation/symmetry but does not itself resolve the ack.
    FlowCreated { request_id: RequestId, flow: Flow },

    /// `FlowVersionCreated` — a new immutable version of a flow was created, carrying the full
    /// [`FlowVersion`](crate::FlowVersion) value. This is the **durable definition record**: it is
    /// where a definition enters the stream (the projection keeps it in Storage keyed by
    /// `flow_version_id`), and it is exactly what lets a recovered Engine re-resolve a definition by
    /// id without the caller re-supplying the machine.
    ///
    /// It is emitted for **every** version creation — the `version 1` that accompanies a
    /// [`FlowCreated`](Event::FlowCreated) birth, and, in a future milestone, the versions published
    /// by a standalone "add a version to an existing flow" command. Splitting it out from
    /// `FlowCreated` is precisely what lets that future command emit only this event (and advance the
    /// owning `Flow.latest_flow_version_id` via its applier) without re-creating the flow aggregate.
    ///
    /// `request_id` echoes the originating command's [`RequestId`]; the StreamProcessor completes the
    /// awaiting `create_flow` ack on this event and routes back `flow_version.flow_version_id`.
    FlowVersionCreated {
        request_id: RequestId,
        flow_version: FlowVersion,
    },

    /// Result of `Command::CreateExecution` (a top-level run) or `Command::SpawnBranch` (a child
    /// Parallel branch) — the execution's single creation record. For a child execution `parent`
    /// links it into the owning tree and `state_path` locates its branch `states` table within
    /// the shared machine document (see [`crate::storage::Execution::state_path`]);
    /// `root_execution` is always the top-level run's id (the flat query anchor), carried verbatim
    /// through every nesting level.
    ///
    /// `request_id` is the echoing correlate for a **top-level** `CreateExecution`: it carries the
    /// command's `request_id` back so the awaiting `start` operation is acknowledged by request id
    /// (the same model as `FlowCreated`) once the execution is durably created. A child execution
    /// spawned by a fan-out (`SpawnBranch`) has no client request awaiting it, so it carries the
    /// `nil` placeholder — that variant is never a request acknowledgement.
    ExecutionCreated {
        request_id: RequestId,
        execution: ExecutionValue,
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

    /// Success path began on the execution. Carries the same execution entity with
    /// `status = Completing` and its decided success `output` fixed.
    ExecutionCompleting { execution: ExecutionValue },
    /// Execution succeeded after any owned children drained. Carries the same execution entity with
    /// `status = Completed`.
    ExecutionCompleted { execution: ExecutionValue },

    /// Termination began on the execution. Carries the same execution entity with its final
    /// termination reason already embedded in `status = Terminating(reason)`.
    ExecutionTerminating { execution: ExecutionValue },
    /// Execution terminated after any owned children drained. Carries the same execution entity with
    /// `status = Terminated(reason)`.
    ExecutionTerminated { execution: ExecutionValue },

    /// Result of `Command::ActivateState` — the state was entered and the lifecycle stream records
    /// the full event-carried [`ActivityValue`](crate::ActivityValue) for that moment.
    ///
    /// The value is the Activity's domain entity shape, intentionally excluding projection-only
    /// bookkeeping such as `active_children`. A follower / recovered leader can therefore rebuild the
    /// same activity domain state from the event stream alone, while storage remains free to keep its
    /// own fold-only metadata alongside it.
    StateActivating { activity: ActivityValue },
    /// The state finished activating — emitted by the `StateHandler::activate` **only after** it has
    /// processed the state's input. It is the ed of `StateActivating` and precedes the state's own
    /// follow-up: a `CompleteState`/`TerminateState` sequence or an armed side-effect (e.g. a Wait
    /// resume timer). Full per-entry chain: `StateActivating → StateActivated → …`.
    ///
    /// Carries the same entity-shaped [`ActivityValue`](crate::ActivityValue), now updated to
    /// reflect the activation result (for example, a Task/Parallel processed input or a Map activity
    /// whose `activity_state` now contains its iteration plan).
    StateActivated { activity: ActivityValue },

    /// The state began its success finish (the complete step started; children, if any, may still be
    /// draining). Carries the same Activity entity with `status = Completing`.
    StateCompleting { activity: ActivityValue },
    /// The state finished successfully. Carries the same Activity entity with its terminal `output`
    /// fixed and `status = Completed`.
    StateCompleted { activity: ActivityValue },

    /// The state began terminating. Carries the same Activity entity with the final termination reason
    /// already embedded in `status = Terminating(reason)`.
    StateTerminating { activity: ActivityValue },
    /// The state terminated after its owner/children drained. Carries the same Activity entity with
    /// `status = Terminated(reason)`.
    StateTerminated { activity: ActivityValue },

    /// A timer was armed and the lifecycle stream records the full event-carried
    /// [`TimerValue`](crate::TimerValue) for that moment.
    TimerActivated { timer: TimerValue },
    /// A timer's deadline passed. Carries the same timer entity with `status = Completed`.
    TimerTriggered { timer: TimerValue },
    /// A timer was cancelled before firing. Carries the same timer entity with
    /// `status = Cancelled`.
    TimerCancelled { timer: TimerValue },

    /// Variables assigned by an Activity's `Assign`. Carries the full post-assign variable snapshot
    /// for the owning execution projection so replay does not need to re-merge per-key diffs.
    VariablesAssigned {
        execution: ExecutionId,
        variables: Variables,
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
    /// A `Task` state invoked its `Resource` and the lifecycle stream records the full event-carried
    /// [`TaskValue`](crate::TaskValue) for that moment, with `status = Active` (available for a
    /// worker to claim).
    ///
    /// Like `TimerActivated`, this is the durable single creation record for a leaf side effect: the
    /// task owns no children. Applying it makes the task **claimable** (a worker pulls it via
    /// [`TaskApi::activate`](crate::TaskApi::activate)); the physical call is performed by the
    /// worker, never the engine.
    TaskActivated { task: TaskValue },
    /// A worker claimed the task (`AssignTask`): `status = Activated`, `worker_id` and `lease_until`
    /// recorded. From here only the leasing worker's `CompleteTask`/`FailTask` may settle it; the
    /// worker/lease fields are the durable record a restarted engine needs to keep honoring the
    /// claim.
    TaskLeased { task: TaskValue },
    /// The claimed task's lease elapsed before a settle (`ReleaseTaskLease`): `status` returns to
    /// `Pending` and `worker_id`/`lease_until` are cleared, so the task is re-claimable by any worker
    /// (or the same one, if it stalled then recovered — Zeebe's activation-timeout re-queue).
    TaskLeaseExpired { task: TaskValue },
    /// The task settled successfully. Carries the same task entity with `status = Completed`; the
    /// concrete returned payload is kept separately as `output` because it feeds the owning activity's
    /// `raw_output` rather than becoming part of the task entity itself.
    TaskCompleted { task: TaskValue, output: Value },
    /// The task's registered handler failed. Carries the same task entity with `status = Failed`;
    /// `error` still travels alongside it because the failure drives the owning state's
    /// `Retry`/`Catch`/terminate decision rather than being stored on the task row.
    TaskFailed {
        task: TaskValue,
        error: ExecutionError,
    },
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
    /// while the call was in flight). Carries the same task entity with `status = Cancelled`.
    TaskCancelled { task: TaskValue },
}
