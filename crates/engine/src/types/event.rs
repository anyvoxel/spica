use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::activity::Activity;
use crate::types::error::ExecutionError;
use crate::types::execution::Execution;
use crate::types::flow::Flow;
use crate::types::flow_version::FlowVersion;
use crate::types::id::RequestId;
use crate::types::meta::ObjectReference;
use crate::types::task::Task;
use crate::types::thread::Thread;
use crate::types::timer::Timer;
use crate::types::variables::Variables;

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
    /// [`Flow`](crate::Flow) value (its user-supplied `name` — the flow's sole identity, creation
    /// stamp, `Active` status, and — since a flow is born with its first version — the initial
    /// `latest_version` counter). It fires **only** on a new name: creating a *new version* of an
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
    /// its `{flow_name}-{version}` name), and it is exactly what lets a recovered Engine re-resolve a
    /// definition by reference without the caller re-supplying the machine.
    ///
    /// It is emitted for **every** version creation — the `version 1` that accompanies a
    /// [`FlowCreated`](Event::FlowCreated) birth, and, in a future milestone, the versions published
    /// by a standalone "add a version to an existing flow" command. Splitting it out from
    /// `FlowCreated` is precisely what lets that future command emit only this event (and advance the
    /// owning `Flow.latest_version` counter via its applier) without re-creating the flow aggregate.
    ///
    /// `request_id` echoes the originating command's [`RequestId`]; the StreamProcessor completes the
    /// awaiting `create_flow` ack on this event and routes back the version's `ObjectReference`.
    FlowVersionCreated {
        request_id: RequestId,
        flow_version: FlowVersion,
    },

    /// Result of `Command::CreateExecution` — a **top-level run**'s single creation record. A
    /// top-level `Execution` is its own flat query anchor: it has no `parent`, no `root_execution`
    /// (it IS the root) and no branch `state_path` (its states resolve against the machine's
    /// top-level `states`). Fan-out children (`Parallel` branches / `Map` items) are **not**
    /// executions — they are [`Thread`]s, created via [`ThreadCreated`](Event::ThreadCreated).
    ///
    /// `request_id` is the echoing correlate for the `CreateExecution` command: it carries the
    /// command's `request_id` back so the awaiting `start` operation is acknowledged by request id
    /// (the same model as `FlowCreated`) once the execution is durably created.
    ExecutionCreated {
        request_id: RequestId,
        execution: Execution,
    },

    /// Success path began on the execution. Carries the same execution entity with
    /// `status = Completing` and its decided success `output` fixed.
    ExecutionCompleting { execution: Execution },
    /// Execution succeeded after any owned children drained. Carries the same execution entity with
    /// `status = Completed`.
    ExecutionCompleted { execution: Execution },

    /// Termination began on the execution. Carries the same execution entity with its final
    /// termination reason already embedded in `status = Terminating(reason)`.
    ExecutionTerminating { execution: Execution },
    /// Execution terminated after any owned children drained. Carries the same execution entity with
    /// `status = Terminated(reason)`.
    ExecutionTerminated { execution: Execution },

    /// Result of `Command::SpawnThread` — a fan-out child **Thread** was created. This is the
    /// single creation record for a `Parallel` branch / `Map` item: a scoped sub-run of the shared
    /// machine that behaves like a self-contained sub-execution but is a distinct [`Thread`] entity
    /// (carrying the top-level `root_execution` anchor and its branch `state_path`). A thread is
    /// always owned by its container Activity, so it is wired into that activity's `active_children`.
    ///
    /// A thread is spawned by a container (fan-out), never by a client request, so this variant
    /// carries no `request_id` — it is never a request acknowledgement.
    ThreadCreated { thread: Thread },
    /// Success path began on the thread. Carries the same thread entity with `status = Completing`
    /// and its decided success `output` fixed.
    ThreadCompleting { thread: Thread },
    /// Thread succeeded after any owned children drained. Carries the same thread entity with
    /// `status = Completed`.
    ThreadCompleted { thread: Thread },

    /// Termination began on the thread. Carries the same thread entity with its final termination
    /// reason already embedded in `status = Terminating(reason)`.
    ThreadTerminating { thread: Thread },
    /// Thread terminated after any owned children drained. Carries the same thread entity with
    /// `status = Terminated(reason)`.
    ThreadTerminated { thread: Thread },

    /// Result of `Command::ActivateState` — the state was entered and the lifecycle stream records
    /// the full event-carried [`Activity`](crate::Activity) for that moment.
    ///
    /// The value is the Activity's domain entity shape, intentionally excluding projection-only
    /// bookkeeping such as `active_children`. A follower / recovered leader can therefore rebuild the
    /// same activity domain state from the event stream alone, while storage remains free to keep its
    /// own fold-only metadata alongside it.
    StateActivating { activity: Activity },
    /// The state finished activating — emitted by the `StateHandler::activate` **only after** it has
    /// processed the state's input. It is the ed of `StateActivating` and precedes the state's own
    /// follow-up: a `CompleteState`/`TerminateState` sequence or an armed side-effect (e.g. a Wait
    /// resume timer). Full per-entry chain: `StateActivating → StateActivated → …`.
    ///
    /// Carries the same entity-shaped [`Activity`](crate::Activity), now updated to
    /// reflect the activation result (for example, a Task/Parallel processed input or a Map activity
    /// whose `activity_state` now contains its iteration plan).
    StateActivated { activity: Activity },

    /// The state began its success finish (the complete step started; children, if any, may still be
    /// draining). Carries the same Activity entity with `status = Completing`.
    StateCompleting { activity: Activity },
    /// The state finished successfully. Carries the same Activity entity with its terminal `output`
    /// fixed and `status = Completed`.
    StateCompleted { activity: Activity },

    /// The state began terminating. Carries the same Activity entity with the final termination reason
    /// already embedded in `status = Terminating(reason)`.
    StateTerminating { activity: Activity },
    /// The state terminated after its owner/children drained. Carries the same Activity entity with
    /// `status = Terminated(reason)`.
    StateTerminated { activity: Activity },

    /// A timer was armed and the lifecycle stream records the full event-carried
    /// [`Timer`](crate::Timer) for that moment.
    TimerActivated { timer: Timer },
    /// A timer's deadline passed. Carries the same timer entity with `status = Completed`.
    TimerTriggered { timer: Timer },
    /// A timer was cancelled before firing. Carries the same timer entity with
    /// `status = Cancelled`.
    TimerCancelled { timer: Timer },

    /// Variables assigned by an Activity's `Assign`. Carries the full post-assign variable snapshot
    /// for the owning **scope** projection (an `Execution` or a fan-out `Thread`) so replay does not
    /// need to re-merge per-key diffs. The scope is addressed structurally: a top-level state assigns
    /// into the `Execution`, a `Parallel`/`Map` branch state into its branch `Thread` — the applier
    /// dispatches on the reference's kind (see `VariablesAssignedApplier`).
    VariablesAssigned {
        scope: ObjectReference,
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
        activity: ObjectReference,
        next: String,
        output: Value,
    },

    // ── Task (external-resource call, M2 lifecycle) ──────────────────────────────
    /// A `Task` state invoked its `Resource` and the lifecycle stream records the full event-carried
    /// [`Task`](crate::Task) for that moment, with `status = Active` (available for a
    /// worker to claim).
    ///
    /// Like `TimerActivated`, this is the durable single creation record for a leaf side effect: the
    /// task owns no children. Applying it makes the task **claimable** (a worker pulls it via
    /// [`TaskApi::poll_tasks`](crate::TaskApi::poll_tasks)); the physical call is performed by the
    /// worker, never the engine.
    TaskActivated { task: Task },
    /// A worker's `ClaimTasks` claimed one batch of tasks: each is `Running` with `worker_id` /
    /// `lease_until` recorded. From here only the leasing worker's `CompleteTask`/`FailTask` may
    /// settle them; the worker/lease fields are the durable record a restarted engine needs to keep
    /// honoring the claim. Batched (one event per poll, not per task) because all claims share one
    /// causal `ClaimTasks` batch — the applier folds each entry against its own `Pending` stake.
    TasksClaimed { tasks: Vec<Task> },
    /// The claimed task's lease elapsed before a settle (`ReleaseTaskLease`): `status` returns to
    /// `Pending` and `worker_id`/`lease_until` are cleared, so the task is re-claimable by any worker
    /// (or the same one, if it stalled then recovered — Zeebe's activation-timeout re-queue).
    TaskLeaseExpired { task: Task },
    /// The task settled successfully. Carries the same task entity with `status = Completed`; the
    /// concrete returned payload is kept separately as `output` because it feeds the owning activity's
    /// `raw_output` rather than becoming part of the task entity itself.
    ///
    /// `request_id` echoes the completing worker's own correlation key (the one its `CompleteTask`
    /// carried) so the StreamProcessor can resolve the awaiting `TaskApi::complete` — the request/response
    /// contract that makes a task settlement report its actual outcome instead of being fire-and-forget.
    TaskCompleted {
        request_id: RequestId,
        task: Task,
        output: Value,
    },
    /// The task settled **abnormally**. Carries the same task entity; its `status` is the outcome:
    /// `Pending` means a `Retry` was scheduled (the task re-queues, claimable no earlier than
    /// `next_available_at`, with the per-retrier attempt counters advanced) — the retry bookkeeping
    /// is folded from the entity, so no separate event is needed; `Failed` (terminal) means the retry
    /// budget is exhausted and `error` drives the owning state's `Catch`/terminate decision rather
    /// than being stored on the task row.
    TaskFailed { task: Task, error: ExecutionError },
    /// The task was cancelled before settling (e.g. the owning activity/execution was terminated
    /// while the call was in flight). Carries the same task entity with `status = Cancelled`.
    TaskCancelled { task: Task },
}
