use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::command::TerminationReason;
use crate::id::{ActivityId, ExecutionId, NodeId, TimerId};
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
    /// Result of `Command::CreateExecution` — the execution's single creation record.
    ExecutionCreated { id: ExecutionId, input: Value },

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
    StateActivating {
        execution: ExecutionId,
        activity: ActivityId,
        state: String,
        input: Value,
    },
    /// The state finished activating — emitted by the `StateHandler::activate` **only after** it has
    /// processed the state's input. It is the ed of `StateActivating` and precedes the state's own
    /// follow-up: a `CompleteState`/`TerminateState` sequence or an armed side-effect (e.g. a Wait
    /// resume timer). Full per-entry chain: `StateActivating → StateActivated → …`.
    StateActivated { activity: ActivityId },

    /// The state began its success finish (the complete step started; children, if any, may still
    /// be draining).
    StateCompleting { activity: ActivityId },
    /// The state finished successfully, producing `output` (the next state's input, or the
    /// execution output if terminal).
    StateCompleted { activity: ActivityId, output: Value },

    /// The state began terminating with `reason` (children may still be draining).
    StateTerminating { activity: ActivityId },
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
    // M2: TaskActivated / TaskCompleting / TaskCompleted / TaskTerminating / TaskTerminated
}
