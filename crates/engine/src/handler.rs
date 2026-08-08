use async_trait::async_trait;
use serde_json::Value;
use spica_asl::StateMachine;

use crate::command::{Command, TerminationReason};
use crate::error::ExecutionError;
use crate::eval_env::EvalEnv;
use crate::event::Event;
use crate::id::{ActivityId, EntryId, ExecutionId, StreamId, TimerId};
use crate::log::{Entry, EntryPayload, Timestamp};
use crate::scope::Scope;
use crate::storage::Storage;

/// Collects the [`Entry`]s a handler emits while handling one Command, enveloping each with the
/// call's `cause_id` / `stream_id` / `timestamp` and a placeholder `entry_id` (the log assigns the
/// position). It is also the source of fresh [`ActivityId`] / [`TimerId`] for entities the handler
/// creates.
///
/// This is the handler's single output + id channel, replacing a returned `Produced` value plus a
/// separate envelope step: the handler emits raw [`Event`]s / [`Command`]s via
/// [`emit_event`](Self::emit_event) / [`emit_command`](Self::emit_command), and the `Collector`
/// turns each into a fully-formed [`Entry`]. The Processor then appends `into_entries()` atomically.
///
/// The Collector does not assign `entry_id` — the [`LogStream`](crate::LogStream) stamps each
/// entry's position at `append` time, so entries leave here with a [`EntryId`](crate::EntryId)::nil()
/// placeholder.
/// `cause_id`/`stream_id`/`timestamp` are still set here; only the position is the log's.
pub struct Collector {
    cause_id: EntryId,
    stream_id: StreamId,
    timestamp: Timestamp,
    entries: Vec<Entry>,
}

impl Collector {
    pub fn new(cause_id: EntryId, stream_id: StreamId, timestamp: Timestamp) -> Self {
        Self {
            cause_id,
            stream_id,
            timestamp,
            entries: Vec::new(),
        }
    }

    /// Emit an [`Event`], enveloped into an [`Entry`] with this call's `cause_id`/`stream_id`/
    /// `timestamp` and a placeholder `entry_id` (the log assigns the real position on append).
    pub fn emit_event(&mut self, event: Event) {
        self.push(EntryPayload::Event(event));
    }

    /// Emit a subsequent [`Command`] (enveloped).
    pub fn emit_command(&mut self, command: Command) {
        self.push(EntryPayload::Command(command));
    }

    fn push(&mut self, payload: EntryPayload) {
        self.entries.push(Entry {
            stream_id: self.stream_id,
            entry_id: EntryId::nil(), // placeholder — the LogStream stamps positions at append.
            cause_id: Some(self.cause_id),
            timestamp: self.timestamp,
            payload,
        });
    }

    /// Allocate a fresh [`ActivityId`] (for a new [`Command::ActivateState`]). Activities are ULIDs
    /// minted in place — no shared counter needed, since the log's causal `entry_id` carries the
    /// ordering of activities.
    pub fn next_activity(&mut self) -> ActivityId {
        ActivityId::new()
    }

    /// Allocate a fresh [`TimerId`] (for a [`Command::ActivateTimer`]). Timers are ULIDs minted in
    /// place — no shared counter needed, since the log carries the causal/`entry_id` ordering.
    pub fn next_timer(&mut self) -> TimerId {
        TimerId::new()
    }

    /// Emit a definitive failure: `TerminateState` (if the failing context is a state) plus
    /// `TerminateExecution` with [`TerminationReason::Failed`]. Handlers own their failures: on an
    /// eval/decision error they emit the failure themselves (cohesive with the site that produced
    /// it), so `handle` always produces an outcome and returns `()`. The activity-level
    /// [`Command::TerminateState`] runs the state's terminate path (StateTerminating +
    /// StateTerminated, plus descendant cleanup) rather than marking the activity in place.
    pub fn terminate(
        &mut self,
        activity: Option<ActivityId>,
        execution: ExecutionId,
        error: ExecutionError,
    ) {
        let reason = TerminationReason::Failed { error };
        if let Some(activity) = activity {
            self.emit_command(Command::TerminateState {
                activity,
                reason: reason.clone(),
            });
        }
        self.emit_command(Command::TerminateExecution {
            id: execution,
            reason,
        });
    }

    /// Convenience for `terminate` at a site where the execution itself failed (no state context).
    pub fn fail_execution(&mut self, execution: ExecutionId, error: ExecutionError) {
        self.terminate(None, execution, error);
    }

    /// Consume the collector, returning the collected [`Entry`]s.
    pub fn into_entries(self) -> Vec<Entry> {
        self.entries
    }
}

/// Generic context every handler receives — no command/execution-specific data. The handler reads
/// [`Storage`] (via `storage`) and extracts what it needs from the [`Command`] itself, so the
/// Processor never branches on Command type.
pub struct HandlerContext<'a> {
    pub sm: &'a StateMachine,
    pub env: &'a mut EvalEnv,
    pub storage: &'a dyn Storage,
}

/// Activity-specific data gathered by `ActivateState` / `CompleteState` / `CompleteTimer` from the
/// [`Command`] payload + [`Storage`], then passed to the state-specific `StateHandler` impls.
/// `kind` lets handlers distinguish the two phases (`activate` vs `complete`) without requiring the
/// state body to re-derive it from the surrounding Command.
pub struct ActivityCtx {
    pub execution: ExecutionId,
    pub exec_input: Value,
    pub input: Value,
    pub scope: Scope,
    pub state_name: String,
    pub kind: CtxKind,
}

/// Whether the surrounding command is entering (`ActivateState`) or finishing (`CompleteState`)
/// the activity — the two moments a state's behavior can differ on (e.g. `Assign` applies during
/// activate and is read during complete's output projection).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CtxKind {
    Activate,
    Complete,
}

/// Handles a [`Command`]: reads `ctx` and emits [`Event`]s / [`Command`]s to `out`.
///
/// The handler **owns its failures**: on an unexpected error (e.g. a JSONata eval error) it emits a
/// failure via [`Collector::terminate`] and returns normally, so `handle` always produces an outcome
/// and (together with the failure path) returns `()`. This keeps each command's success and failure
/// handling cohesive in its handler, not split across a generic default or the Processor.
///
/// Handlers are pure decision-makers: they read `ctx` (definition + current state) and emit to
/// `out`; they perform no I/O — side effects are themselves Commands (e.g. [`Command::ActivateTimer`])
/// dispatched to dedicated side-effect handlers.
#[async_trait]
pub trait CommandHandler: Send + Sync {
    /// The [`Command`] variant this handler serves, identified by a `Default` placeholder instance
    /// standing in only to read its discriminant — the real command instances are built by the
    /// framework's state handlers. The `Processor`'s dispatch table reads this off the handler to
    /// derive its key, so the handler is the single source of truth for which variant it handles.
    /// Takes `&self` (rather than being a `Self: Sized` associated function) so the trait stays
    /// object-safe for the `Box<dyn CommandHandler>` dispatch table.
    fn command(&self) -> Command;

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector);
}
