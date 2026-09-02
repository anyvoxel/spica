use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use spica_asl::StateMachine;

use crate::eval_env::EvalEnv;
use crate::log::{Entry, EntryPayload, Timestamp};
use crate::storage::Storage;
use crate::task_api::ActivatedTask;
use crate::types::activity::Activity;
use crate::types::command::{Command, TerminationReason};
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::id::{ActivityId, EntryId, ExecutionId, RequestId, StreamId, TimerId};
use crate::types::meta::ObjectReference;
use crate::types::reject::{Reject, RejectionType};
use crate::types::variables::Variables;

/// Collects the [`Entry`]s a handler emits while handling one Command, enveloping each with the
/// call's `cause_id` / `stream_id` / `timestamp` and a placeholder `entry_id` (the log assigns the
/// position). It is also the source of fresh [`ActivityId`] / [`TimerId`] for entities the handler
/// creates.
///
/// This is the handler's single output + id channel, replacing a returned `Produced` value plus a
/// separate envelope step: the handler emits raw [`Event`]s / [`Command`]s via
/// [`emit_event`](Self::emit_event) / [`emit_command`](Self::emit_command), and the `Collector`
/// turns each into a fully-formed [`Entry`]. The StreamProcessor then appends `into_entries()` atomically.
///
/// The Collector does not assign `entry_id` — the [`LogStream`](crate::LogStream) stamps each
/// entry's position (and its own `stream_id`) at `append` time, so entries leave here with both an
/// [`EntryId`](crate::EntryId)::nil() and [`StreamId`](crate::StreamId)::nil() placeholder; only
/// `cause_id` is set here (it is the causal link and does not depend on the assigned position).
/// Each entry's `timestamp` is stamped fresh at `push` time, so within one handled Command the
/// emitted Events + follow-up Commands each carry their own write moment rather than sharing a
/// single captured time.
pub struct Collector {
    cause_id: EntryId,
    entries: Vec<Entry>,
    /// Deferred acknowledgement side effects declared by the handler (see [`AckSideEffect`]).
    acks: Vec<AckSideEffect>,
}

impl Collector {
    pub fn new(cause_id: EntryId) -> Self {
        Self {
            cause_id,
            entries: Vec::new(),
            acks: Vec::new(),
        }
    }

    /// Emit an [`Event`], enveloped into an [`Entry`] with this call's `cause_id`, a fresh
    /// `timestamp`, and placeholder `entry_id`/`stream_id` (the log assigns the real positions and
    /// its own stream id on append).
    pub fn emit_event(&mut self, event: Event) {
        self.push(EntryPayload::Event(event));
    }

    /// Emit a subsequent [`Command`] (enveloped).
    pub fn emit_command(&mut self, command: Command) {
        self.push(EntryPayload::Command(command));
    }

    fn push(&mut self, payload: EntryPayload) {
        self.entries.push(Entry {
            stream_id: StreamId::nil(), // placeholder — the LogStream stamps its own id at append.
            entry_id: EntryId::nil(),   // placeholder — the LogStream stamps positions at append.
            cause_id: Some(self.cause_id),
            // Stamped per-entry rather than reusing a fixed value: `Timestamp` is audit metadata
            // used for neither ordering nor decisions (ordering is by `entry_id`), so each push can
            // cheaply record its own write moment.
            timestamp: Timestamp::now(),
            payload,
        });
    }

    /// Allocate a fresh [`ActivityId`] (for a new [`Command::ActivateState`]). Activities are ULIDs
    /// minted in place — no shared counter needed, since the log's causal `entry_id` carries the
    /// ordering of activities.
    pub fn next_activity(&mut self) -> ActivityId {
        ActivityId::new()
    }

    /// Allocate a fresh [`TimerId`] (for a timer armed inline via `emit_timer`). Timers are ULIDs
    /// minted in place — no shared counter needed, since the log carries the causal/`entry_id`
    /// ordering.
    pub fn next_timer(&mut self) -> TimerId {
        TimerId::new()
    }

    /// Allocate a fresh [`ExecutionId`] (for a child execution spawned by a `Parallel` branch).
    /// Executions are ULIDs minted in place — the log's causal `entry_id`/`root_execution` carry the
    /// ordering/ownership, so no shared counter is needed.
    pub fn next_execution(&mut self) -> ExecutionId {
        ExecutionId::new()
    }

    /// Allocate a fresh task uid ([`ulid::Ulid`], for a [`Command::ActivateTask`]). The task's
    /// internal `uid` is a ULID minted in place (no shared counter — the log carries the causal
    /// `entry_id` ordering). The worker addresses a task by its **canonical name**, not this uid
    /// (see `task_api::ActivatedTask`), so the uid never leaves the engine over the worker protocol.
    pub fn next_task(&mut self) -> ulid::Ulid {
        ulid::Ulid::new()
    }

    /// Emit a definitive failure: `TerminateState` (if the failing context is a state) plus
    /// `TerminateExecution` with [`TerminationReason::Failed`]. Handlers own their failures: on an
    /// eval/decision error they emit the failure themselves (cohesive with the site that produced
    /// it), so `handle` always produces an outcome and returns `()`. The activity-level
    /// [`Command::TerminateState`] runs the state's terminate path (StateTerminating +
    /// StateTerminated, plus descendant cleanup) rather than marking the activity in place.
    pub fn terminate(
        &mut self,
        activity: Option<ObjectReference>,
        execution: ObjectReference,
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
            // Internal sites know the exact incarnation, so the uid is set as a matching guard.
            name: execution.name.clone(),
            uid: Some(execution.uid),
            reason,
        });
    }

    /// Convenience for `terminate` at a site where the execution itself failed (no state context).
    pub fn fail_execution(&mut self, execution: ObjectReference, error: ExecutionError) {
        self.terminate(None, execution, error);
    }

    /// Consume the collector, returning the collected [`Entry`]s and any deferred acknowledgement
    /// side effects. The [`StreamProcessor`](crate::StreamProcessor) uses both: it appends the entries, then
    /// executes each ack once its [`Event`] has been applied to Storage.
    pub fn into_parts(self) -> (Vec<Entry>, Vec<AckSideEffect>) {
        (self.entries, self.acks)
    }

    /// Consume the collector, returning the collected [`Entry`]s, discarding any deferred acks.
    /// Convenience for single-shot [`dispatch`](crate::StreamProcessor::dispatch) callers (e.g. tests)
    /// that drive a stream by hand without an [`AckRouter`](crate::AckRouter) to complete acks into.
    pub fn into_entries(self) -> Vec<Entry> {
        self.entries
    }

    /// Whether the collector has produced no entries. Handlers use this to guarantee a dispatched
    /// command always leaves a causally-tied follow-up, emitting a confirmation when it would
    /// otherwise be silent (`ProcessChildCompleted`'s no-op receipt).
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Declare an acknowledgement to be executed once `event` has been applied to Storage, waking
    /// the awaiting `request_id` with `event`. Side effects are deferred rather than executed at
    /// dispatch: the StreamProcessor — which alone knows when an Entry is durably appended *and* applied
    /// — correlates this with the applied `event` and completes the pending ack then, the Zeebe
    /// post-commit side-effect model (see [`AckSideEffect`]).
    pub fn ack_request(&mut self, request_id: RequestId, event: Event) {
        self.acks.push(AckSideEffect::CompleteRequest {
            request_id,
            event: Box::new(event),
        });
    }

    /// Declare an acknowledgement that delivers a **granted task set** to the awaiting `request_id` —
    /// the response channel of a `poll_tasks` (`TaskApi::poll_tasks`). Unlike `CompleteRequest`, there is
    /// no single event to correlate: the granted list is decided by the `ClaimTasksHandler` at discovery,
    /// and delivered once the producing command's events are applied (`AckOutcome::Granted`). The
    /// collector's own `cause_id` (the producing command's entry) is carried so the StreamProcessor can
    /// fire it when any of that command's events is applied.
    pub fn ack_request_tasks(&mut self, request_id: RequestId, tasks: Vec<ActivatedTask>) {
        self.acks.push(AckSideEffect::CompleteRequestTasks {
            request_id,
            cause_id: self.cause_id,
            tasks,
        });
    }

    /// Refuse a client-originated awaiting command: emit a [`Reject`] record (a command refused
    /// application — the engine's `COMMAND_REJECTION` analogue), so the awaiting caller is woken
    /// with the reason instead of a handler that silently `return`s with no response entry.
    ///
    /// This is the mechanism that upholds the *every command has a subsequent entry* invariant for
    /// commands that cannot be applied: a well-formed command that fails its preconditions (malformed
    /// payload, target already exists, …) produces exactly one response record — either an
    /// [`Event`] (applied) or a [`Reject`](Self::reject). Unlike an [`Event`] ack there is **no**
    /// deferred side effect to correlate: the `Reject` carries its own `request_id`, so the StreamProcessor
    /// awakes the awaiter directly as it reads the record (see the `EntryPayload::Reject` arm in
    /// `processor.rs`) — no projection is folded for a refusal.
    ///
    /// Only **client-originated awaiting** commands (`request_id` present) reject; an internal
    /// follow-up no-op (a replayed terminal command whose state already advanced past it) stays
    /// silent — there is no one to respond to. A command whose refusal has no awaiting caller still
    /// emits the `Reject` record for the durable audit trail.
    ///
    /// - `request_id` — the command's correlating [`RequestId`] (the same id that would have echoed
    ///   on its success `Event`).
    /// - `rejection_type` — the stable, machine-readable [`RejectionType`] classification.
    /// - `rejection_reason` — the human-readable explanation.
    pub fn reject(
        &mut self,
        request_id: RequestId,
        rejection_type: RejectionType,
        rejection_reason: impl Into<String>,
    ) {
        self.push(EntryPayload::Reject(Reject {
            request_id,
            rejection_type,
            rejection_reason: rejection_reason.into(),
        }));
    }
}

/// Generic context every handler receives — no command/execution-specific data. The handler reads
/// [`Storage`] (via `storage`) and extracts what it needs from the [`Command`] itself, so the
/// StreamProcessor never branches on Command type.
pub struct HandlerContext<'a> {
    pub env: &'a mut EvalEnv,
    pub storage: &'a dyn Storage,
    /// The StreamProcessor's per-version machine cache. Handlers resolve the machine an execution is
    /// bound to through [`Self::machine`], never from a single in-memory `sm` — so a recovered
    /// Engine re-resolves definitions by id from storage instead of re-supplying them.
    pub definitions: &'a mut HashMap<ObjectReference, Arc<StateMachine>>,
}

impl HandlerContext<'_> {
    /// Resolve — and cache — the state machine version `flow_version` references, loading it lazily
    /// from `Storage` on first use. Executions bind only to a (never-reused) version reference;
    /// the machine content is fetched here and cached per reference in the StreamProcessor, so the same definition
    /// is loaded at most once per version and a recovered Engine can re-resolve it without the caller
    /// re-supplying the machine.
    ///
    /// Returns [`ExecutionError::InvalidDefinition`] if the version no longer exists in storage
    /// (e.g. its definition was GC'd).
    pub async fn machine(
        &mut self,
        flow_version: &ObjectReference,
    ) -> Result<Arc<StateMachine>, ExecutionError> {
        if let Some(m) = self.definitions.get(flow_version) {
            return Ok(m.clone());
        }
        let ver = self
            .storage
            .get_flow_version(flow_version)
            .await?
            .ok_or_else(|| {
                ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                    "flow version {flow_version} not found in storage"
                )))
            })?;
        // The stored form is the raw ASL string; parse it into the model here, then cache. The
        // definition is always parseable (validated at the create boundary and re-checked by the
        // handler), so a failure here indicates corruption in Storage — surface it as such rather
        // than propagating a serde error.
        let machine = Arc::new(
            serde_json::from_str::<StateMachine>(&ver.definition).map_err(|e| {
                ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                    "stored flow definition for {flow_version} is not parseable: {e}"
                )))
            })?,
        );
        self.definitions
            .insert(flow_version.clone(), machine.clone());
        Ok(machine)
    }

    /// Resolve — and cache — the state machine a [`ScopeRecord`] binds to, then return it. For a
    /// top-level `Execution` the version is carried on the scope; for a fan-out `Thread` it is
    /// derived from the thread's root `execution` (the whole tree shares one definition — see
    /// [`crate::storage::resolve_scope_flow_version`]). Kept on the context so the resolve
    /// (which may read `Storage` for the thread case) stays one call at every machine-using site.
    pub async fn machine_for_scope(
        &mut self,
        scope: &crate::storage::ScopeRecord,
    ) -> Result<Arc<StateMachine>, ExecutionError> {
        // The immutable borrow of `storage` for resolving the version ends before the mutable
        // `machine` call below, so there is no aliasing of `self`.
        let flow_version = crate::storage::resolve_scope_flow_version(self.storage, scope).await?;
        self.machine(&flow_version).await
    }
}

/// Activity-specific data gathered by `ActivateState` / `CompleteState` / `TriggerTimer` from the
/// [`Command`] payload + [`Storage`], then passed to the state-specific `StateHandler` impls.
/// `kind` lets handlers distinguish the two phases (`activate` vs `complete`) without requiring the
/// state body to re-derive it from the surrounding Command.
///
/// `activity` is the **single source of truth** for the activity's domain state (its execution,
/// inputs, state path, retry count, …). Everything a handler reads about the activity comes from
/// there — previously `ActivityCtx` also carried flattened copies of several of those fields, and
/// they drifted from `activity`, so they were removed in favor of reading `ctx.activity.*`.
pub struct ActivityCtx {
    /// The event-carried activity entity value for the lifecycle moment currently being handled.
    /// Handlers read the canonical domain fields from here so their behavior follows the same value
    /// model the event stream carries, rather than depending on projection-only storage details.
    pub activity: Activity,
    /// The owning execution's `state_path` — the path to the enclosing `states` table this activity
    /// resolves against. A `Parallel` handler extends it by `/states/<parallel>/branches/<idx>` to
    /// build each child execution's path. `None` for a top-level execution (states resolve at the
    /// machine's top-level `states`).
    pub execution_state_path: Option<jsonptr::PointerBuf>,
    /// The raw input the owning execution entered with — distinct from `activity.raw_input` (the
    /// input *this state* received), so `$states.context.Execution` can bind the execution's
    /// original input while the state processes its own.
    pub exec_input: Value,
    pub variables: Variables,
    /// Whether the surrounding command is entering (`ActivateState`) or finishing (`CompleteState`)
    /// the activity — the two moments a state's behavior can differ on.
    pub kind: CtxKind,
}

impl ActivityCtx {
    /// The complete JSON Pointer (RFC 6901) to this state's definition within the shared machine
    /// document. Read off the canonical `activity` value (see its `state_path` field for details).
    pub fn state_path(&self) -> &jsonptr::PointerBuf {
        &self.activity.state_path
    }

    /// The leaf name of the state this activity runs — the final (decoded) token of its `state_path`
    /// (its key in the enclosing `states` table). Derives the identity that used to be a stored
    /// `Activity.state_name` field.
    pub fn state_name(&self) -> String {
        crate::handlers::state_name_from_path(self.activity.state_path.as_ptr())
    }
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
/// handling cohesive in its handler, not split across a generic default or the StreamProcessor.
///
/// Handlers are pure decision-makers: they read `ctx` (definition + current state) and emit to
/// `out`; they perform no I/O — side effects are themselves Commands dispatched to dedicated
/// side-effect handlers.
#[async_trait]
pub trait CommandHandler: Send + Sync {
    /// The [`Command`] variant this handler serves, identified by a `Default` placeholder instance
    /// standing in only to read its discriminant — the real command instances are built by the
    /// framework's state handlers. The `StreamProcessor`'s dispatch table reads this off the handler to
    /// derive its key, so the handler is the single source of truth for which variant it handles.
    /// Takes `&self` (rather than being a `Self: Sized` associated function) so the trait stays
    /// object-safe for the `Box<dyn CommandHandler>` dispatch table.
    fn command(&self) -> Command;

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector);
}

/// A request/execution acknowledgement that a handler *declares* but does not execute, to be
/// completed **after** the corresponding [`Event`] has been applied to Storage — the in-process
/// analogue of Zeebe's side effects, which Zeebe also runs after the transaction commits rather
/// than as part of it. Response delivery is a post-commit effect precisely because it must not race
/// the projection: a caller must never see its acknowledgement before the event that produced it is
/// durable and folded into Storage.
///
/// The handler that decides an operation's outcome declares the ack (via
/// [`Collector::ack_request`] / [`Collector::ack_execution`]) instead of the StreamProcessor *guessing*
/// which Event resolves which awaited request from a hand-written match — the handler is the single
/// source of truth for its own completion. The StreamProcessor keeps declared acks as `pending` and, in
/// its event-apply arm, executes the ones that correlate to the just-applied Event, then delivers.
/// Correlation is by event *variant* plus a stable identity (the `request_id` a request ack carries
/// into its echoed Event) — deliberately not full value equality, because Event payloads do not
/// round-trip through the LogStream byte-for-byte.
#[derive(Debug, Clone, PartialEq)]
pub enum AckSideEffect {
    /// Acknowledge a request awaiting a specific echoed Event (e.g. the [`Event::FlowVersionCreated`]
    /// a `CreateFlow` caller awaits, or the [`Event::ExecutionCreated`] a `start` caller awaits),
    /// delivering that Event once it is applied. Boxed like [`AckOutcome::Applied`]: the awaited
    /// `Event` can be large, and this is a one-per-request value — keeping it boxed keeps the
    /// `AckSideEffect` enum's variants close in size (so `Vec<AckSideEffect>` stays dense on the
    /// ready path, and Clippy's `large_enum_variant` stays quiet).
    CompleteRequest {
        request_id: RequestId,
        event: Box<Event>,
    },
    /// Acknowledge a `ClaimTasks` request by delivering the granted task set (`AckOutcome::Granted`)
    /// once any of the producing command's events is applied — no single event to correlate, so the
    /// list (already decided by the handler at discovery) is carried directly, keyed to the command's
    /// `cause_id`.
    CompleteRequestTasks {
        request_id: RequestId,
        cause_id: EntryId,
        tasks: Vec<ActivatedTask>,
    },
}
