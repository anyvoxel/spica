use std::collections::HashMap;
use std::mem::discriminant;
use std::sync::Arc;

use spica_asl::StateMachine;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::StreamExt;
use tracing::{debug, info, instrument, warn};

use crate::applier::{ApplierContext, EventDispatcher};
use crate::command::Command;
use crate::error::ExecutionError;
use crate::eval_env::EvalEnv;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::handlers::{
    ActivateStateHandler, ActivateTimerHandler, CancelTimerHandler, ChildFinalizedHandler,
    CompleteExecutionHandler, CompleteStateHandler, CompleteTimerHandler, CreateExecutionHandler,
    TerminateExecutionHandler, TerminateStateHandler,
};
use crate::id::{EntryId, ExecutionId, StreamId};
use crate::log::{Entry, EntryPayload, LogStream, Timestamp};
use crate::scheduler::SchedulerHandle;
use crate::storage::Storage;

/// Registers one or more [`CommandHandler`]s into a `Discriminant<Command>` dispatch map.
///
/// Each handler knows which [`Command`] variant it serves via [`CommandHandler::command`], which
/// returns that variant as a `Default` placeholder (used only to read its discriminant — real
/// commands are built by the state handlers). The handler type is therefore the single source of
/// truth for its own key; there is no hand-written placeholder to keep in sync. `$handler` is
/// captured as a `path` so it can serve both as a type (`<… as CommandHandler>`) and as a
/// `Default`-constructible value (`<$handler>::default()`).
///
/// Recursive: `command_handler_entry!` handles the first handler and recurses into the
/// `command_handler_entry!`-rest form; the tail emits nothing. The map key type is fixed by the
/// map's declared type, so the macro doesn't need to name `Command`.
macro_rules! command_handler_entry {
    // A single entry: derive the key from the handler's own `command()`, then recurse on the rest.
    ($map:expr, $handler:path $(, $rest:path)*) => {{
        let sample = <$handler as CommandHandler>::command(&<$handler>::default());
        $map.insert(std::mem::discriminant(&sample), Box::new(<$handler>::default()));
        command_handler_entry!($map $(, $rest)*);
    }};
    ($map:expr) => {};
}

/// Drives a single execution: reads [`Entry`]s from a [`LogStream`], dispatches [`Command`]s to
/// the appropriate handler (producing more entries, appended atomically with causal links), and
/// applies [`Event`]s to [`Storage`] via the [`EventDispatcher`] — which also feeds timer
/// scheduling to an owned [`SchedulerHandle`].
///
/// The Processor holds a dispatch table mapping each [`Command`] variant (by
/// [`Discriminant`](std::mem::Discriminant)) to its [`CommandHandler`], so `dispatch` is a
/// table lookup — no `match`.
pub struct Processor {
    sm: StateMachine,
    execution: ExecutionId,
    env: EvalEnv,
    handlers: HashMap<std::mem::Discriminant<Command>, Box<dyn CommandHandler + Send + Sync>>,
}

impl Processor {
    pub fn new(sm: StateMachine, execution: ExecutionId) -> Self {
        let mut handlers: HashMap<
            std::mem::Discriminant<Command>,
            Box<dyn CommandHandler + Send + Sync>,
        > = HashMap::new();
        command_handler_entry!(
            handlers,
            CreateExecutionHandler,
            CompleteExecutionHandler,
            TerminateExecutionHandler,
            ActivateStateHandler,
            CompleteStateHandler,
            TerminateStateHandler,
            ActivateTimerHandler,
            CompleteTimerHandler,
            CancelTimerHandler,
            ChildFinalizedHandler
        );
        Processor {
            sm,
            execution,
            env: EvalEnv::new(),
            handlers,
        }
    }

    /// Runs the Processor as a **service**: tails the stream indefinitely, dispatching Commands and
    /// applying Events. Does NOT exit when a single execution completes — multiple executions share
    /// the stream and are all processed. Returns `Ok(())` only when the stream is closed (log shut
    /// down). For M1's synchronous `Engine::start`, a caller-tail loop (see `run_until_terminal`) is
    /// what actually drives the Processor.
    ///
    /// Real-time scheduling: armed timers are *facts* (`TimerActivated`), and the actual
    /// wall-clock delay is a side effect driven by applying that event — the run loop feeds each
    /// event through an [`EventDispatcher`], and the `TimerActivated` applier hands the arm to an
    /// owned [`SchedulerHandle`]. The scheduler owns a single [`DelayQueue`] loop and, on expiry,
    /// routes a `CompleteTimer` *back into this loop* via a channel; this loop is the single writer
    /// to the logstream (the log assigns each entry's position, so no caller-run id counter is
    /// needed) and is equally reachable by an external `TerminateExecution` while a timer is
    /// pending — the earlier deadlock where an inline `sleep` blocked the whole stream can't recur.
    #[instrument(skip_all, fields(execution = %self.execution))]
    pub async fn run<L: LogStream, S: Storage>(
        &mut self,
        logstream: &L,
        storage: &mut S,
    ) -> Result<(), ExecutionError> {
        let env = Arc::new(Mutex::new(std::mem::take(&mut self.env)));
        // Channel over which the scheduler returns a fired timer's CompleteTimer to this loop.
        let (fire_tx, mut fire_rx) = mpsc::unbounded_channel::<(StreamId, EntryId, Command)>();
        let (_scheduler, _handle) = SchedulerHandle::spawn(fire_tx);
        let dispatcher = EventDispatcher::new();

        let mut stream = logstream.stream_read(EntryId::new(1));
        loop {
            tokio::select! {
                entry = stream.next() => {
                    let Some(entry) = entry else {
                        return Ok(());
                    };
                    match entry {
                        Entry {
                            entry_id,
                            stream_id,
                            payload: EntryPayload::Command(command),
                            ..
                        } => {
                            debug!(entry_id = %entry_id, command = ?command, "dispatching command");
                            let entries = {
                                let mut env_g = env.lock().await;
                                let mut out = Collector::new(entry_id, stream_id, Timestamp::now());
                                let mut ctx = HandlerContext { sm: &self.sm, env: &mut env_g, storage: &*storage };
                                let handler = self.handlers
                                    .get(&discriminant(&command))
                                    .expect("a handler is registered for every Command variant");
                                handler.handle(&command, &mut ctx, &mut out).await;
                                out.into_entries()
                            };
                            debug!(entries = entries.len(), "command produced entries");
                            logstream.append(entries).await?;
                        }
                        Entry {
                            entry_id,
                            stream_id,
                            payload: EntryPayload::Event(event),
                            ..
                        } => {
                            log_event(&event);
                            // Apply the event's projection and feed any timer side effects to the
                            // scheduler. `cause_id` for a scheduled timer is this event's own
                            // entry_id, so the later CompleteTimer causally hangs off this event.
                            let mut ctx = ApplierContext {
                                storage: &mut *storage,
                                scheduler: &_scheduler,
                                stream_id,
                                cause_id: entry_id,
                            };
                            dispatcher.apply(&mut ctx, &event).await?;
                        }
                    }
                }
                Some((stream_id, cause_id, command)) = fire_rx.recv() => {
                    // A timer's deadline elapsed: the scheduler routed CompleteTimer here. Envelope
                    // + append (the log stamps the position); the next `stream.next()` picks it up
                    // and dispatches it normally.
                    let entry = Entry {
                        stream_id,
                        entry_id: EntryId::nil(), // placeholder — the log assigns the real position.
                        cause_id: Some(cause_id),
                        timestamp: Timestamp::now(),
                        payload: EntryPayload::Command(command),
                    };
                    logstream.append(vec![entry]).await?;
                }
            }
        }
    }

    /// Routes a [`Command`] to its handler, building the [`HandlerContext`] from Storage and a
    /// [`Collector`] for output. Returns the [`Entry`]s the handler emitted (already enveloped with
    /// `cause_id`/`stream_id`/`timestamp` and a placeholder `entry_id`), ready to append atomically
    /// (the append stamps the real positions).
    ///
    /// Single-shot, synchronous dispatch usable outside `run` (tests, [`Engine::submit`]'s
    /// consumers). Timer *scheduling* is not part of dispatch: applying a `TimerActivated` event is
    /// what arms a timer, and that only happens in `run` where the [`SchedulerHandle`] lives — so a
    /// single-shot caller driven purely by `dispatch` cannot fire timers (it must drive `run`).
    pub async fn dispatch<S: Storage>(
        &mut self,
        command: &Command,
        storage: &S,
        cause_id: EntryId,
        stream_id: StreamId,
    ) -> Result<Vec<Entry>, ExecutionError> {
        let mut out = Collector::new(cause_id, stream_id, Timestamp::now());
        let mut ctx = HandlerContext {
            sm: &self.sm,
            env: &mut self.env,
            storage,
        };
        let handler = self
            .handlers
            .get(&discriminant(command))
            .expect("a handler is registered for every Command variant");
        handler.handle(command, &mut ctx, &mut out).await;
        Ok(out.into_entries())
    }
}

/// Emits a human-friendly tracing line for each [`Event`] as it is applied to Storage — the
/// detailed execution trace. Milestones (execution started / terminal) are `info`; per-phase
/// (activating/activated/completing/completed) and timers are `debug`; failures are `warn`.
pub(crate) fn log_event(event: &Event) {
    match event {
        Event::ExecutionCreated { id, input } => {
            info!(execution = %id, input = %input, "execution created");
        }
        Event::ExecutionCompleting { id, output } => {
            debug!(execution = %id, output = %output, "execution completing");
        }
        Event::ExecutionCompleted { id, output } => {
            info!(execution = %id, output = %output, "execution completed");
        }
        Event::ExecutionTerminating { id, reason } => {
            warn!(execution = %id, reason = %format!("{reason:?}"), "execution terminating");
        }
        Event::ExecutionTerminated { id, reason } => {
            warn!(execution = %id, reason = %format!("{reason:?}"), "execution terminated");
        }
        Event::StateActivating {
            activity,
            state,
            input,
            ..
        } => {
            info!(activity = %activity, state, input = %input, "entered state");
        }
        Event::StateActivated { activity } => {
            debug!(activity = %activity, "state activated");
        }
        Event::StateCompleting { activity } => {
            debug!(activity = %activity, "state completing");
        }
        Event::StateCompleted { activity, output } => {
            debug!(activity = %activity, output = %output, "state completed");
        }
        Event::StateTerminating { activity } => {
            warn!(activity = %activity, "state terminating");
        }
        Event::StateTerminated { activity, reason } => {
            warn!(activity = %activity, reason = %format!("{reason:?}"), "state terminated");
        }
        Event::TimerActivated {
            parent,
            timer,
            purpose,
            deadline,
        } => {
            debug!(timer = %timer, parent = ?parent, purpose = %format!("{purpose:?}"),
                   deadline_ms = deadline.as_millis(), "timer activated");
        }
        Event::TimerCompleted { timer } => {
            debug!(timer = %timer, "timer completed");
        }
        Event::TimerCancelled { timer } => {
            debug!(timer = %timer, "timer cancelled");
        }
        Event::VariablesAssigned { assignments, .. } => {
            let keys: Vec<&String> = assignments.keys().collect();
            debug!(keys = ?keys, "variables assigned");
        }
        Event::StateTransitioned { activity, next, .. } => {
            info!(activity = %activity, next = %next, "state routed to next");
        }
    }
}
