use super::{
    ActivateStateHandler, CompleteStateHandler, ContinueCompleteHandler, ContinueTerminateHandler,
    TerminateStateHandler,
};
use crate::elements::state_handler::StateHandlerRegistry;
use crate::elements::states::{
    ChoiceStateHandlerFactory, FailStateHandlerFactory, MapStateHandlerFactory,
    ParallelStateHandlerFactory, PassStateHandlerFactory, SucceedStateHandlerFactory,
    TaskStateHandlerFactory, WaitStateHandlerFactory,
};
use crate::elements::{
    ActivityElementBehavior, Container, ExecutionElementBehavior, FlowElementBehavior,
    TaskElementBehavior, ThreadElementBehavior,
};
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::Command;
use crate::types::deadline::DeadlineTarget;
use crate::types::reject::RejectionType;

/// Registers one or more `StateHandlerFactory`s into the shared [`StateHandlerRegistry`].
///
/// Each factory already knows which variant it serves via its [`StateHandlerFactory::state`], so the
/// registry keys itself off that discriminant on `insert` — the handler type is the single source of
/// truth for its own table key; there is no hand-written placeholder to keep in sync. `$handler` is
/// captured as a `path` because the factories are unit structs: the fragment serves both as a type
/// (`<… as StateHandlerFactory>`) and as a unit-struct value (`&$handler`, `Box::new($handler)`).
///
/// Recursive: `state_handler_entry!` handles the first handler and recurses into the `-rest` form;
/// the tail emits nothing. This lets a single invocation register any number of handlers.
macro_rules! state_handler_entry {
    ($reg:expr, $handler:path $(, $rest:path)*) => {{
        $reg.insert(Box::new($handler));
        state_handler_entry!($reg $(, $rest)*);
    }};
    ($reg:expr) => {};
}

/// The shared `State` → `StateHandlerFactory` registry, built once per `StreamProcessor` and reused by
/// both the `ActivateState` and `CompleteState` command handlers (and the inline child-settled
/// cascade). Routing by [`Discriminant`](std::mem::Discriminant) mirrors the `Command` table and keeps
/// each state's activate/complete behavior in one impl.
pub(crate) fn build_state_handlers() -> StateHandlerRegistry {
    let mut h = StateHandlerRegistry::new();
    state_handler_entry!(
        h,
        PassStateHandlerFactory,
        ChoiceStateHandlerFactory,
        SucceedStateHandlerFactory,
        FailStateHandlerFactory,
        WaitStateHandlerFactory,
        TaskStateHandlerFactory,
        ParallelStateHandlerFactory,
        MapStateHandlerFactory
    );
    h
}

/// A fired deadline is handed to the element that recorded it: the two behaviours re-read their own
/// row and decide what the instant means to them. An element that is gone is refused rather than
/// ignored — the fire outlived the row it belongs to.
async fn fire_deadline(
    target: &DeadlineTarget,
    deadline: crate::Timestamp,
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
) -> Result<(), ProcessingError> {
    match target {
        DeadlineTarget::Execution(execution) => {
            let container = ExecutionElementBehavior::open(ctx.storage, execution.clone()).await?;
            container.on_deadline_fired(ctx, out, deadline).await
        }
        DeadlineTarget::Activity(activity) => {
            // A fired deadline for a vanished activity is refused, not skipped: the fire is a durable
            // command and `dispatch_once` holds every command to an answer.
            let container = ActivityElementBehavior::open(ctx.storage, activity.clone())
                .await
                .map_err(|err| match err {
                    ProcessingError::Rejected(..) => ProcessingError::Rejected(
                        RejectionType::NotFound,
                        format!(
                            "trigger_deadline: activity {activity} does not exist; fire refused"
                        ),
                    ),
                    err => err,
                })?;
            container.on_deadline_fired(ctx, out, deadline).await
        }
    }
}

/// Dispatches a [`Command`] to its typed handler. One exhaustive match over all 19 variants, so the
/// `else unreachable!` narrowing a handler used to do on a generic `&Command` is now a compile-time
/// guarantee: each arm hands the handler exactly its own payload/fields, and a new variant fails to
/// compile here rather than panicking at runtime.
///
/// The handler's [`ProcessingError`] is the residual "no outcome was produced" case — every arm's own
/// decision (an emitted batch, or a refusal recorded in-band) is a success — so it passes through
/// untouched for the leader to classify.
pub(crate) async fn dispatch_command(
    command: &Command,
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
) -> Result<(), ProcessingError> {
    match command {
        Command::CreateFlow(p) => FlowElementBehavior::create(p, ctx, out).await,
        Command::CreateExecution(p) => ExecutionElementBehavior::create(p, ctx, out).await,
        Command::SpawnThread(p) => ThreadElementBehavior::spawn(p, ctx, out).await,
        Command::CompleteExecution(p) => ExecutionElementBehavior::complete(p, ctx, out).await,
        Command::CompleteThread(p) => ThreadElementBehavior::complete(p, ctx, out).await,
        Command::TerminateExecution(p) => ExecutionElementBehavior::terminate(p, ctx, out).await,
        Command::TerminateThread(p) => ThreadElementBehavior::terminate(p, ctx, out).await,
        Command::ActivateState(p) => ActivateStateHandler.handle(p, ctx, out).await,
        Command::CompleteState(p) => CompleteStateHandler.handle(p, ctx, out).await,
        Command::TerminateState(p) => TerminateStateHandler.handle(p, ctx, out).await,
        Command::ActivateTask(p) => TaskElementBehavior::activate(p, ctx, out).await,
        Command::ClaimTasks(p) => TaskElementBehavior::claim(p, ctx, out).await,
        Command::CompleteTask(p) => TaskElementBehavior::complete(p, ctx, out).await,
        Command::FailTask(p) => TaskElementBehavior::fail(p, ctx, out).await,
        Command::TriggerDeadline { target, deadline } => {
            fire_deadline(target, *deadline, ctx, out).await
        }
        Command::CancelTask { task } => TaskElementBehavior::cancel(task, ctx, out).await,
        Command::ContinueComplete { owner } => {
            ContinueCompleteHandler.handle(owner, ctx, out).await
        }
        Command::ContinueTerminate { owner } => {
            ContinueTerminateHandler.handle(owner, ctx, out).await
        }
    }
}
