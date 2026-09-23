use super::state_handler::StateHandlerRegistry;
use super::states::{
    ChoiceStateHandlerFactory, FailStateHandlerFactory, MapStateHandlerFactory,
    ParallelStateHandlerFactory, PassStateHandlerFactory, SucceedStateHandlerFactory,
    TaskStateHandlerFactory, WaitStateHandlerFactory,
};
use super::{
    ActivateStateHandler, ActivateTaskHandler, CancelTaskHandler, CancelTimerHandler,
    ClaimTasksHandler, CompleteExecutionHandler, CompleteStateHandler, CompleteTaskHandler,
    CompleteThreadHandler, ContinueCompleteHandler, ContinueTerminateHandler,
    CreateExecutionHandler, CreateFlowHandler, FailTaskHandler, ReleaseTaskLeaseHandler,
    SpawnThreadHandler, TerminateExecutionHandler, TerminateStateHandler, TerminateThreadHandler,
    TriggerTimerHandler,
};
use crate::handler::{Collector, HandlerContext};
use crate::types::command::Command;

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

/// Dispatches a [`Command`] to its typed handler. One exhaustive match over all 20 variants, so the
/// `else unreachable!` narrowing a handler used to do on a generic `&Command` is now a compile-time
/// guarantee: each arm hands the handler exactly its own payload/fields, and a new variant fails to
/// compile here rather than panicking at runtime.
pub(crate) async fn dispatch_command(
    command: &Command,
    ctx: &mut HandlerContext<'_>,
    out: &mut Collector<'_>,
) {
    match command {
        Command::CreateFlow(p) => CreateFlowHandler.handle(p, ctx, out).await,
        Command::CreateExecution(p) => CreateExecutionHandler.handle(p, ctx, out).await,
        Command::SpawnThread(p) => SpawnThreadHandler.handle(p, ctx, out).await,
        Command::CompleteExecution(p) => CompleteExecutionHandler.handle(p, ctx, out).await,
        Command::CompleteThread(p) => CompleteThreadHandler.handle(p, ctx, out).await,
        Command::TerminateExecution(p) => TerminateExecutionHandler.handle(p, ctx, out).await,
        Command::TerminateThread(p) => TerminateThreadHandler.handle(p, ctx, out).await,
        Command::ActivateState(p) => ActivateStateHandler.handle(p, ctx, out).await,
        Command::CompleteState(p) => CompleteStateHandler.handle(p, ctx, out).await,
        Command::TerminateState(p) => TerminateStateHandler.handle(p, ctx, out).await,
        Command::ActivateTask(p) => ActivateTaskHandler.handle(p, ctx, out).await,
        Command::ClaimTasks(p) => ClaimTasksHandler.handle(p, ctx, out).await,
        Command::CompleteTask(p) => CompleteTaskHandler.handle(p, ctx, out).await,
        Command::FailTask(p) => FailTaskHandler.handle(p, ctx, out).await,
        Command::TriggerTimer { timer } => TriggerTimerHandler.handle(timer, ctx, out).await,
        Command::CancelTimer { timer } => CancelTimerHandler.handle(timer, ctx, out).await,
        Command::ReleaseTaskLease { task } => ReleaseTaskLeaseHandler.handle(task, ctx, out).await,
        Command::CancelTask { task } => CancelTaskHandler.handle(task, ctx, out).await,
        Command::ContinueComplete { owner } => {
            ContinueCompleteHandler.handle(owner, ctx, out).await
        }
        Command::ContinueTerminate { owner } => {
            ContinueTerminateHandler.handle(owner, ctx, out).await
        }
    }
}
