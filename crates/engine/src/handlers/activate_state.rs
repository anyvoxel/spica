use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::ActivateState;
use crate::types::error::{ExecutionError, RuntimeError};

/// Dispatches `Command::ActivateState` to the matching
/// [`StateHandlerRegistry::create`](crate::handlers::state_handler::StateHandlerRegistry::create)
/// bound handler. It is deliberately thin: it resolves the owning scope + state definition only far
/// enough to create a handler bound to it, then hands the command + resolved definition to the base
/// [`StateHandler::activate`](crate::handlers::state_handler::StateHandler::activate),
/// which owns the whole orchestration (constructing the activity, emitting
/// `StateActivating`/`StateActivated`, and running the state's activate hooks). The base re-loads
/// the scope to build the activity context (working-overlay reads are cheap); this dispatcher never
/// builds it.
pub struct ActivateStateHandler;

impl Default for ActivateStateHandler {
    fn default() -> Self {
        Self
    }
}

impl ActivateStateHandler {
    pub(crate) async fn handle(
        &self,
        p: &ActivateState,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let payload = p;
        let ActivateState {
            execution,
            owner,
            state_path,
            ..
        } = payload;

        // TODO：owner 解析不到时把整个 execution 终结掉，对于一条本身没有问题的命令来说太重了；
        // 这里是否应该改为 reject，等 「状态无法绑定 owner」 的语义定下来后再定。
        // Resolve the owning thread + its machine/state definition just far enough to pick the right
        // handler. No activity is minted here — the base `StateHandler::activate` constructs it (and
        // re-checks the scope's liveness), so a resolution failure (thread/definition gone) fails the
        // execution directly: nothing has been persisted to attach a state-level terminate to. An
        // activity's owner is always a `Thread` (see `emit_transition`), so the row is read directly.
        // A fault reading the owning thread is not a decision about this state — it is returned so the
        // leader can retry the command, or refuse it once the retry budget is spent.
        let thread = match ctx.storage.get_thread(owner).await? {
            Some(t) => t,
            None => {
                out.terminate(
                    None,
                    execution.clone(),
                    ExecutionError::Runtime(RuntimeError::StateNotFound(format!("thread {owner}"))),
                );
                return Ok(());
            }
        };
        let sm = fail_or!(
            result,
            out,
            None,
            execution.clone(),
            ctx.machine_for_thread(&thread).await
        );
        // `Command::ActivateState` carries the state's full path, so it is self-locating: the
        // lookup is the document's own walk, and the enclosing `States` table is never inferred
        // from the owning scope's stored path.
        let state_def = fail_or!(
            result,
            out,
            None,
            execution.clone(),
            sm.state_at(state_path).map_err(ExecutionError::from)
        );

        // Every `State` variant has a registered factory (see `build_state_handlers` + the
        // `registry.len() == 8` coverage test), so a miss here is an engine regression — fail loud
        // rather than mislabel it as an invalid flow definition.
        let handler = ctx
            .state_handlers
            .create(state_def)
            .expect("state type has no registered handler: engine regression, not a flow error");
        handler.activate(ctx, out, payload).await;

        Ok(())
    }
}
