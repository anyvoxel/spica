use super::resolve_state_from_path;
use crate::handler::{Collector, HandlerContext};
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
    ) {
        let payload = p;
        let ActivateState {
            execution,
            owner,
            state_path,
            ..
        } = payload;

        // TODO：这个名称叫做 Scope 肯定是不合适的，需要修改一下；而且如果 Scope 是不存在的话，是不是不应该是 terminate，而应该是 reject？对 Err 的处理也不对，应该是一个其他的处理方式（因为有些临时的错误应该是可以重试的，而不是直接 terminate 掉）
        // Resolve the owning scope + its machine/state definition just far enough to pick the right
        // handler. No activity is minted here — the base `StateHandler::activate` constructs it (and
        // re-checks the scope's liveness), so a resolution failure (scope/definition gone) fails the
        // execution directly: nothing has been persisted to attach a state-level terminate to.
        let scope = match crate::storage::load_scope_ref(ctx.storage, owner).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                out.terminate(
                    None,
                    execution.clone(),
                    ExecutionError::Runtime(RuntimeError::StateNotFound(format!(
                        "execution {execution}"
                    ))),
                );
                return;
            }
            Err(e) => {
                out.terminate(None, execution.clone(), e);
                return;
            }
        };
        let sm = fail_or!(
            out,
            None,
            execution.clone(),
            ctx.machine_for_scope(&scope).await
        );
        let state_def = fail_or!(
            out,
            None,
            execution.clone(),
            resolve_state_from_path(&sm, state_path)
        );

        // Every `State` variant has a registered factory (see `build_state_handlers` + the
        // `registry.len() == 8` coverage test), so a miss here is an engine regression — fail loud
        // rather than mislabel it as an invalid flow definition.
        let handler = ctx
            .state_handlers
            .create(state_def)
            .expect("state type has no registered handler: engine regression, not a flow error");
        handler.activate(ctx, out, payload).await;
    }
}
