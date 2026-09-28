use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::CompleteState;
use crate::types::error::{ExecutionError, RuntimeError};

/// Handles `Command::CompleteState`: the success finish of the running activity bound to it.
/// Dispatches to the matching
/// [`StateHandler::complete`](crate::handlers::state_handler::StateHandler::complete),
/// which emits the projection
/// (`StateCompleting` + `StateCompleted`) and the transition. If the activity owns children (M2
/// states only), the ed is deferred until they finish — only M1 synchronous/timed states reach here
/// childless (Wait fires its own timer before `CompleteState`).
pub struct CompleteStateHandler;

impl Default for CompleteStateHandler {
    fn default() -> Self {
        Self
    }
}

impl CompleteStateHandler {
    pub(crate) async fn handle(
        &self,
        p: &CompleteState,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let CompleteState { activity, .. } = p;
        // Resolve the owning scope + its machine/state definition just far enough to pick the right
        // handler — the base `StateHandler::complete` owns the whole orchestration (re-loading the
        // activity, folding the command's raw result, running the status/children/scope guards,
        // reconstructing the activity and variables, and delegating to the per-state finish). Mirror
        // the `ActivateStateHandler` dispatch: this dispatcher builds no context itself and forwards
        // the payload verbatim.
        let act = match ctx.storage.get_activity(activity).await? {
            Some(a) => a,
            None => {
                out.terminate(
                    Some(activity.clone()),
                    crate::types::meta::ObjectReference::nil(),
                    ExecutionError::Runtime(RuntimeError::StateNotFound(format!(
                        "activity {activity}"
                    ))),
                );
                return Ok(());
            }
        };
        // An activity's owner is always a `Thread` (see `emit_transition`), so the row is read
        // directly.
        let scope_ref = act
            .value
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        let Some(thread) = ctx.storage.get_thread(&scope_ref).await? else {
            return Ok(()); // owning scope gone — nothing to complete into.
        };
        let sm = fail_or!(
            result,
            out,
            Some(activity.clone()),
            scope_ref.clone(),
            ctx.machine_for_thread(&thread).await
        );
        // The state to complete is the one this activity names: its own `state_path` locates the
        // definition inside the machine the owning thread binds to (a branch/item activity carries
        // the deeper path, a top-level one `/States/<name>`).
        let state_def = fail_or!(
            result,
            out,
            Some(activity.clone()),
            scope_ref.clone(),
            sm.state_at(&act.value.state_path)
                .map_err(ExecutionError::from)
        );

        // Every `State` variant has a registered factory (see `build_state_handlers` + the
        // `registry.len() == 8` coverage test), so a miss here is an engine regression — fail loud
        // rather than mislabel it as an invalid flow definition.
        let handler = ctx
            .state_handlers
            .create(state_def)
            .expect("state type has no registered handler: engine regression, not a flow error");
        handler.complete(ctx, out, p).await;

        Ok(())
    }
}
