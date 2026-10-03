use crate::RejectionType;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::ActivateState;
use crate::types::error::ExecutionError;

/// Dispatches `Command::ActivateState` to the matching
/// [`StateHandlerRegistry::create`](crate::handlers::state_handler::StateHandlerRegistry::create)
/// bound handler. It is deliberately thin: it reads the owning thread once — resolving the state
/// definition from that same row and screening its liveness — then hands the command, the resolved
/// definition and the row it read to the base
/// [`StateHandler::activate`](crate::handlers::state_handler::StateHandler::activate),
/// which owns the whole orchestration (constructing the activity, emitting
/// `StateActivating`/`StateActivated`, and running the state's activate hooks). Every precondition
/// lives here because this is the one place the scope is read.
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

        // Resolve the owning thread + its machine/state definition just far enough to pick the right
        // handler, and screen the scope here — the base `StateHandler::activate` receives this row
        // rather than re-reading it. No activity is minted here: nothing is persisted for a refused
        // command to attach a state-level terminate to, and the command itself may be perfectly valid,
        // so ending the run would kill a run that did nothing wrong. The slot admits only a `Thread`,
        // so the row is read directly. A fault reading it is not a decision about this state — it is
        // returned so the leader can retry the command.
        let thread = match ctx.storage.get_thread(owner).await? {
            Some(t) => t,
            None => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!(
                        "activate_state: owning thread {owner} of execution {execution} does not exist"
                    ),
                ));
            }
        };
        // A thread past Running has already had its outcome opened (completing, or swept by an
        // ancestor's termination), so a fresh activation on it names the wrong incarnation rather than
        // duplicating anything — refused like the sibling handlers' non-Running guards. Checked before
        // the definition lookup below: the scope's own status is the cheaper and more fundamental gate.
        if !thread.value.status.is_running() {
            let phase = thread.value.status.phase();
            tracing::warn!(thread = %owner, status = ?thread.value.status,
                "activation arrived for a thread that is not Running; refused");
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("activate_state: thread {owner} is already {phase}; activation refused"),
            ));
        }
        // As in `CompleteStateHandler`: an engine fault is the leader's to retry, anything else is the
        // projection's and refused like the miss above.
        let sm = match ctx.machine_for_thread(&thread).await {
            Ok(sm) => sm,
            Err(ExecutionError::Infra(e)) => return Err(ProcessingError::Unexpected(e.into())),
            Err(e) => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("activate_state: thread {owner} cannot resolve its machine: {e}"),
                ));
            }
        };
        // `Command::ActivateState` carries the state's full path, so it is self-locating: the
        // lookup is the document's own walk, and the enclosing `States` table is never inferred
        // from the owning scope's stored path. Nothing validates a successor path before it is
        // dispatched — `emit_transition` and the `Choice` router both extend it with an unchecked
        // `sibling(next)` — so a miss here is how a flow whose `Next`/`StartAt` names no reachable
        // state surfaces, refused on the same footing as the two misses above.
        let state_def = match sm.state_at(state_path) {
            Ok(def) => def,
            Err(e) => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!(
                        "activate_state: state {state_path:?} is not defined by the machine that thread {owner} binds to: {e}"
                    ),
                ));
            }
        };

        // Every `State` variant has a registered factory (see `build_state_handlers` + the
        // `registry.len() == 8` coverage test), so a miss here is an engine regression — fail loud
        // rather than mislabel it as an invalid flow definition.
        let handler = ctx
            .state_handlers
            .create(state_def)
            .expect("state type has no registered handler: engine regression, not a flow error");
        // The base receives the already-read, already-screened row (the payload carries its own owner,
        // so nothing is threaded beside it), so it has no gate to re-run and no store fault of its own
        // to surface; a refusal from it is still the leader's to route, not this dispatcher's to
        // reinterpret.
        handler.activate(ctx, out, payload, &thread).await?;

        Ok(())
    }
}
