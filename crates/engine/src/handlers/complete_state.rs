use crate::RejectionType;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::CompleteState;
use crate::types::error::ExecutionError;

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
        // Resolve the owning thread + its machine/state definition just far enough to pick the right
        // handler, and screen the rows it read — the base `StateHandler::complete` owns the
        // orchestration (folding the command's raw result, running the children/scope gates,
        // reconstructing the activity and variables, and delegating to the per-state finish) but
        // re-reads nothing. Mirror the `ActivateStateHandler` dispatch: this dispatcher builds no
        // context itself and forwards the payload plus the two rows it read.
        let act = match ctx.storage.get_activity(activity).await? {
            Some(a) => a,
            // The activity a `CompleteState` names is born in the same batch as its activation and
            // nothing ever removes a row, so a miss means the command was forged into the log or the
            // projection is corrupt — the command's own precondition, refused rather than answered.
            // Failing it at the activity level would terminate a row that does not exist:
            // `TerminateState` no-ops on a missing row, and with no resolvable scope the run is not
            // reached either, so that path applied nothing while still logging a terminal failure.
            None => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("complete_state: activity {activity} not found"),
                ));
            }
        };
        // An activity's owner slot admits only a `Thread`, so the row is read directly — no `kind`
        // guard.
        let owner = act.value.meta.owner.clone();
        let Some(thread) = ctx.storage.get_thread(&owner).await? else {
            // The same judgement as the miss above, one hop out: rows are never removed, so an
            // activity that exists without the thread its owner slot names is a corrupt projection or
            // a forged command — this command's own precondition, refused rather than answered. The
            // miss is also the only reason no handler can be picked here, so answering it with nothing
            // would leave the command with neither of the two entries it owes.
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("complete_state: owning thread {owner} of activity {activity} not found"),
            ));
        };
        // Resolving the context this command needs is not a reason to end the run: the command itself
        // may be perfectly valid, so a failure here is either the engine's — returned, and the leader's
        // to retry — or the projection's, refused like the two misses above. Terminating would kill a
        // run that did nothing wrong.
        let sm = match ctx.machine_for_thread(&thread).await {
            Ok(sm) => sm,
            Err(ExecutionError::Infra(e)) => return Err(ProcessingError::Unexpected(e.into())),
            Err(e) => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("complete_state: activity {activity} cannot resolve its machine: {e}"),
                ));
            }
        };
        // The state to complete is the one this activity names: its own `state_path` locates the
        // definition inside the machine the owning thread binds to (a branch/item activity carries
        // the deeper path, a top-level one `/States/<name>`). Activation resolved the same path
        // against the same machine, so a miss is the projection's, not the flow's.
        let state_def = match sm.state_at(&act.value.state_path) {
            Ok(def) => def,
            Err(e) => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!(
                        "complete_state: activity {activity} names state {:?} which its machine does not define: {e}",
                        act.value.state_path
                    ),
                ));
            }
        };

        // A cancel (or a competing terminator) already won on this activity: the command lost the
        // race, so the success finish no longer applies — a target past the state this command expects
        // is its own precondition, refused rather than answered, exactly as the sibling handlers'
        // wrong-state guards are. Recovering the activity's *own* terminal is deliberately not this
        // step's job: the child whose settle stranded it drives that drain (see
        // `crate::handlers::trigger_timer`), so emitting `StateTerminated` from here would duplicate a
        // terminal event the settle path already owns.
        if !act.value.status.is_running() {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "activity {activity} is {}, not Running; cannot complete",
                    act.value.status.phase()
                ),
            ));
        }
        // As in `ActivateStateHandler`: a thread past Running names the wrong incarnation of the scope
        // (completing, or swept by an ancestor's termination), so the finish it would route into has no
        // owner left to accept it. Refused on the activity's footing above rather than swallowed — a
        // silent skip would leave this command with neither of the two entries it owes.
        if !thread.value.status.is_running() {
            let phase = thread.value.status.phase();
            tracing::warn!(thread = %owner, status = ?thread.value.status,
                "completion arrived for a thread that is not Running; refused");
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("complete_state: thread {owner} is already {phase}; completion refused"),
            ));
        }

        // Every `State` variant has a registered factory (see `build_state_handlers` + the
        // `registry.len() == 8` coverage test), so a miss here is an engine regression — fail loud
        // rather than mislabel it as an invalid flow definition.
        let handler = ctx
            .state_handlers
            .create(state_def)
            .expect("state type has no registered handler: engine regression, not a flow error");
        handler.complete(ctx, out, p, &act, &thread).await?;

        Ok(())
    }
}
