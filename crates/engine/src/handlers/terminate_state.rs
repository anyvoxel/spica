use crate::ActivityStatus as S;
use crate::RejectionType;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::storage::ActivityRecord;
use crate::types::command::TerminateState;
use crate::types::error::ExecutionError;

/// Handles `Command::TerminateState`: the abnormal finish of the activity bound to it. Resolves the
/// owning thread + its machine/state definition just far enough to pick the right handler and screen
/// the rows it read, then delegates to the base
/// [`StateHandler::terminate`](crate::handlers::state_handler::StateHandler::terminate), which owns
/// the terminate orchestration — mirroring how
/// [`CompleteStateHandler`](crate::handlers::CompleteStateHandler) dispatches to
/// [`StateHandler::complete`](crate::handlers::state_handler::StateHandler::complete).
///
/// It is also the one place a failing state's **scope** is taken down: an activity is always owned by
/// a [`Thread`](crate::Thread) (the slot's own type — a top-level run's derived root thread, or a
/// fan-out branch/item thread), so the site that opened the failure never has to name it, and the
/// root-relays-to-the-run dialect lives here rather than at every failing site.
#[derive(Default)]
pub struct TerminateStateHandler;

impl TerminateStateHandler {
    pub(crate) async fn handle(
        &self,
        p: &TerminateState,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let TerminateState { activity, .. } = p;
        // Nothing ever removes a row, and an activity row is written by the batch that activates it —
        // before any sweep could name it — so a miss is the log and the projection disagreeing (a forged
        // command, or a corrupt store) rather than an activity that outlived its teardown. A fault
        // reading the row is neither, and is returned so the leader can retry it.
        let act = match ctx.storage.get_activity(activity).await? {
            Some(a) => a,
            None => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!(
                        "terminate_state: activity {activity} does not exist; termination refused"
                    ),
                ));
            }
        };
        // Status dispatch: the Fail + TerminateExecution cascade can fire two `TerminateState`s at the
        // same activity (its own failure, then an ancestor's sweep — or two branches failing in
        // parallel), but the terminal the drain owns is produced exactly once and a second kick must
        // not reconstruct it. A `Running`/`Completing` target is a live state being redirected to
        // failure — resolve and run its terminate step. Any other status means the terminate was
        // already opened (`Terminating`) or finished (`Terminated`/`Completed`): the duplicate is
        // refused outright, mirroring Complete's strictness instead of re-emitting a `StateTerminated`
        // and/or replaying the settle — the settled children already keep the closer advancing.
        match act.value.status {
            // Running, or Completing, are legitimate pre-failure states: a state can fail either
            // before the complete step opens (Running) or while it is in progress (Completing, since
            // the state's `complete` opens with `StateCompleting` before it projects). Both must be
            // redirected from success to failure — resolve the state and run its terminate step.
            S::Running | S::Completing => self.resolve_and_terminate(ctx, out, p, &act).await,
            S::Terminating(_) | S::Terminated(_) | S::Completed => {
                let phase = match act.value.status {
                    S::Terminating(_) => "terminating",
                    S::Terminated(_) => "terminated",
                    S::Completed => "completed",
                    _ => unreachable!("dispatch matched only the non-live statuses"),
                };
                Err(ProcessingError::Rejected(
                    RejectionType::InvalidState,
                    format!(
                        "terminate_state: activity {} is {phase}; termination refused — the drain \
                         owns its terminal",
                        act.value.meta.object_ref(),
                    ),
                ))
            }
        }
    }

    /// Resolve the owning thread + its machine/state definition to reach the per-state
    /// [`StateHandler::terminate`](crate::handlers::state_handler::StateHandler::terminate), mirroring
    /// `CompleteStateHandler` exactly: each of these rows is a hard precondition of this command, so a
    /// read that comes up short (a forged/corrupt projection) is refused rather than answered with a
    /// generic sweep — a terminate whose state cannot be named would guess at the state's own teardown
    /// logic, which no fallback should. An infra fault reading the machine is still the leader's to
    /// retry.
    async fn resolve_and_terminate(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        p: &TerminateState,
        act: &ActivityRecord,
    ) -> Result<(), ProcessingError> {
        let owner = act.value.meta.owner.clone();
        // An activity's owner slot admits only a `Thread`, so the row is read directly — no `kind`
        // guard. Rows are never removed, so an activity that exists without the thread its owner names
        // is a corrupt projection or a forged command — this command's own precondition, refused
        // exactly as the complete step refuses the same miss.
        let Some(thread) = ctx.storage.get_thread(&owner).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!(
                    "terminate_state: owning thread {owner} of activity {} not found",
                    act.value.meta.object_ref(),
                ),
            ));
        };
        // Resolving the machine the thread binds to is not a reason to invent a terminate the state
        // never sanctioned: like the complete step, an unresolvable machine is the projection's doing,
        // refused rather than swept. An infra fault is the engine's, and the leader's to retry.
        let sm = match ctx.machine_for_thread(&thread).await {
            Ok(sm) => sm,
            Err(ExecutionError::Infra(e)) => return Err(ProcessingError::Unexpected(e.into())),
            Err(e) => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!(
                        "terminate_state: activity {} cannot resolve its machine: {e}",
                        act.value.meta.object_ref(),
                    ),
                ));
            }
        };
        // The state to terminate is the one this activity names: its own `state_path` locates the
        // definition inside the machine the owning thread binds to. Activation resolved the same path
        // against the same machine, so a miss is the projection's, not the flow's.
        let state_def = match sm.state_at(&act.value.state_path) {
            Ok(def) => def,
            Err(e) => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!(
                        "terminate_state: activity {} names state {:?} which its machine does not define: {e}",
                        act.value.meta.object_ref(),
                        act.value.state_path,
                    ),
                ));
            }
        };
        // Every `State` variant has a registered factory (see `build_state_handlers`), so a miss here
        // is an engine regression — fail loud rather than mislabel it as an invalid flow definition.
        let handler = ctx
            .state_handlers
            .create(state_def)
            .expect("state type has no registered handler: engine regression, not a flow error");
        handler.terminate(ctx, out, p, act, &thread).await
    }
}
