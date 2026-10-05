use crate::RejectionType;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::handlers::container::{ActivityContainer, Container, ExecutionContainer};
use crate::types::event::Event;
use crate::types::meta::{HasRawObjectRef, ObjectRef, TimerOwner};
use crate::types::timer::TimerKind;

/// Handles `TriggerTimer`: a timer's deadline elapsed. A fire for a timer that is gone or no longer
/// `Active` is refused, not acted on — the durable record of a fire that lost its race.
///
/// The fire is recorded and then handed to the owner's [`Container`], exactly as a cancel is
/// (`CancelTimerHandler`): the owner's *kind* names the container, and what the settle *means* to that
/// owner is the owner's own affair. So a `Wait`'s `Seconds` resumes the state that armed it, a `Task`'s
/// `TimeoutSeconds` fails it, and a run's own `TimeoutSeconds` terminates the run — three effects
/// obtained by two containers rather than by a third field on the timer. The timer itself carries no
/// purpose: *who* armed it is `meta.owner`, and *why* is that owner's `TimeoutSeconds`/`Seconds`.
#[derive(Default)]
pub struct TriggerTimerHandler;

impl TriggerTimerHandler {
    pub(crate) async fn handle(
        &self,
        timer: &ObjectRef<TimerKind>,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        // A timer row is written by the batch that arms it, and the scheduler only learns to fire it
        // once that batch is durable — so a miss is the log and the projection disagreeing (a forged
        // command, or a corrupt store), not a fire that outlived its timer. Nothing ever removes a row,
        // so an already-fired timer keeps its row and lands in the status guard below instead.
        let act = match ctx.storage.get_timer(timer).await? {
            Some(t) => t,
            None => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("trigger_timer: timer {timer} does not exist; fire refused"),
                ));
            }
        };

        // A duplicate fire, or one racing the `CancelTimer` that swept the timer first: the transition
        // refuses a timer past `Active`, leaving it untouched. The fire is recorded rather than
        // swallowed, so the durable log explains why the deadline went unenforced.
        let mut fired = act.value();
        if let Err(why) = fired.mark_triggered(ctx.now()) {
            tracing::warn!(
                timer = %timer,
                status = ?act.value.status,
                owner = ?act.value.meta.owner,
                "fire arrived for a timer that is no longer Active; refused: {why}"
            );
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("trigger_timer: timer {timer} cannot fire: {why}"),
            ));
        }

        out.append_event(Event::TimerTriggered { timer: fired })
            .await;

        // Both containers below re-read their owner, and the settle's own batch has just rewritten the
        // timer row (`TimerTriggered` folds `Completed` in and detaches the child) — so what each hook
        // decides is decided against the post-fire projection, never against the row read above.
        //
        // A container is resolved *after* the fire is recorded — the fire is the fact, and it stands
        // even for an owner that is gone — but *before* the reaction, so the absent owner is logged
        // rather than left indistinguishable from a reaction that did nothing.
        match act.value.meta.owner.clone() {
            TimerOwner::Activity(owner) => {
                let Some(container) = ActivityContainer::open(ctx.storage, owner).await? else {
                    tracing::warn!(
                        timer = %timer,
                        "fire arrived for a timer whose owning activity is gone; no reaction"
                    );
                    return Ok(());
                };
                container
                    .after_child_completed(ctx, out, timer.as_raw_object_ref())
                    .await;
            }
            TimerOwner::Execution(owner) => {
                let Some(container) = ExecutionContainer::open(ctx.storage, owner).await? else {
                    tracing::warn!(
                        timer = %timer,
                        "fire arrived for a timer whose owning execution is gone; no reaction"
                    );
                    return Ok(());
                };
                container
                    .after_child_completed(ctx, out, timer.as_raw_object_ref())
                    .await;
            }
        }

        Ok(())
    }
}
