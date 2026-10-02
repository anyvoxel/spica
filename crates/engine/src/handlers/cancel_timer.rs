use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::event::Event;
use crate::types::meta::{HasRawObjectRef, ObjectRef};
use crate::types::reject::RejectionType;
use crate::types::timer::TimerKind;

/// Handles `CancelTimer`: marks an armed timer cancelled. After recording the timer's terminal state,
/// runs the inline child-settled reaction: a cancel is often the last thing draining a
/// Completing/Terminating owner, so [`child_completed::child_settled`] lets the owner converge in the
/// same batch.
///
/// Both guards refuse, because a command owes one followup entry and neither arm has an `Event` to
/// give — but on different footings. A timer row the store has never seen is an invariant violation:
/// only a sweep that just read the timer off a live owner's `active_children` issues this command, and
/// the row is born in the same batch as that very child edge (see `TimerActivatedApplier`). A timer
/// that is no longer `Active` is instead a *race*: the scheduler's fire is deliberately decoupled from
/// the dispatch loop (`spica-scheduler`), so a `TriggerTimer` can overtake a sweep's `CancelTimer` and
/// leave it arriving at a timer that already fired — hence `InvalidState` rather than the `NotFound`
/// the absent row earns.
///
/// A read that *faults* is neither: it is the engine's own failure, so it propagates as
/// [`ProcessingError::Unexpected`] for the leader to retry before giving up.
#[derive(Default)]
pub struct CancelTimerHandler;

impl CancelTimerHandler {
    pub(crate) async fn handle(
        &self,
        timer: &ObjectRef<TimerKind>,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let Some(act) = ctx.storage.get_timer(timer).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("timer {timer} not found; cancel dropped"),
            ));
        };
        if !act.value.status.is_active() {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "timer {timer} is {:?}, not Active; cancel refused",
                    act.value.status
                ),
            ));
        }
        let mut timer_value = act.value.clone();
        timer_value.cancel(ctx.now());
        out.append_event(Event::TimerCancelled { timer: timer_value })
            .await;

        // TODO：修改为使用 Container 模式
        super::child_completed::child_settled(
            ctx,
            out,
            act.value.meta.owner.clone().into_raw_object_ref(),
            timer.as_raw_object_ref().clone(),
        )
        .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_testing::MockReadonlyStorageTxn;

    use super::CancelTimerHandler;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, ProcessingError};
    use crate::handlers::dispatch::build_state_handlers;
    use crate::handlers::fixtures::{at, object_ref};
    use crate::storage::TimerRecord;
    use crate::types::event::Event;
    use crate::types::execution::ExecutionKind;
    use crate::types::id::EntryId;
    use crate::types::meta::{ObjectMeta, ObjectRef};
    use crate::types::reject::RejectionType;
    use crate::types::timer::TimerKind;
    use crate::{
        ActivityKind, EntryPayload, StorageError, Timer, TimerOwner, TimerPurpose, TimerStatus,
        Timestamp,
    };

    /// The timer's owner slot — an activity, the scope whose deadline it is.
    fn activity_ref() -> ObjectRef<ActivityKind> {
        object_ref("execution-0", 90)
    }

    /// The cancel target.
    fn timer_ref() -> ObjectRef<TimerKind> {
        object_ref("execution-0", 92)
    }

    /// The armed timer a sweep cancels: owned by [`activity_ref`] through the `Activity` variant of
    /// [`TimerOwner`], which is the only scope a `WaitResume` deadline is armed under.
    fn seeded_timer(status: TimerStatus) -> TimerRecord {
        let value = Timer {
            meta: ObjectMeta::builder(timer_ref().uid())
                .name(timer_ref().name().clone())
                .at(at())
                .with_owner(TimerOwner::Activity(activity_ref())),
            execution: object_ref::<ExecutionKind>("execution", 70),
            purpose: TimerPurpose::WaitResume,
            status,
            deadline: Timestamp::from_millis(2_000),
        };
        let mut row = TimerRecord::from_value(value);
        row.born(at());
        row
    }

    /// The handler exercised against a **mock** read-only store: no `InMemoryStorage`, no working
    /// overlay, so these pin the handler's *own* decision and nothing else. The store answers exactly
    /// the reads the handler makes and panics on any other, which is itself an assertion about how far
    /// the handler got — the two guard cases below must never reach the owner read.
    async fn cancel_over(
        store: &MockReadonlyStorageTxn,
    ) -> Result<Vec<EntryPayload>, ProcessingError> {
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(at()));
        let ids: Arc<dyn IdGenerator> = Arc::new(CountingIdGenerator::new());
        let mut out = Collector::new(EntryId::new(1), None, clock.clone(), ids.clone());
        let mut env = EvalEnv::new();
        let mut definitions = HashMap::new();
        let state_handlers = build_state_handlers();
        let mut ctx = HandlerContext {
            env: &mut env,
            storage: store,
            clock,
            ids,
            definitions: &mut definitions,
            state_handlers: &state_handlers,
        };
        CancelTimerHandler
            .handle(&timer_ref(), &mut ctx, &mut out)
            .await?;
        Ok(out.into_entries().into_iter().map(|e| e.payload).collect())
    }

    /// No row: the *command's* failure, refused with the classification the leader records. The
    /// dispatch is only ever issued by a sweep that just read this timer off a live owner's
    /// `active_children`, so a missing row is the projection disagreeing with itself — not a duplicate
    /// cancel to swallow. The owner is never read: a refusal must not depend on the container
    /// resolving.
    #[tokio::test]
    async fn a_missing_row_is_the_commands_own_refusal() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_timer()
            .times(1)
            .return_const(Ok::<_, StorageError>(None));
        let err = cancel_over(&store)
            .await
            .expect_err("a cancel with no timer row must be refused");
        let ProcessingError::Rejected(ty, reason) = err else {
            panic!("a missing row is the command's failure, not the engine's: {err:?}");
        };
        assert_eq!(ty, RejectionType::NotFound);
        assert!(
            reason.contains(&timer_ref().to_string()),
            "the refusal names the timer it could not find: {reason}"
        );
    }

    /// A timer that already fired or was already cancelled is refused rather than swallowed: the
    /// scheduler's fire is decoupled from the dispatch loop, so a `TriggerTimer` overtaking this
    /// sweep's `CancelTimer` lands here. Both terminal ends are covered — the refusal names the state
    /// it found, so the two are told apart by the reason alone. The owner is never read: a refusal
    /// must not depend on the container resolving.
    #[tokio::test]
    async fn an_already_terminal_timer_is_a_wrong_state_refusal() {
        for status in [TimerStatus::Cancelled, TimerStatus::Completed] {
            let mut store = MockReadonlyStorageTxn::new();
            store
                .expect_get_timer()
                .times(1)
                .return_const(Ok(Some(seeded_timer(status))));
            let err = cancel_over(&store)
                .await
                .expect_err("a cancel of a terminal timer must be refused");
            let ProcessingError::Rejected(ty, reason) = err else {
                panic!("a terminal timer is the command's refusal, not the engine's: {err:?}");
            };
            assert_eq!(ty, RejectionType::InvalidState);
            assert!(
                reason.contains(&timer_ref().to_string()),
                "the refusal names the timer it could not cancel: {reason}"
            );
            assert!(
                reason.contains(&format!("{status:?}")),
                "the refusal names the state the timer was found in: {reason}"
            );
        }
    }

    /// A live timer: the handler's own event, and nothing besides — the cancel moment stamped. The
    /// owner read that follows resolves nothing, so the settle ends there; what the container does
    /// with the drained child is `child_settled`'s subject, not this handler's.
    #[tokio::test]
    async fn an_active_timer_emits_its_own_cancel() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_timer()
            .times(1)
            .return_const(Ok(Some(seeded_timer(TimerStatus::Active))));
        store
            .expect_get_activity()
            .return_const(Ok::<_, StorageError>(None));
        let chain = cancel_over(&store)
            .await
            .expect("a live timer cancels cleanly");
        let EntryPayload::Event(Event::TimerCancelled { timer }) = &chain[0] else {
            panic!("a cancelled timer emits TimerCancelled: {chain:?}");
        };
        assert_eq!(timer.status, TimerStatus::Cancelled);
        assert_eq!(timer.meta.name, *timer_ref().name());
        assert_eq!(timer.meta.updated_at, at());
    }
}
