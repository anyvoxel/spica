use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::types::command::TimerPurpose;
use crate::types::execution::ExecutionKind;
use crate::types::meta::{ObjectKind, ObjectKindMarker, ObjectMeta, ObjectRef, TimerOwner};
use spica_machinery::Timestamp;

/// Lifecycle status of a Timer. Kept separate from `ExecutionStatus` / `ActivityStatus` because a
/// timer has a strictly simpler shape — it never initiates its own completion; it is armed by a
/// state or the execution and either fires (`Completed`) or is cancelled (`Cancelled`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimerStatus {
    Active,
    Completed,
    Cancelled,
}

impl TimerStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, TimerStatus::Active)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, TimerStatus::Completed | TimerStatus::Cancelled)
    }

    /// The status as one word per variant — what a durable rejection reason may interpolate, so every
    /// handler's refusal names the same phase the trace line carries rather than the status's `Debug`.
    pub fn phase(&self) -> &'static str {
        match self {
            TimerStatus::Active => "Active",
            TimerStatus::Completed => "Completed",
            TimerStatus::Cancelled => "Cancelled",
        }
    }
}

/// The [`ObjectKindMarker`] tying a [`Timer`]'s meta to [`ObjectKind::Timer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimerKind;

impl ObjectKindMarker for TimerKind {
    const KIND: ObjectKind = ObjectKind::Timer;
    /// A timer is armed by the scope whose deadline it is: the top-level `Execution` for an
    /// execution timeout, the waiting `Activity` for a `WaitResume` or a task retry/timeout. See
    /// [`TimerOwner`].
    type OwnedBy = TimerOwner;
}

/// The event-/domain-carried value of a Timer.
///
/// A timer is a leaf side-effect node armed by an `Execution` (`ExecutionTimeout`) or an
/// `Activity` (`WaitResume` / task retry / task timeout). The value carries only the timer's own
/// domain facts — storage may wrap it to keep the domain/projection boundary explicit, just as it
/// does for `Activity`.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Timer {
    /// Shared identity + timing metadata. `meta.uid` is the timer's stable identity; the domain
    /// `created_at`/`updated_at` (stamped at each lifecycle-transition emit) live inside `meta`.
    pub meta: ObjectMeta<TimerKind>,
    /// The execution this timer belongs to — the flat top-level anchor, **always** a run (a
    /// `WaitResume` armed inside a `Parallel` branch still names the tree's root run, so the timer's
    /// `{execution.name}-{suffix}` generated name points at it; the *immediate* scope that armed the
    /// timer is `meta.owner`, see [`TimerOwner`]).
    pub execution: ObjectRef<ExecutionKind>,
    pub purpose: TimerPurpose,
    pub status: TimerStatus,
    /// Absolute wall-clock moment the timer fires. Persisting the absolute deadline (not a relative
    /// duration) keeps the timer row self-contained: a replay can derive "how long is left" from
    /// `deadline - now` without re-arming based on a stale relative count.
    pub deadline: Timestamp,
}

impl Timer {
    /// Move this timer to `Cancelled` at `at`: the row copies forward with only the terminal status
    /// and the transition stamp moved. Only an `Active` timer has a cancellation to take, so a timer
    /// that already fired or was cancelled is **refused** — and left untouched; the invariant lives
    /// here rather than at each caller, so no call site can forget to ask first.
    ///
    /// The `Err` is one sentence naming *why* the transition was declined (the object's own state),
    /// for the caller to fold into its durable rejection reason — the caller alone knows which object
    /// and which command it is refusing.
    ///
    /// The whole `meta` must travel unchanged. A timer may be custom-named
    /// (`{execution.name}-{suffix}`), and re-deriving that name would address a node its owner never
    /// added as a child — the child edge would then never detach, so the cancelling sweep would leave
    /// the timer live forever.
    pub fn mark_cancelled(&mut self, at: Timestamp) -> Result<(), String> {
        if !self.status.is_active() {
            return Err(format!(
                "only an Active timer can be cancelled, but it is {}",
                self.status.phase()
            ));
        }
        self.status = TimerStatus::Cancelled;
        self.meta.with_update_at(at);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::meta::{ObjectName, ObjectRef, TimerOwner};

    fn ts(ms: u64) -> Timestamp {
        Timestamp::from_millis(ms)
    }

    /// An `Active` `WaitResume` timer, born at `ts(0)` whose name is **not** the `obj-<uid>` a
    /// re-derivation would produce — a carried-over name is what the cancel has to preserve.
    fn active_timer() -> Timer {
        Timer {
            meta: ObjectMeta::builder(ulid::Ulid::from(7u128))
                .name(ObjectName::from_parsed("execution-0").expect("a valid object name"))
                .timestamps(ts(0), ts(0))
                .with_owner(TimerOwner::Activity(ObjectRef::new(
                    ObjectName::from_parsed("execution-0").expect("a valid object name"),
                    ulid::Ulid::from(7u128),
                ))),
            execution: ObjectRef::new(
                ObjectName::from_parsed("execution-0").expect("a valid object name"),
                ulid::Ulid::nil(),
            ),
            purpose: TimerPurpose::WaitResume,
            status: TimerStatus::Active,
            deadline: ts(5_000),
        }
    }

    /// A cancel moves the status and the transition stamp and nothing else: the identity its owner
    /// tracks the timer by (`RawObjectRef` — name and uid together), its deadline, purpose and execution
    /// all survive, so the applier can still detach the child edge under the name it was added with.
    #[test]
    fn cancel_moves_only_the_status_and_the_stamp() {
        let mut t = active_timer();
        let before = t.meta.raw_object_ref();
        t.mark_cancelled(ts(200)).expect("an active timer cancels");
        assert_eq!(t.status, TimerStatus::Cancelled);
        assert_eq!(t.meta.created_at, ts(0));
        assert_eq!(t.meta.updated_at, ts(200));
        assert_eq!(t.meta.raw_object_ref(), before);
        assert!(t.status.is_terminal(), "a cancelled timer is terminal");
        assert_eq!(t.deadline, ts(5_000));
        assert_eq!(t.purpose, TimerPurpose::WaitResume);
        assert_eq!(t.execution.uid(), ulid::Ulid::nil());
    }

    /// A timer's slot admits the two scopes that arm one and nothing else: a `WaitResume` round-trips
    /// with its waiting activity (the fixture's own owner), an execution timeout with its run, and a
    /// payload carrying any other kind is refused at the slot — so a fire never has to ask at runtime
    /// which kind of scope it belongs to.
    #[test]
    fn a_timer_slot_admits_only_the_scopes_that_arm_one() {
        let timer = active_timer();
        let mut json = serde_json::to_value(&timer).expect("timer serializes");
        assert_eq!(json["meta"]["owner"]["kind"], serde_json::json!("Activity"));
        assert_eq!(
            serde_json::from_value::<Timer>(json.clone())
                .expect("the slot admits its own kind")
                .meta
                .owner,
            timer.meta.owner
        );

        let mut run_owned = timer.clone();
        run_owned.meta = run_owned
            .meta
            .with_owner(TimerOwner::Execution(ObjectRef::new(
                ObjectName::from_parsed("execution-0").expect("a valid object name"),
                ulid::Ulid::from(7u128),
            )));
        let roundtripped: Timer =
            serde_json::from_value(serde_json::to_value(&run_owned).expect("timer serializes"))
                .expect("an execution timeout's owner round-trips");
        assert_eq!(roundtripped.meta.owner, run_owned.meta.owner);

        json["meta"]["owner"]["kind"] = serde_json::json!("Thread");
        let err = serde_json::from_value::<Timer>(json).expect_err("a thread never arms a timer");
        let msg = err.to_string();
        assert!(msg.contains("reference kind mismatch"), "{msg}");
        assert!(msg.contains("admits only Execution or Activity"), "{msg}");
    }

    /// A timer past `Active` refuses the transition and is left exactly as it was — a fired timer is
    /// never rewritten into a cancelled one, and a second cancel never re-stamps. This is the guard
    /// the handlers used to keep before calling: it lives on the type now, so no call site can skip it.
    #[test]
    fn a_terminal_timer_refuses_to_be_cancelled_and_stays_untouched() {
        for status in [TimerStatus::Cancelled, TimerStatus::Completed] {
            let mut t = active_timer();
            t.status = status;
            let before = t.clone();
            let reason = t
                .mark_cancelled(ts(300))
                .expect_err("a terminal timer has no cancellation to take");
            assert!(
                reason.contains(status.phase()),
                "the reason names the state the timer was found in: {reason}"
            );
            assert_eq!(t, before, "a refused cancel writes nothing");
        }
    }
}
