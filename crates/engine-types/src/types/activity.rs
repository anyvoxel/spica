use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;

use crate::types::command::TerminationReason;
use crate::types::execution::ExecutionKind;
use crate::types::meta::{ObjectKind, ObjectKindMarker, ObjectMeta, ObjectRef};
use crate::types::task::RetryState;
use crate::types::thread::ThreadKind;
use spica_asl::StatePath;
use spica_machinery::Timestamp;

/// Lifecycle status of an Activity — the execution of a single state within an Execution.
///
/// Mirrors `ExecutionStatus` exactly (`Running` -> `Completing` -> `Completed` success, and
/// `Running` -> `Terminating` -> `Terminated` abnormal): a state activity and its owning execution
/// share the same "winding-down while children drain" model. Kept as a **separate type** so a state's
/// status can't be confused for its execution's — a container `Activity` (`Map`/`Parallel`) is
/// `Running` while owning its children, then drains through `Completing`/`Terminating` just like the
/// execution does.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ActivityStatus {
    Running,
    /// Success finish initiated; waiting on owned children to drain.
    Completing,
    /// Abnormal finish initiated with its final reason already decided; waiting on owned children to
    /// terminate before the terminal ed lands.
    Terminating(TerminationReason),
    Completed,
    Terminated(TerminationReason),
}

impl ActivityStatus {
    pub fn is_running(&self) -> bool {
        matches!(self, ActivityStatus::Running)
    }
    pub fn is_completing(&self) -> bool {
        matches!(self, ActivityStatus::Completing)
    }
    pub fn is_terminating(&self) -> bool {
        matches!(self, ActivityStatus::Terminating(_))
    }
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ActivityStatus::Completed | ActivityStatus::Terminated(_)
        )
    }
    /// The status as one word per variant — what a durable rejection reason may interpolate. `{:?}`
    /// is unusable there: `Terminating`/`Terminated` carry a nested termination reason, whose `Debug`
    /// would be embedded in the log's own text.
    pub fn phase(&self) -> &'static str {
        match self {
            ActivityStatus::Running => "Running",
            ActivityStatus::Completing => "Completing",
            ActivityStatus::Terminating(_) => "Terminating",
            ActivityStatus::Completed => "Completed",
            ActivityStatus::Terminated(_) => "Terminated",
        }
    }
}

/// The **state-specific runtime repository** of an Activity — data only a container state carries,
/// kept as a typed enum instead of `Option`/empty-collection fields on the shared skeleton.
///
/// The shared lifecycle skeleton (`id`/`parent`/`state`/`status`/`raw_input`/`input`/`retry_state`/
/// `raw_output`/`output`/...) lives on `Activity` directly because every state needs it; this
/// enum holds only what *some* states need. A state gaining a runtime repository adds a variant
/// rather than widening the shared struct. `Activity.activity_state` is `None` while the state
/// holds no state-specific data (Pass/Task/Choice/Succeed/Fail), and `Some` once one materializes.
///
/// - `Parallel(ParallelActivityState)` — the branch index → child execution fan-out map, so
///   convergence aggregates branch outputs in declaration order.
/// - `Map(MapActivityState)` — the `Map` iteration plan (items/total/cap) harvested from the
///   activation product on `Event::StateActivated`. The running completed/failed tallies are **not**
///   stored here: they are derived live from the terminal status of the parallel child executions, so
///   a follower rebuilds them from each child's own terminal event without extra projections.
/// - `Wait(WaitActivityState)` — the absolute moment the `Wait` resumes, harvested the same way. A
///   `Wait` holds no other runtime data; the instant rides `StateActivated` so every later event of
///   the activity carries it, which is what a client asks when it wants "when does this state
///   resume" without having to find and filter the activity's `Seconds` timer child.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ActivityState {
    Parallel(ParallelActivityState),
    Map(MapActivityState),
    Wait(WaitActivityState),
}

/// A `Parallel` state's activity-level runtime repository — the ordered fan-out mapping. The
/// shared activity value already carries the lifecycle skeleton; this payload holds only the
/// `Parallel`-specific state that exists because this activity is executing a `Parallel`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ParallelActivityState {
    /// Branch index → child thread, populated by `Event::ThreadCreated` (from each thread's own
    /// `index`) as branches fan out, so convergence can aggregate outputs in declaration order.
    pub branches: HashMap<usize, ObjectRef<ThreadKind>>,
}

/// A `Wait` state's activity-level runtime repository — the absolute instant the wait resumes.
///
/// Nothing decides from it: the `Seconds` timer, armed from this same value at activation, is
/// what actually resumes the state (matching [`Execution::deadline`](crate::Execution) and
/// [`Task::deadline`](crate::Task), where the enforcing timer likewise stays the actor). Resolving
/// it in the activation step rather than when the timer is armed makes the instant a property of
/// the entering activity, so a client can read "when will this state resume" off the activity alone
/// instead of searching its children for the timer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WaitActivityState {
    /// The absolute moment the state's `Seconds`/`Timestamp` resolves to, measured from the
    /// activity's entry instant.
    pub resume_at: Timestamp,
}

/// The `Map` state's activity-level runtime repository — the **static activation plan** projected
/// from the `Map` activation product on `Event::StateActivated`. It drives the bounded-concurrency
/// replenish loop in `MapStateHandler`:
///
/// - `items` is the iterable array the `Map` was entered with; `total` is its length (an empty Map
///   converges immediately to an empty array).
/// - `max_concurrency` is the `MaxConcurrency` cap (0 = unlimited, spawn every item up front).
/// - `children` is the item index → child execution map, populated incrementally by
///   `Event::ThreadCreated` as items fan out (the same path `Parallel` branches take), so
///   convergence can aggregate item outputs in index order.
///
/// The running `completed`/`failed` tallies are deliberately **not** stored here — they are derived
/// live from the terminal status of the `children` executions. This keeps the projection
/// redundancy-free: a follower can rebuild the tallies from each child's own terminal event, so no
/// separate settle bookkeeping event is needed.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MapActivityState {
    pub items: Vec<Value>,
    pub total: usize,
    pub max_concurrency: usize,
    /// Item index → child thread, populated by `ThreadCreated` as items fan out.
    pub children: HashMap<usize, ObjectRef<ThreadKind>>,
}

/// The [`ObjectKindMarker`] tying an [`Activity`]'s meta to [`ObjectKind::Activity`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActivityKind;

impl ObjectKindMarker for ActivityKind {
    const KIND: ObjectKind = ObjectKind::Activity;
    /// A state runs inside exactly one scope, and that scope is always a [`Thread`](crate::Thread) —
    /// the derived root thread for a top-level run, or a fan-out branch's thread. So the slot is an
    /// [`ObjectRef`] rather than a union: an activity owned by an `Execution` directly is unrepresentable.
    type OwnedBy = ObjectRef<ThreadKind>;
}

/// The event-carried domain value of an Activity.
///
/// This is the entity-shaped payload Activity lifecycle events carry. It intentionally excludes
/// projection-only bookkeeping such as `active_children`; those remain on `storage::ActivityRecord`, the
/// storage projection row, so event payloads stay focused on the Activity's own domain state.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Activity {
    /// Shared identity + timing metadata. `meta.uid` is the activity's identity (durable object uid);
    /// the domain `created_at`/`updated_at` (stamped at each lifecycle-transition emit) live inside
    /// `meta`. Use [`Self::raw_object_ref`](crate::types::meta::ObjectMeta::raw_object_ref) to obtain the
    /// canonical [`RawObjectRef`].
    pub meta: ObjectMeta<ActivityKind>,
    /// The execution this activity belongs to — **always** the top-level [`Execution`](crate::types::execution::Execution)'s reference
    /// (the flat query anchor shared by the whole tree), regardless of how deep the activity sits in
    /// a `Parallel` branch / `Map` item. The activity's *immediate* container — the scope it lives
    /// inside — is **not** stored here; it is `meta.owner`, and it is always a [`Thread`](crate::Thread)
    /// (see [`ActivityKind::OwnedBy`]): a top-level run's states are owned by the run's derived root
    /// thread, a fan-out branch's by that branch's thread. So `execution` names a real `Execution` by
    /// construction, while the scope edge carries its own type.
    pub execution: ObjectRef<ExecutionKind>,
    /// The complete JSON Pointer (RFC 6901) to this state's definition within the shared machine
    /// document, e.g. `/States/P2` (top-level) or `/States/P1/Branches/0/States/P2` (inside a
    /// Parallel branch). The leaf state name (the activity's identity — the state's key in the
    /// enclosing `States` table) is **derived** as the pointer's last token, so it is not duplicated
    /// here. Carried on `Event::StateActivating` so a follower / recovered leader records exactly
    /// where the activity lives without re-deriving it from the machine + parent chain.
    pub state_path: StatePath,
    pub status: ActivityStatus,
    /// The **raw** input this state received on entry — the value carried on `Event::StateActivating`.
    /// For a top-level start it is the execution's original input; on a State→State hop it is the
    /// predecessor's output; inside a `Parallel`/`Map` child it is that branch/item's arguments. It is
    /// kept verbatim, distinct from `input` (the value after the state's own input preprocessing,
    /// e.g. projecting `Arguments`), so a follower / auditor can inspect both the original and the
    /// processed view of what the state ran on.
    pub raw_input: Value,
    /// The input this state actually processes — the state's **processed** input after the
    /// dialogue-level input preprocessing (e.g. projecting `Arguments`) ran on `raw_input`. `None`
    /// before processing: `StateActivating` pins it (the entry input lives verbatim in `raw_input`);
    /// the processed value is carried on `Event::StateActivated` and forwarded by every later
    /// lifecycle event. Kept distinct from `raw_input` so an auditor can compare the original and
    /// the processed view; for a state that consumes its raw input verbatim (no `Arguments`) the
    /// processed input is a copy of `raw_input`.
    pub input: Option<Value>,
    /// The state's **raw result** before any complete-step `Output` projection. For a `Task` this is
    /// the `Resource`'s returned payload; for a synchronous state with no distinct raw result it is
    /// the processed `input` — the same
    /// derivation `$states.result` uses). Unlike `input` (whose processed view isn't known at entry,
    /// so `StateActivating` pins it to `Null`), the raw result *is* derivable before the complete step
    /// runs, so both complete-phase events (`StateCompleting`/`StateCompleted`) carry it rather than a
    /// `null`. Kept distinct from `output` so the engine preserves both the pre-projection and the
    /// final projected view of the state's result.
    pub raw_output: Option<Value>,
    /// The **state-specific runtime repository** — data only some states' activities carry.
    /// `None` while the state holds no state-specific runtime data (Pass/Task/Choice/Succeed/Fail),
    /// `Some` once one materializes. A `Parallel` holds its branch index → child execution fan-out
    /// map, a `Map` its iteration plan harvested from the activation product, and a `Wait` the
    /// absolute instant it resumes. Kept as an enum so state-specific data is *typed* (not a bunch of
    /// `Option`s/empty collections polluting the shared skeleton) and grows by adding a variant.
    pub activity_state: Option<ActivityState>,
    /// Retry-specific runtime state: total retry count exposed to `$states.context.State.RetryCount`
    /// plus per-retrier attempt metadata (`attempt_count` and `last_retry_at`). `None` until a retry
    /// has actually occurred (an activity without a recorded retry carries no run-state).
    pub retry_state: Option<RetryState>,
    /// The terminal output once `StateCompleted` lands — the value after the state's complete-step
    /// `Output` projection (or the raw result / processed input when no `Output` is present).
    pub output: Option<Value>,
}

impl Activity {
    /// The total retry count, exposed to `$states.context.State.RetryCount` — `0` until a retry has
    /// occurred (the field is `None` then).
    pub fn retry_count(&self) -> u32 {
        self.retry_state.as_ref().map(|r| r.attempts).unwrap_or(0)
    }

    /// Enter the success finish at `at`, folding in the **raw result** the state's complete step was
    /// handed: what the state produced is known as soon as its finish begins, and the terminal event
    /// only carries that value forward. The projected `output` is decided later, at
    /// [`Self::mark_completed`], once [`crate::ActivityStatus::Completing`]'s children have drained.
    ///
    /// Only a `Running` activity has a finish to begin, so one already finishing or terminal is
    /// **refused** and left untouched — the invariant lives here rather than at each caller, so no call
    /// site can forget to ask first.
    ///
    /// The `Err` is one sentence naming *why* the transition was declined (the object's own state), for
    /// the caller to fold into its durable rejection reason — the caller alone knows which object and
    /// which command it is refusing.
    pub fn mark_completing(&mut self, raw_output: Value, at: Timestamp) -> Result<(), String> {
        if !self.status.is_running() {
            return Err(format!(
                "only a Running activity can begin completing, but it is {}",
                self.status.phase()
            ));
        }
        self.status = ActivityStatus::Completing;
        self.raw_output = Some(raw_output);
        self.meta.with_update_at(at);
        Ok(())
    }

    /// Land the success terminal at `at` with the value the complete step's projection produced: the
    /// finish owned no child or has drained the ones it did, so that value is final and this is the
    /// only place it is written. `raw_output` keeps the pre-projection view the finish opened with.
    ///
    /// Only a `Completing` activity has a terminal to land: one still `Running` has not begun its
    /// finish (its output is undecided), and one already terminal has landed — both are **refused**, so
    /// the terminal can never be jumped to from a phase that skipped the finish.
    pub fn mark_completed(&mut self, output: Value, at: Timestamp) -> Result<(), String> {
        if !self.status.is_completing() {
            return Err(format!(
                "only a Completing activity can complete, but it is {}",
                self.status.phase()
            ));
        }
        self.status = ActivityStatus::Completed;
        self.output = Some(output);
        self.meta.with_update_at(at);
        Ok(())
    }

    /// Begin the abnormal finish at `at`: the activity is `Terminating(reason)`, waiting on its owned
    /// children. The reason is fixed **here** rather than at the terminal — what an activity terminates
    /// with is decided when its teardown begins, and the terminal only carries that value forward.
    ///
    /// An activity can begin terminating from **either** live phase: a `Running` activity a terminate
    /// command reaches, or a `Completing` one whose own state's complete step turns into a failure (a
    /// `Fail`). One already `Terminating` (mid-sweep) or terminal has no teardown to begin — both are
    /// **refused** and left untouched, so no call site can start two teardowns or rewrite a landed one.
    pub fn mark_terminating(
        &mut self,
        reason: TerminationReason,
        at: Timestamp,
    ) -> Result<(), String> {
        if self.status.is_terminating() || self.status.is_terminal() {
            return Err(format!(
                "only a Running or Completing activity can begin terminating, but it is {}",
                self.status.phase()
            ));
        }
        self.status = ActivityStatus::Terminating(reason);
        self.meta.with_update_at(at);
        Ok(())
    }

    /// Land the abnormal terminal at `at`, once no child is left to drain. The reason is **not** taken
    /// here: the teardown already fixed it, and callers advance the very value that teardown produced,
    /// so the terminal can never re-decide — or drop — what the activity terminated with.
    ///
    /// Only a `Terminating` activity has a terminal to land: one still `Running`/`Completing` has not
    /// begun its teardown (its reason is undecided), and one already terminal has landed — both are
    /// **refused**, so the terminal can never be jumped to from a phase that skipped the teardown.
    pub fn mark_terminated(&mut self, at: Timestamp) -> Result<(), String> {
        let reason = match &self.status {
            ActivityStatus::Terminating(reason) => reason.clone(),
            other => {
                return Err(format!(
                    "only a Terminating activity can terminate, but it is {}",
                    other.phase()
                ));
            }
        };
        self.status = ActivityStatus::Terminated(reason);
        self.meta.with_update_at(at);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::meta::ObjectName;
    use crate::types::thread::ThreadKind;
    use spica_machinery::Timestamp;

    /// An activity's owner slot admits a `Thread` and nothing else: it reads back with the same
    /// owner, and a payload whose `owner` carries another kind is refused at the slot — an activity
    /// is always owned by the scope whose machine it runs in, never by a run or a timer.
    #[test]
    fn an_activity_slot_admits_only_a_thread_owner() {
        let owner = ObjectRef::<ThreadKind>::new(
            ObjectName::plain("execution").unwrap(),
            ulid::Ulid::new(),
        );
        let mut json = serde_json::to_value(
            ObjectMeta::<ActivityKind>::builder(ulid::Ulid::new())
                .at(Timestamp::from_millis(0))
                .with_owner(owner.clone()),
        )
        .expect("meta serializes");
        assert_eq!(json["owner"]["kind"], serde_json::json!("Thread"));
        let back: ObjectMeta<ActivityKind> =
            serde_json::from_value(json.clone()).expect("the slot admits its own kind");
        assert_eq!(back.owner, owner);

        json["owner"]["kind"] = serde_json::json!("Execution");
        let err = serde_json::from_value::<ObjectMeta<ActivityKind>>(json)
            .expect_err("an activity is never owned by a run");
        let msg = err.to_string();
        assert!(msg.contains("reference kind mismatch"), "{msg}");
        assert!(msg.contains("admits only Thread"), "{msg}");
    }

    /// A still-`Running` activity with no raw result yet, eligible for a success finish.
    fn running_activity() -> Activity {
        Activity {
            meta: ObjectMeta::<ActivityKind>::builder(ulid::Ulid::from(7u128))
                .name(ObjectName::from_parsed("execution-0").expect("a valid object name"))
                .at(Timestamp::from_millis(0))
                .with_owner(ObjectRef::<ThreadKind>::new(
                    ObjectName::from_parsed("execution-0").expect("a valid object name"),
                    ulid::Ulid::nil(),
                )),
            execution: ObjectRef::new(
                ObjectName::from_parsed("execution-0").expect("a valid object name"),
                ulid::Ulid::nil(),
            ),
            state_path: StatePath::from(jsonptr::PointerBuf::new()),
            status: ActivityStatus::Running,
            raw_input: Value::Null,
            input: None,
            raw_output: None,
            activity_state: None,
            retry_state: None,
            output: None,
        }
    }

    /// Opening the finish folds the raw result in, moves the status and the transition stamp, and
    /// touches nothing else — the identity, input and pre-decision fields all survive so the deferred
    /// drain can read the result back off the row to project with.
    #[test]
    fn completing_moves_only_the_status_input_and_stamp() {
        let mut activity = running_activity();
        activity
            .mark_completing(
                serde_json::json!({ "worker": "a" }),
                Timestamp::from_millis(200),
            )
            .expect("a Running activity opens its finish");
        assert_eq!(activity.status, ActivityStatus::Completing);
        assert_eq!(
            activity.raw_output,
            Some(serde_json::json!({ "worker": "a" }))
        );
        assert_eq!(activity.meta.created_at, Timestamp::from_millis(0));
        assert_eq!(activity.meta.updated_at, Timestamp::from_millis(200));
        assert!(
            activity.output.is_none(),
            "output is decided at the terminal, not the finish"
        );
    }

    /// Landing the success terminal writes only the projected output and the stamp — and only from
    /// `Completing`, so the terminal can never be jumped to from a phase that skipped the finish.
    #[test]
    fn completed_writes_the_output_and_the_stamp() {
        let mut activity = running_activity();
        activity
            .mark_completing(
                serde_json::json!({ "raw": true }),
                Timestamp::from_millis(100),
            )
            .expect("a Running activity opens its finish");
        activity
            .mark_completed(
                serde_json::json!({ "done": true }),
                Timestamp::from_millis(120),
            )
            .expect("a Completing activity lands its terminal");
        assert_eq!(activity.status, ActivityStatus::Completed);
        assert_eq!(activity.output, Some(serde_json::json!({ "done": true })));
        assert_eq!(
            activity.raw_output,
            Some(serde_json::json!({ "raw": true })),
            "the raw result the finish opened with survives the projection"
        );
        assert_eq!(activity.meta.updated_at, Timestamp::from_millis(120));
    }

    /// An activity past `Running` refuses the finish and stays exactly as it was — a cancel or an
    /// earlier finish racing the command never gets overwritten by the one it lost to.
    #[test]
    fn a_non_running_activity_refuses_to_complete_and_stays_untouched() {
        for (status, reject) in [
            (ActivityStatus::Completing, "only a Running activity"),
            (ActivityStatus::Completed, "only a Running activity"),
            (
                ActivityStatus::Terminating(TerminationReason::Cancelled),
                "only a Running activity",
            ),
        ] {
            let mut activity = running_activity();
            activity.status = status;
            let before = activity.clone();
            let reason = activity
                .mark_completing(serde_json::json!({}), Timestamp::from_millis(300))
                .expect_err("only a Running activity opens its finish");
            assert!(
                reason.contains(reject),
                "reason names the refusal: {reason}"
            );
            assert_eq!(activity, before, "a refused finish writes nothing");
        }
    }

    /// The success terminal can only land from `Completing`: a still-`Running` activity has not begun
    /// its finish (its output is undecided), and one already terminal has landed — both are refused
    /// so the terminal can never be jumped to from a phase that skipped the finish.
    #[test]
    fn only_a_completing_activity_lands_the_terminal() {
        for status in [ActivityStatus::Running, ActivityStatus::Completed] {
            let mut activity = running_activity();
            activity.status = status;
            let before = activity.clone();
            let reason = activity
                .mark_completed(serde_json::json!({}), Timestamp::from_millis(400))
                .expect_err("only a Completing activity lands its terminal");
            assert!(
                reason.contains("only a Completing activity can complete"),
                "reason names the refusal: {reason}"
            );
            assert_eq!(activity, before, "a refused terminal writes nothing");
        }
    }

    /// Beginning the teardown fixes the reason and moves the stamp — from a live phase (`Running`, or
    /// the `Completing` a `Fail`'s complete step opens), and touching nothing else.
    #[test]
    fn terminating_fixes_the_reason_and_the_stamp() {
        for status in [ActivityStatus::Running, ActivityStatus::Completing] {
            let mut activity = running_activity();
            activity.status = status;
            activity
                .mark_terminating(
                    TerminationReason::Failed {
                        error: crate::types::error::ExecutionError::Runtime(
                            crate::types::error::RuntimeError::StateFailed {
                                state: "P".to_string(),
                                error: "States.Fail".to_string(),
                                output: Box::new(Value::Null),
                            },
                        ),
                    },
                    Timestamp::from_millis(300),
                )
                .expect("a live activity begins its teardown");
            assert!(
                activity.status.is_terminating(),
                "the activity is mid-teardown: {:?}",
                activity.status
            );
            assert_eq!(activity.meta.updated_at, Timestamp::from_millis(300));
            assert!(
                activity.output.is_none(),
                "output is only set by a successful complete"
            );
        }
    }

    /// Landing the abnormal terminal carries the reason the teardown fixed, never one re-decided here,
    /// and moves only the status and the stamp.
    #[test]
    fn terminated_carries_the_teardowns_reason() {
        let mut activity = running_activity();
        let reason = TerminationReason::Failed {
            error: crate::types::error::ExecutionError::Runtime(
                crate::types::error::RuntimeError::StateFailed {
                    state: "P".to_string(),
                    error: "States.Fail".to_string(),
                    output: Box::new(Value::Null),
                },
            ),
        };
        activity
            .mark_terminating(reason.clone(), Timestamp::from_millis(100))
            .expect("a Running activity begins its teardown");
        activity
            .mark_terminated(Timestamp::from_millis(120))
            .expect("a Terminating activity lands its terminal");
        assert_eq!(activity.status, ActivityStatus::Terminated(reason));
        assert_eq!(activity.meta.updated_at, Timestamp::from_millis(120));
    }

    /// An activity already winding down (`Terminating`) or landed (`Terminated`/`Completed`) refuses a
    /// second teardown and stays untouched — one teardown is already in flight, and starting another
    /// would rewrite the reason the first fixed.
    #[test]
    fn a_winding_down_activity_refuses_a_second_teardown_and_stays_untouched() {
        for status in [
            ActivityStatus::Terminating(TerminationReason::Cancelled),
            ActivityStatus::Terminated(TerminationReason::Cancelled),
            ActivityStatus::Completed,
        ] {
            let mut activity = running_activity();
            activity.status = status;
            let before = activity.clone();
            let reason = activity
                .mark_terminating(TerminationReason::Cancelled, Timestamp::from_millis(400))
                .expect_err("only a live activity begins its teardown");
            assert!(
                reason.contains("only a Running or Completing activity"),
                "reason names the refusal: {reason}"
            );
            assert_eq!(activity, before, "a refused teardown writes nothing");
        }
    }

    /// The abnormal terminal can only land from `Terminating`: a still-`Running`/`Completing` activity
    /// has not begun its teardown (its reason is undecided), and one already terminal has landed — both
    /// are refused so the terminal can never be jumped to from a phase that skipped the teardown.
    #[test]
    fn only_a_terminating_activity_lands_the_terminal() {
        for status in [
            ActivityStatus::Running,
            ActivityStatus::Completing,
            ActivityStatus::Completed,
        ] {
            let mut activity = running_activity();
            activity.status = status;
            let before = activity.clone();
            let reason = activity
                .mark_terminated(Timestamp::from_millis(500))
                .expect_err("only a Terminating activity lands its terminal");
            assert!(
                reason.contains("only a Terminating activity can terminate"),
                "reason names the refusal: {reason}"
            );
            assert_eq!(activity, before, "a refused terminal writes nothing");
        }
    }
}
