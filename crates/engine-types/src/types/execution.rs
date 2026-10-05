use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;

use crate::types::command::TerminationReason;
use crate::types::flow_version::FlowVersionKind;
use crate::types::meta::{NoOwner, ObjectKind, ObjectKindMarker, ObjectMeta, ObjectRef};
use spica_machinery::Timestamp;

/// Lifecycle status of an [`Execution`].
///
/// The state machine is: `Running` -> `Completing` -> `Completed` (success) and
/// `Running` -> `Terminating` -> `Terminated` (abnormal). `Completing`/`Terminating` are real,
/// observable phases (not same-batch glitches): while a node owns active children it stays in the
/// winding-down phase until every child terminates and the shared cascade emits the `ed` event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExecutionStatus {
    Running,
    /// Success finish initiated; waiting on owned children (e.g. the execution's timeout timer).
    Completing,
    /// Abnormal finish initiated with its final reason already decided; waiting on owned children to
    /// terminate before the terminal ed lands.
    Terminating(TerminationReason),
    Completed,
    Terminated(TerminationReason),
}

impl ExecutionStatus {
    pub fn is_running(&self) -> bool {
        matches!(self, ExecutionStatus::Running)
    }
    pub fn is_completing(&self) -> bool {
        matches!(self, ExecutionStatus::Completing)
    }
    pub fn is_terminating(&self) -> bool {
        matches!(self, ExecutionStatus::Terminating(_))
    }
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ExecutionStatus::Completed | ExecutionStatus::Terminated(_)
        )
    }
    /// The terminal termination reason, if this status settled by `Terminating`/`Terminated`.
    pub fn termination_reason(&self) -> Option<&TerminationReason> {
        match self {
            ExecutionStatus::Terminating(r) | ExecutionStatus::Terminated(r) => Some(r),
            ExecutionStatus::Running | ExecutionStatus::Completing | ExecutionStatus::Completed => {
                None
            }
        }
    }
    /// The status as one word per variant — what a durable rejection reason may interpolate. `{:?}`
    /// is unusable there: `Terminating`/`Terminated` carry a nested termination reason, whose `Debug`
    /// would be embedded in the log's own text.
    pub fn phase(&self) -> &'static str {
        match self {
            ExecutionStatus::Running => "Running",
            ExecutionStatus::Completing => "Completing",
            ExecutionStatus::Terminating(_) => "Terminating",
            ExecutionStatus::Completed => "Completed",
            ExecutionStatus::Terminated(_) => "Terminated",
        }
    }
}

/// The [`ObjectKindMarker`] tying an [`Execution`]'s meta to [`ObjectKind::Execution`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionKind;

impl ObjectKindMarker for ExecutionKind {
    const KIND: ObjectKind = ObjectKind::Execution;
    /// A run is the root of its own object tree — a branch or item is a `Thread`, never an
    /// `Execution` — so the slot can never be filled (see [`NoOwner`]).
    type OwnedBy = NoOwner;
}

/// The event-/domain-carried value of an execution.
///
/// This is the execution entity shape the lifecycle stream carries. It is intentionally limited to
/// durable execution identity/lifecycle facts; runtime conveniences such as variable scope cursors
/// live on the storage projection instead of being repeated on every execution lifecycle event.
///
/// `Execution` is the entity of a **top-level run only** — the run a client started. A `Parallel`
/// branch or `Map` item is a different, dedicated entity type ([`Thread`](crate::Thread)) with its
/// own `state_path`/owner; it is **never** an `Execution`. Consequently an `Execution` needs neither a
/// `state_path` (it always resolves against the machine's top-level `States`) nor a
/// `root_execution` (it is its own root — its `raw_object_ref()` IS the flat query anchor). Removing those
/// two fields is exactly what makes the type self-describing: no consumer must inspect fields to
/// decide whether an `Execution` is a root or a branch, because it is always a root.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Execution {
    /// The object identity + shared metadata (k8s-style `ObjectMeta` reuse). **`meta.uid` IS the
    /// execution's never-reused identity ulid** (there is no separate bare `id` — [`Self::raw_object_ref`]
    /// bundles `meta.name` + `meta.uid` for anyone who must address this execution). `meta.name` is a
    /// generated placeholder (`obj-<uid>`) until user naming (P2); the domain
    /// `created_at`/`updated_at` live here likewise. A top-level execution has **no owner** — the
    /// tree's root is owned by nothing.
    pub meta: ObjectMeta<ExecutionKind>,
    /// The flow version this execution is bound to (its state machine definition), addressed as a
    /// typed [`ObjectRef`] (`{kind: FlowVersion, name: <flow>-<version>, uid}`). This is the
    /// CCES analogue of Zeebe's `processDefinitionKey`: the execution references a **never-reused,
    /// immutable** version (never the mutable flow name), so it always resolves its machine against
    /// exactly the definition it was created on — even after the flow is updated or its name is
    /// deleted and re-created. Every entity in a tree (a `Thread` inherited from its container)
    /// shares the same version.
    pub flow_version: ObjectRef<FlowVersionKind>,
    pub status: ExecutionStatus,
    /// The absolute moment the state machine's `TimeoutSeconds` expires, `Some` iff the definition
    /// sets one. The run is terminated by the timer armed from this *same* instant (both written from
    /// one computation at creation), so this is the run's own answer to "when is it due", where a
    /// client would otherwise have to find and filter the timer child — and the `TimedOut` the timer's
    /// fire terminates the run with. The run-level counterpart of a state's
    /// [`Task::deadline`](crate::Task).
    #[serde(default)]
    pub deadline: Option<Timestamp>,
    /// The original execution input.
    pub input: Value,
    /// The execution's decided success output. It is written when `ExecutionCompleting` lands and is
    /// the terminal output once `ExecutionCompleted` lands.
    pub output: Option<Value>,
}

impl Execution {
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }

    /// Enter the success finish at `at`: the run is `Completing`, waiting on its owned children. The
    /// output is fixed **here** rather than at the terminal — what a run completes with is decided
    /// when its finish begins, and the terminal event only carries that value forward.
    ///
    /// Only a `Running` run has a finish to begin, so one that is already finishing or terminal is
    /// **refused** and left untouched — the invariant lives here rather than at each caller, so no call
    /// site can forget to ask first.
    ///
    /// The `Err` is one sentence naming *why* the transition was declined (the object's own state),
    /// for the caller to fold into its durable rejection reason — the caller alone knows which object
    /// and which command it is refusing.
    pub fn mark_completing(&mut self, output: Value, at: Timestamp) -> Result<(), String> {
        if !self.status.is_running() {
            return Err(format!(
                "only a Running execution can begin completing, but it is {}",
                self.status.phase()
            ));
        }
        self.status = ExecutionStatus::Completing;
        self.output = Some(output);
        self.meta.with_update_at(at);
        Ok(())
    }

    /// Land the success terminal at `at`, once no child is left to drain. The output is **not**
    /// written here: the finish already fixed it, and callers advance the very value that finish
    /// produced, so the terminal can never re-decide — or drop — what the run completes with.
    ///
    /// Only a `Completing` run has a terminal to land: a run still `Running` has not begun its finish
    /// (its output is undecided), and one already terminal has landed — both are **refused**, so the
    /// terminal can never be jumped to from a phase that skipped the finish.
    pub fn mark_completed(&mut self, at: Timestamp) -> Result<(), String> {
        if !self.status.is_completing() {
            return Err(format!(
                "only a Completing execution can be completed, but it is {}",
                self.status.phase()
            ));
        }
        self.status = ExecutionStatus::Completed;
        self.meta.with_update_at(at);
        Ok(())
    }

    /// Begin the abnormal finish at `at`: the run is `Terminating(reason)`, waiting on its owned
    /// children. The reason is fixed **here** rather than at the terminal — what a run terminates with
    /// is decided when its teardown begins, and the terminal only carries that value forward.
    ///
    /// Only a `Running` run has a teardown to begin, so one already finishing or terminal is
    /// **refused** and left untouched, exactly as [`Self::mark_completing`] — the invariant lives here
    /// rather than at each caller, so no call site can forget to ask first.
    pub fn mark_terminating(
        &mut self,
        reason: TerminationReason,
        at: Timestamp,
    ) -> Result<(), String> {
        if !self.status.is_running() {
            return Err(format!(
                "only a Running execution can begin terminating, but it is {}",
                self.status.phase()
            ));
        }
        self.status = ExecutionStatus::Terminating(reason);
        self.meta.with_update_at(at);
        Ok(())
    }

    /// Land the abnormal terminal at `at`, once no child is left to drain. The reason is **not** taken
    /// here: the teardown already fixed it, and callers advance the very value that teardown produced,
    /// so the terminal can never re-decide — or drop — what the run terminated with.
    ///
    /// Only a `Terminating` run has a terminal to land: a run still `Running` has not begun its
    /// teardown (its reason is undecided), and one already terminal has landed — both are **refused**,
    /// so the terminal can never be jumped to from a phase that skipped the teardown.
    pub fn mark_terminated(&mut self, at: Timestamp) -> Result<(), String> {
        let reason = match &self.status {
            ExecutionStatus::Terminating(reason) => reason.clone(),
            other => {
                return Err(format!(
                    "only a Terminating execution can be terminated, but it is {}",
                    other.phase()
                ));
            }
        };
        self.status = ExecutionStatus::Terminated(reason);
        self.meta.with_update_at(at);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::types::meta::ObjectName;

    fn ts(ms: u64) -> Timestamp {
        Timestamp::from_millis(ms)
    }

    /// A running run born at `ts(0)` with no output — each case drives the transition it is about.
    fn running_execution() -> Execution {
        Execution {
            meta: ObjectMeta::builder(ulid::Ulid::new())
                .timestamps(ts(0), ts(0))
                .with_owner(NoOwner::new()),
            flow_version: ObjectRef::new(
                ObjectName::from_parsed("flow-1").expect("a version name is a valid object name"),
                ulid::Ulid::nil(),
            ),
            status: ExecutionStatus::Running,
            deadline: None,
            input: Value::Null,
            output: None,
        }
    }

    /// The success finish fixes the output where it begins: `Completing` already carries the value the
    /// terminal will hand back, and only the transition stamp moves — `created_at` rides unchanged.
    #[test]
    fn beginning_the_finish_fixes_the_output_before_the_terminal() {
        let mut exec = running_execution();
        exec.mark_completing(json!({ "n": 1 }), ts(500))
            .expect("a Running execution has a finish to begin");

        assert_eq!(exec.status, ExecutionStatus::Completing);
        assert_eq!(exec.output, Some(json!({ "n": 1 })));
        assert_eq!(exec.meta.created_at, ts(0));
        assert_eq!(exec.meta.updated_at, ts(500));
    }

    /// Landing the terminal advances the status onto the value the finish already fixed — the output
    /// is carried, not re-written — and stamps the moment the terminal lands, not the one the finish
    /// began.
    #[test]
    fn the_terminal_lands_on_the_value_the_finish_fixed() {
        let mut exec = running_execution();
        exec.mark_completing(json!({ "n": 1 }), ts(500))
            .expect("a Running execution has a finish to begin");
        exec.mark_completed(ts(900))
            .expect("a Completing execution has a terminal to land");

        assert_eq!(exec.status, ExecutionStatus::Completed);
        assert_eq!(
            exec.output,
            Some(json!({ "n": 1 })),
            "the terminal carries the value the finish fixed"
        );
        assert_eq!(exec.meta.created_at, ts(0));
        assert_eq!(exec.meta.updated_at, ts(900));
        assert!(exec.is_terminal());
    }

    /// The terminal writes nothing but the status and the transition stamp: a `Completing` row whose
    /// output was never fixed — one crafted outside `mark_completing` — keeps the output it had, so no
    /// caller can lose or invent a value on the way to the terminal.
    #[test]
    fn the_terminal_does_not_touch_the_output() {
        let mut exec = running_execution();
        exec.status = ExecutionStatus::Completing;
        exec.mark_completed(ts(900))
            .expect("a Completing execution has a terminal to land");

        assert_eq!(exec.output, None, "the terminal never invents an output");
        assert_eq!(exec.status, ExecutionStatus::Completed);
        assert_eq!(exec.meta.updated_at, ts(900));
    }

    /// A finish only begins from `Running`: a run already finishing or terminal has none to begin, and
    /// is refused and left untouched — so no call site can restart a finish, or re-decide the output
    /// one already fixed.
    #[test]
    fn only_a_running_execution_begins_a_finish() {
        for status in [ExecutionStatus::Completing, ExecutionStatus::Completed] {
            let mut exec = running_execution();
            exec.status = status.clone();
            exec.output = Some(json!({ "decided": true }));
            let before = exec.clone();
            let reason = exec
                .mark_completing(json!({ "ignored": true }), ts(500))
                .expect_err("a run past Running has no finish to begin");
            assert!(
                reason.contains(status.phase()),
                "the refusal names the state the run was found in: {reason}"
            );
            assert_eq!(exec, before, "a refused finish writes nothing");
        }
    }

    /// The terminal only lands from `Completing`: a run still `Running` has not begun its finish (its
    /// output is undecided) and one already terminal has landed — both refused, so the terminal can
    /// never be jumped to from a phase that skipped the finish.
    #[test]
    fn only_a_completing_execution_lands_the_terminal() {
        for status in [ExecutionStatus::Running, ExecutionStatus::Completed] {
            let mut exec = running_execution();
            exec.status = status.clone();
            let before = exec.clone();
            let reason = exec
                .mark_completed(ts(900))
                .expect_err("only a Completing run has a terminal to land");
            assert!(
                reason.contains(status.phase()),
                "the refusal names the state the run was found in: {reason}"
            );
            assert_eq!(exec, before, "a refused terminal writes nothing");
        }
    }

    /// The teardown fixes the reason where it begins: `Terminating` already carries the value the
    /// terminal will hand forward, and only the transition stamp moves — `created_at` rides unchanged.
    #[test]
    fn beginning_the_teardown_fixes_the_reason_before_the_terminal() {
        let mut exec = running_execution();
        exec.mark_terminating(TerminationReason::Cancelled, ts(500))
            .expect("a Running execution has a teardown to begin");

        assert_eq!(
            exec.status,
            ExecutionStatus::Terminating(TerminationReason::Cancelled)
        );
        assert_eq!(exec.meta.created_at, ts(0));
        assert_eq!(exec.meta.updated_at, ts(500));
    }

    /// Landing the abnormal terminal advances the status onto the reason the teardown already fixed —
    /// it is read off the run's own status, never passed in — and stamps the moment the terminal
    /// lands, not the one the teardown began.
    #[test]
    fn the_abnormal_terminal_lands_on_the_reason_the_teardown_fixed() {
        let mut exec = running_execution();
        exec.mark_terminating(TerminationReason::TimedOut, ts(500))
            .expect("a Running execution has a teardown to begin");
        exec.mark_terminated(ts(900))
            .expect("a Terminating execution has a terminal to land");

        assert_eq!(
            exec.status,
            ExecutionStatus::Terminated(TerminationReason::TimedOut)
        );
        assert_eq!(exec.meta.created_at, ts(0));
        assert_eq!(exec.meta.updated_at, ts(900));
        assert!(exec.is_terminal());
    }

    /// A teardown only begins from `Running`: a run already finishing or terminal has none to begin, and
    /// is refused and left untouched — so no call site can restart a teardown, or re-decide the reason
    /// one already fixed.
    #[test]
    fn only_a_running_execution_begins_a_teardown() {
        for status in [
            ExecutionStatus::Completing,
            ExecutionStatus::Terminating(TerminationReason::Cancelled),
            ExecutionStatus::Completed,
            ExecutionStatus::Terminated(TerminationReason::Cancelled),
        ] {
            let mut exec = running_execution();
            exec.status = status.clone();
            let before = exec.clone();
            let reason = exec
                .mark_terminating(TerminationReason::TimedOut, ts(500))
                .expect_err("a run past Running has no teardown to begin");
            assert!(
                reason.contains(status.phase()),
                "the refusal names the state the run was found in: {reason}"
            );
            assert_eq!(exec, before, "a refused teardown writes nothing");
        }
    }

    /// The abnormal terminal only lands from `Terminating`: a run still `Running` has not begun its
    /// teardown (its reason is undecided), and one already terminal has landed — both refused, so the
    /// terminal can never be jumped to from a phase that skipped the teardown.
    #[test]
    fn only_a_terminating_execution_lands_the_abnormal_terminal() {
        for status in [
            ExecutionStatus::Running,
            ExecutionStatus::Completing,
            ExecutionStatus::Completed,
            ExecutionStatus::Terminated(TerminationReason::Cancelled),
        ] {
            let mut exec = running_execution();
            exec.status = status.clone();
            let before = exec.clone();
            let reason = exec
                .mark_terminated(ts(900))
                .expect_err("only a Terminating run has a terminal to land");
            assert!(
                reason.contains(status.phase()),
                "the refusal names the state the run was found in: {reason}"
            );
            assert_eq!(exec, before, "a refused terminal writes nothing");
        }
    }
}
