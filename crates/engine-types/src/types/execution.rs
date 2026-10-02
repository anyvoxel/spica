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
    /// sets one. Nothing decides from it — the run is terminated by the `ExecutionTimeout` timer whose
    /// `deadline` is the *same* instant (both written from one computation at creation) — so this is
    /// the run's own answer to "when is it due", where a client would otherwise have to find and filter
    /// the timer child. It is the run-level counterpart of a state's [`Task::deadline`](crate::Task).
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
    pub fn begin_completing(&mut self, output: Value, at: Timestamp) {
        self.status = ExecutionStatus::Completing;
        self.output = Some(output);
        self.meta.with_update_at(at);
    }

    /// Land the success terminal at `at`, once no child is left to drain. The output is **not**
    /// written here: the finish already fixed it, and callers advance the very value that finish
    /// produced, so the terminal can never re-decide — or drop — what the run completes with.
    pub fn complete(&mut self, at: Timestamp) {
        self.status = ExecutionStatus::Completed;
        self.meta.with_update_at(at);
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
        exec.begin_completing(json!({ "n": 1 }), ts(500));

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
        exec.begin_completing(json!({ "n": 1 }), ts(500));
        exec.complete(ts(900));

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

    /// The terminal writes nothing but the status and the transition stamp: a run taken straight to
    /// the terminal keeps the output it had, so no caller can lose a fixed value on the way.
    #[test]
    fn the_terminal_does_not_touch_the_output() {
        let mut exec = running_execution();
        exec.complete(ts(900));

        assert_eq!(exec.output, None, "the terminal never invents an output");
    }
}
