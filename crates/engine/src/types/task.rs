use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;

use crate::log::Timestamp;
use crate::types::meta::{ObjectKind, ObjectMeta, ObjectReference};

/// Per-retrier retry bookkeeping for a single `Retry` entry, carried **on the task** (Zeebe-style
/// entity reuse) so a task decides its own retries without revisiting the owning activity.
///
/// `attempt_count` is how many retry opportunities this retrier has already consumed — the counter
/// behind `MaxAttempts` enforcement and the backoff step. `last_retry_at` records when this retrier
/// last scheduled a retry (observational state, useful for inspection and future policies); the
/// authoritative **when the next retry may fire** fact is `Task::next_available_at`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct RetrierAttemptState {
    /// How many retries this retrier has already scheduled.
    pub attempt_count: u32,
    /// When this retrier last scheduled a retry, if it has ever done so.
    pub last_retry_at: Option<Timestamp>,
}

/// A single `Retry` entry, **resolved and frozen onto the task at its first activation**.
///
/// The task self-decides retry (whether an error matches, whether budget remains, and the backoff
/// delay) purely from this plan — it never needs to revisit the owning state's definition. That
/// self-containment is what lets a task be partitioned by `resource` independently of its activity /
/// execution. The ASL optional fields are pre-defaulted (per the spec) so the plan carries no `None`s.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Error names this retrier matches (`States.ALL` wildcard, `States.TaskFailed`, or an exact
    /// name) — see [`crate::types::execution`] error-name matching.
    pub error_equals: Vec<String>,
    /// Seconds before the first retry (spec default 1).
    pub interval_seconds: i64,
    /// Maximum retry attempts for this retrier (spec default 3; 0 = never retry).
    pub max_attempts: i64,
    /// Backoff multiplier per attempt (spec default 2.0).
    pub backoff_rate: f64,
    /// Cap on a single backoff delay, in seconds (spec: none).
    pub max_delay_seconds: Option<i64>,
}

impl RetryPolicy {
    /// Resolve an ASL `Retrier` into a frozen policy with the spec defaults applied, so the task
    /// needs no further definition look-up to decide a retry. `IntervalSeconds`/`BackoffRate` may be
    /// JSONata expressions; the engine resolves them once here at activation (frozen — never
    /// re-evaluated on retry, matching the frozen-`arguments` decision).
    pub fn resolve(retrier: &spica_asl::Retrier) -> Self {
        Self {
            error_equals: retrier.error_equals.clone(),
            // The accessors own the spec defaults, so the frozen plan carries no None (and the
            // engine never needs to know which default a field's absence implies).
            interval_seconds: retrier.interval_seconds(),
            max_attempts: retrier.max_attempts(),
            backoff_rate: retrier.backoff_rate(),
            max_delay_seconds: retrier.max_delay_seconds,
        }
    }

    /// The backoff delay in seconds for a retry after `attempt` prior attempts of *this* retrier:
    /// `interval_seconds * backoff_rate^attempt`, capped at `max_delay_seconds`, floored at 1.
    pub fn backoff_for_attempt(&self, attempt: u32) -> u64 {
        let raw = (self.interval_seconds as f64) * self.backoff_rate.powf(attempt as f64);
        let capped = match self.max_delay_seconds {
            Some(max) if max > 0 => raw.min(max as f64),
            _ => raw,
        };
        capped.ceil().max(1.0) as u64
    }
}

/// The shared **retry run-state** of any retryable entity — a `Task` (single external call) or a
/// `Map`/`Parallel` activity (a whole-batch run). This is the *runtime* half of a retry: the
/// counters that advance as retries are scheduled, and the backoff gate that controls re-claim.
///
/// The retry **policy** (`retry_plan`) is deliberately **not** here — it is static definition, not
/// runtime state. A `Task` freezes its plan onto itself (self-containment for per-`resource`
/// partitioning); a `Map`/`Parallel` activity resolves its `Retry` array from its ASL definition at
/// failure time. Both entities advance this same run-state.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct RetryState {
    /// Total retries scheduled by this entity so far — the value `$states.context.State.RetryCount`
    /// exposes (an activity binds it from its own `retry_state.attempts`).
    pub attempts: u32,
    /// Per-retrier attempt counters, indexed by position in the resolved `retry_plan`, so each
    /// retrier's `max_attempts`/backoff ladder is independent. A missing entry means it never fired.
    pub retrier_attempts: Vec<RetrierAttemptState>,
    /// Earliest wall-clock moment this entity may run again, `Some` only while it waits out a
    /// backoff. A task's poll/assign gates on it; `None` = immediately eligible or already claimed.
    pub next_available_at: Option<Timestamp>,
}

/// Lifecycle status of a Task (the spica name for what Zeebe calls a *job*). Like `TimerStatus`, a
/// task is a leaf side-effect node: it never initiates its own completion — it is either claimed
/// and settled by a worker, or cancelled. The two non-terminal states model the Zeebe job lifecycle:
/// a task is created **pending** (`Pending`), a worker **claims** it (`Running`, leased to that
/// worker until `lease_until`), and only the leasing worker's `CompleteTask`/`FailTask` settles it
/// — otherwise it re-queues (`Pending`) when its lease expires. Splitting the worker out of the
/// engine's process is what this lifecycle exists for: the engine stays the single writer that
/// validates each transition, the worker is just a (possibly remote) claimant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Created and waiting for a worker to claim it (Zeebe `ACTIVATABLE`/queued). An unclaimed task
    /// sits in this state indefinitely — the engine never predicts whether a worker will appear.
    Pending,
    /// Leased to a worker for `Task::lease_until`; the worker is executing it (Zeebe
    /// `ACTIVATED`).
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    /// Whether this task is still waiting for a worker to claim it (Zeebe `ACTIVATABLE`).
    pub fn is_pending(&self) -> bool {
        matches!(self, TaskStatus::Pending)
    }

    /// Whether a worker currently leases this task (settlement is validated against the lease).
    pub fn is_running(&self) -> bool {
        matches!(self, TaskStatus::Running)
    }

    /// Whether the task is in flight (pending or running) — i.e. not yet settled/cancelled. Used by
    /// cascade/sweep to decide whether a node still needs draining.
    pub fn is_in_flight(&self) -> bool {
        !self.is_terminal()
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
        )
    }
}

/// The event-/domain-carried value of a Task.
///
/// A task is an in-flight external call invoked by a `Task` state (`"Type": "Task"`) — a call to
/// a connected `Resource` with projected `arguments` as input. The value carries the task's own
/// domain facts; storage may wrap it so the domain/projection boundary stays explicit, just as it
/// does for `Activity` and `Timer`.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
    /// Shared identity + timing metadata. The domain `created_at`/`updated_at` (stamped at each
    /// lifecycle-transition emit) live inside `meta`, whose `uid` is the task's stable identity.
    pub meta: ObjectMeta,
    /// The owning top-level run (the flat execution anchor), always carried so a task (wherever it
    /// lives in a branch) is traceable to, and nameable from, its root run — the same ownership
    /// channel an `Activity`/`Timer` carries. `meta.owner` is the immediate invoking activity; this
    /// is the run itself.
    pub execution: ObjectReference,
    /// The `Resource` URI the task calls (a downstream service / activity identifier).
    pub resource: String,
    /// The projected `arguments` passed to the resource as its input payload.
    pub arguments: Value,
    pub status: TaskStatus,
    /// Optional deadline (the state's `TimeoutSeconds`) after which the task is treated as failed
    /// with `States.Timeout`. `None` if the Task state has no timeout.
    pub deadline: Option<Timestamp>,
    /// The worker that currently leases this task, set when a worker claims it (`Running`). A
    /// `CompleteTask`/`FailTask` is accepted only from this worker (the Zeebe lease-ownership
    /// invariant); cleared when the lease expires or the task settles.
    #[serde(default)]
    pub worker_id: Option<String>,
    /// Wall-clock lease expiry for the claiming worker; `Some` iff `status == Running`. When it
    /// passes without a settle, the task returns to `Pending` (re-claimable). Distinct from
    /// `deadline` (the ASL `TimeoutSeconds` terminal backstop): the lease re-queues on a crashed /
    /// stalled worker, the deadline eventually fails the task.
    #[serde(default)]
    pub lease_until: Option<Timestamp>,
    /// The frozen `Retry` policy resolved at the task's first activation (empty = no retry). The
    /// task decides retry from this alone, so it never revisits the owning state's definition.
    #[serde(default)]
    pub retry_plan: Vec<RetryPolicy>,
    /// The task's retry **run-state** (`attempts` / `retrier_attempts` / `next_available_at`) — the
    /// shared `RetryState` every retryable entity carries. Policy lives in `retry_plan`; this holds
    /// only what advances as retries fire.
    #[serde(default)]
    pub retry_state: RetryState,
}

impl Task {
    /// The task's stable identity, derived from `meta` — the canonical `obj-<uid>` reference a
    /// caller uses to address the task. Retries re-use the same entity, so `meta` (and hence this
    /// reference) is stable across retry attempts.
    pub fn reference(&self) -> ObjectReference {
        ObjectReference::new(ObjectKind::Task, self.meta.name.clone(), self.meta.uid)
    }
}
