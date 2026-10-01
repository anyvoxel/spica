use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;

use crate::types::activity::ActivityKind;
use crate::types::execution::ExecutionKind;
use crate::types::meta::{ObjectKind, ObjectKindMarker, ObjectMeta, ObjectRef};
use spica_machinery::Timestamp;

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
/// worker until `lease_expires_at`), and only the leasing worker's `CompleteTask`/`FailTask` settles
/// it — otherwise it is claimable again once that instant passes. Splitting the worker out of the
/// engine's process is what this lifecycle exists for: the engine stays the single writer that
/// validates each transition, the worker is just a (possibly remote) claimant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Created and waiting for a worker to claim it (Zeebe `ACTIVATABLE`/queued). An unclaimed task
    /// sits in this state indefinitely — the engine never predicts whether a worker will appear.
    Pending,
    /// Leased to a worker until `Task::lease_expires_at`; the worker is executing it (Zeebe
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

/// The [`ObjectKindMarker`] tying a [`Task`]'s meta to [`ObjectKind::Task`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskKind;

impl ObjectKindMarker for TaskKind {
    const KIND: ObjectKind = ObjectKind::Task;
    /// A task is always invoked by the `Task` state's activity — exactly one kind, so the slot is an
    /// [`ObjectRef`] rather than a union: a task whose owner is not an activity is unrepresentable.
    type OwnedBy = ObjectRef<ActivityKind>;
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
    pub meta: ObjectMeta<TaskKind>,
    /// The owning top-level run (the flat execution anchor), always carried so a task (wherever it
    /// lives in a branch) is traceable to, and nameable from, its root run — the same ownership
    /// channel an `Activity`/`Timer` carries. `meta.owner` is the immediate invoking activity; this
    /// is the run itself.
    pub execution: ObjectRef<ExecutionKind>,
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
    /// invariant); cleared when the task settles or is re-claimed. It is deliberately *not* cleared
    /// when the lease lapses: a stale lease leaves the task claimable (see
    /// [`Task::is_claimable_at`]) but still leased, so the lapsed worker's late settle is accepted —
    /// the work is already done, and re-queueing it would run it twice.
    #[serde(default)]
    pub worker_id: Option<String>,
    /// Wall-clock lease expiry for the claiming worker; `Some` iff `status == Running`. Once it
    /// passes without a settle the task is claimable again — by another worker (which takes it over)
    /// or by the same one re-polling. Distinct from `deadline` (the ASL `TimeoutSeconds` terminal
    /// backstop): the lease frees a crashed / stalled worker's task for re-claim, the deadline
    /// eventually fails the task.
    #[serde(default)]
    pub lease_expires_at: Option<Timestamp>,
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
    /// Whether a worker may claim this task *at* `now`: a `Pending` task whose retry backoff gate has
    /// lapsed, or a `Running` task whose delivery lease has expired.
    ///
    /// The lease half is the engine's **lazy re-claim**: no timer is armed per claim, so an
    /// un-settled lease is reclaimed by whichever poll observes it expired rather than by a deadline
    /// firing. Liveness therefore depends on a worker polling again — the price of not carrying a
    /// durable timer per claim. Both the discovery scan (storage) and the claim's conditional fold
    /// (the applier) decide through this one predicate, so what a poll grants is exactly what the fold
    /// accepts.
    pub fn is_claimable_at(&self, now: Timestamp) -> bool {
        match self.status {
            TaskStatus::Pending => !self
                .retry_state
                .next_available_at
                .is_some_and(|at| now < at),
            TaskStatus::Running => self.lease_expires_at.is_some_and(|at| at <= now),
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled => false,
        }
    }

    /// Mark this task claimed: leased to `worker_id` until `lease_expires_at`, stamped as claimed at
    /// `at`. The single write behind a claim, so every grant leases identically.
    ///
    /// No timer is armed for the lease: expiry is decided lazily by whichever poll next observes
    /// [`Self::is_claimable_at`], so a claim writes no side-effect child and the only durable trace of
    /// the window is `lease_expires_at` here. The retry backoff gate is spent by the claim — cleared so
    /// a re-claim after a lapsed lease is immediately claimable rather than inheriting a stale wait.
    pub fn claim(&mut self, worker_id: &str, lease_expires_at: Timestamp, at: Timestamp) {
        self.status = TaskStatus::Running;
        self.worker_id = Some(worker_id.to_string());
        self.lease_expires_at = Some(lease_expires_at);
        // `created_at` is already carried on `meta`; only the transition moment moves.
        self.meta.with_update_at(at);
        self.retry_state.next_available_at = None;
    }

    /// Mark this task cancelled at `at`: the row copies forward with only the terminal status and
    /// the transition stamp moved. The delivery lease is deliberately left as it stands — the worker's
    /// own call is not disturbed by a cancel, and the terminal status is what withholds the task from
    /// every future poll.
    ///
    /// The whole `meta` must travel unchanged. Re-deriving the name from the kind and uid alone would
    /// rename the node, and a task's owner matches the child it holds by name — a renamed task never
    /// drains, so the teardown that cancelled it stalls.
    pub fn cancel(&mut self, at: Timestamp) {
        self.status = TaskStatus::Cancelled;
        self.meta.with_update_at(at);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::meta::ObjectName;

    fn ts(ms: u64) -> Timestamp {
        Timestamp::from_millis(ms)
    }

    /// A task in `status`, born at `ts(0)`, with no lease and no retry gate — each case arms the
    /// fields it is about.
    fn at_status(status: TaskStatus) -> Task {
        Task {
            meta: ObjectMeta::builder(ulid::Ulid::new())
                .timestamps(ts(0), ts(0))
                .with_owner(ObjectRef::new(
                    ObjectName::plain("invoke").unwrap(),
                    ulid::Ulid::new(),
                )),
            execution: ObjectRef::new(
                ObjectName::from_parsed("execution-0").expect("a generated form is a valid name"),
                ulid::Ulid::nil(),
            ),
            resource: "service-a".to_string(),
            arguments: Value::Null,
            status,
            deadline: None,
            worker_id: None,
            lease_expires_at: None,
            retry_plan: Vec::new(),
            retry_state: RetryState::default(),
        }
    }

    /// A `Pending` task is claimable immediately; a retry gate defers it exactly until the gate
    /// instant — the boundary itself is claimable, since the gate is a *wait-until*, not a
    /// *wait-past* (a retry may fire the moment its backoff is up).
    #[test]
    fn pending_is_claimable_once_its_backoff_gate_is_reached() {
        let mut t = at_status(TaskStatus::Pending);
        assert!(
            t.is_claimable_at(ts(0)),
            "an ungated task is claimable at once"
        );
        t.retry_state.next_available_at = Some(ts(500));
        assert!(!t.is_claimable_at(ts(499)));
        assert!(t.is_claimable_at(ts(500)));
    }

    /// A `Running` task is withheld from every poll until its delivery lease lapses — the boundary
    /// being claimable is what makes a crashed worker's task recoverable by the next poll. A
    /// `Running` task carrying no lease is never claimable: it is leased to somebody, with no
    /// instant at which that lease ends.
    #[test]
    fn running_is_claimable_only_once_its_lease_lapses() {
        let mut t = at_status(TaskStatus::Running);
        assert!(
            !t.is_claimable_at(ts(0)),
            "a Running task with no lease is unclaimable"
        );
        t.lease_expires_at = Some(ts(1_000));
        assert!(!t.is_claimable_at(ts(999)));
        assert!(t.is_claimable_at(ts(1_000)));
    }

    /// A settled task is never claimable, however stale its lease and gate look — the terminal
    /// status is what withholds it, not the fields a claim would have left behind.
    #[test]
    fn terminal_tasks_are_never_claimable() {
        for status in [
            TaskStatus::Completed,
            TaskStatus::Failed,
            TaskStatus::Cancelled,
        ] {
            let mut t = at_status(status);
            t.lease_expires_at = Some(ts(1));
            t.retry_state.next_available_at = Some(ts(1));
            assert!(
                !t.is_claimable_at(ts(10_000)),
                "{status:?} must not be claimable"
            );
        }
    }

    /// A claim is the whole transition in one write: leased to the worker until the expiry, stamped
    /// at the claim moment (only `updated_at` moves — the birth stamp is not a claim's to move), and
    /// the backoff gate is spent by the claim rather than carried into the leased lifetime.
    #[test]
    fn claim_leases_stamps_and_spends_the_gate() {
        let mut t = at_status(TaskStatus::Pending);
        t.retry_state.next_available_at = Some(ts(500));
        t.claim("w1", ts(1_000), ts(200));
        assert_eq!(t.status, TaskStatus::Running);
        assert_eq!(t.worker_id.as_deref(), Some("w1"));
        assert_eq!(t.lease_expires_at, Some(ts(1_000)));
        assert_eq!(t.meta.created_at, ts(0));
        assert_eq!(t.meta.updated_at, ts(200));
        assert_eq!(t.retry_state.next_available_at, None);
    }

    /// The two halves of one contract: a claimed task is exactly what `is_claimable_at` withholds
    /// until the lease it was just given lapses — and a re-claim moves the lease to the new worker,
    /// which is how a poll takes over a lapsed lease.
    #[test]
    fn a_reclaim_moves_the_lease_and_agrees_with_claimability() {
        let mut t = at_status(TaskStatus::Running);
        t.worker_id = Some("stale".to_string());
        t.lease_expires_at = Some(ts(100));
        // Lapsed, so a poll may claim it: taking over leaves the new lease and owner behind.
        assert!(t.is_claimable_at(ts(200)));
        t.claim("w2", ts(1_000), ts(200));
        assert_eq!(t.worker_id.as_deref(), Some("w2"));
        assert_eq!(t.lease_expires_at, Some(ts(1_000)));
        assert!(!t.is_claimable_at(ts(999)));
        assert!(t.is_claimable_at(ts(1_000)));
    }

    /// A cancel moves the status and the transition stamp and nothing else: the identity its owner
    /// matches the child by (`reference` — name and uid together), the delivery lease, and the retry
    /// run-state all survive, so the cancelling sweep can still detach the edge it holds.
    #[test]
    fn cancel_moves_only_the_status_and_the_stamp() {
        let mut t = at_status(TaskStatus::Running);
        t.worker_id = Some("w1".to_string());
        t.lease_expires_at = Some(ts(1_000));
        t.retry_state.attempts = 2;
        let before = t.meta.reference();
        t.cancel(ts(200));
        assert_eq!(t.status, TaskStatus::Cancelled);
        assert_eq!(t.meta.created_at, ts(0));
        assert_eq!(t.meta.updated_at, ts(200));
        assert_eq!(t.meta.reference(), before);
        assert_eq!(
            t.worker_id.as_deref(),
            Some("w1"),
            "the worker's own call is not disturbed by a cancel"
        );
        assert_eq!(t.lease_expires_at, Some(ts(1_000)));
        assert_eq!(t.retry_state.attempts, 2);
    }

    /// A cancelled task is terminal, so no poll may ever grant it again — including the poll that
    /// would otherwise see the still-recorded lease lapse. The terminal status outranks the lease
    /// fields a claim left behind.
    #[test]
    fn a_cancelled_task_is_terminal_and_never_claimable() {
        let mut t = at_status(TaskStatus::Running);
        t.claim("w1", ts(1_000), ts(0));
        assert!(!t.is_claimable_at(ts(999)));
        t.cancel(ts(200));
        assert!(t.status.is_terminal());
        assert!(
            !t.is_claimable_at(ts(1_000)),
            "a lapsed lease must not free a cancelled task"
        );
        assert!(!t.is_claimable_at(ts(10_000)));
    }

    /// A task's owner slot admits an `Activity` and nothing else: the row reads back with the same
    /// owner, and a payload whose `owner` carries another kind is refused — so no reader ever holds a
    /// task whose owner is not the activity that invoked it.
    #[test]
    fn a_task_slot_admits_only_an_activity_owner() {
        let owner =
            ObjectRef::<ActivityKind>::new(ObjectName::plain("invoke").unwrap(), ulid::Ulid::new());
        let mut task = at_status(TaskStatus::Pending);
        task.meta = task.meta.with_owner(owner.clone());
        let mut json = serde_json::to_value(&task).expect("task serializes");
        assert_eq!(json["meta"]["owner"]["kind"], serde_json::json!("Activity"));
        let back: Task = serde_json::from_value(json.clone()).expect("the slot admits its kind");
        assert_eq!(back.meta.owner, owner);

        json["meta"]["owner"]["kind"] = serde_json::json!("Thread");
        let err =
            serde_json::from_value::<Task>(json).expect_err("a task is never owned by a thread");
        assert!(err.to_string().contains("reference kind mismatch"), "{err}");
    }
}
