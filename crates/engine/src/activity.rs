use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::command::TerminationReason;
use crate::id::{ActivityId, ExecutionId, NodeId};
use crate::log::Timestamp;

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
}

/// Per-retrier retry bookkeeping for a single `Retry` entry.
///
/// `attempt_count` tracks how many retry opportunities this retrier has already consumed; it is the
/// counter used for `MaxAttempts` enforcement and for computing the next backoff step. `last_retry_at`
/// records when this retrier most recently scheduled a retry. That timestamp is observational state
/// useful for inspection and future policies; the authoritative **when the retry will fire** fact still
/// lives on the paired `Timer { purpose: TaskRetryDelay, deadline }`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct RetrierAttemptState {
    /// How many retries this retrier has already scheduled.
    pub attempt_count: u32,
    /// When this retrier last scheduled a retry, if it has ever done so.
    pub last_retry_at: Option<Timestamp>,
}

/// Retry-specific runtime state for an Activity.
///
/// This state is **orthogonal** to `ActivityState`: every activity may use `Retry`, not only
/// container states, so retry bookkeeping lives in its own dedicated payload rather than being mixed
/// into the per-state-type repository. It records two different retry views:
///
/// - `retry_count` — the total number of retries this activity has scheduled so far; this is what
///   `$states.context.State.RetryCount` exposes to JSONata and what operators usually want to inspect
///   at a glance.
/// - `retrier_attempts` — per-`Retry`-entry state, indexed by the retrier's position in the ASL
///   `Retry` array, so each retrier's `MaxAttempts`/backoff sequence is independent of the others.
///
/// The next retry's **deadline** is deliberately not stored here: the schedule fact belongs to the
/// timer tree (`TimerPurpose::TaskRetryDelay` + `Timer.deadline`). `RetryState` answers "which retry
/// attempt are we on, and when did this retrier last schedule one?"; the timer answers "when will the
/// next retry fire?".
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct RetryState {
    /// Total retries scheduled for this activity so far. Bound to
    /// `$states.context.State.RetryCount`.
    pub retry_count: u32,
    /// Per-retrier retry state, indexed by the retrier's position in the state's `Retry` array.
    /// A missing entry means the retrier has not been used yet.
    pub retrier_attempts: Vec<RetrierAttemptState>,
}

/// The **state-specific runtime repository** of an Activity — data only a container state carries,
/// kept as a typed enum instead of `Option`/empty-collection fields on the shared skeleton.
///
/// The shared lifecycle skeleton (`id`/`parent`/`state`/`status`/`raw_input`/`input`/`retry_state`/
/// `raw_output`/`output`/...) lives on `ActivityValue` directly because every state needs it; this
/// enum holds only what *some* states need. A new container state (or a Map gaining
/// `ItemSelector`/`ToleratedFailure*`) adds a variant rather than widening the shared struct.
///
/// - `Leaf` — no state-specific runtime data (Pass/Wait/Task/Choice/Succeed/Fail).
/// - `Parallel(ParallelActivityState)` — the branch index → child execution fan-out map, so
///   convergence aggregates branch outputs in declaration order.
/// - `Map(MapActivityState)` — the `Map` iteration plan (items/total/cap) harvested from the
///   activation product on `Event::StateActivated`. The running completed/failed tallies are **not**
///   stored here: they are derived live from the terminal status of the parallel child executions, so
///   a follower rebuilds them from each child's own terminal event without extra projections.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub enum ActivityState {
    #[default]
    Leaf,
    Parallel(ParallelActivityState),
    Map(MapActivityState),
}

/// A `Parallel` state's activity-level runtime repository — the ordered fan-out mapping. The
/// shared activity value already carries the lifecycle skeleton; this payload holds only the
/// `Parallel`-specific state that exists because this activity is executing a `Parallel`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ParallelActivityState {
    /// Branch index → child execution, populated by `Event::ParallelBranchSpawned` as branches fan
    /// out, so convergence can aggregate outputs in declaration order.
    pub branches: HashMap<usize, ExecutionId>,
}

/// The `Map` state's activity-level runtime repository — the **static activation plan** projected
/// from the `Map` activation product on `Event::StateActivated`. It drives the bounded-concurrency
/// replenish loop in `MapStateHandler`:
///
/// - `items` is the iterable array the `Map` was entered with; `total` is its length (an empty Map
///   converges immediately to an empty array).
/// - `max_concurrency` is the `MaxConcurrency` cap (0 = unlimited, spawn every item up front).
/// - `children` is the item index → child execution map, populated incrementally by
///   `Event::ParallelBranchSpawned` as items fan out (the same path `Parallel` branches take), so
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
    /// Item index → child execution, populated by `ParallelBranchSpawned` as items fan out.
    pub children: HashMap<usize, ExecutionId>,
}

/// The event-carried domain value of an Activity.
///
/// This is the entity-shaped payload Activity lifecycle events carry. It intentionally excludes
/// projection-only bookkeeping such as `active_children`; those remain on `storage::Activity`, the
/// storage projection row, so event payloads stay focused on the Activity's own domain state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivityValue {
    pub id: ActivityId,
    /// The direct owning execution of this activity. Kept explicitly so queries can group an
    /// execution's activities without re-deriving it through `parent`.
    pub execution: ExecutionId,
    /// The top-level execution this activity belongs to (the flat query anchor shared by the whole
    /// tree).
    pub root_execution: ExecutionId,
    /// The owning node — in M1 always `NodeId::Execution`; M2 lets one activity parent another
    /// (`Parallel`/`Map` children).
    pub parent: NodeId,
    /// The complete JSON Pointer (RFC 6901) to this state's definition within the shared machine
    /// document, e.g. `/states/P2` (top-level) or `/states/P1/branches/0/states/P2` (inside a
    /// Parallel branch). The leaf state name (the activity's identity — the state's key in the
    /// enclosing `states` table) is **derived** as the pointer's last token, so it is not duplicated
    /// here. Carried on `Event::StateActivating` so a follower / recovered leader records exactly
    /// where the activity lives without re-deriving it from the machine + parent chain.
    pub state_path: jsonptr::PointerBuf,
    pub status: ActivityStatus,
    /// The **raw** input this state received on entry — the value carried on `Event::StateActivating`.
    /// For a top-level start it is the execution's original input; on a State→State hop it is the
    /// predecessor's output; inside a `Parallel`/`Map` child it is that branch/item's arguments. It is
    /// kept verbatim, distinct from `input` (the value after the state's own input preprocessing,
    /// e.g. projecting `Arguments`), so a follower / auditor can inspect both the original and the
    /// processed view of what the state ran on.
    pub raw_input: Value,
    /// The input this state actually processes — the result of the state's dialogue-level input
    /// preprocessing applied to `raw_input`. For states that consume their raw input verbatim
    /// (Pass/Choice/Wait/Succeed/Fail and Map/Parallel activations without `Arguments`), this is a
    /// copy of `raw_input` (never `None`); for a `Task`/`Parallel` with `Arguments` it is the
    /// projected arguments (the value a `$states` projection would see). Kept so the processed
    /// view is directly inspectable for debugging/auditing without re-running the projection.
    pub input: Value,
    /// The state's **raw result** before any complete-step `Output` projection. For a `Task` this is
    /// the `Resource`'s returned payload; for states whose logical result is produced entirely inside
    /// the complete step this stays `None` until/if such a raw result is explicitly materialized.
    /// Kept distinct from `output` so the engine preserves both the pre-projection and the final
    /// projected view of the state's result.
    pub raw_output: Option<Value>,
    /// The **state-specific runtime repository** — data only a container state's activity carries.
    /// `Leaf` for every non-container state (Pass/Wait/Task/Choice/...), which hold no state-specific
    /// runtime data. A `Parallel` holds its branch index → child execution fan-out map; a `Map` holds
    /// its iteration plan harvested from the activation product. Kept as an enum so state-specific
    /// data is *typed* (not a bunch of `Option`s/empty collections polluting the shared skeleton) and
    /// grows by adding a variant for a new container state.
    pub activity_state: ActivityState,
    /// Retry-specific runtime state: total retry count exposed to `$states.context.State.RetryCount`
    /// plus per-retrier attempt metadata (`attempt_count` and `last_retry_at`).
    pub retry_state: RetryState,
    /// The terminal output once `StateCompleted` lands — the value after the state's complete-step
    /// `Output` projection (or the raw result / processed input when no `Output` is present).
    pub output: Option<Value>,
}
