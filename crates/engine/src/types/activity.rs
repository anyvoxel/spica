use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::command::TerminationReason;
use crate::types::meta::{ObjectKind, ObjectMeta, ObjectReference};
// `RetryState` is the shared retry run-state defined alongside the task types it references
// (`task::RetrierAttemptState`); an activity embeds the same struct a task does.
use crate::types::task::RetryState;

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

/// The **state-specific runtime repository** of an Activity — data only a container state carries,
/// kept as a typed enum instead of `Option`/empty-collection fields on the shared skeleton.
///
/// The shared lifecycle skeleton (`id`/`parent`/`state`/`status`/`raw_input`/`input`/`retry_state`/
/// `raw_output`/`output`/...) lives on `Activity` directly because every state needs it; this
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
    /// Branch index → child execution, populated by `Event::ThreadCreated` (from each thread's own
    /// `index`) as branches fan out, so convergence can aggregate outputs in declaration order.
    pub branches: HashMap<usize, ObjectReference>,
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
    /// Item index → child execution, populated by `ThreadCreated` as items fan out.
    pub children: HashMap<usize, ObjectReference>,
}

/// The event-carried domain value of an Activity.
///
/// This is the entity-shaped payload Activity lifecycle events carry. It intentionally excludes
/// projection-only bookkeeping such as `active_children`; those remain on `storage::ActivityRecord`, the
/// storage projection row, so event payloads stay focused on the Activity's own domain state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Activity {
    /// Shared identity + timing metadata. `meta.uid` is the activity's identity (durable object uid);
    /// the domain `created_at`/`updated_at` (stamped at each lifecycle-transition emit) live inside
    /// `meta`. Use [`Activity::reference`] to obtain the canonical [`ObjectReference`].
    pub meta: ObjectMeta,
    /// The execution this activity belongs to — **always** the top-level [`Execution`]'s reference
    /// (the flat query anchor shared by the whole tree), regardless of how deep the activity sits in
    /// a `Parallel` branch / `Map` item. The activity's *immediate* container — the scope it lives
    /// inside (`Execution` or `Thread`) — is **not** stored here; it is `meta.owner`. So `execution`
    /// names a real `Execution` by construction: a top-level run owns its own activities directly,
    /// and a nested activity's owner is a `Thread`, never another `Execution`. (Renamed from
    /// `root_execution`: the old `execution` duplicated `meta.owner` and could hold a `Thread`,
    /// which the field's name lied about.)
    pub execution: ObjectReference,
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
    /// view is directly inspectable for debugging/auditing without re-running the projection. It is
    /// **not** determined at entry, so `StateActivating` pins it to `Null`; the processed value is
    /// carried on `Event::StateActivated`.
    pub input: Value,
    /// The state's **raw result** before any complete-step `Output` projection. For a `Task` this is
    /// the `Resource`'s returned payload; for a synchronous state with no distinct raw result it is
    /// the processed `input` (see [`state_raw_result`](crate::handlers::state_raw_result) — the same
    /// derivation `$states.result` uses). Unlike `input` (whose processed view isn't known at entry,
    /// so `StateActivating` pins it to `Null`), the raw result *is* derivable before the complete step
    /// runs, so both complete-phase events (`StateCompleting`/`StateCompleted`) carry it rather than a
    /// `null`. Kept distinct from `output` so the engine preserves both the pre-projection and the
    /// final projected view of the state's result.
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

impl Activity {
    /// The canonical [`ObjectReference`] for this activity, derived from its `meta` (kind, generated
    /// `obj-<uid>` name, and uid) — the identity every other object uses to reference the activity.
    pub fn reference(&self) -> ObjectReference {
        ObjectReference::new(ObjectKind::Activity, self.meta.name.clone(), self.meta.uid)
    }
}
