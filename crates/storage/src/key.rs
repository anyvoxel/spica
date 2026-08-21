//! Canonical key encoding for the [`Storage`](spica_engine::Storage) projections.
//!
//! Every persisted row lives at a **single**, human-debuggable text key of the form
//!
//! ```text
//! /<tenant>/<namespace>/<kind>/<identifier>
//! ```
//!
//! (prefixed by `/` and joined by `/`). The leading two segments are a fixed `Scope` (tenant +
//! namespace) — the reservoir the engine fills in [Phase A](Scope::default_scope) and threads
//! per-call in Phase B. Three consequences follow from this shape:
//!
//! - **Fixed position = fixed meaning.** Tenant/namespace are always the first two segments, so a
//!   range-scan over `/<t>/<ns>/<kind>/` addresses exactly one kind in one scope — never a
//!   variable-structure ambiguity (contrast a filesystem-style key where scope depth varies).
//! - **`/` is the segment separator**, so no segment value may contain `/` (and none may be empty).
//!   `FlowName` already forbids `/` (its charset is `[A-Za-z0-9_]`); `Scope` validation enforces the
//!   same rule for tenant/namespace plus a lowercase `[a-z0-9_-]` charset so ordering/scans stay
//!   deterministic and case-collisions are impossible.
//! - **Kinds are flat, single-address-space.** Entity rows never nest under another entity, so no
//!   prefix scan is polluted by foreign rows.
//!
//! ```text
//! Reserved spellings
//!   _index   — namespace for *derived indexes* (lookup tables), structurally separate from entity rows.
//!   _global  — the reserved namespace value marking a global / non-namespaced entity. Not yet produced:
//!              every current entity is tenant data, so Phase A emits only the default scope.
//! ```
//!
//! The phase-A layout (default scope):
//!
//! | kind | key | value |
//! |---|---|---|
//! | `execution` | `/<t>/<ns>/execution/<exec_id>` | Execution row |
//! | `activity` | `/<t>/<ns>/activity/<act_id>` | Activity row |
//! | `timer` | `/<t>/<ns>/timer/<timer_id>` | Timer row |
//! | `task` | `/<t>/<ns>/task/<task_id>` | Task row |
//! | `flow` | `/<t>/<ns>/flow/<flow_name>` | Flow row (name is the primary key) |
//! | `flowversion` | `/<t>/<ns>/flowversion/<fvid>` | FlowVersion row |
//! | `_index` | `/<t>/<ns>/_index/flowversion/<flow_id>/<ver-8hex>` | → `FlowVersionId` |
//! | `_global` | `/<t>/<ns>/_global/last_processed_position` | resume watermark (`i64`) |
//!
//! The version index tail is fixed-width **lowercase hex** (`{:08x}`) so that, within one
//! `flow_id`, a range scan yields versions in ascending ordinal order (lexicographic order of a
//! zero-padded hex string equals numeric order).

use spica_engine::{
    ActivityId, ExecutionError, ExecutionId, FlowId, FlowName, FlowVersionId, TaskId, TimerId,
};

/// The two fixed scope segments of every key (tenant + namespace).
///
/// Segment values must be non-empty, at most 64 chars, `[a-z0-9_-]`, and never contain `/` (which is
/// the key's segment separator). Enforced in [`Scope::new`] so an invalid scope fails fast at the
/// builder boundary rather than producing an ambiguous/possibly-colliding key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Scope {
    tenant: String,
    namespace: String,
}

impl Scope {
    /// Construct a scope, validating both segments against the key rules.
    pub fn new(tenant: &str, namespace: &str) -> Result<Self, ExecutionError> {
        validate_segment("tenant", tenant)?;
        validate_segment("namespace", namespace)?;
        Ok(Self {
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
        })
    }

    /// The single default scope used in **Phase A** until per-call tenant plumbing lands (Phase B).
    /// Every Phase-A row is tenant data, so one scope suffices; the placeholder `_`/`default` keeps
    /// the two-segment shape explicit without implying a real tenant.
    ///
    /// TODO(scope-addressing): Phase A works only because every row lives under this one pinned
    /// scope — the id-keyed reads (`get_flow_version`, `get_execution`, …) "don't need" a scope only
    /// because the KeyBuilder already bakes it in. **A globally-unique id is not self-locating**:
    /// the physical key is `<scope>/<kind>/<id>`, so in a shared multi-scope store an id alone cannot
    /// even construct the read key. Consequence: once multi-tenancy is real (Phase B), `scope` must
    /// thread through the **whole addressing path** — recorded on the command/event (single source
    /// of truth), and carried by *every* Storage read, not just the name-keyed `get_flow_by_name`.
    /// Two self-consistent models: (1) every `Storage` method takes a scope; (2) per-tenant store
    /// instances behind a `Scope -> Storage` router, scope coming from the command either way.
    pub fn default_scope() -> Self {
        Self::new("_", "default").expect("default scope is a valid key prefix")
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

/// Validate a single scope segment: non-empty, ≤64 chars, lowercase `[a-z0-9_-]`, no `/`.
fn validate_segment(kind: &str, raw: &str) -> Result<(), ExecutionError> {
    let bytes = raw.as_bytes();
    let valid = !raw.is_empty()
        && raw.len() <= 64
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_' || *b == b'-');
    if !valid {
        return Err(ExecutionError::InvalidDefinition(format!(
            "invalid {kind} {raw:?}: must be 1..=64 chars of [a-z0-9_-] and never contain '/'"
        )));
    }
    Ok(())
}

/// The entity kinds addressable as flat rows. `_index` and `_global` are *reserved spellings* — no
/// entity kind may be named that way — so the `Kind` variants never overlap the index namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    Execution,
    Activity,
    Timer,
    Task,
    Flow,
    FlowVersion,
}

impl Kind {
    /// The kind's key segment (a stable, single-word token). `FlowVersion` uses `flowversion` — the
    /// same spelling appears as the `_index` target for the version lookup table.
    pub fn segment(self) -> &'static str {
        match self {
            Kind::Execution => "execution",
            Kind::Activity => "activity",
            Kind::Timer => "timer",
            Kind::Task => "task",
            Kind::Flow => "flow",
            Kind::FlowVersion => "flowversion",
        }
    }
}

/// Builds the canonical key for every storage row / index under one [`Scope`].
///
/// A single `KeyBuilder` holds a `Scope` and derives every key from it, so kind encoding is defined
/// in one place (not inlined as string concatenation across the store) and Phase B only has to swap
/// the scope the builder is constructed with. All keys are produced by joining slash-`/`-separated
/// segments with a leading slash.
#[derive(Debug, Clone)]
pub struct KeyBuilder {
    scope: Scope,
}

impl KeyBuilder {
    pub fn new(scope: Scope) -> Self {
        Self { scope }
    }

    /// Join the scope + an entity row's `kind`/`identifier` into the canonical text key.
    fn row(&self, kind: Kind, identifier: &str) -> Vec<u8> {
        join(&[
            self.scope.tenant(),
            self.scope.namespace(),
            kind.segment(),
            identifier,
        ])
    }

    /// Execution row: `/<t>/<ns>/execution/<exec_id>`.
    pub fn execution(&self, id: ExecutionId) -> Vec<u8> {
        self.row(Kind::Execution, &id.0.to_string())
    }

    /// Activity row: `/<t>/<ns>/activity/<act_id>`.
    pub fn activity(&self, id: ActivityId) -> Vec<u8> {
        self.row(Kind::Activity, &id.0.to_string())
    }

    /// Timer row: `/<t>/<ns>/timer/<timer_id>`.
    pub fn timer(&self, id: TimerId) -> Vec<u8> {
        self.row(Kind::Timer, &id.0.to_string())
    }

    /// Task row: `/<t>/<ns>/task/<task_id>`.
    pub fn task(&self, id: TaskId) -> Vec<u8> {
        self.row(Kind::Task, &id.0.to_string())
    }

    /// The task-row keys prefix `/<t>/<ns>/task/` — the range start for a forward scan over **all**
    /// task rows (e.g. [`Storage::activatable_tasks`](spica_engine::Storage::activatable_tasks),
    /// which has no id to point-read and must enumerate). The trailing `/` binds the scan to exactly
    /// the `task` kind even though its id segment is appended verbatim.
    pub fn task_prefix(&self) -> Vec<u8> {
        join(&[
            self.scope.tenant(),
            self.scope.namespace(),
            Kind::Task.segment(),
            "",
        ])
    }

    /// Flow row: `/<t>/<ns>/flow/<flow_name>` — the name *is* the primary key, so the identifier
    /// segment is the name verbatim. `FlowName`'s `[A-Za-z0-9_]` charset already excludes `/`, making
    /// it a safe segment.
    pub fn flow(&self, name: &FlowName) -> Vec<u8> {
        self.row(Kind::Flow, name.as_str())
    }

    /// FlowVersion row: `/<t>/<ns>/flowversion/<fvid>`.
    pub fn flow_version(&self, id: FlowVersionId) -> Vec<u8> {
        self.row(Kind::FlowVersion, &id.0.to_string())
    }

    /// The version lookup key under the reserved `_index` namespace:
    /// `/<t>/<ns>/_index/flowversion/<flow_id>/<ver-8hex>` → value is the `FlowVersionId`. The
    /// fixed-width lowercase-hex version keeps a range scan over the flow's prefix in ordinal order.
    pub fn flow_version_index(&self, flow_id: FlowId, version: u32) -> Vec<u8> {
        join(&[
            self.scope.tenant(),
            self.scope.namespace(),
            "_index",
            Kind::FlowVersion.segment(),
            &flow_id.0.to_string(),
            &format!("{version:08x}"),
        ])
    }

    /// Global (non-entity) progress scalar under the reserved `_global` namespace:
    /// `/<t>/<ns>/_global/last_processed_position`. Holds the StreamProcessor's resume watermark; a single
    /// fixed key per store rather than a per-row address (it is a scalar, not an entity).
    pub fn last_processed_position(&self) -> Vec<u8> {
        join(&[
            self.scope.tenant(),
            self.scope.namespace(),
            "_global",
            "last_processed_position",
        ])
    }
}

/// Join segments with `/`, prefixed by a leading `/` (so the key is fully self-delimiting).
fn join(segments: &[&str]) -> Vec<u8> {
    let mut key = Vec::new();
    for s in segments {
        key.push(b'/');
        key.extend_from_slice(s.as_bytes());
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    fn scope(tenant: &str, ns: &str) -> Scope {
        Scope::new(tenant, ns).unwrap()
    }

    #[test]
    fn entity_row_keys_match_the_spec() {
        let kb = KeyBuilder::new(scope("acme", "prod"));
        let exec = ExecutionId::from(Ulid::new());
        let act = ActivityId::from(Ulid::new());
        let tim = TimerId::from(Ulid::new());
        let task = TaskId::from(Ulid::new());
        let fvid = FlowVersionId::from(Ulid::new());
        let name = FlowName::new("order").unwrap();

        let exec_key = String::from_utf8(kb.execution(exec)).unwrap();
        assert!(exec_key.starts_with("/acme/prod/execution/"));
        assert!(exec_key.ends_with(&exec.0.to_string()));
        assert!(
            String::from_utf8(kb.activity(act))
                .unwrap()
                .starts_with("/acme/prod/activity/")
        );
        assert!(
            String::from_utf8(kb.timer(tim))
                .unwrap()
                .starts_with("/acme/prod/timer/")
        );
        assert!(
            String::from_utf8(kb.task(task))
                .unwrap()
                .starts_with("/acme/prod/task/")
        );
        assert_eq!(
            String::from_utf8(kb.flow(&name)).unwrap(),
            "/acme/prod/flow/order"
        );
        assert!(
            String::from_utf8(kb.flow_version(fvid))
                .unwrap()
                .starts_with("/acme/prod/flowversion/")
        );
    }

    #[test]
    fn different_kinds_never_share_a_key() {
        let kb = KeyBuilder::new(scope("acme", "prod"));
        let id = Ulid::new();
        let exec = String::from_utf8(kb.execution(ExecutionId::from(id))).unwrap();
        // Same raw ULID under different kinds must not collide.
        let act = String::from_utf8(kb.activity(ActivityId::from(id))).unwrap();
        let fv = String::from_utf8(kb.flow_version(FlowVersionId::from(id))).unwrap();
        assert_ne!(exec, act);
        assert_ne!(exec, fv);
    }

    #[test]
    fn version_index_is_fixed_width_ordered() {
        let kb = KeyBuilder::new(scope("acme", "prod"));
        let flow = FlowId::from(Ulid::new());
        let v1 = kb.flow_version_index(flow, 1);
        let v2 = kb.flow_version_index(flow, 2);
        let v10 = kb.flow_version_index(flow, 10);
        // Zero-padded lowercase hex: lexicographic order == numeric order.
        assert!(v1 < v2 && v2 < v10);
        assert!(
            String::from_utf8(v1.clone())
                .unwrap()
                .ends_with("/00000001")
        );
        assert!(String::from_utf8(v10).unwrap().ends_with("/0000000a"));
        // Two flows never share a version key even with identical version ordinals.
        let other = FlowId::from(Ulid::new());
        assert_ne!(
            kb.flow_version_index(flow, 1),
            kb.flow_version_index(other, 1)
        );
    }

    #[test]
    fn scope_validates_segments() {
        assert!(Scope::new("acme", "prod").is_ok());
        assert!(Scope::new("", "prod").is_err()); // empty tenant
        assert!(Scope::new("acme", "").is_err()); // empty ns
        assert!(Scope::new("ac/e", "prod").is_err()); // slash forbidden
        assert!(Scope::new("Acme", "prod").is_err()); // uppercase forbidden
        assert!(Scope::new("ac me", "prod").is_err()); // space forbidden
        assert!(Scope::new(&"a".repeat(65), "prod").is_err()); // too long
        // The Phase-A default scope is valid and emits the two-segment prefix.
        assert_eq!(
            String::from_utf8(
                KeyBuilder::new(Scope::default_scope()).execution(ExecutionId::from(Ulid::nil()))
            )
            .unwrap(),
            "/_/default/execution/00000000000000000000000000"
        );
    }
}
