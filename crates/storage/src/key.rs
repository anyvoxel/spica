//! Canonical key encoding for the [`Storage`](spica_engine_types::Storage) projections.
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
//!   `FlowName` already forbids `/` (its charset is `[A-Za-z0-9_]`), and tenant/namespace are
//!   [`ScopeName`]s — the same `PlainName` charset, so they ban `/` too. Both facts hold **by type**:
//!   neither segment is ever a bare `String` at this layer, so no second check exists to drift.
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
//! | `execution` | `/<t>/<ns>/execution/<name>` | Execution row (name is the primary key) |
//! | `thread` | `/<t>/<ns>/thread/<name>` | Thread row (name is the primary key) |
//! | `activity` | `/<t>/<ns>/activity/<name>` | Activity row (name is the primary key) |
//! | `task` | `/<t>/<ns>/task/<name>` | Task row (name is the primary key) |
//! | `flow` | `/<t>/<ns>/flow/<flow_name>` | Flow row (name is the primary key) |
//! | `flowversion` | `/<t>/<ns>/flowversion/<flow_name>-<ver>` | FlowVersion row |
//! | `_index` | `/<t>/<ns>/_index/children/<p_kind>/<p_name>/<c_kind>/<c_name>` | ownership edge (value: child ref) |
//! | `_index` | `/<t>/<ns>/_index/variables/<thread_name>` | a thread's variable scope (`Variables` map) |
//! | `_global` | `/<t>/<ns>/_global/last_processed_position` | resume watermark (`i64`) |
//!
//! A flow version's key embeds its own `ObjectName` `{flow_name}-{version}` (a plain **decimal**
//! ordinal, no reserved `_index` table). Decimal is human-readable but does **not** order
//! lexicographically (`order-10` sorts before `order-2`), so a range scan over the
//! `/<t>/<ns>/flowversion/{flow_name}-` prefix enumerates a flow's versions and callers order them
//! by the `version` field, never by key order.
//!
//! `_index` holds the tables whose content is **derived** from the event stream yet is not part of
//! an entity's own value — so far the **ownership edge** (one row per `(parent, child)` pair,
//! enumerating a parent's live children by a prefix scan over the parent) and a thread's **variable
//! scope**. Both are kept off the entity row for the same reason: an embedded set, or an embedded
//! scope, would have to be re-attached by every `put_*`, and a writer that forgot would silently
//! drop the parent's children (leaving a container forever undrained) or wipe the run's variables.
//! As separate cells they cannot be clobbered by the entity's own writes.

use spica_engine_types::{
    ActivityKind, ExecutionKind, FlowName, ObjectKind, ObjectName, ObjectRef, RawObjectRef,
    ScopeName, TaskKind, ThreadKind,
};

/// The two fixed scope segments of every key (tenant + namespace).
///
/// Both segments are [`ScopeName`]s, so the key rules hold **by construction**: a `ScopeName` is
/// already a 4..=64 plain name over `[A-Za-z0-9_]` with `-` reserved, and `/` (the key's segment
/// separator) is not in that alphabet. `Scope::new` therefore has nothing left to check and cannot
/// fail — the validation lives in the type, not in a second check here.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Scope {
    tenant: ScopeName,
    namespace: ScopeName,
}

impl Scope {
    pub fn new(tenant: ScopeName, namespace: ScopeName) -> Self {
        Self { tenant, namespace }
    }

    /// The single default scope used in **Phase A** until per-call tenant plumbing lands (Phase B).
    /// Every Phase-A row is tenant data, so one scope suffices; the placeholder pair keeps the
    /// two-segment shape explicit without implying a real tenant. It is the same pair
    /// [`ObjectMetaBuilder`](spica_engine_types::ObjectMetaBuilder) stamps on an object, so a row's
    /// key prefix and its record's own `meta.tenant`/`meta.namespace` never disagree.
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
        let default = || ScopeName::new("default").expect("`default` is a valid plain name");
        Self::new(default(), default())
    }

    pub fn tenant(&self) -> &str {
        self.tenant.as_str()
    }

    pub fn namespace(&self) -> &str {
        self.namespace.as_str()
    }
}

/// The entity kinds addressable as flat rows. `_index` and `_global` are *reserved spellings* — no
/// entity kind may be named that way — so the `Kind` variants never overlap the index namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    Execution,
    Thread,
    Activity,
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
            Kind::Thread => "thread",
            Kind::Activity => "activity",
            Kind::Task => "task",
            Kind::Flow => "flow",
            Kind::FlowVersion => "flowversion",
        }
    }

    /// The storage kind for an engine [`ObjectKind`] — the reverse of the entity reference's kind.
    /// Every entity kind maps onto an exactly-one storage row kind (see the module's key table), so
    /// the Query `ListObjects` facade can derive the scan prefix from an `ObjectKind` alone.
    pub fn from_object(kind: ObjectKind) -> Kind {
        match kind {
            ObjectKind::Execution => Kind::Execution,
            ObjectKind::Thread => Kind::Thread,
            ObjectKind::Activity => Kind::Activity,
            ObjectKind::Task => Kind::Task,
            ObjectKind::Flow => Kind::Flow,
            ObjectKind::FlowVersion => Kind::FlowVersion,
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

    /// Execution row: `/<t>/<ns>/execution/<name>` — keyed by the reference's **addressing `name`**
    /// (a user name, or a system `obj-<uid>` for children), which is now the execution's unique
    /// primary key; the `uid` is a secondary attribute, not the storage key. A name is a safe single
    /// segment (both the user and generated charsets ban `/`).
    pub fn execution(&self, reference: &ObjectRef<ExecutionKind>) -> Vec<u8> {
        self.row(Kind::Execution, &reference.name().as_str())
    }

    /// Thread row: `/<t>/<ns>/thread/<name>` — keyed by the reference's addressing `name` (a generated
    /// `obj-<uid>` for fan-out sub-runs), exactly like the execution row, so every node kind shares one
    /// uniform, human-debuggable key scheme.
    pub fn thread(&self, reference: &ObjectRef<ThreadKind>) -> Vec<u8> {
        self.row(Kind::Thread, &reference.name().as_str())
    }

    /// Activity row: `/<t>/<ns>/activity/<name>`. The activity's generated `obj-<uid>` name is its
    /// primary key (aligned with executions/flows).
    pub fn activity(&self, reference: &ObjectRef<ActivityKind>) -> Vec<u8> {
        self.row(Kind::Activity, &reference.name().as_str())
    }

    /// Task row: `/<t>/<ns>/task/<name>`. The task's generated `obj-<uid>` name is its primary key
    /// (aligned with executions/flows).
    pub fn task(&self, reference: &ObjectRef<TaskKind>) -> Vec<u8> {
        self.row(Kind::Task, &reference.name().as_str())
    }

    /// The task-row keys prefix `/<t>/<ns>/task/` — the range start for a forward scan over **all**
    /// task rows (e.g. [`Storage::activatable_tasks`](spica_engine_types::Storage::activatable_tasks),
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

    /// The generic range-start for a forward scan over **every row of one kind** in this scope:
    /// `/<t>/<ns>/<kind.segment()>/`. The trailing `/` binds the scan to exactly that kind — the
    /// Query `ListObjects` read facade enumerates a kind by ranging this prefix (k8s-style LIST in
    /// storage-key order). Distinct from [`Self::task_prefix`] only in that it takes any [`Kind`].
    pub fn kind_prefix(&self, kind: Kind) -> Vec<u8> {
        join(&[
            self.scope.tenant(),
            self.scope.namespace(),
            kind.segment(),
            "",
        ])
    }

    /// Flow row: `/<t>/<ns>/flow/<flow_name>` — the name *is* the primary key, so the identifier
    /// segment is the name verbatim. `FlowName`'s `[A-Za-z0-9_]` charset already excludes `/`, making
    /// it a safe segment.
    pub fn flow(&self, name: &FlowName) -> Vec<u8> {
        self.row(Kind::Flow, name.as_str())
    }

    /// FlowVersion row: `/<t>/<ns>/flowversion/<flow_name>-<ver>`. The identifier segment is
    /// the version's own `ObjectName` (`{flow_name}-{version}`, see
    /// [`FlowVersion::version_name`](spica_engine_types::FlowVersion::version_name)) — a generated name
    /// whose only `/`-forbidden char would be `/` (banned), so it is a safe single segment.
    pub fn flow_version(&self, name: &ObjectName) -> Vec<u8> {
        self.row(Kind::FlowVersion, &name.as_str())
    }

    /// The range-start for enumerating every version of `flow_name` in ordinal order:
    /// `/<t>/<ns>/flowversion/{flow_name}-`. The trailing `-` (the generated-name separator) binds
    /// the scan to exactly this flow's versions — no other name shares the `{flow_name}-` prefix.
    pub fn flow_version_prefix(&self, flow_name: &FlowName) -> Vec<u8> {
        join(&[
            self.scope.tenant(),
            self.scope.namespace(),
            Kind::FlowVersion.segment(),
            &format!("{flow_name}-"),
        ])
    }

    /// The range-start enumerating **every child of one parent**:
    /// `/<t>/<ns>/_index/children/<parent_kind>/<parent_name>/`. The trailing `/` binds the scan to
    /// exactly this parent's edges, so one parent's children are one contiguous range.
    pub fn child_edge_prefix(&self, parent: &RawObjectRef) -> Vec<u8> {
        let name = parent.name.as_str();
        join(&[
            self.scope.tenant(),
            self.scope.namespace(),
            "_index",
            "children",
            Kind::from_object(parent.kind).segment(),
            name.as_str(),
            "",
        ])
    }

    /// One ownership **edge** row — `parent` owns `child`:
    /// `/<t>/<ns>/_index/children/<parent_kind>/<parent_name>/<child_kind>/<child_name>`, whose value
    /// is the child's full [`RawObjectRef`] (kind + name + uid). The uniform, kind-independent
    /// membership test that replaces a set embedded on the parent row.
    ///
    /// Both ends spell their **kind** into the key instead of trusting the name alone. Names would in
    /// fact separate today (generated names share one partition-local counter, and `-` is reserved so
    /// the user and generated spaces never overlap), but entity rows are keyed `<kind>/<name>`
    /// *because* a name does not identify a row — this table is not the place to take that back, and
    /// a future second counter would silently merge two same-named parents' children.
    pub fn child_edge(&self, parent: &RawObjectRef, child: &RawObjectRef) -> Vec<u8> {
        let parent_name = parent.name.as_str();
        let child_name = child.name.as_str();
        join(&[
            self.scope.tenant(),
            self.scope.namespace(),
            "_index",
            "children",
            Kind::from_object(parent.kind).segment(),
            parent_name.as_str(),
            Kind::from_object(child.kind).segment(),
            child_name.as_str(),
        ])
    }

    /// A thread's **variable scope**, a derived lookup value under the reserved `_index` namespace:
    /// `/<t>/<ns>/_index/variables/<thread_name>`. The scope is not part of the thread's
    /// event-carried value, so it gets its own cell: a whole-row `put_thread` can then never
    /// clobber it, and an assignment never rewrites the thread row.
    ///
    /// No kind segment: a scope belongs to a `Thread` and nothing else, so the name alone addresses
    /// it (unlike an ownership edge, whose *both* ends are any kind).
    pub fn variables(&self, thread_name: &ObjectName) -> Vec<u8> {
        let name = thread_name.as_str();
        join(&[
            self.scope.tenant(),
            self.scope.namespace(),
            "_index",
            "variables",
            name.as_str(),
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

    /// Partition-local monotonic counter for generated-object names:
    /// `/<t>/<ns>/_global/next_generated_seq`. One scalar per processing partition (bound to this
    /// key's scope), so a generated name's suffix is unique **within the partition with zero
    /// cross-partition coordination** (Zeebe's per-partition key generator). It is rebuilt by
    /// re-folding the create events on replay, so it needs no separate log entry. Never global —
    /// a global counter would force cross-partition synchronization.
    pub fn next_generated_seq(&self) -> Vec<u8> {
        join(&[
            self.scope.tenant(),
            self.scope.namespace(),
            "_global",
            "next_generated_seq",
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
    use spica_engine_types::{
        ActivityKind, ExecutionKind, HasRawObjectRef, NoOwner, ObjectMeta, PlainName, TaskKind,
    };
    use ulid::Ulid;

    fn scope(tenant: &str, ns: &str) -> Scope {
        Scope::new(ScopeName::new(tenant).unwrap(), ScopeName::new(ns).unwrap())
    }

    /// An execution reference for the given uid (`obj-<uid>` generated name), matching
    /// `meta.raw_object_ref()` so the uid/key round-trips.
    fn exec_ref(uid: Ulid) -> ObjectRef<ExecutionKind> {
        ObjectRef::new(
            PlainName::new("child")
                .unwrap()
                .generated_from_key(uid.0 as u64),
            uid,
        )
    }

    /// An activity reference for the given uid (`obj-<uid>` generated name), matching
    /// `meta.raw_object_ref()` so the uid/key round-trips.
    fn act_ref(uid: Ulid) -> ObjectRef<ActivityKind> {
        ObjectRef::new(
            PlainName::new("child")
                .unwrap()
                .generated_from_key(uid.0 as u64),
            uid,
        )
    }

    /// A task reference for the given uid (`obj-<uid>` generated name), matching
    /// `meta.raw_object_ref()` so the uid/key round-trips.
    fn task_ref(uid: Ulid) -> ObjectRef<TaskKind> {
        ObjectRef::new(
            PlainName::new("child")
                .unwrap()
                .generated_from_key(uid.0 as u64),
            uid,
        )
    }

    #[test]
    fn entity_row_keys_match_the_spec() {
        let kb = KeyBuilder::new(scope("acme", "prod"));
        let exec = exec_ref(Ulid::new());
        let act = act_ref(Ulid::new());
        let task = task_ref(Ulid::new());
        let vname = PlainName::new("order").unwrap().generated_from_key(1);
        let name = FlowName::new("order").unwrap();

        let exec_key = String::from_utf8(kb.execution(&exec)).unwrap();
        assert!(exec_key.starts_with("/acme/prod/execution/"));
        // Keyed by the addressing name (`obj-<uid>` here), not the bare uid.
        assert!(exec_key.ends_with(exec.name().as_str().as_str()));
        assert!(
            String::from_utf8(kb.activity(&act))
                .unwrap()
                .starts_with("/acme/prod/activity/")
        );
        assert!(
            String::from_utf8(kb.task(&task))
                .unwrap()
                .starts_with("/acme/prod/task/")
        );
        assert_eq!(
            String::from_utf8(kb.flow(&name)).unwrap(),
            "/acme/prod/flow/order"
        );
        assert!(
            String::from_utf8(kb.flow_version(&vname))
                .unwrap()
                .starts_with("/acme/prod/flowversion/")
        );
    }

    #[test]
    fn different_kinds_never_share_a_key() {
        let kb = KeyBuilder::new(scope("acme", "prod"));
        let id = Ulid::new();
        let exec = String::from_utf8(kb.execution(&exec_ref(id))).unwrap();
        // Same raw ULID under different kinds must not collide.
        let act = String::from_utf8(kb.activity(&act_ref(id))).unwrap();
        let fv = String::from_utf8(
            kb.flow_version(&PlainName::new("order").unwrap().generated_from_key(1)),
        )
        .unwrap();
        assert_ne!(exec, act);
        assert_ne!(exec, fv);
    }

    #[test]
    fn version_name_is_decimal_and_prefix_bound() {
        use spica_engine_types::FlowVersion;
        let kb = KeyBuilder::new(scope("acme", "prod"));
        let flow = FlowName::new("order").unwrap();
        // Names follow `{flow}-{decimal}`, human-readable rather than a hex/zero-padded code.
        let v1 = kb.flow_version(&FlowVersion::version_name(&flow, 1));
        let v2 = kb.flow_version(&FlowVersion::version_name(&flow, 2));
        let v10 = kb.flow_version(&FlowVersion::version_name(&flow, 10));
        assert!(String::from_utf8(v1.clone()).unwrap().ends_with("/order-1"));
        assert!(String::from_utf8(v2.clone()).unwrap().ends_with("/order-2"));
        assert!(
            String::from_utf8(v10.clone())
                .unwrap()
                .ends_with("/order-10")
        );
        // Decimal breaks name ordering ("order-10" sorts before "order-2"): a prefix scan must order
        // by the `version` field, never by key order.
        assert!(v10 < v2);
        // Two flows never share a version key even with identical version ordinals.
        let other = FlowName::new("checkout").unwrap();
        assert_ne!(
            kb.flow_version(&FlowVersion::version_name(&flow, 1)),
            kb.flow_version(&FlowVersion::version_name(&other, 1))
        );
        // The prefix scan start covers exactly one flow's versions; the trailing `-` binds it.
        assert_eq!(
            String::from_utf8(kb.flow_version_prefix(&flow)).unwrap(),
            "/acme/prod/flowversion/order-"
        );
    }

    #[test]
    fn child_edges_are_one_range_per_parent() {
        let kb = KeyBuilder::new(scope("acme", "prod"));
        let parent = exec_ref(Ulid::new()).into_raw_object_ref();
        let a = act_ref(Ulid::new()).into_raw_object_ref();
        let b = act_ref(Ulid::new()).into_raw_object_ref();

        let edge_a = String::from_utf8(kb.child_edge(&parent, &a)).unwrap();
        // Both ends spell their kind, and the value side is the child's own name — so a raw dump of
        // the edge table reads as `parent-kind/parent-name/child-kind/child-name`.
        assert!(
            edge_a.starts_with("/acme/prod/_index/children/execution/child-"),
            "{edge_a}"
        );
        assert!(edge_a.contains("/activity/child-"), "{edge_a}");
        // The prefix is exactly this parent's range — a trailing `/` keeps it from covering a parent
        // whose name merely extends this one.
        assert!(
            edge_a.starts_with(&String::from_utf8(kb.child_edge_prefix(&parent)).unwrap()),
            "{edge_a}"
        );
        // Distinct children of one parent never collide.
        assert_ne!(
            edge_a,
            String::from_utf8(kb.child_edge(&parent, &b)).unwrap()
        );

        // A same-named parent of another kind is a *different* parent with its own set (see
        // `child_edge`'s note on why the kind is not dropped).
        let name = PlainName::new("order").unwrap().generated_from_key(7);
        let exec_parent = RawObjectRef::new(ObjectKind::Execution, name.clone(), Ulid::new());
        let thread_parent = RawObjectRef::new(ObjectKind::Thread, name.clone(), exec_parent.uid);
        assert_ne!(
            kb.child_edge(&exec_parent, &a),
            kb.child_edge(&thread_parent, &a)
        );
        assert_ne!(
            kb.child_edge_prefix(&exec_parent),
            kb.child_edge_prefix(&thread_parent)
        );
        // An edge never lands inside either end's own entity-row range.
        assert!(
            !kb.child_edge(&parent, &a)
                .starts_with(&kb.kind_prefix(Kind::Execution))
        );
        assert!(
            !kb.child_edge(&parent, &a)
                .starts_with(&kb.kind_prefix(Kind::Activity))
        );
    }

    #[test]
    fn a_threads_variable_scope_is_an_index_cell_beside_its_row() {
        let kb = KeyBuilder::new(scope("acme", "prod"));
        let name = PlainName::new("order").unwrap().generated_from_key(7);
        let key = String::from_utf8(kb.variables(&name)).unwrap();
        // The scope is addressed by the thread's name alone — no kind segment, since only a `Thread`
        // owns a scope.
        assert_eq!(key, "/acme/prod/_index/variables/order-7");
        // It never lands in the thread entity-row range, so `put_thread` and an assignment can never
        // write over one another.
        assert!(
            !kb.variables(&name)
                .starts_with(&kb.kind_prefix(Kind::Thread))
        );
        // Distinct threads keep distinct scopes.
        assert_ne!(
            kb.variables(&name),
            kb.variables(&PlainName::new("order").unwrap().generated_from_key(8))
        );
    }

    #[test]
    fn the_default_scope_is_the_object_meta_scope() {
        // A `Scope` no longer has rules to enforce: its segments are `ScopeName`s, so the key's
        // segment rules (4..=64 plain names, `-` reserved, no `/`) hold by construction — pinned
        // by `PlainName`'s own tests in `spica-machinery`, not restated here. What is left to check
        // is that the Phase-A default scope is *representable* and that it is the same pair
        // `ObjectMetaBuilder` stamps, so a row's key prefix and its record's own scope agree.
        // The kind is irrelevant here (only the scope is read), so any marker serves.
        let stamped = ObjectMeta::<ExecutionKind>::builder(Ulid::nil()).with_owner(NoOwner::new());
        let default = Scope::default_scope();
        assert_eq!(default.tenant(), stamped.tenant.as_str());
        assert_eq!(default.namespace(), stamped.namespace.as_str());
        assert_eq!(
            String::from_utf8(
                KeyBuilder::new(Scope::default_scope()).execution(&exec_ref(Ulid::nil()))
            )
            .unwrap(),
            "/default/default/execution/child-0"
        );
    }
}
