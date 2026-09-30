//! Scoped identity metadata shared by every spica object — the k8s-style `ObjectMeta` reuse.
//!
//! The object identity model is `(tenant, namespace, kind, name)` plus a `uid` for sameness:
//!
//! - The **name layer** ([`ObjectName`], [`PlainName`], [`ScopeName`], `GeneratedName`) lives in
//!   the leaf kernel [`spica_machinery::name`] — naming is a pure, engine-agnostic rule (a
//!   user-supplied segment bans `-`, which is reserved for the system's `generateName` suffix), so
//!   it belongs in the bottom crate shared by every layer. It is re-exported here so engine users
//!   keep a single import path.
//! - This module adds the **kind** ([`ObjectKind`]), the **meta** envelope ([`ObjectMeta`]), and the
//!   reference form ([`ObjectReference`]) built on top — the pieces that couple naming to the
//!   engine's object model.
//!
//! See `docs/identity-and-partitioning-design.md` for the full design.

use std::marker::PhantomData;

use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

use crate::types::error::{ExecutionError, RuntimeError};
use spica_machinery::Timestamp;

pub use spica_machinery::name::{ObjectName, PlainName, ScopeName};

/// The type of a spica object — the k8s "Kind" of its [`ObjectMeta`]. The reference on an
/// [`ObjectReference`] carries this kind, so a single value both names an object *and* discriminates
/// its role (the former node-only `NodeId`/`NodeKind` enums re-encoded the same information by hand);
/// among the node kinds it covers `Flow`/`FlowVersion` too, which are not nodes in the tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ObjectKind {
    Flow,
    FlowVersion,
    Execution,
    /// A scoped sub-run of the shared machine — the entity a `Parallel` branch or `Map` item executes
    /// (a container-neutral fan-out unit; see [`crate::Thread`]). Structurally distinct from
    /// [`ObjectKind::Execution`], which is always a top-level run.
    Thread,
    Activity,
    Timer,
    Task,
}

impl ObjectKind {
    /// The stable lowercase string form used inside the derived address string.
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectKind::Flow => "flow",
            ObjectKind::FlowVersion => "flowversion",
            ObjectKind::Execution => "execution",
            ObjectKind::Thread => "thread",
            ObjectKind::Activity => "activity",
            ObjectKind::Timer => "timer",
            ObjectKind::Task => "task",
        }
    }

    /// Parse the lowercase string form back into a kind; `None` for an unknown string.
    ///
    /// Must mirror [`Self::as_str`] arm for arm: the two are a hand-maintained inverse pair, so a kind
    /// added to one silently loses its round-trip in the other (as `Thread` once did) — the
    /// `object_kind_roundtrips_via_string` test is what keeps them in step.
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "flow" => ObjectKind::Flow,
            "flowversion" => ObjectKind::FlowVersion,
            "execution" => ObjectKind::Execution,
            "thread" => ObjectKind::Thread,
            "activity" => ObjectKind::Activity,
            "timer" => ObjectKind::Timer,
            "task" => ObjectKind::Task,
            _ => return None,
        })
    }
}

impl std::fmt::Display for ObjectKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// The compile-time map from an object type to its [`ObjectKind`], declared once per object type by a
/// zero-sized marker (`ActivityKind`, `TaskKind`, …).
///
/// [`ObjectMeta`] is parameterised by it, so a meta cannot be built for one kind and read back as
/// another: the kind is never a stored value that could drift from the record holding it, and
/// [`ObjectMeta::reference`] derives it from the type alone. The marker is also the extension slot for
/// typing an object's owning reference (`OwnerReference<O>`), where an object type will name the marker
/// its owner must carry.
pub trait ObjectKindMarker {
    const KIND: ObjectKind;
}

/// The wire anchor of an [`ObjectMeta`]'s kind: a zero-sized value that holds no state of its own.
///
/// Writing it emits `K::KIND`; reading it rejects a payload whose `kind` disagrees with `K`. The kind
/// therefore stays on the wire — a foreign-kind row cannot be silently re-typed into the record that
/// read it — while remaining impossible to drift: there is no stored value that could disagree with
/// the type, only the type's own constant round-tripped. The error names the offending type via
/// [`std::any::type_name`], because the fault is a wrong *type*, not wrong data.
#[derive(Debug, Clone, PartialEq)]
struct KindTag<K>(PhantomData<fn() -> K>);

impl<K: ObjectKindMarker> Serialize for KindTag<K> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        K::KIND.serialize(s)
    }
}

impl<'de, K: ObjectKindMarker> Deserialize<'de> for KindTag<K> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let seen = ObjectKind::deserialize(d)?;
        if seen != K::KIND {
            return Err(serde::de::Error::custom(format!(
                "object meta kind mismatch: the payload carries kind {seen:?}, but this meta is \
                 typed as {} and expects {:?}",
                std::any::type_name::<K>(),
                K::KIND,
            )));
        }
        Ok(KindTag(PhantomData))
    }
}

/// Common metadata shared by every spica object (k8s-style `ObjectMeta` reuse).
///
/// The object's kind is the `K` parameter, not a mutable field: `ObjectMeta<TaskKind>` *is* a task's
/// meta, so the kind cannot be edited into disagreement with the record that holds it, the per-object
/// `Task::reference`/`Timer::reference`/… helpers collapse into one [`Self::reference`], and a payload
/// whose `kind` disagrees with the reading type is rejected rather than silently re-typed.
///
/// Consolidates the identity, scoping, and timing facts that were historically copied across the
/// domain entities, so future shared fields (e.g. optimistic-concurrency `resource_version`) land
/// here exactly once. `labels` and `annotations` are deliberately **not** designed (per decision).
///
/// Identity split (see `docs/identity-and-partitioning-design.md` §6):
/// - [`Self::name`] is the **addressing / idempotency** key — a user name for persistent objects,
///   a generated name for transient ones.
/// - [`Self::uid`] is the **sameness** key — a name can be re-used across delete+recreate, so `uid`
///   is what tells two references to the same *object* apart. It is opaque and never reused.
///
/// [`Self::owner`] links each object to its single owning parent in the object tree (a node is owned
/// by its execution; a child execution by the container that spawned it). `owner` is the **single
/// parent edge** for every entity — the former ad-hoc `Execution.parent`/`Activity.parent`/
/// `Task.parent`/`Timer.parent` handle fields have migrated into it, and the parent is read back
/// from the reference's [`ObjectKind`]. `root_execution` is a
/// separate flat top-of-tree query anchor, **not** the owner (the owner is the direct parent).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ObjectMeta<K: ObjectKindMarker> {
    /// The kind, emitted as `K::KIND` and validated against `K` on read (see [`KindTag`]). Declared
    /// first so the wire keeps the field order it had before this meta was parameterised.
    kind: KindTag<K>,
    /// Top-level isolation boundary — which organization owns the object.
    pub tenant: ScopeName,
    /// Scoping within the tenant — a project / team / environment.
    pub namespace: ScopeName,
    /// Addressing key — user-supplied (no `-`) or system-generated (`-`-suffixed).
    pub name: ObjectName,
    /// Opaque, system-minted, never-reused incarnation id (k8s `uid`).
    pub uid: ulid::Ulid,
    /// When the object was born (its creation event landed); see `ObjectMeta::born`.
    pub created_at: Timestamp,
    /// When the object was last updated; see [`Self::with_update_at`].
    pub updated_at: Timestamp,
    // TODO(OwnerReference<O>)：K 是 owner 强类型化的落点——让 owner 的 marker 由本类型决定，
    // 例如 Task 的 owner 必须是 Activity，从而消掉 container.rs 里那类运行时 kind 判断。
    /// The object that owns this one, if any. Same `tenant`/`namespace` scope is *inherited* from
    /// this object's own scope (spica ownership never crosses a scope), so the reference carries
    /// only `kind`/`name`/`uid`. `None` for roots (a top-level `Execution` with no container parent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<OwnerReference>,
}

impl<K: ObjectKindMarker> ObjectMeta<K> {
    /// Start building fresh meta for a `K` — the kind is `K`'s, never an argument. See
    /// [`ObjectMetaBuilder`].
    pub fn builder(uid: ulid::Ulid) -> ObjectMetaBuilder<K> {
        ObjectMetaBuilder::new(uid)
    }

    /// The canonical reference to this object — the one implementation for every kind, replacing the
    /// per-object `reference()` helpers that each hard-coded their own kind.
    pub fn reference(&self) -> ObjectReference {
        ObjectReference::new(K::KIND, self.name.clone(), self.uid)
    }

    /// Record a mutation at `at`: advances `updated_at`, leaves `created_at`.
    pub fn with_update_at(&mut self, at: Timestamp) {
        self.updated_at = at;
    }

    /// Attach the owning object (consumes `self` so it composes with construction credit flows):
    /// `ObjectMeta::builder(...).build().with_owner(owner)`. The reference is same-scope by
    /// construction — see [`Self::owner`].
    pub fn with_owner(mut self, owner: OwnerReference) -> Self {
        self.owner = Some(owner);
        self
    }
}

/// A fluent constructor for `ObjectMeta<K>`, replacing the four small variant constructors
/// (`born` / `born_placeholder` / `born_named` / `placeholder_with_times`) that varied along three
/// orthogonal axes (name source, scope, time split). `uid` is the one required input — the kind comes
/// from `K`, so it cannot be passed wrong or overwritten; a `default`/`default` scope and a
/// `child-<uid>` generated name are the **defaults** — so the placeholder case is the zero-config path
/// and real naming is one `.name(...)` setter. That single seam keeps ~50 call sites stable when P2
/// user naming lands (the former `born_placeholder`/`placeholder_with_times` TODO), instead of a
/// fourth constructor variant.
pub struct ObjectMetaBuilder<K: ObjectKindMarker> {
    tenant: ScopeName,
    namespace: ScopeName,
    name: ObjectName,
    uid: ulid::Ulid,
    created_at: Timestamp,
    updated_at: Timestamp,
    /// Present only to keep `K` a parameter of the builder (E0392) — the kind it stands for is `K::KIND`.
    _kind: PhantomData<fn() -> K>,
}

impl<K: ObjectKindMarker> ObjectMetaBuilder<K> {
    /// Start building fresh meta for a `K`. The `default`/`default` scope, the uid-derived `child-<uid>`
    /// name and `created_at == updated_at` are defaults — override them with the setters. The static
    /// scope/name `expect`s cannot panic.
    pub fn new(uid: ulid::Ulid) -> Self {
        let tenant = ScopeName::new("default").expect("static literal is a valid segment");
        let namespace = ScopeName::new("default").expect("static literal is a valid segment");
        let name = PlainName::new("child")
            .expect("static literal is a valid segment")
            .generated_from_key(uid.0 as u64);
        Self {
            tenant,
            namespace,
            name,
            uid,
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
            _kind: PhantomData,
        }
    }

    pub fn tenant(mut self, tenant: ScopeName) -> Self {
        self.tenant = tenant;
        self
    }

    pub fn namespace(mut self, namespace: ScopeName) -> Self {
        self.namespace = namespace;
        self
    }

    /// The object's real addressing name (a user `FlowName`, a known generated name), replacing the
    /// uid-derived default.
    pub fn name(mut self, name: ObjectName) -> Self {
        self.name = name;
        self
    }

    /// Born with `created_at == updated_at == at`.
    pub fn at(mut self, at: Timestamp) -> Self {
        self.created_at = at;
        self.updated_at = at;
        self
    }

    /// Explicit distinct birth/update stamps (a lifecycle test may want `created != updated`).
    pub fn timestamps(mut self, created: Timestamp, updated: Timestamp) -> Self {
        self.created_at = created;
        self.updated_at = updated;
        self
    }

    pub fn build(self) -> ObjectMeta<K> {
        ObjectMeta {
            kind: KindTag(PhantomData),
            tenant: self.tenant,
            namespace: self.namespace,
            name: self.name,
            uid: self.uid,
            created_at: self.created_at,
            updated_at: self.updated_at,
            owner: None,
        }
    }
}

/// A reference to a spica object, carrying enough to locate it and confirm sameness.
///
/// Mirrors the k8s `OwnerReference` minimalism (`kind` + `name` + `uid`): `name` locates the object
/// and `uid` confirms sameness across delete+recreate. `tenant`/`namespace` are deliberately not
/// carried — a reference addresses an object in the *same* scope as its holder (spica ownership and
/// referencing never cross a scope), so the full scope (tenant/namespace) is inherited from the
/// holder rather than re-encoded on the reference.
///
/// Used as both:
/// - an **owner** ([`ObjectMeta::owner`]) — the object `<kind>/<name>` that owns this one; and
/// - a general **handle** to any object (e.g. [`crate::FlowVersion::reference`]) — the
///   `(name, uid)` pair, where `name` is the referenced
///   object's own `ObjectName` and `uid` its `meta.uid`.
///
/// [`OwnerReference`] is a type alias of this struct — a reference *is* just an owner-style
/// reference, so the two names address the same type.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObjectReference {
    /// The referenced object's kind.
    pub kind: ObjectKind,
    /// The referenced object's name (user or generated).
    pub name: ObjectName,
    /// The referenced object's incarnation id — confirms the reference points at the *same* object
    /// even if a name has since been re-used.
    pub uid: ulid::Ulid,
}

/// The owning-object reference of an [`ObjectMeta`]. See [`ObjectReference`].
pub type OwnerReference = ObjectReference;

impl ObjectReference {
    /// Build a reference from already-validated parts.
    pub fn new(kind: ObjectKind, name: ObjectName, uid: ulid::Ulid) -> Self {
        Self { kind, name, uid }
    }

    /// A placeholder reference with a nil uid — used where a value is matched only by variant + a
    /// single echoed key (e.g. the Execution `flow_version` in an ack echo), never by full equality.
    pub fn nil() -> Self {
        Self::new(
            ObjectKind::FlowVersion,
            PlainName::new("flow")
                .expect("static literal is a valid segment")
                .generated_from_key(0),
            ulid::Ulid::nil(),
        )
    }
}

impl std::fmt::Display for ObjectReference {
    /// Same-scope form `{kind}/{name}`. The address is scope-relative by construction (the
    /// referenced object shares the holder's tenant/namespace); `uid` is carried as a separate
    /// field, not here.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.kind, self.name)
    }
}

impl std::str::FromStr for ObjectReference {
    type Err = ExecutionError;

    /// Parse the `{kind}/{name}` same-scope form back into a reference. The `uid` cannot be encoded
    /// in this string form and must be supplied by the caller (see [`ObjectKind::parse`] /
    /// [`ObjectName::from_parsed`]).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split('/');
        let kind_raw = parts.next().ok_or(invalid_owner_ref(s))?;
        let kind =
            crate::types::meta::ObjectKind::parse(kind_raw).ok_or_else(|| invalid_owner_ref(s))?;
        let name = ObjectName::from_parsed(parts.next().ok_or(invalid_owner_ref(s))?)?;
        if parts.next().is_some() {
            return Err(invalid_owner_ref(s));
        }
        // A parsed reference carries no incarnation id (a ULID is not printable here); the caller
        // reconciles `uid` against the live object. `nil` marks "not yet resolved".
        Ok(Self::new(kind, name, ulid::Ulid::nil()))
    }
}

fn invalid_owner_ref(s: &str) -> ExecutionError {
    ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
        "invalid owner reference {s:?}: expected {{kind}}/{{name}}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(ms: u64) -> Timestamp {
        Timestamp::from_millis(ms)
    }

    /// A stand-in marker so the meta's own tests need no real object type.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestKind;
    impl ObjectKindMarker for TestKind {
        const KIND: ObjectKind = ObjectKind::Execution;
    }

    /// A second marker, so the mismatch case has somewhere to mismatch to.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct OtherTestKind;
    impl ObjectKindMarker for OtherTestKind {
        const KIND: ObjectKind = ObjectKind::Task;
    }

    #[test]
    fn object_kind_roundtrips_via_string() {
        // `all` and the index arms below are one enumeration split in two, so a new variant cannot
        // escape the round-trip the way `Thread` once did: the match is exhaustive (a new variant
        // breaks the compile), and the arm's index must hold that same variant — a variant left out
        // of `all` fails `all[i] == k` instead of silently skipping its assertions.
        let all = [
            ObjectKind::Flow,
            ObjectKind::FlowVersion,
            ObjectKind::Execution,
            ObjectKind::Thread,
            ObjectKind::Activity,
            ObjectKind::Timer,
            ObjectKind::Task,
        ];
        for k in all {
            let i = match k {
                ObjectKind::Flow => 0,
                ObjectKind::FlowVersion => 1,
                ObjectKind::Execution => 2,
                ObjectKind::Thread => 3,
                ObjectKind::Activity => 4,
                ObjectKind::Timer => 5,
                ObjectKind::Task => 6,
            };
            assert_eq!(all[i], k);
            assert_eq!(ObjectKind::parse(k.as_str()), Some(k));
            assert_eq!(format!("{k}"), k.as_str());
        }
        assert_eq!(ObjectKind::parse("wat"), None);
    }

    #[test]
    fn object_meta_born_and_with_update_at() {
        // The kind is only known from the type, so a meta no longer fed to a record must be annotated.
        let mut meta = ObjectMeta::<TestKind>::builder(ulid::Ulid::new())
            .tenant(ScopeName::new("myco").unwrap())
            .namespace(ScopeName::new("default").unwrap())
            .name(
                PlainName::new("checkout")
                    .unwrap()
                    .generated_from_key(123_456),
            )
            .at(ts(1000))
            .build();
        assert_eq!(meta.created_at, meta.updated_at);
        assert_eq!(meta.owner, None); // roots have no owner
        assert_eq!(meta.reference().kind, ObjectKind::Execution); // the kind is `K`'s, not a field
        meta.with_update_at(ts(2000));
        assert_eq!(meta.updated_at, ts(2000));
        assert_eq!(meta.created_at, ts(1000)); // created_at is immutable
    }

    #[test]
    fn object_meta_roundtrips_with_the_kind_on_the_wire() {
        let meta = ObjectMeta::<TestKind>::builder(ulid::Ulid::new())
            .at(ts(1000))
            .build();
        let json = serde_json::to_value(&meta).expect("meta serializes");
        // The spelling is load-bearing: `"Execution"` is what the wire carried before the meta was
        // parameterised, so pinning it keeps rows written earlier readable.
        assert_eq!(
            json.get("kind"),
            Some(&serde_json::json!("Execution")),
            "wire: {json}"
        );
        let back: ObjectMeta<TestKind> = serde_json::from_value(json).expect("meta deserializes");
        assert_eq!(back, meta);
        assert_eq!(back.reference().kind, ObjectKind::Execution);
    }

    #[test]
    fn object_meta_rejects_a_foreign_kind_on_the_wire() {
        let meta = ObjectMeta::<TestKind>::builder(ulid::Ulid::new())
            .at(ts(1000))
            .build();
        let json = serde_json::to_value(&meta).expect("meta serializes");
        let err = serde_json::from_value::<ObjectMeta<OtherTestKind>>(json)
            .expect_err("a Task-typed meta must not read an Execution payload");
        let msg = err.to_string();
        assert!(msg.contains("kind mismatch"), "{msg}");
        assert!(msg.contains("carries kind Execution"), "{msg}");
        assert!(msg.ends_with("expects Task"), "{msg}");
    }

    #[test]
    fn object_meta_rejects_a_missing_kind_on_the_wire() {
        let mut json = serde_json::to_value(
            ObjectMeta::<TestKind>::builder(ulid::Ulid::new())
                .at(ts(1000))
                .build(),
        )
        .expect("meta serializes");
        json.as_object_mut()
            .expect("meta is an object")
            .remove("kind");
        let err = serde_json::from_value::<ObjectMeta<TestKind>>(json)
            .expect_err("the kind is required, never defaulted");
        assert!(err.to_string().contains("kind"), "{err}");
    }

    #[test]
    fn owner_reference_roundtrips() {
        let owner = OwnerReference::new(
            ObjectKind::Activity,
            PlainName::new("checkout")
                .unwrap()
                .generated_from_key(424_242),
            // Built with a nil uid: the string form cannot carry a uid, so a round-tripped reference
            // resolves to `nil` and must be reconciled against the live owner by the caller.
            ulid::Ulid::nil(),
        );
        // Same-scope Display form `{kind}/{name}` roundtrips (uid is not printable; parse yields nil).
        assert_eq!(owner.to_string(), "activity/checkout-424242");
        let reparsed: OwnerReference = owner.to_string().parse().unwrap();
        assert_eq!(reparsed, owner);
        assert_eq!(reparsed.uid, ulid::Ulid::nil());

        // Malformed references are rejected.
        assert!("activity".parse::<OwnerReference>().is_err()); // missing name
        assert!("wat/checkout".parse::<OwnerReference>().is_err()); // bad kind
        assert!("activity/a/b".parse::<OwnerReference>().is_err()); // too many parts
    }

    #[test]
    fn object_reference_roundtrips_through_display_and_fromstr() {
        let r = ObjectReference::new(
            ObjectKind::FlowVersion,
            PlainName::new("order").unwrap().generated_from_key(1),
            ulid::Ulid::new(),
        );
        // Display is the scope-relative `kind/name` form (uid is a separate field, not encoded), so
        // `FromStr` reconstructs the same kind+name with a nil uid (the caller supplies the uid).
        let s = r.to_string();
        assert_eq!(s, format!("flowversion/{}", r.name));
        let parsed = s
            .parse::<ObjectReference>()
            .expect("reference Display reparses");
        assert_eq!(parsed.kind, r.kind);
        assert_eq!(parsed.name, r.name);
    }

    #[test]
    fn object_reference_nil_is_a_stable_placeholder() {
        // `ObjectReference::nil` is a recognizable placeholder (never matches a real reference with a
        // fresh uid), and its Display is stable.
        let nil = ObjectReference::nil();
        assert!(nil.uid.is_nil());
        assert_eq!(ObjectReference::nil().to_string(), "flowversion/flow-0");
        assert_ne!(
            nil,
            ObjectReference::new(
                ObjectKind::FlowVersion,
                PlainName::new("flow").unwrap().generated_from_key(0),
                ulid::Ulid::new(),
            )
        );
    }
}
