//! Scoped identity metadata shared by every spica object — the k8s-style `ObjectMeta` reuse.
//!
//! The object identity model is `(tenant, namespace, kind, name)` plus a `uid` for sameness:
//!
//! - The **name layer** ([`ObjectName`], [`PlainName`], [`ScopeName`], [`GeneratedName`]) lives in
//!   the leaf kernel [`spica_machinery::name`] — naming is a pure, engine-agnostic rule (a
//!   user-supplied segment bans `-`, which is reserved for the system's `generateName` suffix), so
//!   it belongs in the bottom crate shared by every layer. It is re-exported here so engine users
//!   keep a single import path.
//! - This module adds the **kind** ([`ObjectKind`]), the **meta** envelope ([`ObjectMeta`]), and the
//!   reference/address forms ([`ObjectReference`] / [`ObjectAddress`]) built on top — the pieces
//!   that couple naming to the engine's object model.
//!
//! See `docs/identity-and-partitioning-design.md` for the full design.

use serde::{Deserialize, Serialize};

use crate::log::Timestamp;
use crate::types::error::{ExecutionError, RuntimeError};

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
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "flow" => ObjectKind::Flow,
            "flowversion" => ObjectKind::FlowVersion,
            "execution" => ObjectKind::Execution,
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

/// Common metadata shared by every spica object (k8s-style `ObjectMeta` reuse).
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
/// from the reference's [`ObjectKind`](ObjectKind). `root_execution` is a
/// separate flat top-of-tree query anchor, **not** the owner (the owner is the direct parent).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectMeta {
    /// The object's kind (k8s "Kind").
    pub kind: ObjectKind,
    /// Top-level isolation boundary — which organization owns the object.
    pub tenant: ScopeName,
    /// Scoping within the tenant — a project / team / environment.
    pub namespace: ScopeName,
    /// Addressing key — user-supplied (no `-`) or system-generated (`-`-suffixed).
    pub name: ObjectName,
    /// Opaque, system-minted, never-reused incarnation id (k8s `uid`).
    pub uid: ulid::Ulid,
    /// When the object was born (its creation event landed); see [`Self::born`].
    pub created_at: Timestamp,
    /// When the object was last touched; see [`Self::touch`].
    pub updated_at: Timestamp,
    /// The object that owns this one, if any. Same `tenant`/`namespace` scope is *inherited* from
    /// this object's own scope (spica ownership never crosses a scope), so the reference carries
    /// only `kind`/`name`/`uid`. `None` for roots (a top-level `Execution` with no container parent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<OwnerReference>,
}

impl ObjectMeta {
    /// Birth stamp: `created_at == updated_at == at`. Mirrors the domain
    /// `created_at/updated_at` contract (stamped at event construction, not from log entry meta).
    pub fn born(
        kind: ObjectKind,
        tenant: ScopeName,
        namespace: ScopeName,
        name: ObjectName,
        uid: ulid::Ulid,
        at: Timestamp,
    ) -> Self {
        Self {
            kind,
            tenant,
            namespace,
            name,
            uid,
            created_at: at,
            updated_at: at,
            owner: None,
        }
    }

    /// Pilot-only constructor for entities whose real `tenant`/`namespace`/`name` only arrive with
    /// user naming (P2). Fills a `default`/`default` scope and a system-generated `<base>-<uid>`
    /// name; `uid` is the caller's id, so `meta.uid` always matches the typed id. TODO(meta-uid):
    /// replace with real scope/name at the creation command boundary once user naming lands.
    pub fn born_placeholder(kind: ObjectKind, uid: ulid::Ulid, at: Timestamp) -> Self {
        Self::placeholder_with_times(kind, uid, at, at)
    }

    /// Like [`Self::born_placeholder`] but with a **caller-supplied real `name`** (`default`/
    /// `default` scope, uid = caller's id). For entities whose addressing name is already known at
    /// creation — e.g. a `Flow`, which carries a user `FlowName` today. Once P2 user naming lands,
    /// the `default` scope is replaced the same way `born_placeholder` is.
    pub fn born_named(kind: ObjectKind, name: ObjectName, uid: ulid::Ulid, at: Timestamp) -> Self {
        let tenant = ScopeName::new("default").expect("static literal is a valid segment");
        let namespace = ScopeName::new("default").expect("static literal is a valid segment");
        Self {
            kind,
            tenant,
            namespace,
            name,
            uid,
            created_at: at,
            updated_at: at,
            owner: None,
        }
    }

    /// Like [`Self::born_placeholder`], but with an explicit `created`/`updated` (a lifecycle test
    /// may want `created != updated`). Both `expect`s are on statically-valid literals / a ULID
    /// suffix, so they cannot panic.
    pub fn placeholder_with_times(
        kind: ObjectKind,
        uid: ulid::Ulid,
        created: Timestamp,
        updated: Timestamp,
    ) -> Self {
        let tenant = ScopeName::new("default").expect("static literal is a valid segment");
        let namespace = ScopeName::new("default").expect("static literal is a valid segment");
        let name = ObjectName::generated_with_suffix("child", &uid.to_string())
            .expect("a ULID is a valid alphanumeric generated suffix");
        Self {
            kind,
            tenant,
            namespace,
            name,
            uid,
            created_at: created,
            updated_at: updated,
            owner: None,
        }
    }

    /// Record a mutation at `at`: advances `updated_at`, leaves `created_at`.
    pub fn touch(&mut self, at: Timestamp) {
        self.updated_at = at;
    }

    /// Attach the owning object (consumes `self` so it composes with construction credit flows):
    /// `born(...).with_owner(owner)`. The reference is same-scope by construction — see [`Self::owner`].
    pub fn with_owner(mut self, owner: OwnerReference) -> Self {
        self.owner = Some(owner);
        self
    }

    /// The derived single-string address of this object.
    pub fn address(&self) -> ObjectAddress {
        ObjectAddress::new(
            self.tenant.clone(),
            self.namespace.clone(),
            self.kind,
            self.name.clone(),
        )
    }
}

/// A reference to a spica object, carrying enough to locate it and confirm sameness.
///
/// Mirrors the k8s `OwnerReference` minimalism (`kind` + `name` + `uid`): `name` locates the object
/// and `uid` confirms sameness across delete+recreate. `tenant`/`namespace` are deliberately not
/// carried — a reference addresses an object in the *same* scope as its holder (spica ownership and
/// referencing never cross a scope), so the full address is derived with [`Self::address`].
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
            ObjectName::generated_with_suffix("flow", "00000000")
                .expect("static placeholder version name is valid"),
            ulid::Ulid::nil(),
        )
    }

    /// The canonical `obj-<uid>` reference for a freshly-minted object of `kind` — the reference
    /// born with the uid (e.g. a `Collector::next_activity` id). Centralizes the `obj-<uid>` name
    /// rule so every mint site builds the same canonical reference.
    pub fn for_uid(kind: ObjectKind, uid: ulid::Ulid) -> Self {
        Self::new(
            kind,
            ObjectName::generated_with_suffix("child", &uid.to_string())
                .expect("a ULID is a valid alphanumeric generated suffix"),
            uid,
        )
    }

    /// The referenced object's full address, resolved within the **caller's** `tenant`/`namespace`
    /// (references never cross a scope, so the referenced object lives in the same scope as its
    /// holder — see [`Self`]).
    pub fn address(&self, tenant: &ScopeName, namespace: &ScopeName) -> ObjectAddress {
        ObjectAddress::new(
            tenant.clone(),
            namespace.clone(),
            self.kind,
            self.name.clone(),
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

/// The derived single-string address of an object: `{tenant}/{namespace}/{kind}/{name}`.
///
/// This is a **view** of the structured identity — the struct fields are the single source of
/// truth; the string is the form used for references, CLI args, foreign keys, and address parsing.
/// `/` is safe as the level separator because every user segment bans `/`. `uid` is deliberately
/// **not** part of the address string: `name` locates an object, `uid` (held on [`ObjectMeta`])
/// confirms sameness against name reuse — see `docs/identity-and-partitioning-design.md` §6.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObjectAddress {
    pub tenant: ScopeName,
    pub namespace: ScopeName,
    pub kind: ObjectKind,
    pub name: ObjectName,
}

impl ObjectAddress {
    /// Build an address from already-validated parts.
    pub fn new(
        tenant: ScopeName,
        namespace: ScopeName,
        kind: ObjectKind,
        name: ObjectName,
    ) -> Self {
        Self {
            tenant,
            namespace,
            kind,
            name,
        }
    }
}

impl std::fmt::Display for ObjectAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}",
            self.tenant, self.namespace, self.kind, self.name
        )
    }
}

impl std::str::FromStr for ObjectAddress {
    type Err = ExecutionError;

    /// Parse `{tenant}/{namespace}/{kind}/{name}` back into an address. The four parts are
    /// re-validated on the way in, so a malformed address cannot construct an invalid `ObjectMeta`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split('/');
        let tenant = ScopeName::new(parts.next().ok_or(invalid_addr(s))?)?;
        let namespace = ScopeName::new(parts.next().ok_or(invalid_addr(s))?)?;
        let kind_raw = parts.next().ok_or(invalid_addr(s))?;
        let kind =
            crate::types::meta::ObjectKind::parse(kind_raw).ok_or_else(|| invalid_addr(s))?;
        let name = ObjectName::from_parsed(parts.next().ok_or(invalid_addr(s))?)?;
        if parts.next().is_some() {
            return Err(invalid_addr(s));
        }
        Ok(Self::new(tenant, namespace, kind, name))
    }
}

fn invalid_addr(s: &str) -> ExecutionError {
    ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
        "invalid object address {s:?}: expected {{tenant}}/{{namespace}}/{{kind}}/{{name}}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(ms: u64) -> Timestamp {
        Timestamp::from_millis(ms)
    }

    #[test]
    fn object_kind_roundtrips_via_string() {
        for k in [
            ObjectKind::Flow,
            ObjectKind::FlowVersion,
            ObjectKind::Execution,
            ObjectKind::Activity,
            ObjectKind::Timer,
            ObjectKind::Task,
        ] {
            assert_eq!(ObjectKind::parse(k.as_str()), Some(k));
            assert_eq!(format!("{k}"), k.as_str());
        }
        assert_eq!(ObjectKind::parse("wat"), None);
    }

    #[test]
    fn object_meta_born_touch_and_address() {
        let mut meta = ObjectMeta::born(
            ObjectKind::Execution,
            ScopeName::new("myco").unwrap(),
            ScopeName::new("default").unwrap(),
            ObjectName::generated_with_suffix("checkout", "a1b2c3").unwrap(),
            ulid::Ulid::new(),
            ts(1000),
        );
        assert_eq!(meta.created_at, meta.updated_at);
        assert_eq!(meta.owner, None); // roots have no owner
        meta.touch(ts(2000));
        assert_eq!(meta.updated_at, ts(2000));
        assert_eq!(meta.created_at, ts(1000)); // created_at is immutable

        // with_owner attaches a same-scope reference and composes with construction.
        let owned = meta.clone().with_owner(OwnerReference::new(
            ObjectKind::Execution,
            ObjectName::generated_with_suffix("checkout", "a1b2c3").unwrap(),
            ulid::Ulid::new(),
        ));
        let owner = owned.owner.expect("with_owner attaches the reference");
        assert_eq!(
            owner.address(&meta.tenant, &meta.namespace).to_string(),
            "myco/default/execution/checkout-a1b2c3"
        );

        let addr = meta.address();
        assert_eq!(addr.to_string(), "myco/default/execution/checkout-a1b2c3");
    }

    #[test]
    fn owner_reference_roundtrips_and_derives_address_in_scope() {
        let tenant = ScopeName::new("myco").unwrap();
        let namespace = ScopeName::new("default").unwrap();
        let owner = OwnerReference::new(
            ObjectKind::Activity,
            ObjectName::generated_with_suffix("checkout", "act42").unwrap(),
            // Built with a nil uid: the string form cannot carry a uid, so a round-tripped reference
            // resolves to `nil` and must be reconciled against the live owner by the caller.
            ulid::Ulid::nil(),
        );
        // Same-scope Display form `{kind}/{name}` roundtrips (uid is not printable; parse yields nil).
        assert_eq!(owner.to_string(), "activity/checkout-act42");
        let reparsed: OwnerReference = owner.to_string().parse().unwrap();
        assert_eq!(reparsed, owner);
        assert_eq!(reparsed.uid, ulid::Ulid::nil());

        // Full address resolves within the owned object's scope.
        assert_eq!(
            owner.address(&tenant, &namespace).to_string(),
            "myco/default/activity/checkout-act42"
        );

        // Malformed references are rejected.
        assert!("activity".parse::<OwnerReference>().is_err()); // missing name
        assert!("wat/checkout".parse::<OwnerReference>().is_err()); // bad kind
        assert!("activity/a/b".parse::<OwnerReference>().is_err()); // too many parts
    }

    #[test]
    fn object_address_roundtrips_and_rejects_malformed() {
        // User-named address roundtrips through the Display form.
        let a = ObjectAddress::new(
            ScopeName::new("myco").unwrap(),
            ScopeName::new("default").unwrap(),
            ObjectKind::Activity,
            ObjectName::plain("act42").unwrap(),
        );
        assert_eq!(a.to_string(), "myco/default/activity/act42");
        assert_eq!(a.to_string().parse::<ObjectAddress>().unwrap(), a);

        // Generated child names (containing '-') also roundtrip — parsing must accept both flavors.
        let child = ObjectAddress::new(
            ScopeName::new("myco").unwrap(),
            ScopeName::new("default").unwrap(),
            ObjectKind::Task,
            ObjectName::generated_with_suffix("checkout", "t9f8").unwrap(),
        );
        assert_eq!(child.to_string(), "myco/default/task/checkout-t9f8");
        assert_eq!(child.to_string().parse::<ObjectAddress>().unwrap(), child);

        // Malformed addresses are rejected.
        assert!("myco/default/execution".parse::<ObjectAddress>().is_err()); // missing name
        assert!(
            "myco/default/wat/checkout"
                .parse::<ObjectAddress>()
                .is_err()
        ); // bad kind
        assert!(
            "myco/default/execution/checkout/extra"
                .parse::<ObjectAddress>()
                .is_err()
        ); // too many
        assert!(
            "/default/execution/checkout"
                .parse::<ObjectAddress>()
                .is_err()
        ); // missing tenant
        assert!(
            "myco/default/execution/foo/bar"
                .parse::<ObjectAddress>()
                .is_err()
        ); // name has '/'
    }

    #[test]
    fn address_orders_so_scope_prefixes_group() {
        // Addresses sort so that all objects under one scope share a contiguous prefix — the
        // property that makes per-scope range scans cheap (Bigtable/Cassandra-style).
        let mk = |ns: &str, name: &str| {
            ObjectAddress::new(
                ScopeName::new("myco").unwrap(),
                ScopeName::new(ns).unwrap(),
                ObjectKind::Execution,
                ObjectName::plain(name).unwrap(),
            )
            .to_string()
        };
        let mut v = vec![
            mk("bravo", "xyz2"),
            mk("alpha", "xyz1"),
            mk("alpha", "xyz2"),
        ];
        v.sort();
        assert_eq!(
            v,
            vec![
                "myco/alpha/execution/xyz1",
                "myco/alpha/execution/xyz2",
                "myco/bravo/execution/xyz2"
            ]
        );
    }

    #[test]
    fn object_reference_roundtrips_through_display_and_fromstr() {
        let r = ObjectReference::new(
            ObjectKind::FlowVersion,
            ObjectName::generated_with_suffix("order", "00000001").unwrap(),
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
        assert_eq!(
            ObjectReference::nil().to_string(),
            "flowversion/flow-00000000"
        );
        assert_ne!(
            nil,
            ObjectReference::new(
                ObjectKind::FlowVersion,
                ObjectName::generated_with_suffix("flow", "00000000").unwrap(),
                ulid::Ulid::new(),
            )
        );
    }
}
