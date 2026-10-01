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
//!   reference form ([`RawObjectRef`]) built on top — the pieces that couple naming to the
//!   engine's object model.
//!
//! See `docs/identity-and-partitioning-design.md` for the full design.

use std::marker::PhantomData;

use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

use crate::types::activity::ActivityKind;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::execution::ExecutionKind;
use crate::types::thread::ThreadKind;
use spica_machinery::Timestamp;

pub use spica_machinery::name::{ObjectName, PlainName, ScopeName};

/// The type of a spica object — the k8s "Kind" of its [`ObjectMeta`]. The reference on an
/// [`RawObjectRef`] carries this kind, so a single value both names an object *and* discriminates
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

/// The compile-time map from an object type to its [`ObjectKind`] and to what may own it, declared
/// once per object type by a zero-sized marker (`ActivityKind`, `TaskKind`, …).
///
/// [`ObjectMeta`] is parameterised by it, so a meta cannot be built for one kind and read back as
/// another: the kind is never a stored value that could drift from the record holding it, and
/// [`ObjectMeta::reference`] derives it from the type alone. [`Self::OwnedBy`] applies the same
/// discipline one edge up the tree, so reading an owner never means branching on a runtime kind.
pub trait ObjectKindMarker {
    const KIND: ObjectKind;

    /// What stands in this object's owner slot: exactly one kind ([`ObjectRef`]), one of a fixed set
    /// (an entity's own sum type), or nothing at all ([`NoOwner`] — a root object).
    type OwnedBy: OwnerKindMarker;
}

/// What may stand in an owner slot, and how it crosses the wire — the bound on
/// [`ObjectKindMarker::OwnedBy`].
///
/// A slot's whole optionality lives here: [`Self::to_raw_object_ref`] is the only owner read that may
/// answer "no address" (a root's [`NoOwner`]), and [`Self::from_wire`] is the only place an absent
/// `owner` is admitted. The total (non-optional) form of the same read is one level up, in
/// [`HasRawObjectRef`], which an empty slot deliberately does not implement — so the engine's owner reads
/// are total on owned objects and unrepresentable on roots.
pub trait OwnerKindMarker: Clone + std::fmt::Debug + PartialEq {
    /// The owner's [`RawObjectRef`], or `None` for a slot that holds no address at all.
    fn to_raw_object_ref(&self) -> Option<&RawObjectRef>;

    /// Fold the wire's optional `owner` into the slot. The absent form is admitted only where the
    /// slot's own type says so, so "an owned object always names its owner" is refused at the read
    /// instead of being trusted by every later reader.
    fn from_wire(kind: ObjectKind, owner: Option<Self>) -> Result<Self, MissingOwner>
    where
        Self: Sized,
    {
        owner.ok_or(MissingOwner {
            kind,
            slot: std::any::type_name::<Self>(),
        })
    }
}

mod sealed {
    /// Restricts [`super::HasRawObjectRef`] to the slots this module defines: a foreign impl could claim
    /// an erasure its own type does not have.
    pub trait Sealed {}
}

/// A reference slot that carries an **address** — the engine's face of any typed reference, where
/// [`OwnerKindMarker`] is the wire's.
///
/// This is the single **erasure seam**: [`Self::as_raw_object_ref`] is the one way a typed
/// reference — an owner slot or a plain typed field — becomes the flat reference the storage
/// contract, `active_children` and command payloads speak. A `Deref` to [`RawObjectRef`] would
/// hide that seam and re-open `.kind` — the read a typed field exists to forbid — so the trait
/// deliberately exposes no `kind()`: a reference's kind is either matched at compile time through
/// the field's own type, or dropped at this one greppable call.
///
/// [`NoOwner`] is not one, and the omission is load-bearing:
/// `meta.owner.into_raw_object_ref()` compiles on an owned object and does not compile on a
/// root's meta, so "a root has no owner" needs no `expect`, no `Option`, and no runtime kind check
/// anywhere.
pub trait HasRawObjectRef: OwnerKindMarker + sealed::Sealed {
    /// Erase to the flat reference form — where the type stops being carried.
    fn as_raw_object_ref(&self) -> &RawObjectRef;

    /// The same erasure for an owner the caller owns: the seam sites (storage's `add_child`/
    /// `remove_child`, command payloads) take the flat reference by value, and cloning it out of a
    /// borrow only to move it again is the borrow checker's business leaking into every caller.
    fn into_raw_object_ref(self) -> RawObjectRef;
}

/// A flat reference whose kind is not the one the slot it was read into admits.
///
/// Its own type rather than an [`ExecutionError`](crate::ExecutionError) on purpose: a mismatch is
/// the *payload's* fault, so a caller must refuse the command — while an `ExecutionError` propagated
/// with a bare `?` lands in the engine's internal-fault arm, which the leader **retries**. Not being
/// convertible means the wrong classification cannot be written by accident.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KindMismatch {
    /// The kind the flat reference carried.
    seen: ObjectKind,
    /// The one kind the slot admits.
    expected: ObjectKind,
    /// The marker type naming the slot (`ObjectRef<ActivityKind>` …), so the fault names the *type*
    /// that was expected, not merely a kind constant it admits.
    slot: &'static str,
}

impl std::fmt::Display for KindMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "reference kind mismatch: the payload carries kind {:?}, but this field admits only {:?} \
             ({})",
            self.seen, self.expected, self.slot,
        )
    }
}

impl std::error::Error for KindMismatch {}

/// A payload that carries no `owner`, read into a slot that only ever holds one.
///
/// Its own type, like [`KindMismatch`]: the fault is the *payload's* — an object whose slot names
/// an owner is minted together with it and never loses it, so a row without one cannot be read. Not an
/// [`ExecutionError`](crate::ExecutionError) for the same reason a mismatch is not: propagating it
/// with `?` would land it in the internal-fault arm, which the leader retries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingOwner {
    /// The kind of the object whose slot was left empty.
    kind: ObjectKind,
    /// The slot type that requires an owner (`ObjectRef<ActivityKind>` …), so the fault names the type
    /// that was expected rather than only the object that came up short.
    slot: &'static str,
}

impl std::fmt::Display for MissingOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "owner is missing: a {:?} is always owned (its slot is {}), but the payload carries no \
             `owner`",
            self.kind, self.slot,
        )
    }
}

impl std::error::Error for MissingOwner {}

/// The owner slot of a **root** object: no owner at all.
///
/// "A top-level `Execution` has no parent" is the *type* of its slot rather than an empty `Option`, so
/// no reader unwraps to learn it: the slot is [`Self`], and [`Self`] is not an [`HasRawObjectRef`] — there
/// is no address to read and none to set. It is constructible because a root's meta must be built;
/// it cannot stand in an owned slot, which admits only references.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NoOwner(());

impl NoOwner {
    /// The one value of a root's owner slot.
    pub fn new() -> Self {
        Self(())
    }
}

/// A typed reference to an object **of kind `K::KIND`** — the one reference form a field may hold
/// when its kind is fixed by the field's meaning rather than by the value (an owner slot, a
/// `Task`'s flat `execution` anchor, a `FlowVersion` a run binds to).
///
/// The kind is still a value in memory (the wire carries it, and rows written before the field was
/// typed stay readable), but it is private and written through exactly two gates: the checked
/// conversion from an [`RawObjectRef`] and `Deserialize`. Every other path — including reading it
/// back — goes through [`HasRawObjectRef::as_raw_object_ref`].
pub struct ObjectRef<K: ObjectKindMarker>(RawObjectRef, PhantomData<fn() -> K>);

// Hand-written so `K` needs no `Clone`/`Debug`/`PartialEq` of its own: the marker is a
// `PhantomData<fn() -> K>`, present only to name `K::KIND`, and every value lives in the inner
// `RawObjectRef`. A derive would demand those bounds from every marker for nothing.
impl<K: ObjectKindMarker> Clone for ObjectRef<K> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), PhantomData)
    }
}

impl<K: ObjectKindMarker> std::fmt::Debug for ObjectRef<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(std::any::type_name::<K>())
            .field(&self.0)
            .finish()
    }
}

impl<K: ObjectKindMarker> PartialEq for ObjectRef<K> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<K: ObjectKindMarker> Eq for ObjectRef<K> {}

// Delegates to the inner reference so a typed reference can key a map — the machine cache
// (`HandlerContext::definitions`) is keyed by version. The kind is carried by `K` and adds nothing a
// hash could hold, and `K` must not be required to implement `Hash` itself.
impl<K: ObjectKindMarker> std::hash::Hash for ObjectRef<K> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<K: ObjectKindMarker> std::fmt::Display for ObjectRef<K> {
    /// The same scope-relative `{kind}/{name}` form an [`RawObjectRef`] prints, so log sites read
    /// identically whether the field they hold is typed or flat.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<K: ObjectKindMarker> ObjectRef<K> {
    /// Build from already-known parts. The kind comes from `K`, so it cannot be passed wrong.
    pub fn new(name: ObjectName, uid: ulid::Ulid) -> Self {
        Self(RawObjectRef::new(K::KIND, name, uid), PhantomData)
    }

    /// The referenced object's name.
    pub fn name(&self) -> &ObjectName {
        &self.0.name
    }

    /// The referenced object's incarnation id.
    pub fn uid(&self) -> ulid::Ulid {
        self.0.uid
    }

    /// A placeholder of this kind with a nil uid. [`RawObjectRef::nil`] is the same placeholder
    /// before a slot has picked a kind; this one keeps the *kind* honest where only the kind carries
    /// meaning — a value matched by variant and a single echoed key, never by full equality.
    pub fn nil() -> Self {
        Self::new(
            ObjectName::from_parsed("unset").expect("a static literal is a valid object name"),
            ulid::Ulid::nil(),
        )
    }
}

impl<K: ObjectKindMarker> TryFrom<RawObjectRef> for ObjectRef<K> {
    type Error = KindMismatch;

    /// Checked entry from the flat form: the one place a foreign kind is rejected instead of being
    /// carried along until some later reader guesses wrong.
    fn try_from(reference: RawObjectRef) -> Result<Self, Self::Error> {
        if reference.kind != K::KIND {
            return Err(KindMismatch {
                seen: reference.kind,
                expected: K::KIND,
                slot: std::any::type_name::<K>(),
            });
        }
        Ok(Self(reference, PhantomData))
    }
}

impl<K: ObjectKindMarker> OwnerKindMarker for ObjectRef<K> {
    fn to_raw_object_ref(&self) -> Option<&RawObjectRef> {
        Some(&self.0)
    }
}

impl<K: ObjectKindMarker> sealed::Sealed for ObjectRef<K> {}

impl<K: ObjectKindMarker> HasRawObjectRef for ObjectRef<K> {
    fn as_raw_object_ref(&self) -> &RawObjectRef {
        &self.0
    }

    fn into_raw_object_ref(self) -> RawObjectRef {
        self.0
    }
}

impl<K: ObjectKindMarker> Serialize for ObjectRef<K> {
    /// Writes exactly the `{kind, name, uid}` object the slot held before it was typed.
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(s)
    }
}

impl<'de, K: ObjectKindMarker> Deserialize<'de> for ObjectRef<K> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let seen = RawObjectRef::deserialize(d)?;
        // Rejected here, on the way in, rather than repaired into a slot that cannot hold it: a row
        // naming a foreign owner is a row this type cannot read, and a log entry is no different.
        ObjectRef::try_from(seen).map_err(serde::de::Error::custom)
    }
}

/// The owner slot of a **Thread**: the one scope a thread runs in, of the two that exist.
///
/// A fan-out thread is owned by the container `Parallel`/`Map` activity that spawned it; a **root**
/// thread is the scope a whole top-level run executes in, so it is owned by the `Execution` itself.
/// The two are different *kinds*, and the drain cascade sends each to a different parent, so the slot
/// is a sum rather than a widened reference: dispatch is an exhaustive `match` and no reader compares
/// a runtime kind to learn which parent it holds. The wire's `kind` decides a variant exactly once,
/// on the way in ([`Self::deserialize`]) — parse, don't validate.
#[derive(Debug, Clone, PartialEq)]
pub enum ThreadOwner {
    Execution(ObjectRef<ExecutionKind>),
    Activity(ObjectRef<ActivityKind>),
}

impl Serialize for ThreadOwner {
    /// The variant's own reference — the union adds no field of its own, so the wire keeps the
    /// `{kind, name, uid}` object the slot held before it was typed.
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            ThreadOwner::Execution(r) => r.serialize(s),
            ThreadOwner::Activity(r) => r.serialize(s),
        }
    }
}

impl<'de> Deserialize<'de> for ThreadOwner {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let seen = RawObjectRef::deserialize(d)?;
        Ok(match seen.kind {
            ObjectKind::Execution => {
                ThreadOwner::Execution(ObjectRef::try_from(seen).map_err(serde::de::Error::custom)?)
            }
            ObjectKind::Activity => {
                ThreadOwner::Activity(ObjectRef::try_from(seen).map_err(serde::de::Error::custom)?)
            }
            // A kind outside the union is a payload no thread could have carried: refused here rather
            // than repaired into a slot with no variant for it.
            other => {
                return Err(serde::de::Error::custom(format!(
                    "reference kind mismatch: the payload carries kind {other:?}, but this slot admits \
                     only Execution or Activity owners ({})",
                    std::any::type_name::<ThreadOwner>(),
                )));
            }
        })
    }
}

impl OwnerKindMarker for ThreadOwner {
    fn to_raw_object_ref(&self) -> Option<&RawObjectRef> {
        Some(self.as_raw_object_ref())
    }
}

impl sealed::Sealed for ThreadOwner {}

impl HasRawObjectRef for ThreadOwner {
    fn as_raw_object_ref(&self) -> &RawObjectRef {
        match self {
            ThreadOwner::Execution(r) => r.as_raw_object_ref(),
            ThreadOwner::Activity(r) => r.as_raw_object_ref(),
        }
    }

    fn into_raw_object_ref(self) -> RawObjectRef {
        match self {
            ThreadOwner::Execution(r) => r.into_raw_object_ref(),
            ThreadOwner::Activity(r) => r.into_raw_object_ref(),
        }
    }
}

/// The owner slot of a **Timer**: the scope whose deadline it is, of the two scopes that arm one.
///
/// An `ExecutionTimeout` is armed by the top-level run itself, while a `WaitResume`, a task retry or a
/// task timeout is armed by the activity that is waiting — so the slot is the same `Execution`-or-
/// `Activity` sum as a thread's, kept as its own type because *which* two scopes may own a timer is a
/// fact about timers: widening a timer's owner later must not widen a thread's by accident.
#[derive(Debug, Clone, PartialEq)]
pub enum TimerOwner {
    Execution(ObjectRef<ExecutionKind>),
    Activity(ObjectRef<ActivityKind>),
}

impl Serialize for TimerOwner {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            TimerOwner::Execution(r) => r.serialize(s),
            TimerOwner::Activity(r) => r.serialize(s),
        }
    }
}

impl<'de> Deserialize<'de> for TimerOwner {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let seen = RawObjectRef::deserialize(d)?;
        Ok(match seen.kind {
            ObjectKind::Execution => {
                TimerOwner::Execution(ObjectRef::try_from(seen).map_err(serde::de::Error::custom)?)
            }
            ObjectKind::Activity => {
                TimerOwner::Activity(ObjectRef::try_from(seen).map_err(serde::de::Error::custom)?)
            }
            other => {
                return Err(serde::de::Error::custom(format!(
                    "reference kind mismatch: the payload carries kind {other:?}, but this slot admits \
                     only Execution or Activity owners ({})",
                    std::any::type_name::<TimerOwner>(),
                )));
            }
        })
    }
}

impl OwnerKindMarker for TimerOwner {
    fn to_raw_object_ref(&self) -> Option<&RawObjectRef> {
        Some(self.as_raw_object_ref())
    }
}

impl sealed::Sealed for TimerOwner {}

impl HasRawObjectRef for TimerOwner {
    fn as_raw_object_ref(&self) -> &RawObjectRef {
        match self {
            TimerOwner::Execution(r) => r.as_raw_object_ref(),
            TimerOwner::Activity(r) => r.as_raw_object_ref(),
        }
    }

    fn into_raw_object_ref(self) -> RawObjectRef {
        match self {
            TimerOwner::Execution(r) => r.into_raw_object_ref(),
            TimerOwner::Activity(r) => r.into_raw_object_ref(),
        }
    }
}

/// A terminal-failure scope: the two kinds a run's teardown can be directed at.
///
/// Not an owner slot — no object declares it as its [`ObjectKindMarker::OwnedBy`]; it names the two
/// roles whose addressing differs, so a caller holding "the scope above me" cannot reach for the
/// wrong verb. A `Thread` (the branch/item that must be stopped) terminates by reference via
/// `TerminateThread`, while an `Execution` is addressed by name+uid and is reached only for the run
/// itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerScope {
    Execution(ObjectRef<ExecutionKind>),
    Thread(ObjectRef<ThreadKind>),
}

impl OwnerScope {
    /// Recover the scope a flat reference names: the one seam where an address the engine carries
    /// *flat* (a command's `execution`, an activity's `execution` anchor or owner) becomes a scope
    /// type again. Those anchors are minted by the engine itself, so a reference that names no scope
    /// is an anomaly, not a decision: it yields `None` — the caller records the failure without a
    /// scope and the log names the address — rather than panicking the processor on a corrupt payload.
    pub fn of_reference(reference: &RawObjectRef) -> Option<Self> {
        match reference.kind {
            ObjectKind::Execution => ObjectRef::<ExecutionKind>::try_from(reference.clone())
                .ok()
                .map(OwnerScope::Execution),
            ObjectKind::Thread => ObjectRef::<ThreadKind>::try_from(reference.clone())
                .ok()
                .map(OwnerScope::Thread),
            _ => None,
        }
    }
}

impl OwnerKindMarker for NoOwner {
    fn to_raw_object_ref(&self) -> Option<&RawObjectRef> {
        None
    }

    /// The empty slot has nothing to fold: a payload that *carries* an `owner` never reaches here,
    /// because no [`NoOwner`] deserializes from one (see its `Deserialize`) — that refusal happens one
    /// step earlier, at the value.
    fn from_wire(_kind: ObjectKind, owner: Option<Self>) -> Result<Self, MissingOwner> {
        debug_assert!(
            owner.is_none(),
            "an owner cannot be read into an empty slot"
        );
        Ok(NoOwner::new())
    }
}

impl Serialize for NoOwner {
    /// Unreachable through an [`ObjectMeta`], which omits an empty owner entirely
    /// ([`owner_is_absent`]); present so the type carries no panic of its own.
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_unit()
    }
}

impl<'de> Deserialize<'de> for NoOwner {
    fn deserialize<D: Deserializer<'de>>(_d: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "a root object cannot be owned: the payload carries an `owner`",
        ))
    }
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
/// [`Self::owner`] links each object to its single owning parent in the object tree. `owner` is the
/// **single parent edge** for every entity — the former ad-hoc `Execution.parent`/`Activity.parent`/
/// `Task.parent`/`Timer.parent` handle fields have migrated into it — and its type is
/// [`ObjectKindMarker::OwnedBy`], so which kinds may occupy it is fixed by the object type: no reader
/// has to branch on a runtime kind to know who owns what. A root object (a top-level `Execution`)
/// declares [`NoOwner`], so it has no parent *by type*. `root_execution` is a separate flat
/// top-of-tree query anchor, **not** the owner (the owner is the direct parent).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "MetaRepr<K>")]
#[serde(bound(
    serialize = "K::OwnedBy: Serialize",
    deserialize = "K::OwnedBy: Deserialize<'de>"
))]
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
    /// The object that owns this one, typed by [`ObjectKindMarker::OwnedBy`] — **always a value**: a
    /// slot that names an owner holds its reference outright, and a root's slot is [`NoOwner`], so
    /// "no owner" is a type rather than a missing field. Same `tenant`/`namespace` scope is
    /// *inherited* from this object's own scope (spica ownership never crosses a scope), so the
    /// reference carries only `kind`/`name`/`uid`. Serialized away exactly where the slot is empty
    /// ([`owner_is_absent`]), so a root's row still carries no `owner` key.
    #[serde(skip_serializing_if = "owner_is_absent")]
    pub owner: K::OwnedBy,
}

/// Whether an owner slot holds no address at all — the one case the wire omits `owner` for.
fn owner_is_absent<T: OwnerKindMarker>(owner: &T) -> bool {
    owner.to_raw_object_ref().is_none()
}

/// The **wire form** of an [`ObjectMeta`]: `owner` is optional *there* (a root's row carries none),
/// and this is where that optionality is read out and folded into the slot's own type
/// ([`OwnerKindMarker::from_wire`]). A separate mirror is unavoidable: serde's derive admits an absent
/// field only through `Option`/`Default`, and an owner slot must never default to "no owner".
#[derive(Deserialize)]
#[serde(bound(deserialize = "K::OwnedBy: Deserialize<'de>"))]
struct MetaRepr<K: ObjectKindMarker> {
    kind: KindTag<K>,
    tenant: ScopeName,
    namespace: ScopeName,
    name: ObjectName,
    uid: ulid::Ulid,
    created_at: Timestamp,
    updated_at: Timestamp,
    #[serde(default)]
    owner: Option<K::OwnedBy>,
}

impl<K: ObjectKindMarker> TryFrom<MetaRepr<K>> for ObjectMeta<K> {
    type Error = MissingOwner;

    fn try_from(repr: MetaRepr<K>) -> Result<Self, Self::Error> {
        Ok(ObjectMeta {
            kind: repr.kind,
            tenant: repr.tenant,
            namespace: repr.namespace,
            name: repr.name,
            uid: repr.uid,
            created_at: repr.created_at,
            updated_at: repr.updated_at,
            owner: K::OwnedBy::from_wire(K::KIND, repr.owner)?,
        })
    }
}

impl<K: ObjectKindMarker> ObjectMeta<K> {
    /// Start building fresh meta for a `K` — the kind is `K`'s, never an argument. See
    /// [`ObjectMetaBuilder`].
    pub fn builder(uid: ulid::Ulid) -> ObjectMetaBuilder<K> {
        ObjectMetaBuilder::new(uid)
    }

    /// The canonical reference to this object — the one implementation for every kind, replacing the
    /// per-object `reference()` helpers that each hard-coded their own kind.
    pub fn reference(&self) -> RawObjectRef {
        RawObjectRef::new(K::KIND, self.name.clone(), self.uid)
    }

    /// The same address as [`Self::reference`], in the type `K` already is — for a site that must
    /// hand it to a typed slot (a command payload's activity/thread id) and would otherwise rebuild
    /// it from `name`/`uid` by hand.
    pub fn typed_reference(&self) -> ObjectRef<K> {
        ObjectRef::new(self.name.clone(), self.uid)
    }

    /// Record a mutation at `at`: advances `updated_at`, leaves `created_at`.
    pub fn with_update_at(&mut self, at: Timestamp) {
        self.updated_at = at;
    }

    /// Re-parent this object: `owner` is this object's slot type, so a wrong-kind owner cannot be
    /// passed, and an ownerless object cannot be detached (`NoOwner` is not an owned slot). The
    /// reference is same-scope by construction — see [`Self::owner`].
    pub fn with_owner(mut self, owner: K::OwnedBy) -> Self {
        self.owner = owner;
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

    /// Finish the object with the owner its slot admits — the **only** way out of the builder, because
    /// a slot that names an owner has no valid value without one. A root (a `Flow`, a top-level
    /// `Execution`) passes [`NoOwner::new`], the value its own slot admits; any other owner cannot be
    /// passed to it, and a meta for an owned kind cannot be built without one.
    pub fn with_owner(self, owner: K::OwnedBy) -> ObjectMeta<K> {
        ObjectMeta {
            kind: KindTag(PhantomData),
            tenant: self.tenant,
            namespace: self.namespace,
            name: self.name,
            uid: self.uid,
            created_at: self.created_at,
            updated_at: self.updated_at,
            owner,
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
/// A **raw** reference: its kind is a stored value, not the type parameter [`ObjectRef`] carries.
/// It is what the wire, a storage key and the genuinely heterogeneous seams (`active_children`, a
/// scope that may be any object) speak — so every site holding one is a place typing deliberately
/// stops, and reaching for it on a field that names a single kind is the mistake the name makes
/// visible.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RawObjectRef {
    /// The referenced object's kind.
    pub kind: ObjectKind,
    /// The referenced object's name (user or generated).
    pub name: ObjectName,
    /// The referenced object's incarnation id — confirms the reference points at the *same* object
    /// even if a name has since been re-used.
    pub uid: ulid::Ulid,
}

impl RawObjectRef {
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

    /// Re-establish a flat reference's type after the caller has already dispatched on its `kind`.
    ///
    /// A mixed-kind collection — `active_children` above all — holds addresses on purpose, and every
    /// sweep arm matches the kind it acts on before building a typed command payload. A conversion
    /// that fails there contradicts a check the caller just made, so it is an engine bug loud enough
    /// to panic on rather than a payload to refuse with a rejection.
    pub fn typed<K: ObjectKindMarker>(self) -> ObjectRef<K> {
        ObjectRef::try_from(self)
            .expect("the caller matched the kind before re-typing the reference")
    }
}

impl std::fmt::Display for RawObjectRef {
    /// Same-scope form `{kind}/{name}`. The address is scope-relative by construction (the
    /// referenced object shares the holder's tenant/namespace); `uid` is carried as a separate
    /// field, not here.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.kind, self.name)
    }
}

impl std::str::FromStr for RawObjectRef {
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

    /// A stand-in marker so the meta's own tests need no real object type. Its slot is
    /// [`NoOwner`] — these markers exist to be *owners*, never to have one.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestKind;
    impl ObjectKindMarker for TestKind {
        const KIND: ObjectKind = ObjectKind::Execution;
        type OwnedBy = NoOwner;
    }

    /// A second marker, so the mismatch case has somewhere to mismatch to.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct OtherTestKind;
    impl ObjectKindMarker for OtherTestKind {
        const KIND: ObjectKind = ObjectKind::Task;
        type OwnedBy = NoOwner;
    }

    /// A marker whose owner slot is **typed**, so the slot is exercised end-to-end through the meta
    /// (the root test markers above cannot: their slot is uninhabited).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct OwnedTestKind;
    impl ObjectKindMarker for OwnedTestKind {
        const KIND: ObjectKind = ObjectKind::Execution;
        type OwnedBy = ObjectRef<OtherTestKind>;
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
            .with_owner(NoOwner::new());
        assert_eq!(meta.created_at, meta.updated_at);
        assert_eq!(meta.owner, NoOwner::new()); // roots have no owner — as a type, not a missing field
        assert_eq!(meta.reference().kind, ObjectKind::Execution); // the kind is `K`'s, not a field
        meta.with_update_at(ts(2000));
        assert_eq!(meta.updated_at, ts(2000));
        assert_eq!(meta.created_at, ts(1000)); // created_at is immutable
    }

    #[test]
    fn object_meta_roundtrips_with_the_kind_on_the_wire() {
        let meta = ObjectMeta::<TestKind>::builder(ulid::Ulid::new())
            .at(ts(1000))
            .with_owner(NoOwner::new());
        let json = serde_json::to_value(&meta).expect("meta serializes");
        // The spelling is load-bearing: `"Execution"` is what the wire carried before the meta was
        // parameterised, so pinning it keeps rows written earlier readable.
        assert_eq!(
            json.get("kind"),
            Some(&serde_json::json!("Execution")),
            "wire: {json}"
        );
        // A root's owner slot holds no address, so the key is omitted entirely — the wire shape a
        // reader of pre-typing rows already expects.
        assert_eq!(json.get("owner"), None, "wire: {json}");
        let back: ObjectMeta<TestKind> = serde_json::from_value(json).expect("meta deserializes");
        assert_eq!(back, meta);
        assert_eq!(back.reference().kind, ObjectKind::Execution);
    }

    #[test]
    fn object_meta_rejects_a_foreign_kind_on_the_wire() {
        let meta = ObjectMeta::<TestKind>::builder(ulid::Ulid::new())
            .at(ts(1000))
            .with_owner(NoOwner::new());
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
                .with_owner(NoOwner::new()),
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
    fn raw_object_ref_roundtrips() {
        let owner = RawObjectRef::new(
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
        let reparsed: RawObjectRef = owner.to_string().parse().unwrap();
        assert_eq!(reparsed, owner);
        assert_eq!(reparsed.uid, ulid::Ulid::nil());

        // Malformed references are rejected.
        assert!("activity".parse::<RawObjectRef>().is_err()); // missing name
        assert!("wat/checkout".parse::<RawObjectRef>().is_err()); // bad kind
        assert!("activity/a/b".parse::<RawObjectRef>().is_err()); // too many parts
    }

    #[test]
    fn object_reference_roundtrips_through_display_and_fromstr() {
        let r = RawObjectRef::new(
            ObjectKind::FlowVersion,
            PlainName::new("order").unwrap().generated_from_key(1),
            ulid::Ulid::new(),
        );
        // Display is the scope-relative `kind/name` form (uid is a separate field, not encoded), so
        // `FromStr` reconstructs the same kind+name with a nil uid (the caller supplies the uid).
        let s = r.to_string();
        assert_eq!(s, format!("flowversion/{}", r.name));
        let parsed = s
            .parse::<RawObjectRef>()
            .expect("reference Display reparses");
        assert_eq!(parsed.kind, r.kind);
        assert_eq!(parsed.name, r.name);
    }

    #[test]
    fn object_reference_nil_is_a_stable_placeholder() {
        // `RawObjectRef::nil` is a recognizable placeholder (never matches a real reference with a
        // fresh uid), and its Display is stable.
        let nil = RawObjectRef::nil();
        assert!(nil.uid.is_nil());
        assert_eq!(RawObjectRef::nil().to_string(), "flowversion/flow-0");
        assert_ne!(
            nil,
            RawObjectRef::new(
                ObjectKind::FlowVersion,
                PlainName::new("flow").unwrap().generated_from_key(0),
                ulid::Ulid::new(),
            )
        );
    }

    /// The typed placeholder carries what the flat one cannot: a *kind*. A fixture whose anchor no
    /// assertion ever compares still cannot be built for the wrong kind.
    #[test]
    fn a_typed_nil_keeps_its_kind() {
        let unset = ObjectRef::<OtherTestKind>::nil();
        assert!(unset.uid().is_nil());
        assert_eq!(unset.as_raw_object_ref().kind, ObjectKind::Task);
        assert_eq!(
            serde_json::to_value(&unset).unwrap(),
            serde_json::to_value(unset.as_raw_object_ref()).unwrap()
        );
    }

    #[test]
    fn owner_ref_pins_the_kind_on_the_wire() {
        let owner = ObjectRef::<OtherTestKind>::new(
            PlainName::new("checkout").unwrap().generated_from_key(7),
            ulid::Ulid::new(),
        );
        let json = serde_json::to_value(&owner).expect("owner serializes");
        // The wire is the same flat reference the slot held before it was typed — no wrapper, no
        // extra key: an `ObjectRef` is an `RawObjectRef` plus a type.
        assert_eq!(
            json,
            serde_json::to_value(owner.as_raw_object_ref()).unwrap()
        );

        let back: ObjectRef<OtherTestKind> =
            serde_json::from_value(json).expect("a matching owner deserializes");
        assert_eq!(back, owner);
        assert_eq!(back.name(), owner.name());
        assert_eq!(back.uid(), owner.uid());
        assert_eq!(back.as_raw_object_ref().kind, ObjectKind::Task);
    }

    #[test]
    fn owner_ref_rejects_a_foreign_kind_on_the_wire() {
        let owner =
            ObjectRef::<TestKind>::new(ObjectName::plain("checkout").unwrap(), ulid::Ulid::new());
        let json = serde_json::to_value(&owner).expect("owner serializes");
        let err = serde_json::from_value::<ObjectRef<OtherTestKind>>(json)
            .expect_err("a Task-typed slot must not read an Execution owner");
        let msg = err.to_string();
        assert!(msg.contains("reference kind mismatch"), "{msg}");
        assert!(msg.contains("carries kind Execution"), "{msg}");
        assert!(msg.contains("admits only Task"), "{msg}");
        // The fault names the slot *type*, so the reader learns which marker admitted the wrong kind.
        assert!(msg.contains("OtherTestKind"), "{msg}");
    }

    #[test]
    fn owner_ref_conversion_checks_the_kind() {
        let foreign = RawObjectRef::new(
            ObjectKind::Task,
            ObjectName::plain("checkout").unwrap(),
            ulid::Ulid::new(),
        );
        let err = ObjectRef::<TestKind>::try_from(foreign)
            .expect_err("a Task cannot stand in an Execution-typed slot");
        let msg = err.to_string();
        assert!(msg.contains("reference kind mismatch"), "{msg}");
        assert!(msg.contains("admits only Execution"), "{msg}");

        let matching = RawObjectRef::new(
            ObjectKind::Execution,
            ObjectName::plain("checkout").unwrap(),
            ulid::Ulid::new(),
        );
        let owner = ObjectRef::<TestKind>::try_from(matching).expect("the kind matches");
        assert_eq!(owner.as_raw_object_ref().kind, ObjectKind::Execution);
    }

    /// Every flat anchor a failure site holds (a command's `execution`, an activity's owner) reads
    /// back as the scope it names, and one that names no scope answers `None` — the disposition the
    /// fail paths give a leaf address, which must never be a panic: a corrupt payload cannot be
    /// allowed to wedge the processor into a retry loop.
    #[test]
    fn a_flat_reference_is_read_back_as_the_scope_it_names() {
        let run = RawObjectRef::new(
            ObjectKind::Execution,
            ObjectName::plain("execution").unwrap(),
            ulid::Ulid::new(),
        );
        assert_eq!(
            OwnerScope::of_reference(&run),
            Some(OwnerScope::Execution(ObjectRef::new(
                run.name.clone(),
                run.uid
            )))
        );

        let thread = RawObjectRef::new(
            ObjectKind::Thread,
            ObjectName::plain("parallel").unwrap(),
            ulid::Ulid::new(),
        );
        assert_eq!(
            OwnerScope::of_reference(&thread),
            Some(OwnerScope::Thread(ObjectRef::new(
                thread.name.clone(),
                thread.uid
            )))
        );

        // A nil reference is a `FlowVersion` placeholder: no scope, so no scope termination.
        assert_eq!(OwnerScope::of_reference(&RawObjectRef::nil()), None);
    }

    /// A thread's owner slot admits exactly the two scopes a thread can hang off, and the union adds
    /// nothing to the wire: each variant serializes as the flat reference it replaced, so a row written
    /// before the slot was typed still reads.
    #[test]
    fn thread_owner_roundtrips_both_of_its_scopes() {
        let cases = [
            ThreadOwner::Execution(ObjectRef::new(
                ObjectName::plain("execution").unwrap(),
                ulid::Ulid::new(),
            )),
            ThreadOwner::Activity(ObjectRef::new(
                ObjectName::plain("parallel").unwrap(),
                ulid::Ulid::new(),
            )),
        ];
        for owner in cases {
            let json = serde_json::to_value(&owner).expect("the union serializes");
            assert_eq!(
                json,
                serde_json::to_value(owner.as_raw_object_ref()).unwrap()
            );
            let back: ThreadOwner = serde_json::from_value(json).expect("the union deserializes");
            assert_eq!(back, owner);
        }
    }

    /// A kind no thread could hang off is refused on the way in, naming the slot's own type — the
    /// reader learns which union admitted the kinds it did, not merely that the payload was odd.
    #[test]
    fn thread_owner_refuses_a_foreign_kind() {
        let owner = ObjectRef::<OtherTestKind>::new(
            ObjectName::plain("checkout").unwrap(),
            ulid::Ulid::new(),
        );
        let err = serde_json::from_value::<ThreadOwner>(
            serde_json::to_value(owner).expect("owner serializes"),
        )
        .expect_err("a thread is never owned by a task");
        let msg = err.to_string();
        assert!(msg.contains("reference kind mismatch"), "{msg}");
        assert!(msg.contains("carries kind Task"), "{msg}");
        assert!(msg.contains("admits only Execution or Activity"), "{msg}");
        assert!(msg.contains("ThreadOwner"), "{msg}");
    }

    /// A timer's slot is its own type over the same two scopes, and refusing what *it* does not admit
    /// names `TimerOwner`: the two unions are deliberately separate, so widening one never widens the
    /// other silently.
    #[test]
    fn timer_owner_refuses_a_kind_it_does_not_admit() {
        let thread = RawObjectRef::new(
            ObjectKind::Thread,
            ObjectName::plain("branch").unwrap(),
            ulid::Ulid::new(),
        );
        let err = serde_json::from_value::<TimerOwner>(serde_json::to_value(&thread).unwrap())
            .expect_err("no thread ever arms a timer");
        let msg = err.to_string();
        assert!(msg.contains("carries kind Thread"), "{msg}");
        assert!(msg.contains("admits only Execution or Activity"), "{msg}");
        assert!(msg.contains("TimerOwner"), "{msg}");

        // A thread is a node, never a scope: neither union takes one, so a thread's own owner slot
        // (`ThreadOwner`) refuses this payload too — the two unions overlap only where reality does.
        assert!(
            serde_json::from_value::<ThreadOwner>(serde_json::to_value(&thread).unwrap()).is_err()
        );
    }

    #[test]
    fn a_typed_slot_is_enforced_through_the_meta() {
        let meta = ObjectMeta::<OwnedTestKind>::builder(ulid::Ulid::new())
            .at(ts(1000))
            .with_owner(ObjectRef::<OtherTestKind>::new(
                PlainName::new("checkout").unwrap().generated_from_key(7),
                ulid::Ulid::new(),
            ));
        let json = serde_json::to_value(&meta).expect("meta serializes");
        assert_eq!(json["owner"]["kind"], serde_json::json!("Task"));
        let back: ObjectMeta<OwnedTestKind> =
            serde_json::from_value(json.clone()).expect("the slot accepts its own owner kind");
        assert_eq!(back, meta);

        // A payload whose owner kind the slot does not allow is refused at the slot — after the
        // meta's own kind guard has already passed.
        let mut foreign = json;
        foreign["owner"]["kind"] = serde_json::json!("Activity");
        let err = serde_json::from_value::<ObjectMeta<OwnedTestKind>>(foreign)
            .expect_err("the slot accepts only a Task owner");
        assert!(err.to_string().contains("reference kind mismatch"), "{err}");
    }

    /// The other half of the slot's wire contract: a row that *omits* an owner read into a slot that
    /// only holds one is refused at the read — an owned object is minted together with its owner, so
    /// a row without one is a payload no event ever wrote.
    #[test]
    fn an_owned_slot_refuses_an_absent_owner_on_the_wire() {
        let owner = ObjectRef::<OtherTestKind>::new(
            PlainName::new("checkout").unwrap().generated_from_key(7),
            ulid::Ulid::new(),
        );
        let mut json = serde_json::to_value(
            ObjectMeta::<OwnedTestKind>::builder(ulid::Ulid::new())
                .at(ts(1000))
                .with_owner(owner),
        )
        .expect("meta serializes");
        json.as_object_mut()
            .expect("meta is an object")
            .remove("owner");
        let err = serde_json::from_value::<ObjectMeta<OwnedTestKind>>(json)
            .expect_err("an owner-carrying slot has no value without an owner");
        let msg = err.to_string();
        assert!(msg.contains("owner is missing"), "{msg}");
        assert!(msg.contains("always owned"), "{msg}");
        // The fault names the slot *type*, so the reader learns which slot came up short.
        assert!(msg.contains("OtherTestKind"), "{msg}");
    }

    #[test]
    fn a_root_slot_cannot_be_owned() {
        let payload = serde_json::json!({
            "kind": "Execution",
            "name": "checkout",
            "uid": ulid::Ulid::new().to_string(),
        });
        let err = serde_json::from_value::<NoOwner>(payload)
            .expect_err("no value can stand in a root's owner slot");
        assert!(err.to_string().contains("cannot be owned"), "{err}");
    }

    /// A root's meta refuses a payload that carries an `owner` just as the value does — the refusal is
    /// reached through the meta, which is where every row is read.
    #[test]
    fn a_root_meta_refuses_a_payload_that_carries_an_owner() {
        let owned = ObjectMeta::<OwnedTestKind>::builder(ulid::Ulid::new())
            .at(ts(1000))
            .with_owner(ObjectRef::<OtherTestKind>::new(
                PlainName::new("checkout").unwrap().generated_from_key(7),
                ulid::Ulid::new(),
            ));
        let json = serde_json::to_value(owned).expect("meta serializes");
        let err = serde_json::from_value::<ObjectMeta<TestKind>>(json)
            .expect_err("a root's slot admits no owner");
        assert!(err.to_string().contains("cannot be owned"), "{err}");
    }
}
