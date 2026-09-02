# Identity, Scope & Naming Design

**Status:** Draft (design phase, not yet approved/implemented)
**Owner:** spica-engine
**Scope:** how clients address, isolate, and deduplicate spica objects (`Flow`, `Execution`,
`Activity`, `Timer`, `Task`) under a `tenant → namespace → name` identity, structured after
Kubernetes' `ObjectMeta` + user-managed `name` model.
**Related:** [`../Architecture.md`](../Architecture.md),
[`durable-execution-recovery-design.md`](durable-execution-recovery-design.md) Appendix C/D,
`crates/engine/src/types/id.rs` (`FlowName`), `crates/engine/src/types/execution.rs`.

---

## 1. Purpose

Objects need scoped, *idempotent* identities. The prior draft keyed identity on a composite
`(scope, kind, ULID)` with a self-minted id — but self-minted ids make retry dedup painful (a
client that retries a create can't name the not-yet-known object). Following Kubernetes, we make
the **user-supplied `Name` the addressing and idempotency key** for persistent, client-created
objects, and use **system-generated names (`generateName`-style)** for transient runtime entities,
with `request_id` as the universal command-level idempotency key.

Additionally, because a `Name` can be *re-used* across object lifetimes (delete + recreate), a
**`uid`** (k8s `uid` role) is carried in `ObjectMeta` so two references can decide whether they
point to the *same* object even when the names coincide.

This document records the decisions (`Name`-based identity, reserved-`-` naming rule, random
generated suffix, shared `ObjectMeta` with `uid`, no `labels`), the per-entity mapping, the three
idempotency mechanisms, and how physical routing stays fully internal.

---

## 2. Decision summary

> Identity = **`(tenant, namespace, kind, name)`** — a composite, addressable as one derived string
> `{tenant}/{namespace}/{kind}/{name}`. `Name` is **user-supplied** for persistent objects (its
> idempotency key) and **system-generated** (random-suffix `generateName`) for transient objects.
> `-` is **reserved to the system generator** (user names never contain it). Because a name can
> recur across incarnations, every object also carries a **`uid`** (opaque ULID, k8s `uid`) to
> decide object sameness. Physical sharding is internal and never part of any identity. A shared
> **`ObjectMeta`** struct carries the common metadata every object reuses.

---

## 3. Identity model

### 3.1 Addressing

Every object is addressed by

```
(tenant, namespace, kind, name, uid)
```

- `tenant`, `namespace`, `name` are user-visible string segments; `kind` is a fixed enum
  (`Flow | Execution | Activity | Timer | Task`); `uid` is an opaque ULID.
- **Name** is the *addressing / idempotency* key; **uid** is the *sameness* key. To *look up* an
  object you use `(tenant, namespace, kind, name)`; to *assert it is the same object* as an earlier
  reference you compare `uid` (so a delete+recreate of `checkout` — new `uid` — is never confused
  with the original).

### 3.2 Naming rules

- **User-supplied segments** (`tenant`, `namespace`, and user-supplied `name`) share one charset,
  reusing the existing `FlowName` rule:

  ```
  ^[A-Za-z0-9][A-Za-z0-9_]{0,63}$      // 1..=64, A-Za-z0-9 start, then A-Za-z0-9_, case-sensitive
  ```

- **`-` is reserved.** A user-supplied `name` NEVER contains `-`. Only **system-generated** names
  may contain `-`, and the generator uses it as the suffix separator:

  ```
  system name := <base>-<suffix>        // suffix is random (k8s generateName), e.g. checkout-a1b2c
  ```

  Because user names ban `-`, any name containing `-` is *by construction* a generated object:
  user-name space and generated-name space are disjoint, and "is this generated?" is decidable by
  reading the string — no escaping or ambiguity.

  > **Deliberate divergence from Kubernetes**: k8s allows `-` in user pod names (DNS-1123) and
  > avoids ambiguity by carrying generation in a separate `generateName` field rather than parsing
  > the final name. We accept that divergence in exchange for lexical generation-detection, with
  > `_` retained as the user-facing word separator so readability does not suffer.

- `kind` is not free text; it is a fixed enum.

### 3.3 Per-entity naming

| Entity | Who supplies `name` | Shape | Idempotency of create |
|---|---|---|---|
| **Flow** | **user, required** | the flow name | **Name** (retried "create flow checkout" dedups) |
| **Execution** | user **optional**; default system `generateName` | if omitted: `<flowName>-<suffix>` | Name if given, else `request_id` |
| **Activity / Timer / Task** | **system** only | `<parentName>-<suffix>` | `request_id` (parent command) |
| **FlowVersion** | system, derived from flow | `<flowName>-<version>` | process-only; each publish is a new version |

Notes:
- **Random suffix** (k8s `generateName`): cheap and collision-free by construction; we accept that
  random generation is not byte-deterministic across otherwise-identical commands, because
  command-level idempotency is already guaranteed by `request_id`.
- `Execution` is **optional** user name, not required: like k8s pods, high-cardinality transient
  instances shouldn't force the client to invent unique names. A user *may* supply one as an
  alias/idempotency anchor for a specific run.
- `FlowVersion` uses `-` (not `_`) for consistency with the reserved-`-` rule: every system-
  generated name, including derived `FlowVersion` names, uses the same separator.
- **`uid` incarnations**: delete+recreate of a name yields a *new* `uid` (k8s `uid` role). `Flow`'s
  existing `flow_id` plays this role for flows; system entities' `uid` is their existing ULID.

### 3.4 Uniqueness

- Storage-level uniqueness of address is the composite **`(tenant, namespace, kind, name)`**.
- Physical sameness / incarnation identity is **`uid`** — unique across the whole system, never
  reused (a re-created object gets a fresh `uid`).
- The physical routing map is a **separate, internal layer** (root-execution lineage, see Appendix
  D) and never appears in any identity — client `tenant/namespace/name` and physical shard are
  decoupled, mirroring S3/Cassandra (logical keyspace independent of physical partitioning).

---

## 4. Idempotency — three mechanisms

| Mechanism | Owned by | Solves |
|---|---|---|
| **`Name`** (user-supplied) | persistent, client-created objects (Flow, and optional Execution runs) | retried create of the same *logical object* is a no-op/dedup |
| **`generateName`-derived** (system) | transient entities (Activity/Timer/Task, default-named Execution) | deterministic `<base>-<random-suffix>` guarantees a unique transient name |
| **`request_id`** (client-supplied per command) | **all** entity-creating commands | retried command is recognized by `is_already_applied` and not re-applied — this is what prevents a retried `CreateExecution` from spawning a duplicate run, independent of any name scheme |

`request_id` is the framework-level guarantee (already implemented in CCES); the Name-based
mechanisms are ergonomic accelerators layered on top for the objects that actually have names.

---

## 5. Shared `ObjectMeta` (k8s-style reuse)

One struct, embedded in every object, so the common metadata lives in a single place instead of
being copied per entity (this also consolidates the `created_at`/`updated_at` pair currently
duplicated across `Execution`/`Activity`/`Timer`/`Task`).

```rust
// types/objectmeta.rs (new)
pub struct ObjectMeta {
    /// Kind (k8s GVK "Kind"). Broader than the existing node-only `NodeKind`: includes Flow.
    pub kind: ObjectKind,
    /// Top-level isolation boundary.
    pub tenant: ScopeName,
    /// Scoping within the tenant.
    pub namespace: ScopeName,
    /// Addressing key — user-supplied (no '-') or system-generated (base + '-suffix').
    pub name: ObjectName,
    /// Opaque, system-minted, never-reused incarnation id (k8s `uid`). Because `name` can be
    /// re-used across delete+recreate, `uid` is what tells two references to the same *object*
    /// apart. For a system entity this equals its existing typed ULID (ExecutionId &c.); for a
    /// Flow it is the `FlowId`.
    pub uid: ulid::Ulid,
    /// When this object was born / last touched (the domain timestamps moved here).
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    /// The owning parent in the object tree, if any (k8s `OwnerReference`-style: kind + name + uid,
    /// same tenant/namespace scope inherited). None for roots.
    pub owner: Option<OwnerReference>,   // OwnerReference { kind, name, uid }
    // pub resource_version: u64,   // TODO(meta): optimistic concurrency for projection CAS
}
```

> Decisions applied: `uid` added for sameness; `labels` and `annotations` dropped (not designed) —
> no free-form extensibility fields yet. `owner` added as a single, same-scope parent reference (not
> a list); the ad-hoc `Execution.parent`/`root_execution` handles are the pre-meta form of this and
> should migrate into `owner` when P1/P2 embedding lands.

### 5.1 Embedding

```rust
// Execution — typed handle (ExecutionId) stays; shared fields + uid live in meta
pub struct Execution {
    pub meta: ObjectMeta,          // meta.uid == ExecutionId's ULID
    pub id: ExecutionId,
    pub flow_version_id: FlowVersionId,
    pub root_execution: ExecutionId,
    pub parent: Option<NodeId>,
    pub state_path: Option<jsonptr::PointerBuf>,
    pub status: ExecutionStatus,
    pub input: Value,
    pub output: Option<Value>,
}

// Activity / Timer / Task — same: drop their hand-added created_at/updated_at into meta
pub struct Task { pub meta: ObjectMeta, pub id: TaskId, /* ... */ }

// Flow — has a user name in meta.name; flow_id == meta.uid (incarnation id)
pub struct Flow { pub meta: ObjectMeta, pub flow_id: FlowId, /* ... */ }
```

### 5.2 Why the typed id is *not* folded into `ObjectMeta`

`ExecutionId`/`ActivityId`/`TimerId`/`TaskId` are load-bearing Rust newtypes used in storage keys,
`NodeId`, events, and `ExecutionRecord`. Kubernetes is weakly-typed JSON and can get away with a
single string; we keep type safety. So `ObjectMeta` holds the *identity* facts (name for addressing,
uid for sameness, timestamps), while the typed handle appears on each domain struct. `meta.uid` and
the typed id wrap the same ULID value, which is intentional: typed paths keep their safety; the
string/`uid` path keeps a uniform reference format.

---

## 6. Identity string, `uid`, and typed handle

Three faces, cleanly separated:

- **User `Name`** (in `ObjectMeta`) — the *external addressing* key: idempotency, isolation.
- **`uid`** (in `ObjectMeta`) — the *sameness* key: decides "is this the same object", robust
  against name reuse across delete+recreate.
- **Typed id** (`ExecutionId` etc.) — the *internal handle* used by events and storage, a compact
  ULID minted exactly once under the creating command (deduped by `request_id`, so never
  duplicated). For system entities `typed id == meta.uid`.

A reference to an object therefore pairs **name + uid**: name to locate, uid to confirm identity.

---

## 7. Storage & routing impact

- Projection keys become the composite **`tenant → namespace → kind → name`** (a prefix tree), which
  enables per-scope range scans and per-tenant hard isolation at the key level.
- **`uid`** stays an opaque per-object property (not part of the key tree) — it is the sameness
  handle, never the addressing key, mirroring k8s (`uid` is not the lookup path).
- The **physical** mapping (which shard holds a range) stays **internal** and is derived from
  root-execution lineage (Appendix D) — independent of the client keyspace. An object is therefore
  addressable by its stable name while being freely relocatable physically, like S3.
- Because identity includes `name` (+ `uid`), **moving an object across `tenant`/`namespace`
  changes its identity/address**; such moves must allocate a new name (or be treated as a new
  object).

---

## 8. Phased plan

1. **P0 — types**: add `ScopeName`/`Tenant`/`Namespace`/`ObjectName` (validated, `-`-free for user
   input), `ObjectKind`, `ObjectMeta` (with `uid`), `ObjectAddress` (derived `tenant/ns/kind/name`
   string with parse + roundtrip). Pure addition.
2. **P1 — embed**: give `Execution`/`Activity`/`Timer`/`Task`/`Flow` an `ObjectMeta`, moving
   `created_at`/`updated_at` into it and wiring `uid`; update appliers, events, records, tests.
3. **P2 — user naming**: `Flow` requires a user `name` (already `FlowName`); `Execution` accepts an
   optional name / `generateName` (random suffix). Wire create-idempotency by name.
4. **P3 — storage keys**: project by `tenant/ns/kind/name`; add per-scope scans. Physical routing
   still single-node.
5. **P4 — physical partitioning** (ties to Appendix D): map ranges to shards; keep all physical
   internals hidden.

Each phase is independently shippable; P0–P2 land on a single node with no multi-node change.

---

## 9. Open questions (OPEN)

- **Random-suffix charset/collision policy**: define how the generator guarantees global uniqueness
  (full-length random + retry on the (astronomically rare) name collision against `(tenant, ns,
  kind, name)`).
- **Idempotency is *only* Name-based for user-named objects**: user-specified Name is fully
  idempotent (same name → same object); `uid` plays no role in idempotency there — it only
  distinguishes incarnations after a delete+recreate reuses a name. This is settled, not open.
- **Verify (test, not design): random-suffixed children are minted exactly once.** The Name gives
  these no idempotency (the random suffix differs on every mint), so they rely entirely on
  `request_id` dedup: a retried parent command must not double-mint a child (a second random name /
  second `uid`). CCES `is_already_applied` should guarantee this; add a targeted test that retries,
  e.g., the same `CreateExecution` with one `request_id` and asserts the root (and any create-time
  children) has exactly one name and one `uid`.
- **Retire the ULID handle in favor of `name`-keyed storage** (§6) — defer.
