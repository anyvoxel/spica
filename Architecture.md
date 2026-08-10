# Architecture

This document records the architecture of `spica`, with an emphasis on the CCES-driven execution engine in `crates/engine`.

Its purpose is to capture **why** the system is shaped the way it is, especially for decisions that are not obvious from the code alone:

- what is source-of-truth vs projection
- what an `Event` should carry
- what belongs in storage vs only in runtime bookkeeping
- which values are persisted because replay/export/audit require them

When the implementation changes in a way that affects these invariants, update this document together with the code.

---

## 1. System model

The engine follows a **CCES (Causal Command Event Sourcing)** model.

At a high level:

1. A `Command` asks the engine to perform a transition.
2. A handler validates the command against the current projected state and emits one or more `Event`s and follow-up `Command`s.
3. `Storage` is updated **only** by applying `Event`s.
4. A follower / recovered leader rebuilds the same `Storage` by replaying the event stream in order.

This gives the engine three deliberately separate layers:

- **Commands** — requests for change
- **Events** — facts that happened
- **Storage** — a materialized projection of those facts

`Storage` is therefore **not** the source of truth. The event stream is.

---

## 2. Source of truth and replay boundary

### Decision

The event stream is the only source of truth. `Storage` must be reconstructible from:

- the ordered `Event` stream, plus
- the static `StateMachine` definition

A follower / recovered leader must not need access to hidden in-memory state, prior handler-local computation, or parent-chain reconstruction tricks that are not encoded in the stream.

### Why

This is the central invariant of the engine:

- leadership handoff must be safe
- recovery must be deterministic
- audit / inspection must not depend on re-running transition logic

### Consequence

Any value that is needed during replay but is **not** derivable from:

- already-applied events, and
- the static machine definition

must be carried on an `Event`.

---

## 3. Event payload principle: enough-to-replay-this-transition

### Decision

`Event` payloads are designed around the principle:

> carry enough data to replay this transition, but do not mirror the full storage row.

This means an event payload is usually:

- richer than a bare identifier (`id` only is usually too thin), but
- smaller and semantically cleaner than a storage snapshot

### Why

If an event is too thin, replay/export/audit must either:

- re-run business logic, or
- depend on hidden state

If an event is too fat, it becomes a dump of the storage schema instead of a record of a domain fact.

The correct boundary is:

- include values that were already decided by this transition and must not be recomputed later
- exclude projection-only bookkeeping and internal indexes

### Practical rule

For each field, ask:

1. Is it a domain fact that already exists at this lifecycle point?
2. Is it required for replay / export / audit?
3. Is it non-derivable from prior events + static definition?

If yes, it belongs on the `Event`.

If it is only a projection convenience, internal index, or bookkeeping cache, it belongs in `Storage` instead.

---

## 4. Events are not storage rows

### Decision

`Event`s do **not** directly model storage rows.

However, an event value may still be close to a **domain record** that storage also keeps or derives. The distinction is:

- **domain record / transition fact** — appropriate for an event payload
- **projection row / runtime bookkeeping** — appropriate for storage only

### Why

A storage row often contains fields whose only purpose is to make runtime processing efficient, such as:

- active-child tracking
- retry counters
- indexes
- pending drain state
- container-specific runtime repositories

These are real and important, but they are not always part of the outward domain fact that an event should express.

### Consequence

When designing an event, do **not** ask:

> “What does the storage row look like?”

Instead ask:

> “What fact has just become true, and what must replay/export know about it?”

---

## 5. Lifecycle design: `ing` and `ed` are distinct facts

### Decision

The engine models lifecycle transitions with explicit before/after pairs where needed:

- `StateActivating` -> `StateActivated`
- `StateCompleting` -> `StateCompleted`
- `StateTerminating` -> `StateTerminated`
- similarly for `Execution`

### Why

The two sides of a lifecycle step do different jobs:

- the `ing` event records the start / input / intent to enter a phase
- the `ed` event records what that phase produced

Separating them keeps replay, audit, and debugging precise.

### Consequence

Do not put future-stage facts on an earlier event merely for schema symmetry.

Example:

- `StateActivating` records the state's **raw entry input**
- `StateActivated` records the state's **processed input** and any activation product

So `StateActivating` should not carry `input: None` just to “look like” a fuller object. The processed input is not yet a fact at that point.

---

## 6. State identity uses `state_path`, not a duplicated name field

### Decision

State identity is recorded as a JSON Pointer:

- `state_path: jsonptr::PointerBuf`

The leaf state name is derived from the pointer's last token rather than stored separately.

### Why

A state's identity and location are one concept:

- the full path tells replay exactly where the state definition lives
- the leaf name can be derived from that path

Keeping both `state_path` and `state_name` as separately stored facts would allow drift.

### Consequence

The engine derives state names from `state_path` when needed, rather than persisting a duplicate `state_name` field in storage or events.

---

## 7. Activity dataflow separates entry input, processed input, raw result, and final output

### Decision

An `Activity` stores four distinct value views:

- `raw_input` — the verbatim value received when the state was entered
- `input` — the value the state actually processed after activation-time preprocessing
- `raw_output` — the state's raw result before any complete-step `Output` projection
- `output` — the activity's final completed output after `Output` projection

These values are established at different lifecycle points:

- `StateActivating.input` = raw input
- `StateActivated.input` = processed input
- state-specific settle events / logic produce `raw_output`
- `StateCompleted.output` = final output

For states that do not preprocess input, `StateActivated.input` is a copy of `raw_input`. For states
that do not produce a distinct raw result, complete-step logic may treat `input` as the default
`$states.result`.

### Why

These values answer different questions:

- **What arrived at the state boundary?** -> `raw_input`
- **What value did the state's activate logic actually run on?** -> `input`
- **What raw result did the state produce before output projection?** -> `raw_output`
- **What final value did the state complete with?** -> `output`

Separating them avoids semantic drift — especially for `Task`, where the call payload, the resource's
returned payload, and the state's final output are not the same thing.

### Consequence

- `raw_input` and `input` are immutable after activation.
- Replay must not recompute activation-time preprocessing or raw state results when those have already
  been decided and recorded.
- `$states.input` and `$states.result` are distinct concepts and should not share one storage field.

---

## 8. Container runtime state belongs in typed storage, not generic event clutter

### Decision

State-specific runtime repositories live in typed storage structures, currently under:

- `ActivityState::Leaf`
- `ActivityState::Parallel(ParallelActivityState)`
- `ActivityState::Map(MapActivityState)`

### Why

Container states need extra runtime data, but leaf states do not. Using a typed enum keeps storage explicit and avoids a flat structure full of meaningless `Option`s.

### Consequence

Events carry the **activation products** required to reconstruct these repositories, but the repositories themselves live in typed storage.

Example:

- a `Map` activation computes an iteration plan
- that plan is carried on `StateActivated`
- the applier folds it into `ActivityState::Map`

---

## 9. Export / audit boundary

### Decision

The event stream is the externally meaningful audit surface.

A future exporter should prefer exporting:

- raw `Event`s (with metadata), or
- a clearly documented projection built from those events

but it must not silently depend on hidden storage-only fields.

### Why

If exported data requires internal storage details that are not represented by the event stream, the audit surface is incomplete and replay/export semantics diverge.

### Consequence

When a field matters to external understanding of a transition, it should usually be carried by the event that made it true.

---

## 10. Current architecture decisions

This section lists the current decisions already reflected in the code.

### ADR-001 — Event stream is the only source of truth

- **Status:** Accepted
- **Summary:** `Storage` is a projection of `Event`s; replay rebuilds storage from the stream.

### ADR-002 — Event payloads follow enough-to-replay-this-transition

- **Status:** Accepted
- **Summary:** Events carry the facts required to replay the transition, not a full storage snapshot.

### ADR-003 — State identity is a JSON Pointer

- **Status:** Accepted
- **Summary:** `state_path` is stored as `jsonptr::PointerBuf`; `state_name` is derived from the pointer leaf token.

### ADR-004 — Activity dataflow separates raw input, processed input, raw output, and final output

- **Status:** Accepted
- **Summary:** `StateActivating` records raw entry input; `StateActivated` records processed input; raw state results are stored separately from final completed outputs.

### ADR-005 — Container runtime state is typed storage, reconstructed from event products

- **Status:** Accepted
- **Summary:** `Parallel` / `Map` keep typed runtime repositories in storage; events carry the products needed to rebuild them.

---

## 11. How to use this document

When changing the engine, update this file if the change affects any of the following:

- what the event stream must carry
- what replay assumes is derivable
- which fields are storage-only vs event-worthy
- the lifecycle boundaries of `ing` / `ed` pairs
- the representation of state identity or input/output semantics

For each new architectural decision, prefer adding a short ADR-style subsection with:

- **Context**
- **Decision**
- **Why**
- **Consequences**
- **Affected code**

That keeps the code and the architecture narrative aligned.
