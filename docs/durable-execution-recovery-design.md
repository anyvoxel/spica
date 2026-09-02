# Durable Execution & Crash Recovery Design

**Status:** Draft (design phase, not yet approved/implemented)
**Owner:** spica-engine
**Scope:** leader/follower crash recovery, take-over, and snapshot-based restart for the
CCES execution engine (`crates/engine`). Single-node durable restart first, multi-node later.
**Related:** [`../Architecture.md`](../Architecture.md) (CCES invariants — this design extends
§2 "Source of truth and replay boundary" without changing it).

---

## 1. Purpose

This document records the **why** and **how** of making CCES execution survive crashes and
leader/follower take-over without losing or duplicating work. It captures the model — a single
`lastProcessedPosition` scalar advanced at **apply time**, snapshot semantics, and the recovery
procedure — which the single-node path (S1, §10) now implements in code, and lists the invariants
the implementation must uphold.

It is a design record: it states what we intend to build, the reasons for each choice, and the
phased plan (S1 is implemented; S2+ are future).

> **Research precedent.** Before finalizing the model we studied how **Zeebe** (a production
> distributed workflow engine) solves the same problem end-to-end. It revealed that **a single scalar
> "last processed command position"** — with no process set and no dual cursor — is sufficient, so the
> primary design here is exactly that (§4/§5). The more complex **process-set** scheme from earlier
> drafts is retained only as an **alternative** in [Appendix A](#appendix-a--alternative-the-process-unprocessed-command-set).
> See [Appendix B — Zeebe research](#appendix-b--zeebe-research).
>
> One Zeebe mechanism we **deliberately do not adopt** is encoding the partition into the public key:
> see **[Appendix C — Hiding partition from user-visible identities](#appendix-c--hiding-partition-from-user-visible-identities)**.
>
> When we do shard, the unit is the **root execution** (not the scope), routed via an internal,
> movable **range table** over `reverse(root_execution_id)` — so one namespace can hold many
> executions that spread across partitions while keeping every id opaque:
> **[Appendix D — Partitioning: spreading many executions of one namespace](#appendix-d--partitioning-spreading-many-executions-of-one-namespace)**.

---

## 2. Background: current CCES path

The engine follows CCES (see `Architecture.md`):

- A `Command` and its produced `Event`/follow-up `Command`s live on a durable `LogStream`.
  Dispatch + append is **one atomic step**: a command's produced batch is appended as an atomic,
  causally-linked group. `append` assigns positions and returns the last position
  (`lastAddConfirmed` / **LAC**).
- The `Processor::run` loop tails the log via `stream_read(EntryId::new(1))`. It dispatches
  `Command`s to handlers and folds `Event`s into `Storage` (a rebuildable projection) via appliers.
  `Reject`s wake the awaiting caller.
- `EngineBuilder::with_backends(Box<dyn LogStream>, Box<dyn Storage>)` threads durable backends
  in; `spica-server` opens `RocksLogStream` + `RocksStorage`.

### Current gaps (why this document exists)

The loop started with no role separation and no recovery; the single-node pieces are now in place:

1. **No recovery watermark / decided-command tracking.** After restart we previously could not tell
   which read `Command`s were already applied without replaying from 1. S1 resolves this for the
   single node with a persisted `lastProcessedPosition` scalar (§4). Multi-node ("which commands are
   undecided" with no node-private log) remains open (§8).
2. **`stream_read` hard-coded `EntryId::new(1)`** (was `processor.rs:182`) — replaying from genesis
   does not scale. S1 resumes from `W + 1` instead (§5.2).
3. **In-memory timers/tasks are not in the log.** After take-over the new leader must
   rehydrate them or executions hang.
4. **Exactly-once external side effects.** A crashed leader may have produced a batch that a
   new leader cannot simply re-dispatch (re-dispatch mints **fresh ULIDs/timestamps**, so it
   yields *different* entities/events). Re-dispatch is safe only when the batch is absent.
5. **No committed boundary (LAC)** exposed, and no snapshot format.

---

## 3. Goals / Non-goals

### Goals

- **G1 (single-node, S1):** crash + restart resumes correctly from a durable log + storage,
  without replaying from genesis.
- **G2 (recovery watermark):** a durable, **single scalar** resume point (`lastProcessedPosition`)
  so a restarted Processor resumes from `W + 1` instead of re-deriving the log from position 1.
- **G3 (snapshot):** restart/install from a point-in-time snapshot of the projection, then
  resume from the correct log position.
- **G4 (later, multi-node):** safe leader/follower and take-over.

### Non-goals (explicitly out of scope, tracked as future work)

- Cross-**root-execution** parallel dispatch (see §8 — first implementation is single-worker).
- Multi-node quorum / consensus protocol (BookKeeper-style LAC is a **future** LAC source; §6).
- Raft-style leader election and fencing.
- Full incident handling (a *blocked entity* operator-resolve model is a separate, larger design).

---

## 4. Rules of the System — Design Decisions

The primary model is the **single scalar `lastProcessedPosition`**, adopted from Zeebe's recovery
(Appendix B). It is what S1 implements. The more complex **process-set** scheme is recorded in
[Appendix A](#appendix-a--alternative-the-process-unprocessed-command-set) as an alternative we chose
**not** to adopt.

### 4.1 The single scalar `lastProcessedPosition` (`W`)

Recovery is anchored on **one number** — `lastProcessedPosition` (`W`), the highest log position whose
effects (the batch that produced them) have been applied to the projection. It is persisted as a
single scalar in Storage (§6); everything else derives from it.

The watermark unit is a **batch boundary**, not a bare command or event position — because every
non-empty command batch is terminated by a dedicated `EntryPayload::Noop` entry appended atomically
with the batch itself (`crates/engine/src/types/entry.rs`). This is the MySQL-binlog / Kafka
batch-end-marker analogue: "batch in log ⟺ its terminating `Noop` in log" is guaranteed by append
atomicity, so the `Noop` gives each atomic append a stable, GTID-like identity (its envelope's
`cause_id` is the producing command's position). Advanced to the `Noop`, `W` means "everything at or
below this position is durably applied." A fresh store reads `W = 0` and resumes from position 1.

### 4.2 Advance at apply time — eager-apply per batch at production

`W` advances **when a batch is applied** — and a batch is applied **eagerly at production time**,
immediately after the append that made it durable, not on some later read-back. When the driver
appends a command's `entries ++ [Noop]` and the append returns, it hands the whole batch (with its
real log positions) to `ProcessingStateMachine::apply_batch`. The **leader** folds the batch's
Events **in a single atomic projection transaction** and sets `W = batch_end` (the terminating Noop's
position), all in that one transaction (`crates/engine/src/leader.rs::apply_batch`).

So apply atomicity == append atomicity: a Command's entire effect — every produced Event folded, the
watermark advanced, the correlated acknowledgements drained — commits once, all-or-nothing, exactly
when (and only when) its batch hits the log. Sibling events sharing a `cause_id` are applied together
in the same transaction, so a crash can never observe half a batch (the follower-read-back half-batch
hazard is structurally impossible here — there is no read-back apply in the healthy path).

The load-bearing invariant is **`W` is never ahead of the projection**: the watermark lands in
Storage only in the same transaction that folds the batch, so a restart from `W + 1` never skips
un-applied work. This is the single scalar's core soundness property.

**Implemented atomically.** The fold and the `W` advance are one projection transaction. The
Processor opens `Storage::begin_txn` to get an **owned** `Box<dyn StorageTxn>`, folds each Event
through a `&mut dyn StorageTxn` handle (which structurally cannot begin/commit — commit consumes the
`Box`, so only the Processor can call it). The txn keeps the fold's pending rows in an in-memory
overlay that its own reads resolve first (**read-your-writes**: a row written earlier in the same
fold is visible), so read-modify-write appliers (`add_child`/`put_execution` on one row) compose
correctly — and across the batch's sibling Events, naturally. On the batch's success the Processor
hands `W` to `StorageTxn::commit(Some(W))`, which materializes the overlay — plus the watermark — as a
single RocksDB `WriteBatch`: all-or-nothing. A torn (half-applied) projection or a watermark that
separated from its fold is therefore impossible; the idempotent re-fold no longer has to paper over an
interleaved partial state. This matches Zeebe — "state and `lastProcessedPosition` advance together,
in one RocksDB transaction" (Appendix B.1) — and the log-side append stays its own (already atomic,
fsync'd) step, deliberately separate.

**The leader's read-back is a skip.** Because the leader applied each batch at production, the
live read-back loop (`run_leader`) only sees already-applied work; its Event arm is a **pure skip**
guarded by `is_already_applied` (`entry_id.get() <= self.watermark`, asserted in `debug_assert!`), and
its Noop arm is a no-op. Only the residual crash window (§4.4) — a batch durably appended but not yet
folded before the crash — is folded, and that happens in the dedicated `recover_leader` startup pass
(§4.5), not in the live loop. The `Noop` read-back is a no-op for the leader (`commit_at_noop` returns
`None`); it exists for the log's structured batching and for a follower's atomic apply.

### 4.3 Root commands and `cause_id`

An externally-appended root command has `cause_id = None`. Its own position is still a watermark
unit, but no event carries that position until one of its children folds. Now that `W` advances to the batch's terminating `Noop`, a command whose batch is empty or
reject-only produces **no `Noop` and no advance** — it is simply not a watermark unit. On restart it
may be re-processed once (harmlessly — §4.4), exactly as before.

### 4.4 At-least-once crash window (accepted)

Resuming from `W + 1` skips the region `<= W` entirely — no already-applied batch is re-applied. The
one residual window is a crash that lands **between** "a command's batch is durably appended
(including its terminating `Noop`)" and "`apply_batch` folds it + persists `W`". The residual batch is
read back by the `recover_leader` startup pass and folded **in place**: its Command is skipped (not
re-dispatched — re-dispatching would duplicate the append), its Events are folded into one driver-owned
transaction, and the transaction commits once at the batch's Noop. So the restart neither re-appends
the batch nor re-fires the handler's side effects:

- the log does **not** grow a duplicate batch (append is not idempotent, so this matters), and
- the residual batch's effects are applied **exactly once** (not at-least-once).

The one genuinely harmless "re-processed once" case is a command that produced an empty/reject-only
batch (§4.3) — it appends no `Noop`, advances no `W`, and re-reading it is a no-op. This is the whole
of the single-node crash window; closing even the log-side window (idempotent append / an atomic
log-side watermark) is still deferred to multi-node. Note this is a *different* failure from the
follower sibling-loss hazard the `Noop` solves: that one drops events across a crash, while this
window's duplicate-append risk is what `recover_leader`'s skip-commands-in-place fold removes.

### 4.5 Recovery as a bounded startup pass (`recover_leader`)

Because `W` never runs ahead of the fold region, everything already applied lies at or below `W`; 
there is no process set and no replay of the whole log. On boot the leader runs **`recover_leader`**,
a **bounded startup pass** that reads *forward from `W + 1`* (never from the head) until the durable
tail (`LogStream::read` returning `None`), folding any crash-residual batches (§4.4) that a crash left
above `W`:

- **Command** — skipped: its batch is durable and folded below by this pass; re-dispatching would
  duplicate the append. Only residual *Events* drive the projection.
- **Event** — folded into a **driver-owned** transaction opened at the batch's first Event and held
  across the siblings.
- **Noop** — closes the batch: the still-open transaction commits atomically with `W = Noop` position
  (`StorageTxn::commit`).
- **Reject** — audited via `log_reject`; a restart leaves no awaiting caller, so no ack is delivered
  (the producing client may have left this node).

After the pass, `W` sits on the last durable Noop, all residuals are folded, and `run_leader` tails the
log from `W + 1` with its Event arm a pure skip — a normal process-serve loop follows with no further
recovery code path. This is "recovery as one distinguished pass", not a per-entry fall-through; the
pass is identical in spirit to the follower's batched fold, but the leader drops the `after_commit`
ack drain because there are no in-flight acks on a fresh start. (See `crates/engine/src/stream_processor.rs::recover_leader`.)

### 4.6 Committed boundary: LAC

`LAC` (last-add-confirmed) is the boundary of "safe to serve / safe to fold up to".

- **Single node:** LAC = the local durable RocksDB high-water. Atomic append makes it **leak-free** —
  no committed entry is ever lost on crash.
- **Multi-node:** LAC = a quorum-confirmed position (BookKeeper-style), supplied by a real
  distributed log. Until then, RPC-level acks must not be treated as durable across replicas.

`ack` firing remains tied to the applied event / reject (Zeebe "respond after commit" model) — it
fires only once the decision is durable.

### 4.7 Concurrency model: root-lineage isolation

Dispatch does **not** need to be globally ordered — it needs to be ordered only within a causal
lineage. Two commands may dispatch concurrently iff they never read/write a common projection
key.

**Granularity: the root execution.** Parent/child executions share parent rows, so they must be
serialized — but they share a **root**, so root-lineage ordering covers them. Timer and
task-service firing merely **append new commands**; a `CompleteTimer` lands after its `StartTimer`
command, ordering correctly within the lineage and introducing no cross-root sharing.

**Required invariant:** commands of *different root executions* never touch a common storage key (no
global singleton row, no cross-flow referenced entity). If this is violated, projections no longer
converge under concurrent dispatch.

**First implementation is a single worker**, which trivially satisfies isolation (everything is
serialized). Parallelizing across root lineages is a future optimization. Crucially, when we do shard,
the order-domain assignment stays **internal and invisible to users** (opaque ULID keys) — see
[Appendix C](#appendix-c--hiding-partition-from-user-visible-identities).

---

## 5. Concrete Algorithms

### 5.1 Single-node consume loop

The driver (role-agnostic `StreamProcessor`) tails the log, and hands each entry to the installed
state machine. Before tailing, the leader runs the `recover_leader` startup pass (`boot` below); the
leader's *live* path is then: dispatch → append (+Noop) → **eager-apply** → skip read-back.

```text
boot (single node):
  W = storage.last_processed_position()
  recover_leader(W):                             # bounded startup pass (see §4.5)
    for each entry at W+1, W+2, ... until tail (LogStream::read == None):
      Command:   skip                             # batch durable + folded below; never re-dispatch
      Event(e):  fold into a held txn             # open txn at first Event of the residual batch
      Noop:      commit the held txn, W = noop    # close the batch atomically at its Noop
      Reject:    log_reject(r)                    # audit only; no awaiter, no commit
    W = storage.last_processed_position()         # now on the last durable Noop
  stream = log.stream_read(EntryId(W + 1))        # resume right after the recovered tail
loop:                                             # live process-serve loop (no recovery here)
  entry = stream.next()
  match entry.payload:
    Command(c):
      produced = dispatch(c)                      # reads projection; emits events + follow-ups
      if produced.entries non-empty:
        to_append = produced.entries ++ [Noop(cause_id = c)]   # Noop terminates the atomic batch
        last = log.append(to_append)             # atomic; assigns real positions
        batch = materialize(produced.entries, ..., last)       # real positions + the Noop
        apply_batch(batch)                       # EAGER: fold all Events + W=batch_end in ONE txn
      deliver(produced.grants)                   # grants answered even if the batch was empty
    Event(e):                                    # read-back of this leader's own batch
      skip — everything here is at/below W       # recover_leader folded the residue; apply_batch the rest
    Noop(n):                                     # batch terminator read back
      (leader: no-op — its batch was already applied eagerly)
    Reject(r):
      awaken(r.request_id)                       # no fold, no watermark change
    follow-up Command:                           # a produced command is an ordinary Command
      (handled by the Command arm on its own read-back)
```

In the healthy steady state, the read-back pass skips every Event and no-ops every Noop: all real work
happened once, eagerly, at production. The only Events the leader ever folds outside production are the
crash residuals, and those happen in the `recover_leader` boot pass (before the loop), not in the loop
itself — so the loop's Event arm stays a pure skip. A command whose batch is reject-only or empty
produces no `Noop` and no eager apply; it merely delivers its grants.

### 5.2 Restart (single node)

```text
boot:
  W = storage.last_processed_position()
  recover_leader(W)                              # fold any crash-residual batches above W (§4.5)
  W = storage.last_processed_position()          # the recovered tail (last durable Noop)
  stream = log.stream_read(EntryId(W + 1))       # jump straight to the resume point
  run the §5.1 live loop normally                # no in-loop recovery
```

No replay of the whole log and no process set: entries `<= W` hold only already-applied work, and the
bounded `recover_leader` pass (reading forward from `W + 1` to the tail) folds whatever a crash left
above `W` exactly once (§4.4, §4.5).

### 5.3 Multi-node / take-over (later)

The single scalar is **leader-private**: a follower has no dispatch history, so "which commands are
undecided" cannot be recovered from a scalar alone. Section 8 covers the open design space; the
process-set reconstruction (which makes the answer node-agnostic) is recorded as the alternative in
[Appendix A](#appendix-a--alternative-the-process-unprocessed-command-set).

---

## 6. Persisted Data

| Item | Where | Notes |
|------|-------|-------|
| Projection | `Storage` (Rocks) | rebuilt by fold; future snapshots via RocksDB Checkpoint |
| `lastProcessedPosition` (`W`) | `Storage`, `_global/last_processed_position` | **single scalar**; advanced **eagerly with the whole-batch fold** at production time (to the batch-terminating Noop); `0` = fresh (resume from position 1) |
| `Noop` batch terminators | LogStream | one per non-empty atomic append batch; gives `W` a natural batch granularity and a follower its atomic apply point |
| LAC | LogStream/Meta | single-node = local durable high-water; multi-node = quorum |

There is **no** process set in the primary model: `W` is the only durable recovery state beyond the
(redundantly-rebuildable) projection. No per-command bookkeeping lives in the hot KV path. The
process-set variant (if ever revisited) would persist an additional log-derivable set — see
[Appendix A](#appendix-a--alternative-the-process-unprocessed-command-set).

---

## 7. Invariants (implementation checklist)

1. Dispatch + append is a single atomic step; "decided ⟺ batch present in log".
2. `W` (`lastProcessedPosition`) advances **eagerly, at production time, per whole batch** — in the same
   write group as the batch's fold — it is never ahead of the projection.
3. `W` is a **batch-boundary position**; each non-empty atomic batch ends in a `Noop` whose envelope's
   `cause_id` is the producing command's position.
4. A restarted Processor resumes from `W + 1` and never re-applies any entry `<= W`; its own read-back
   Events at or below `W` are skipped.
5. Single-node recovery is the bounded `recover_leader` startup pass (§4.5) folding crash-residual
   batches above `W` in place — after it, the live loop (§5) folds nothing on read-back and has no
   process set.
6. Acks fire only for durable decisions.
7. Different root executions never share a storage key (isolation invariant for future
   concurrency).

---

## 8. Later: multi-node / follower

- **Which commands are undecided.** The single scalar is **leader-private**: a follower has no
  dispatch history, so it cannot say what still needs dispatching from the scalar alone. Making that
  decidable from the log is the open design problem here; the process-set alternative
  ([Appendix A](#appendix-a--alternative-the-process-unprocessed-command-set)) is the
  node-agnostic answer, should we revisit it.
- **LAC source:** swap single-node local high-water for the distributed log's quorum-confirmed
  lastAddConfirmed.
- **Fencing:** a former leader must not dispatch after a new leader takes over (out of scope but
  required before real multi-node).
- **Follower roles:** serve reads from the projection; no dispatch/append; stand by. A follower
  opens one transaction at a batch's first Event, folds each sibling into it, and commits it
  **atomically at the terminating `Noop`** (`commit_at_noop`) — the exact sibling-loss hazard the
  batch marker was built to solve: applying event-by-event, a crash between siblings would drop
  later siblings, but rounding the apply off at the `Noop` makes the whole batch commit or nothing.
- **Follower bootstrap (seed from a consistency copy).** A new follower has no local projection and
  must not rebuild it by replaying the log from position 1 (too slow). It seeds from a recent
  point-in-time copy instead: the producer takes a **RocksDB checkpoint**
  (`rocksdb::checkpoint::Checkpoint::create_checkpoint`, already bound in the crate — the
  consistent, independently-openable mirror), uploads the directory to remote object storage
  (TOS), and the follower downloads it, `DB::open`s it, reads `last_processed_position()` (`W`),
  and resumes `stream_read(W + 1)` on the shared log. The watermark rides inside the projection, so
  per the "commit-before-valid" discipline (Appendix B.2) the network must still guarantee the log
  is durable/readable at least up to `W + 1` before the follower is exposed.
- **Online checkpointing while a store is being written.** `create_checkpoint` is on-line safe —
  it does not block the write path — and `DB` is `Sync`, so a background snapshotter thread holding
  its own `Arc<DB>` clone can checkpoint concurrently with the single-writer Processor. Because
  each fold is one atomic `WriteBatch` (projection + watermark), a checkpoint always lands on a
  *prefix of fully-committed folds*, so its `(projection, W)` is a consistent application point with
  **no cross-thread synchronization** (a fold is never observed torn). Implementation requirement:
  clone/split the `Arc<DB>` out to the snapshotter *before* handing exclusive `&mut Box<dyn Storage>`
  to the Processor — the trait's `&mut self` write API is the actual barrier here, not RocksDB.
  (Full checkpoint is a correct start; C++ incremental checkpoint exists on newer RocksDB but the
  0.25 crate exposes only the full variant, so refresh-as-full for now.)
- **Take-over:** promote a follower to leader (per the open design above); rehydrate timers/tasks
  before serving.

---

## 9. Open Questions

- **Q1.** For multi-node, how does a follower / take-over decide "which commands are undecided"
  without a node-private log? (The candidate answer, the process set, is recorded in
  [Appendix A](#appendix-a--alternative-the-process-unprocessed-command-set).)
- **Q2.** Snapshot cadence / threshold (time-based vs log-length-based) for the snapshot milestone.
- **Q3.** Is `FlowVersion`-level or global snapshot granularity needed for the index? (Likely global
  Rocks Checkpoint first.)
- **Q4.** Confirm no cross-root shared storage keys exist today (§4.7) before enabling concurrent
  dispatch.

---

## 10. Phased Implementation Plan

### S1 — Single-node durable recovery (G1, G2) — implemented
- Persist `lastProcessedPosition` as a **single scalar** in Storage (`last_processed_position()`,
  `put_last_processed_position()`), advanced **at apply time** along with the fold.
- On boot, `run()` runs the bounded `recover_leader` pass (fold crash-residual batches above `W` in
  place), then tail-resumes from `stream_read(W + 1)` — no replay from position 1 (§4.5, §5.2).
- Validation: `crates/engine/tests/durable_resume.rs` kills/restarts a durable Rocks engine over the
  same log + storage and asserts the restart appends no duplicate work; in-crate unit test
  `recover_leader_folds_crash_residue_once` drives the pass directly.

### S2 — Snapshot (G3)
- Snapshot the projection via RocksDB Checkpoint; `W` lives inside it (in-DB, like Zeebe, §B.2).
- Validation: snapshot at an arbitrary point; restart from it; assert resume == full replay.

### S3 — Multi-node / follower (G4)
- Follower role (read-only); leader/follower identity; take-over per the open design in §8.
- Swap LAC source to quorum (distributed log); add fencing.

---

## Appendix A — Alternative: the process (unprocessed-command) set

This appendix records the more complex recovery scheme that the earlier drafts proposed as the
primary design and that we **considered then chose not to adopt** (see A.8). It is preserved so the
reasoning is not lost; the adopted design is §4/§5, which follows Zeebe (Appendix B). "Process set"
here means a durable, **log-derived set of command ids still awaiting dispatch** — a
node-agnostic reconstruction of "which commands are undecided".

### A.1 Dual read/fold cursors (`R` and `F`) with eager apply

A command's produced batch appends at the log **tail**, so a later-positioned command (positioned
between a command and that command's batch) could otherwise be dispatched **before** the earlier
command's effects are folded. Because all executions share one projection, the leader **eagerly
applies**: as soon as `append` assigns positions, fold the produced events immediately — do not wait
to read them back. This yields two cursors:

- **`R` — read / dispatch position.** Position the loop has read and dispatched up to.
- **`F` — fold position.** Position the leader has eagerly applied up to. `R ≤ F` always.

When the loop later reads back an entry with `entry_id <= F`, it **skips re-folding** (the event arm
already ran); commands still dispatch regardless of `<= F`. `F` stays a scalar because every
appended batch is immediately eager-applied, keeping the applied region a contiguous prefix.

### A.2 The process set (unprocessed-command set)

"Which commands still need dispatching" is materialized as a **set of command ids folded into the
projection**, so every replica reconstructs it identically from the log — no node-private watermark
to transfer.

- On reading a `Command`, **insert** its id.
- On reading **any** causally-linked entry produced by it — its `Event`, its `Reject`, or a
  follow-up `Command` (all carry `cause_id = the command`) — **remove** it.

**Invariant: "in the set" ⟺ "its batch is absent from the log" ⟺ "undecided"** (dispatch + append is
one atomic step). Removal is **read-back-only** (no dispatch-side mutation), so leader and follower
use identical logic — which is why every command must produce at least one causally-linked entry
(A.3), else an empty batch command could never be removed and would re-dispatch forever.

### A.3 Every command produces at least one entry

A `Command` must emit at least one fold-visible, side-effect-free entry (`Event`, `Reject`, or
follow-up `Command`) even for a no-op, so read-back has a guaranteed removal trigger and the set
stays a pure log derivation. This is a **handler contract** — a real constraint on command shapes —
that the single-scalar model does not impose.

### A.4 Snapshot invariant: `S = R`

A snapshot is `(projection, R, F, set)` with `projection == fold(events <= F)`, but it is **tagged as
the read/dispatch position `R`, NOT `F`**: commands in `(R, F]` were never read/dispatched yet their
effects are already folded, so recovering `F` would silently skip them. Tagging `S = R` replays
`(R, LAC]`, re-dispatching survivors and skip-folding already-applied events.

### A.5 Recovery / take-over procedure

1. Load the latest snapshot `(projection, R, F, set)` (or `R = F = 1`, empty set with no snapshot).
2. Read `(R, LAC]` with the **follower** logic (no dispatch side effects): insert `Command`s, fold
   `Event`/`Reject`/follow-up if `entry_id > F` and remove `cause_id` from the set.
3. At `LAC`, if leader, re-dispatch surviving set members **in log order**, eager-apply, rehydrate
   timers/tasks.
4. Then run the normal leader (or follower) loop.

### A.6 Follower loop

Identical skeleton to the leader loop **except** no dispatch, no append, no eager-apply: insert
`Command`s, fold `Event`/`Reject`/follow-up if `entry_id > F`, remove `cause_id`; no scheduler/task
side effects (standby).

### A.7 Worked example (`S = R`)

Let `C1@1, C2@2, C3@3`; `C1`'s batch is `{EvA@4, C1'@5}`. The leader dispatches `C1..C3`, eagerly
applying up to `F = 7`. Snapshot correctly at `S = R = 3` (projection holds `fold(events <= 7)`).
Recovery reads `(3, LAC]`: re-encounters `C1'@5` etc., skips `<= F` folds, re-dispatches any survivor,
serves. Tagging `S = F = 7` would silently skip commands in `(3, 7]` — **lost work**; hence `S = R`.

### A.8 Why Single-Scalar Wins (not adopted)

Compared with §4/§5, the process-set scheme adds a durable set, a mandatory per-command entry
contract (A.3), dual cursors with eager apply, and a distinguished replay phase that rebuilds the set
before dispatch. The single scalar needs none of that for single-node recovery — and Zeebe's
production experience (Appendix B) shows the scalar is sufficient where one strictly-ordered
position sequence exists. We therefore do **not** adopt the process set; it remains the candidate
only if multi-node take-over ever requires a node-agnostic "undecided" answer (§8, Q1).

---

This appendix records a source-level study of **Zeebe** (the `camunda/camunda` monorepo, default
branch `main`; modules under `zeebe/`). It directly informed the model in §4 and is retained as a
research reference. It answers two questions end-to-end: *how Zeebe recovers after installing a
snapshot*, and *how it restarts after recovery*.

### B.1 The central idea: a single scalar `lastProcessedPosition`

Zeebe's recovery is anchored on **one number**: the `lastProcessedPosition`, defined as the
highest **command** whose effects have been applied to state. Everything else derives from it.

| Position | Meaning | Persisted? | Role |
|---|---|---|---|
| `lastProcessedPosition` | last **command** position whose effects were applied (local consumption watermark) | **In RocksDB** (`LAST_PROCESSED_EVENT_KEY`, `DbLastProcessedPositionState`) | **The snapshot's resume point** |
| `lastWrittenPosition` | highest log position this processor itself appended (its follow-up effects) | memory only | not a resume point; rebuilt from the last record read during replay |
| committed / LAC | the log's durability high-water | derived from raft `LogStorage.seekToEnd()` | hard upper bound on what replay/processing reads |

Key invariant: **state and `lastProcessedPosition` advance together, in one RocksDB transaction**
(`ProcessingStateMachine.updateState()` applies events to state; `finalizeCommandProcessing()`
does `markAsProcessed(command.position)` in the same DB write). So the projection always equals
"effects of all processed commands" — never ahead, never behind, with respect to the watermark.

### B.2 Snapshot: RocksDB checkpoint, "commit-before-valid"

- **Capture** (`AsyncSnapshotDirector`, per-partition actor, fixed rate): `db.createSnapshot(dir)`
  is a **RocksDB native checkpoint** (hard links to SSTs) — a near-zero-cost point-in-time view
  that does not block live writes. The checkpoint implicitly carries `lastProcessedPosition` because
  that value lives in RocksDB's default column family.
- **commit-before-valid:** the transient snapshot is only flipped into a committed snapshot after
  the **last written position has been raft-committed** and the journal flushed
  (`AsyncSnapshotDirector.snapshot()`). Rationale: the state in the snapshot depends on log entries
  (the events), so the log must be durable up to at least that point before the snapshot can stand
  alone. This is the multi-node version of our LAC ≥ snapshot-position invariant.
- **Identity:** snapshot metadata/ID records `{index, term, processedPosition, exportedPosition,
  brokerId, checksum}`; `persist()` writes metadata + checksum and `ATOMIC_MOVE`s the pending dir
  into `snapshots/<id>/`. Received snapshots are streamed in chunks, checksum-verified, then
  installed the same way.

### B.3 Install + recovery = replay events from the resume point to the tail

`StreamProcessor` phases: `INITIAL → REPLAY → PROCESSING → FAILED/PAUSED`.

1. `recoverFromSnapshot()`: open the snapshot DB, read `getLastSuccessfulProcessedRecordPosition()`
   (= `lastProcessedPosition`), `logStreamReader.seekToNextEvent(snapshotPosition)`.
2. `ReplayStateMachine.startRecover(snapshotPosition)`: replay reads strictly **after** the snapshot
   position, and for each event applies it only if `sourceEventPosition > snapshotPosition ||`
   `sourceEventPosition < 0`. This deterministically rebuilds state (and the key generator) from
   **events only** — command handlers are **not re-run**; there are no user side effects.
3. As batches are replayed, `markAsProcessed(batchSourceEventPosition)` advances the watermark inside
   the same DB transaction, so state and position stay consistent and the next snapshot reflects
   replay progress.
4. On exhaustion (read to the log tail / committed high-water), replay computes
   `lastProcessedPosition = lastSourceEventPosition`, `lastWrittenPosition = lastReadRecordPosition`,
   and `onRecovered()` flips `REPLAY → PROCESSING`, seeding `ProcessingStateMachine` seeked just after
   the resume point. From there it processes **commands** only, as the log grows.

**Modern Zeebe is replay-only.** There is no reprocessing state machine, no replayer, no divergence
check on current `main` (the old 8.0–8.1 `ReProcessingStateMachine` + `"Inconsistent log detected"`
command-re-execution path was removed). `StreamProcessorMode.REPLAY` is a pure-replay mode for
followers/export: it never creates a writer and never processes commands.

### B.4 Why the "appended 101–103 but folded only to 98" problem vanishes

The confusing case from our own earlier drafts — a batch appended at positions 101–103 while the
projection seems to reflect only 98 — does not exist in Zeebe's model, for two reasons:

1. **Zeebe keys on command positions, not event positions.** `101–103` are not "unprocessed work";
   they are the durable physical slots where a processed command's decision-output landed. The
   projection is *defined* as the effects of all processed commands, so there is no "state at 98
   while the log is at 103" inconsistency — the boundary is a **command** position.
2. **Events self-identify their parent command** via `sourceEventPosition` (= our `cause_id`). The
   replay filter `source > snapshotPosition` means: an event whose source command is `≤` the snapshot
   boundary is **already reflected in the snapshot and skipped** (no double-fold); an event whose
   source command is `>` the boundary is **re-applied** (no loss). So "resume at 98" is exactly right:
   101–103 belong to a processed (≤ 98) command, so they are skipped with no re-fold and no loss.

Transient windows between "events appended to the log" and "state+watermark committed to Rocks" are
also harmless: **the log is the durability anchor; RocksDB (state + position) is a rebuildable
projection**, so replay reconstructs both if a crash lands exactly between them.

### B.5 Two inferences a reader of the code might draw — one right, one needing care

- **Commands are processed in strict log-position order (right).** A partition has exactly one
  `StreamProcessor` (a single-threaded actor) reading and processing records in position order; it
  never reorders within a partition. Concurrency comes from **partitioning the log by key**, not from
  in-partition parallelism. *Implication for us:* our §4.7 "concurrent dispatch across root lineages
  on one log" is **not** how Zeebe does it — parallel Zeebe-style dispatch means sharding the log
  into partitions, each strictly ordered. This is a meaningful architectural consequence to weigh.
- **Events are applied in position order, not by source order (needs care).** Followers (and replay)
  fold events in **physical position order**. `sourceEventPosition` is an *ownership/watermark*
  marker, not a sort key: it gates the replay boundary and drives `markAsProcessed`. The property
  "among events, larger position ⇒ sourceEventPosition ≥" does hold (it is a consequence of
  position-order processing + atomic batch append), **but it breaks across a command/event boundary**:
  a pre-appended command can sit between an earlier command and that earlier command's events
  (e.g. `C@95 … C@99(pre-existing) … E:src95@100`). Zeebe therefore never sorts by source; it folds by
  position and uses source only for ownership/boundary/watermark.

### B.6 Client commands have no source — and that's fine

Externally-appended commands (e.g. from clients) have **no** `sourceEventPosition` (Zeebe's
`sourceEventPosition < 0` replay branch). This is definitional: `sourceEventPosition`/`cause_id` is a
**one-way child→parent link**; a root command has, by definition, no in-system cause. It does not
break the scalar model because the watermark keys on a command's **own position**, which every
command has:

- live: `lastProcessedPosition` advances to the client command's own position at finalize;
- replay: the watermark advances via the client command's **children** (`cause_id = its position`).

Edge case: a client command whose batch was empty/reject-only leaves no child marking its position,
so replay cannot advance the watermark past it and a new leader re-processes it once. That is safe
(deterministic, nothing committed; external side effects are event-gated, not handler-time).

### B.7 What we adopt vs. drop (research → §4 mapping)

| Earlier draft (now the Appendix A alternative) | Adopted (single scalar) |
|---|---|
| `R` (read) + `F` (fold) cursors | a **single scalar `lastProcessedPosition`** |
| process set (unprocessed-command set) | not needed — "undecided ⟺ command position > watermark" |
| "every command must emit a causally-linked entry" | not required — an empty/reject command advances the scalar at finalize |
| snapshot `(projection, R, F, set)` | RocksDB checkpoint carrying `lastProcessedPosition` (in-DB) |
| implicit snapshot validity | **commit-before-valid** (log durable ≥ snapshot position, multi-node = quorum) |
| events applied "in source order" | events applied in **position order**; `cause_id` only for ownership/boundary/watermark |

This mapping is what the primary design (§4/§5) follows; the rejected left-hand column is preserved
as the alternative in [Appendix A](#appendix-a--alternative-the-process-unprocessed-command-set).

### B.8 Sources

- `StreamProcessor.java`, `ReplayStateMachine.java`, `ProcessingStateMachine.java`,
  `StreamProcessorContext.java`, `Mode`/`StreamProcessorMode.java` —
  `zeebe/stream-platform/src/main/java/io/camunda/zeebe/stream/impl/`
- `DbLastProcessedPositionState.java`, `StreamProcessorDbState.java`, `MutableLastProcessedPositionState.java` —
  `zeebe/stream-platform/src/main/java/io/camunda/zeebe/stream/impl/state/` and `.../state/`
- `LogStream.java`, `LogStreamImpl.java`, `Sequencer.java` — `zeebe/logstreams/src/main/java/io/camunda/zeebe/logstreams/{log, impl/log}/`
- `AsyncSnapshotDirector.java` — `zeebe/broker/src/main/java/io/camunda/zeebe/broker/system/partitions/impl/`
- `StateControllerImpl.java` — `zeebe/broker/.../partitions/impl/`
- `ZeebeTransactionDb.java` — `zeebe/zb-db/src/main/java/io/camunda/zeebe/db/impl/rocksdb/transaction/`
- `FileBasedSnapshotMetadata.java`, `FileBasedReceivedSnapshot.java`, `FileBasedSnapshotStoreImpl.java` — `zeebe/snapshot/src/main/java/io/camunda/zeebe/snapshot/impl/`

---

## Appendix C — Hiding partition from user-visible identities

Zeebe pins work to a partition by **stamping the partition id into the top bits of the entity
key** (`Protocol.encodePartitionId = (partition << KEY_BITS) + key`, `decodePartitionId = key >> 51`;
`DbKeyGenerator` seeds each partition at `encodePartitionId(partition, INITIAL_VALUE)`, and the router
reads the partition back out of the key — never recomputes `% liveCount`). This gives stateless,
lookup-free routing and zero-touch partition growth, at a real cost to us: **the partition becomes
visible and saveable by users**, because the key is a user-facing identity.

### C.1 Why key-encoded partition is wrong for us

We deliberately diverge from Zeebe here. Baking the partition into the public key couples **identity**
("what is this?") with **location** ("where does it live?"), with three concrete harms:

1. **Topology leaks to users.** The high bits of a key reveal cluster layout / load distribution to
   anyone who sees one. That is an internal signal we do not want exposed.
2. **Persisted-key coupling.** Users may store a key (database row, message, log) and thereby
   implicitly promise "this partition must exist forever." Any future migration or re-partition is
   then hostage to every saved key. Location was meant to be movable; now it cannot be.
3. **Migration becomes structurally impossible, not merely hard.** Identity never changes by
   definition; folding location into it makes relocating = destroying the identity. This is the same
   conflation that makes Zeebe's split/merge difficult (points are stamped once from birth topology,
   with no rewrite path), except we would be exporting that difficulty to our users too.

Our current id scheme is **ULID** (`mint_*` in `crates/engine/src/id.rs`): opaque, stable, and
identity-only. Stamping partition bits into a ULID would break its purpose and surface system
internals. So we keep identity opaque.

### C.2 Design principle: identity opaque, location internal

> **Principle.** User-visible ids (`ExecutionId`, `ActivityId`, …) are opaque and carry **no**
> system-internal routing/shard information, so a persisted id never leaks topology and never encodes
> a location the system must keep forever. Which order-domain (root lineage) a run belongs to is an
> **internal attribute** assigned by the engine when the execution starts and recorded in storage —
> never visible in or derivable from a public key. Sharding for concurrency (the §4.7 generalization)
> is an internal routing concern, not an identity concern.

This does **not** change the §4 model: the single-scalar watermark, snapshot, and recovery are all
**per order-domain** regardless of how the domain is selected. The scalar is sound wherever there is
one strictly-ordered position sequence (one log domain); hiding the domain from users does not weaken
it.

### C.3 The engineering trade-off (three options when we actually shard)

| Approach | User-visible identity | Routing cost | Growth / migration |
|---|---|---|---|
| Key-encode partition (Zeebe) | **leaks partition** | none (stateless) | zero-touch add; split/merge hard; persisted keys couple users |
| Stable / consistent hash over opaque id | opaque | none (stateless) | add moves a fraction of existing entities (not zero-touch); opaque ids stay valid |
| Internal routing binding (entity → shard, in storage or a separate index) | opaque | one internal lookup per route | add does not move existing bindings; migration is a controlled, internal operation |

We prefer the **opaque** rows. Even when performance matters, the lookup can be served from the same
RocksDB projection the command handling already reads. A concrete choice between consistent-hash and
explicit binding is deferred until the sharding work is actually started (out of scope for S1–S3);
this appendix only fixes the **principle** that identity stays opaque and location stays internal.

### C.4 What gets recorded

- Public keys: ULID, opaque, no partition/shard bits (unchanged from today).
- Execution start: engine internally picks the order-domain (root lineage / future shard) and records
  it in the execution row; the client is never asked for or shown it.
- Concurrency plan (§4.7): "parallel dispatch" means multiple order-domains, each a scalar watermark;
  the isolation invariant (different root executions never share a storage key) is exactly what makes
  the internal domain assignment correct and invisible.

---

## Appendix D — Partitioning: spreading many executions of one namespace across partitions

Appendix C fixed the principle (identity opaque, location internal) and left the routing
mechanism open. This appendix picks the concrete mechanism, driven by the requirement that a
**single namespace can hold many independent executions and still scale out**: executions are the
unit we partition, *not* the scope.

### D.1 Unit: the root execution (one lineage), not the scope

`execution 1` and `execution 2` may share `/tenant1/namespace1/` and still land on **different
partitions**. The partition unit is the **root execution** — one lineage (the root execution plus
its activities, child executions, timers, tasks) is exactly the order-domain of §4/Appendix B and
must stay on one partition (one log, one scalar `lastProcessedPosition`). Different roots are
independent, so they can be spread freely. This is the instance-to-partition shape.

The invariant that makes this sound is the §4.7 isolation invariant restated on executions:
> Only **intra-lineage** (same root, same partition) causal read/write sharing is allowed;
> different roots — even in the same namespace — never write a common storage key.

Because each root is processed as one unit, the isolation check is exactly what proves partition
safety.

### D.2 Routing: a range table, not a per-execution map

A stateless route by **range table**: a small sorted list of `[lo, hi) -> partition` boundaries over
the internal range key (§D.3). Lookup is one binary search; the table is `O(#ranges)`, not
`O(#executions)`, and is cacheable / movable.

This is the payback of opaque identity, pushed further than Appendix C. The id never *stores*
location — it only participates in a query against an **internal, movable boundary table**. A
persisted ULID therefore carries no partition bits that a future rebalance could make stale (unlike
Zeebe, which freezes partition in the key's high bits forever).

### D.3 Range key = `reverse(root_execution_id)` (the hotspot fix)

A raw lexicographic range over a ULID is a **range over creation time**: ULID's leading 48 bits are
a millisecond timestamp, so new executions perpetually mint at the current time and land at the
*frontier* of the range space. Under a creation-heavy namespace (many executions created), all new
writes funnel onto one hot range — the exact workload this appendix is for.

Fix: use **`reverse(root_execution_id)`** as the internal range/locality key. Digit-reversing the
26-char ULID moves the least-significant (random-dominated) bits to the front, so range position is
governed by the 80 random bits: new executions spread uniformly across the range space, with no
frontier hotspot and no same-millisecond clustering. It needs no ULID parsing, is a deterministic
bijection (so it is its own inverse — map a reversed range back to the original id range by
reversing again), and preserves lineage colocation (same root id -> same reversed value).

The public, user-visible id stays the **forward** ULID (opaque, unchanged); the reversed form is
**internal only** — the storage/routing key, never shown to users.

### D.4 Key layout gains a leading lineage dimension

Colocation requires every row of a lineage to share the range key, so the internal layout is no
longer `/scope/<kind>/<ulid>` but:

```
/<tenant>/<namespace>/<reverse(root_exec_ulid)>/<kind>/<ulid>
```

The reversed root-execution id is the **leading locality dimension** (what ranges are defined over
and what makes a lineage co-located); within it, `kind/ulid` is unchanged. This is a real key-layout
change from today, driven by the colocation requirement. The `flow`/`flowversion` metadata rows are
namespace-scoped read-only metadata (see D.6) and resolve against the same dimension.

### D.5 Storage splits easily; an order-domain does not (non-goal)

Honest split of the two halves:

- **Storage half — easy.** RocksDB SSTs are already sorted by key; splitting a range is a logical
  boundary over a contiguous key range (physical file copy). TiKV-style.
- **Order-domain half — hard, and independent of the mapping.** Each partition is a durable command
  **log** + a `Processor` + a scalar `lastProcessedPosition`. Splitting a live order-domain means
  forking one log mid-stream into two, each with its own continuing positions, watermark, and
  processor. A range table makes the *boundary* cheap to move, but does **not** make a live log
  forkable — which is the real reason Zeebe (even with a movable boundary dream) does not split.

**Non-goal (first implementation, and matches Zeebe's stance): online split of a live
order-domain.** Keep the set of order-domains (ranges) **fixed/static**; grow by adding a range at
the top level and migrating *idle/terminal* lineages to it (moving a *running* lineage would drag
its live log + watermark, so it is a TODO, not a growth path). Because the mapping is a movable
range rather than a burned-in hash, this leaves the door open to online split later.

### D.6 Open points for this appendix

- **Start-assignment policy:** load-aware / least-active vs round-robin for selecting a range at
  execution birth (default: least-active, so a new empty range warms evenly).
- **Flow metadata ownership:** `flow`/`flowversion` must be readable on every partition that hosts
  executions of that namespace, else all executions of one flow pile onto one partition (the
  scope-bottleneck we are trying to avoid). Because they are read-only metadata — not mutable
  execution state — replicating them does not violate order-domain isolation. `CreateFlow` writes an
  authoritative control point, then fans out read-only replicas.
- **External ops by id** (`cancel` / `status` / `wait` / future external messages): route via
  `reverse(root_execution_id)` range lookup, like any other op.
- **Range-key form:** `reverse(root_execution_id)` (adopted here) vs extracting the random segment
  explicitly — equivalent in spread; reversal kept for invertibility and zero parsing.

### D.7 Concrete split protocol — working draft (NOT final, known open issues)

This is a **candidate** per-partition split flow for one source partition `l1` splitting into `l2`/`l3`,
recorded so the reasoning is not lost. It is **not approved**; several correctness questions remain (see
"Open issues" below). TiKV does the equivalent by proposing the split as a raft admin command
(`BatchSplit`) that shares the leader's total order, then **rejecting at apply** writes that cross the
cut — but TiKV's apply is a pure fold (no self-generated writes), so reject-at-apply does not port
directly to CCES, where the leader generates follow-up commands while processing.

**Proposed flow.** In `l1` the log is `C1, E2, C3, C4, E5` (position = index).

1. Leader receives split instruction → **appends `SC6`** (the split marker) and waits until it is
   durable — establish the barrier *before* stopping work, so a crash after rejection is recoverable
   to `SC6`.
2. **Forbid external client writes** (router/epoch gate) once `SC6` is durable.
3. Leader keeps processing its already-read stock, appending its own follow-ups (`C7, E8, E9` — these
   land *after* `SC6` in position, because appends go to the tail).
4. When the loop reaches `SC6`, the leader **appends `SE10`** — a durable Command→Event record ("the
   split executed here"), so replicas replay `SC6 → SE10` and reconstruct the same split
   deterministically — and enters the split flow.
5. Build `l2`/`l3`: **base = snapshot of the projection at the split's *applied watermark* `W`**, plus
   the tail reproduced by **replay + the snapshot filter** (`fold event iff source > W || source < 0;
   carry undecided commands`), rewriting `cause_id`:
   - parent already folded into the base snapshot → `source < 0` (parent effects already in base);
   - parent also carried in the tail → remap to the target's local position.
6. `l2`/`l3` serve from local position 0; the source `l1` fences the moved lineages.

**Key correction baked in (why migration is not "everything positioned after `SC6`").** The leader
eager-applies its own generated events: `E8, E9` (positions 8, 9 > `SC6`) are **already folded** into
the projection by the time the split happens, while `C7` (a follow-up *command* of `C1`, position 7) is
**undecided** (the loop touched `SC6` at position 6 before dispatching `C7`). So "migrate all entries
after `SC6`" would re-apply `E8`/`E9` (**double-apply**). Position order ≠ applied order: the migration
set must be **undecided work relative to `W`**, obtained by the snapshot + replay filter, not by a
position cut.

**Open issues (why this is a draft, not the answer):**
- How `W` and the carried tail interact with the eager-apply `F`/command-placement so the folded region
  stays consistent (position-order vs applied-order interleave).
- The residual in-flight and the boundary `timer`/`task` fire that lands mid-handoff: must be captured
  exactly once via fencing the source scheduler and re-registering the lineage's triggers in the target.
- Whether `SC6` at a fixed early position (with the leader draining past it) is equivalent to "drain the
  already-read cursor first, then append the marker last" (the latter has an empty/post-free tail).
- Failure recovery split into: commit-before (raft rollback + full retry) vs commit-after (idempotent
  re-materialization of `l2`/`l3` from the deterministic cut; availability gap, no data loss).

---

*This document is a design record. Once approved, the phases in §10 are implemented under
AGENTS.md (propose → confirm → implement → `cargo test` / `cargo clippy` / `cargo fmt --check`), and
the `TODO(recovery+design)` note in `Processor::run` is updated to match as the code lands.*
