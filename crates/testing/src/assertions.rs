//! Assertions over an appended **entry chain** — the invariant shape every run's log must have.
//!
//! These read a run's records the way a follower does, and they state the rules that hold for *every*
//! append rather than pinning a per-record table: a gap in the positions, a stamp that went backwards,
//! or a batch left uncommitted fails here once, with its own message, instead of showing up as a
//! wholesale diff. The log's shape is what replay and the commit protocol rest on, so it is checked
//! wherever a chain is read, not only in the cases that happen to state it.

use std::collections::{HashMap, HashSet};

use serde_json::Value;
use spica_engine_types::EntryId;
use spica_engine_types::types::entry::{Entry, EntryPayload};

/// The envelope contract, checked over the whole stream rather than pinned per record: what holds for
/// *every* append is a rule, so a violation fails once with its own message instead of showing up as a
/// wholesale diff of a per-record table.
///
/// - **positions** — one stream, and a dense 1-based position run: a gap means a lost append, an
///   un-stamped `nil` position means an entry reached the log un-materialized, and a follower's
///   watermark arithmetic (`W + 1`) depends on both;
/// - **stamps** — the append stamps never go backwards (see [`assert_append_times`]);
/// - **causes** — the batch/commit graph a follower replays by (see `assert_causal_chain`).
pub fn assert_envelope(entries: &[&Entry]) {
    let first = entries.first().expect("a run appends at least one entry");
    for (position, entry) in entries.iter().enumerate() {
        assert_eq!(
            entry.stream_id, first.stream_id,
            "the entry at index {position} carries a different stream id than the rest"
        );
        assert_eq!(
            entry.entry_id.get(),
            (position + 1) as i64,
            "the entry at index {position} does not hold its own position; the log stamps a dense \
             run starting at 1"
        );
    }
    assert_append_times(entries);
    assert_causal_chain(entries);
}

/// The log's own append stamps, in append order. The log *is* the order (by entry id), but a stamp
/// that went backwards would make a time-sorted reading of the same history disagree with the causal
/// one — the audit trail's value is that both agree.
fn assert_append_times(entries: &[&Entry]) {
    let mut previous: Option<u64> = None;
    for (position, entry) in entries.iter().enumerate() {
        let at = entry.timestamp.as_millis();
        if let Some(previous) = previous {
            assert!(
                at >= previous,
                "the append stamp went backwards at the entry at index {position}: {at} < {previous} ms"
            );
        }
        previous = Some(at);
    }
}

/// The causal graph. Every record a handler appends is stamped with the position of the command it
/// was reacting to, so the stream is a sequence of **batches** — and a follower's commit watermark
/// means exactly "the last batch's last record". The rules below state that shape:
///
/// 1. a cause points **backwards at a `Command`** — handlers append only in reaction to one, so a
///    cause naming an event, or a later entry, is a broken link;
/// 2. a command's records form **one** run, and a `Noop` carrying that same cause closes it. A run
///    reopened after closing, a run left unclosed, or a `Noop` displaced from the run's end all break
///    "one batch, committed once" — the invariant both replay and the commit protocol rest on;
/// 3. a record with **no** cause is a worker-initiated append: it is a `Command`/`Reject`, the log's
///    terminator `Noop` follows it, and it can never sit inside a batch.
///
/// The `Noop` terminators are deliberately absent from the chain table (they carry no payload worth
/// reading), so this is the only thing standing between a lost commit marker and a green suite.
fn assert_causal_chain(entries: &[&Entry]) {
    // The batch in progress: its cause, and `None` once its commit marker has closed it.
    let mut open: Option<EntryId> = None;
    let mut committed: HashSet<EntryId> = HashSet::new();
    for (position, entry) in entries.iter().enumerate() {
        let Some(cause) = entry.cause_id else {
            assert!(
                open.is_none(),
                "the entry at index {position} has no cause but interrupts the batch of {}",
                open.expect("checked by the assertion above")
            );
            // A worker-initiated append is closed by the log's own terminator, so the record after it
            // is that `Noop` — never another record of any kind.
            if !matches!(entry.payload, EntryPayload::Noop) {
                let next = entries.get(position + 1).unwrap_or_else(|| {
                    panic!("the entry at index {position} was never terminated by a Noop")
                });
                assert!(
                    next.cause_id.is_none() && matches!(next.payload, EntryPayload::Noop),
                    "the entry at index {position} is un-terminated: the log closes a \
                     worker-initiated append with a cause-less Noop, found {} instead",
                    payload_kind_of(next)
                );
            }
            continue;
        };

        if open != Some(cause) {
            assert!(
                open.is_none(),
                "batch {} was never committed before batch {cause} started at the entry at index \
                 {position}",
                open.expect("checked by the assertion above")
            );
            assert!(
                committed.insert(cause),
                "batch {cause} was committed and then reopened at the entry at index {position}"
            );
            assert_cause_points_at_a_command(entries, cause, position);
            open = Some(cause);
        }
        if matches!(entry.payload, EntryPayload::Noop) {
            open = None; // This is the batch's commit marker: the run is closed.
        }
    }
    assert!(
        open.is_none(),
        "the stream ends with batch {} uncommitted",
        open.expect("checked by the assertion above")
    );
}

/// Rule 1 of [`assert_causal_chain`]: the cause names an entry that is both **earlier** and a
/// `Command`. `EntryId::nil()` (an un-stamped id) is negative, so it can never pass.
fn assert_cause_points_at_a_command(entries: &[&Entry], cause: EntryId, position: usize) {
    let index = usize::try_from(cause.get() - 1)
        .ok()
        .filter(|index| *index < position);
    assert!(
        matches!(
            index
                .and_then(|index| entries.get(index))
                .map(|e| &e.payload),
            Some(EntryPayload::Command(_))
        ),
        "the entry at index {position} is caused by {cause}, which is not an earlier Command"
    );
}

/// `CreateFlow(CreateFlow { .. })` / `StateCompleting { activity: .. }` → the variant name. Read off
/// the payload's derived `Debug` rendering rather than a hand-written match, so the helper does not
/// have to be extended for every new variant.
pub fn payload_kind(payload: &EntryPayload) -> String {
    let rendered = match payload {
        EntryPayload::Command(command) => format!("{command:?}"),
        EntryPayload::Event(event) => format!("{event:?}"),
        EntryPayload::Reject(reject) => format!("{reject:?}"),
        EntryPayload::Noop => unreachable!("filtered out by learned_chain"),
    };
    rendered
        .split(['(', ' ', '{'])
        .next()
        .unwrap_or_default()
        .to_string()
}

/// [`payload_kind`] for an entry that may be a `Noop`: the envelope checks report on entries a chain
/// drops, so they need a kind for the one payload [`payload_kind`] refuses to name.
fn payload_kind_of(entry: &Entry) -> String {
    match &entry.payload {
        EntryPayload::Noop => "Noop".to_string(),
        other => payload_kind(other),
    }
}

/// The values a payload carries, as a follower reading the log would parse them — minus the
/// externally-tagged wrapper, which only repeats the kind. The payload is serialized (rather than its
/// fields listed by hand) so that a field added to a command or event is *seen* by the timing walk
/// below instead of slipping past it.
pub fn payload_values(payload: &EntryPayload) -> Value {
    let value = match payload {
        EntryPayload::Command(command) => serde_json::to_value(command),
        EntryPayload::Event(event) => serde_json::to_value(event),
        EntryPayload::Reject(reject) => serde_json::to_value(reject),
        EntryPayload::Noop => return Value::Null,
    }
    .expect("an engine payload is serializable");
    match value {
        Value::Object(map) if map.len() == 1 => {
            map.into_iter().next().expect("the length was checked").1
        }
        other => other,
    }
}

/// The timing invariants a stream must satisfy, checked while its chain is read: an object's birth is
/// immutable and its update stamp never goes backwards — per object, and across the chain, which is
/// what an appended stream must satisfy to be replayable in order. A typed chain pins those stamps
/// literally, so it can say what they *are*; the invariants they carry hold for every run, so they are
/// checked for every run rather than only for the cases that happen to state them.
#[derive(Default)]
pub struct Stamps {
    born: HashMap<String, u64>,
    last_update: HashMap<String, u64>,
    last_update_overall: Option<u64>,
    /// Collected rather than panicked on the spot, so one run reports every broken object instead of
    /// only the first.
    violations: Vec<String>,
}

impl Stamps {
    pub fn learn_value(&mut self, value: &Value) {
        match value {
            Value::Object(map) => {
                let uid = map.get("uid").and_then(Value::as_str);
                let created = map.get("created_at").and_then(Value::as_u64);
                let updated = map.get("updated_at").and_then(Value::as_u64);
                if let (Some(uid), Some(created), Some(updated)) = (uid, created, updated) {
                    self.check_times(uid, created, updated);
                }
                for child in map.values() {
                    self.learn_value(child);
                }
            }
            Value::Array(items) => items.iter().for_each(|item| self.learn_value(item)),
            _ => {}
        }
    }

    fn check_times(&mut self, uid: &str, created: u64, updated: u64) {
        match self.born.get(uid) {
            Some(born) if *born != created => self.violations.push(format!(
                "{uid}: born at {born} ms but re-born at {created} ms"
            )),
            Some(_) => {}
            None => {
                self.born.insert(uid.to_string(), created);
            }
        }
        if updated < created {
            self.violations.push(format!(
                "{uid}: updated at {updated} ms before its birth at {created} ms"
            ));
        }
        let previous = self.last_update.insert(uid.to_string(), updated);
        if let Some(previous) = previous
            && updated < previous
        {
            self.violations.push(format!(
                "{uid}: updated at {updated} ms after {previous} ms — went backwards"
            ));
        }
        if let Some(previous) = self.last_update_overall
            && updated < previous
        {
            self.violations.push(format!(
                "the append order and the update stamps disagree: {updated} ms after {previous} ms"
            ));
        }
        self.last_update_overall = Some(updated);
    }

    pub fn assert_times_are_sane(&self) {
        assert!(
            self.violations.is_empty(),
            "timing invariants broken:\n  {}",
            self.violations.join("\n  ")
        );
    }
}

/// Compare a typed chain record by record, naming the diverging record's position and its kind in the
/// report — so a chain that took a different branch says *which* record and *what* it wrote, rather
/// than handing the reader two twenty-record payload dumps to diff by eye.
pub fn assert_typed_chain(actual: &[EntryPayload], expected: &[EntryPayload]) {
    for (position, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        assert_eq!(
            actual,
            expected,
            "record {position} of the chain ({})",
            payload_kind(actual)
        );
    }
    assert_eq!(
        actual.len(),
        expected.len(),
        "chain length; the actual chain as the log holds it:\n{}",
        typed_chain_literals(actual)
    );
}

/// Render a typed chain as the log's own JSON — one record per line, in append order — so a length
/// mismatch shows what the run actually wrote beside the expectation built in code.
pub fn typed_chain_literals(chain: &[EntryPayload]) -> String {
    chain
        .iter()
        .map(|payload| {
            let json = serde_json::to_string(payload).expect("an engine payload is serializable");
            format!("            r#\"{json}\"#,")
        })
        .collect::<Vec<_>>()
        .join("\n")
}
