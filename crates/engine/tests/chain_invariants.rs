//! The envelope around each payload and the `Noop` batch terminators — what a chain assertion is read
//! *through*, rather than pinning record by record — asserted by **rule**. This suite tests the rules
//! themselves: a checker no run can fail is worth nothing, so each case here takes a real run's
//! entries, breaks exactly one thing about them, and requires the rule that names it to fire.
//!
//! The mutations are *replacements*, never deletions: the log's positions are dense and 1-based, so
//! dropping an entry would trip the position rule first and say nothing about the rule under test.
//!
//! The invariants these rules carry are the ones a follower replays by — a batch must be committed by
//! its own marker before the next batch starts, and a cause must name the `Command` a record was
//! appended in reaction to — which is why they are checked on every run, not only here.

mod common;

use common::{assert_envelope, run_raw_entries};
use spica_engine::{Entry, EntryId, EntryPayload, Timestamp};

/// Two chained `Pass` states: enough structure for several batches (the flow creation, the execution
/// start, each state's activation, its completion, the thread/execution close), with no state that
/// behaves differently on two runs.
const TWO_PASSES: &str = r#"{
    "StartAt": "P1",
    "States": {
        "P1": { "Type": "Pass", "Next": "P2" },
        "P2": { "Type": "Pass", "End": true }
    }
}"#;

/// A run's entries as [`assert_envelope`] reads them.
fn envelope(entries: &[Entry]) {
    let refs: Vec<&Entry> = entries.iter().collect();
    assert_envelope(&refs);
}

/// Every position holding a `Noop` **with** a cause — a batch commit marker — in order.
fn commit_markers(entries: &[Entry]) -> Vec<usize> {
    (0..entries.len())
        .filter(|position| {
            matches!(entries[*position].payload, EntryPayload::Noop)
                && entries[*position].cause_id.is_some()
        })
        .collect()
}

/// The first position holding a `Noop` **without** a cause — the log's own terminator for a
/// worker-initiated append.
fn first_terminator(entries: &[Entry]) -> usize {
    (0..entries.len())
        .find(|position| {
            matches!(entries[*position].payload, EntryPayload::Noop)
                && entries[*position].cause_id.is_none()
        })
        .expect("a run appends worker-initiated records, so at least one terminator exists")
}

/// The cause a commit marker closes the batch of.
fn marker_cause(entries: &[Entry], marker: usize) -> spica_engine::EntryId {
    entries[marker]
        .cause_id
        .expect("a commit marker carries the cause of the batch it closes")
}

/// The index of the first record a batch produced. Not `cause` itself: a worker-initiated command is
/// followed by the log's terminator before its records begin.
fn run_start(entries: &[Entry], marker: usize) -> usize {
    let cause = marker_cause(entries, marker);
    (0..entries.len())
        .find(|position| entries[*position].cause_id == Some(cause))
        .expect("a committed batch has at least one record")
}

/// Replace `target`'s **payload** with the one at `donor`, leaving the slot's own envelope (stream,
/// position, stamp) alone — every mutation here breaks one field of one record, never the log's
/// bookkeeping, which the position rule would report first and mask the rule under test.
fn borrow_payload(entries: &mut [Entry], target: usize, donor: usize) {
    entries[target].payload = entries[donor].payload.clone();
}

#[tokio::test]
async fn a_complete_run_satisfies_the_envelope() {
    let entries = run_raw_entries(TWO_PASSES, r#"{"n":1}"#).await;

    envelope(&entries);
}

#[tokio::test]
#[should_panic(expected = "was never committed before")]
async fn a_batch_never_committed_is_caught() {
    let mut entries = run_raw_entries(TWO_PASSES, r#"{"n":1}"#).await;

    // A batch left open when the *next* batch starts: its commit marker stops being a `Noop` — the
    // stand-in for a lost marker, which is what a stream that never closed the batch would show. The
    // records between the two batches cannot simply be removed (the log's positions are dense), so the
    // worker-initiated command between them is re-pointed at the cause the next batch uses: that is
    // what makes the walk meet a different batch while the first one is still open.
    let marker = commit_markers(&entries)[0];
    borrow_payload(&mut entries, marker, marker - 1);
    let next_batch = commit_markers(&entries)[1];
    let between = (marker..next_batch)
        .find(|position| entries[*position].cause_id.is_none())
        .expect("a worker-initiated append separates two processor batches");
    entries[between].cause_id = Some(marker_cause(&entries, next_batch));
    envelope(&entries);
}

#[tokio::test]
#[should_panic(expected = "was committed and then reopened")]
async fn a_reopened_batch_is_caught() {
    let mut entries = run_raw_entries(TWO_PASSES, r#"{"n":1}"#).await;

    // A second copy of one batch's commit marker one slot later: the first copy closes the run, so
    // the marker's cause coming back is the batch committing twice.
    let marker = commit_markers(&entries)[0];
    entries[marker + 1].cause_id = Some(marker_cause(&entries, marker));
    borrow_payload(&mut entries, marker + 1, marker);
    envelope(&entries);
}

#[tokio::test]
#[should_panic(expected = "which is not an earlier Command")]
async fn a_cause_pointing_at_an_event_is_caught() {
    let mut entries = run_raw_entries(TWO_PASSES, r#"{"n":1}"#).await;

    // Point the first record of a batch at an *event* instead of at the command it reacted to: inside
    // the stream's positions, but not a `Command`, so the link is broken although it looks plausible.
    let wrong_cause = commit_markers(&entries).into_iter().find_map(|marker| {
        let start = run_start(&entries, marker);
        (0..start)
            .find(|position| matches!(entries[*position].payload, EntryPayload::Event(_)))
            .map(|event| (start, entries[event].entry_id))
    });
    let (start, event_id) = wrong_cause.expect("a later batch starts after an event was written");
    entries[start].cause_id = Some(event_id);
    envelope(&entries);
}

#[tokio::test]
#[should_panic(expected = "interrupts the batch of")]
async fn a_causeless_record_inside_a_batch_is_caught() {
    let mut entries = run_raw_entries(TWO_PASSES, r#"{"n":1}"#).await;

    // A record with no cause in the middle of a batch — the shape a stray worker-initiated append
    // would take if it landed between a batch's records.
    let marker = commit_markers(&entries)[0];
    entries[marker - 1].cause_id = None;
    envelope(&entries);
}

#[tokio::test]
#[should_panic(expected = "is un-terminated")]
async fn a_missing_append_terminator_is_caught() {
    let mut entries = run_raw_entries(TWO_PASSES, r#"{"n":1}"#).await;

    // The terminator after a worker-initiated command stops being a `Noop`: the append is then never
    // closed, and the rule reports the terminator it expected to find.
    let terminator = first_terminator(&entries);
    borrow_payload(&mut entries, terminator, terminator + 1);
    envelope(&entries);
}

#[tokio::test]
#[should_panic(expected = "does not hold its own position")]
async fn a_position_gap_is_caught() {
    let mut entries = run_raw_entries(TWO_PASSES, r#"{"n":1}"#).await;

    // A position the log never stamped — an append that went missing rather than one that was undone.
    entries[2].entry_id = EntryId::new(42);
    envelope(&entries);
}

#[tokio::test]
#[should_panic(expected = "the append stamp went backwards")]
async fn an_append_stamp_going_backwards_is_caught() {
    let mut entries = run_raw_entries(TWO_PASSES, r#"{"n":1}"#).await;

    // A stamp strictly earlier than its predecessor's: from here on the causal and the time-sorted
    // reading of the same history disagree. (Strictly earlier than the *first* stamp, so a run whose
    // entries all share one millisecond is caught just the same.)
    let before_everything = entries[0].timestamp.as_millis() - 1;
    entries[2].timestamp = Timestamp::from_millis(before_everything);
    envelope(&entries);
}
