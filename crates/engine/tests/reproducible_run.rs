//! Reproducibility: the same definition, run twice, writes the same log.
//!
//! A run has three inputs that would otherwise be ambient — *when* everything happens (the injected
//! [`Clock`](spica_machinery::Clock)), *what everything is called* (the injected
//! [`IdGenerator`](spica_machinery::IdGenerator)), and the `request_id`s the caller mints to correlate
//! its acks. With all three supplied, a run stops being a fresh improvisation and becomes a repeatable
//! function of its definition: the log of one run equals the log of the next, entry for entry.
//!
//! That equality is what this suite asserts, and it asserts it on the **whole** stream — the envelope
//! (`Entry` itself: positions, causes, append stamps) and the `Noop` batch terminators included, not
//! only the records a chain asserts. A chain says what *one* run wrote; this says that what a run
//! writes is a function of its definition, so a uid that changed between two runs, a stamp that
//! drifted, or a cause that landed on the wrong entry all fail.
//!
//! The second case is the first one's teeth: moving a single seam by one number must break the
//! equality. Without it, `assert_eq!` passing could mean "the logs repeat" or merely "the logs are
//! empty", and the difference matters — it is the whole claim.

mod common;

use std::time::Duration;

use common::{await_log, virtual_run_from};
use spica_engine::{Entry, EntryPayload, Event};

/// A definition that reaches a terminal state only through a fired timer: the boot cascade mints the
/// flow, version, execution, root thread and each activity's uid, arms the `Wait`'s timer against the
/// virtual clock, and the advance below resumes it. One run therefore exercises both seams at once and
/// writes ~50 entries (the envelope and the `Noop` terminators included), so the equality compared
/// below is over a stream with real content.
const REPRO_FLOW: &str = r#"{
    "StartAt": "P",
    "States": {
        "P": { "Type": "Pass", "Next": "W" },
        "W": { "Type": "Wait", "Seconds": 5, "Next": "S" },
        "S": { "Type": "Succeed" }
    }
}"#;

const REPRO_INPUT: &str = r#"{"n":1}"#;

/// The `Wait`'s deadline is the epoch plus its 5 seconds; advancing past it is the one move every run
/// of [`REPRO_FLOW`] needs to finish.
const PAST_THE_WAIT: Duration = Duration::from_secs(6);

/// Run `REPRO_FLOW` to its terminal state with the identities beginning at `first_id`, and return the
/// entire log it wrote.
async fn log_of_a_run(first_id: u64) -> Vec<Entry> {
    let (client, log, execution) = virtual_run_from(REPRO_FLOW, REPRO_INPUT, first_id).await;

    // Park on the armed timer before moving the clock: the `Wait` computes its deadline when it is
    // entered, so an advance issued any earlier would be arithmetic on a deadline that does not exist
    // yet — and the run would then hang rather than differ.
    await_log(&log, "the Wait's timer to be armed", |entries| {
        entries.iter().any(|entry| {
            matches!(
                &entry.payload,
                EntryPayload::Event(Event::TimerActivated { .. })
            )
        })
    })
    .await;
    client.advance(PAST_THE_WAIT);

    tokio::time::timeout(
        Duration::from_secs(5),
        client.wait_for_execution(&execution),
    )
    .await
    .expect("the run reaches a terminal state without real waiting")
    .expect("the run reaches a terminal state");

    let entries = log.entries();
    client.stop().await;
    entries
}

#[tokio::test]
async fn the_same_definition_writes_the_same_log_twice() {
    let first = log_of_a_run(1).await;
    let second = log_of_a_run(1).await;

    // The comparison is only worth making over a stream that has the content it claims: the fired
    // timer proves the run passed through the clock's advance, so the two logs below are compared over
    // a run that really happened rather than over two empty vectors.
    assert!(
        first.iter().any(|entry| matches!(
            &entry.payload,
            EntryPayload::Event(Event::TimerTriggered { .. })
        )),
        "the run must reach the Wait's timer before its two logs are compared"
    );

    assert_eq!(
        first, second,
        "a run of the same definition repeats itself exactly: same ids, same stamps, same causes"
    );
}

#[tokio::test]
async fn a_run_from_a_different_id_range_is_a_different_log() {
    let first = log_of_a_run(1).await;
    let moved = log_of_a_run(1_000).await;

    // Same definition, same input, same clock — only the injected identities differ, so this is the
    // mutation that shows the equality above is sensitive to the seam it credits.
    assert_ne!(
        first, moved,
        "the log carries the injected identities, so a different id range cannot write the same stream"
    );
}
