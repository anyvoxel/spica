//! The virtual-time seam: what a run's clock is when the test owns it, and what the seam does when
//! nobody does.
//!
//! Every deadline an engine can reach — a `Wait`'s resume, a task's `TimeoutSeconds`, a delivery
//! lease, a retry backoff — is decided against an injected [`Clock`](spica_machinery::Clock), which is
//! what makes it testable at the *boundary* rather than at "some time later": under the wall clock a
//! case can only observe a deadline by waiting for it, so every bound has to be a second or two long
//! and an off-by-one that fires a second early is invisible inside the sleep.
//!
//! The cases that exercise such a deadline live in the typed lifecycle suites (`wait_lifecycle.rs`,
//! `task_lifecycle.rs`), each as a `TypedCase` whose run is put on a
//! [`ManualClock`](spica_machinery::ManualClock) nothing can move but the test (see
//! [`VirtualClient`](common::VirtualClient)): a 60-second `Wait` and a 30-second timeout are both
//! reached in no real time at all, and reached *exactly*. The clock is moved to one second short of a
//! deadline (and the chain asserted silent for it), then onto it (and asserted to fire), with a
//! `TimerActivated`'s `deadline` pinned as an **absolute instant** — so the arithmetic the engine did is
//! asserted, not merely that something fired. A deadline with no timer behind it reads the same way: a
//! claim's delivery lease is crossed by moving the clock and observing the poll that reclaims it. The one
//! real pause those cases take is
//! [`NOT_ENOUGH_REAL_TIME`], and it is deliberately not a wait on the deadline: it is room for the
//! engine's dispatch to make progress in, so that an implementation still keyed on elapsed real time
//! fails there instead of passing.
//!
//! What is left in this file is the other half of that seam: under the wall clock it must do *nothing*.

mod common;

use common::{NOT_ENOUGH_REAL_TIME, await_log};
use spica_engine::{EntryPayload, Event};

/// `tick` exists only for a caller that moves the clock itself; under the wall clock "time has moved,
/// look again" is what the scheduler's own wait already does, and the call is a documented no-op
/// (`Scheduler::tick`'s default). This pins that: a tick appends nothing at all — it is a re-read, not
/// an event — so a run under the wall clock cannot have its shape changed by the virtual-time API.
#[tokio::test]
async fn ticking_under_the_wall_clock_appends_nothing() {
    let definition = r#"{
        "StartAt": "P",
        "States": { "P": { "Type": "Pass", "End": true } }
    }"#;
    let (builder, log) = common::recording_builder();
    let client = common::LocalClient::start(builder)
        .await
        .expect("the engine boots under the wall clock");
    let flow_version = client
        .create_flow(
            spica_engine::FlowName::new("lifecycle_flow")
                .expect("a static literal is a valid name"),
            definition,
        )
        .await
        .expect("the flow is created");
    let execution = client
        .start_for_revision(
            spica_engine::ObjectName::plain("lifecycle_execution")
                .expect("a static literal is a valid name"),
            flow_version,
            serde_json::json!({ "n": 1 }),
        )
        .await
        .expect("the execution starts");
    client
        .wait_for_execution(&execution)
        .await
        .expect("the run reaches a terminal state");
    await_log(&log, "the closing sweep to land", |entries| {
        entries.iter().any(|entry| {
            matches!(
                &entry.payload,
                EntryPayload::Event(Event::ExecutionCompleted { .. })
            )
        })
    })
    .await;
    let settled = log.entries().len();

    client.tick();
    tokio::time::sleep(NOT_ENOUGH_REAL_TIME).await;
    assert_eq!(
        log.entries().len(),
        settled,
        "a tick with no due deadline must be completely inert: {:?}",
        &log.entries()[settled..]
    );
    client.stop().await;
}

/// A run's `deadline` is the run's own answer to "when is it due"; the `ExecutionTimeout` timer is what
/// actually enforces it. Both are written from one computation when the run is created, so the two
/// carriers must name the *same* instant — a reader that found them disagreeing would have to trust one
/// of them arbitrarily. The clock is then moved onto that instant, so the value asserted is the one that
/// really fired rather than one that merely looks right.
#[tokio::test]
async fn a_runs_deadline_is_the_instant_its_timeout_timer_carries() {
    let definition = r#"{
        "StartAt": "W",
        "TimeoutSeconds": 30,
        "States": { "W": { "Type": "Wait", "Seconds": 600, "End": true } }
    }"#;
    let (client, log, execution) = common::virtual_run(definition, "{}").await;
    // The timer must be armed *and* handed to the scheduler before the clock passes it: a late arm
    // would still fire (its deadline is already reached) and the instant compared below would then be
    // the run's own only by accident.
    await_log(&log, "the execution timeout to be armed", |entries| {
        entries.iter().any(|entry| {
            matches!(
                &entry.payload,
                EntryPayload::Event(Event::TimerActivated { timer })
                    if timer.purpose == spica_engine::TimerPurpose::ExecutionTimeout
            )
        })
    })
    .await;
    let deadline = spica_engine::Timestamp::from_millis(common::VIRTUAL_EPOCH_MILLIS + 30_000);

    tokio::time::sleep(NOT_ENOUGH_REAL_TIME).await;
    client.advance(std::time::Duration::from_secs(30));
    let terminal = client
        .wait_for_execution(&execution)
        .await
        .expect("the run terminates once its timeout fires");
    assert_eq!(
        terminal.deadline,
        Some(deadline),
        "the terminal snapshot carries the run's own deadline"
    );

    let entries = log.entries();
    let born = entries
        .iter()
        .find_map(|entry| match &entry.payload {
            EntryPayload::Event(Event::ExecutionCreated(created)) => {
                Some(created.execution.deadline)
            }
            _ => None,
        })
        .expect("the run is created");
    let armed = entries
        .iter()
        .find_map(|entry| match &entry.payload {
            EntryPayload::Event(Event::TimerActivated { timer })
                if timer.purpose == spica_engine::TimerPurpose::ExecutionTimeout =>
            {
                Some(timer.deadline)
            }
            _ => None,
        })
        .expect("the execution timeout is armed");
    assert_eq!(born, Some(deadline), "the run's own deadline");
    assert_eq!(
        born,
        Some(armed),
        "the deadline and the timer that enforces it are one instant"
    );
    client.stop().await;
}
