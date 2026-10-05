//! What a teardown must **not** route: a `Catch` fires only for a failure the state **itself** produced.
//!
//! The golden suites pin what a run writes, record for record; this pins the one fact a chain can only
//! state by leaving it out — a swept activity reaching its catcher's `Next`. The failure that ends this
//! run belongs to a *sibling* branch, and the sweep that reaches the other branch's task carries
//! `Cancelled`, so the catcher stays unbuilt. The positive half — that the same `Catch` *does* route
//! when the task fails on its own — is the golden `task_catch_routes_on_a_failed_task`; without it this
//! absence would also be satisfied by a state that had lost its `Catch` altogether.

mod common;

use std::time::Duration;

use common::{await_log, virtual_run_from};
use serde_json::json;
use spica_engine::{
    ActivityStatus, Command, EntryPayload, Event, ExecutionError, ExecutionStatus, RuntimeError,
    TerminationReason,
};

/// Branch 0 lingers on a timer and *then* fails, so branch 1's task is already in flight when the
/// sweep reaches it — a catcher that never routed because nothing live was swept would prove nothing.
/// Branch 1's task is never claimed by a worker: it is still `Running` when its sibling's failure takes
/// the `Parallel` down.
const SWEEP_FLOW: &str = r#"{
    "StartAt": "P",
    "States": {
        "P": {
            "Type": "Parallel",
            "End": true,
            "Branches": [
                { "StartAt": "Linger", "States": {
                    "Linger": { "Type": "Wait", "Seconds": 5, "Next": "Boom" },
                    "Boom": { "Type": "Fail", "Error": "BranchBoom", "Cause": "nope" }
                } },
                { "StartAt": "T", "States": {
                    "T": {
                        "Type": "Task",
                        "Resource": "r",
                        "Catch": [ { "ErrorEquals": ["States.ALL"], "Next": "Recovered" } ],
                        "Next": "Done"
                    },
                    "Recovered": { "Type": "Pass", "End": true },
                    "Done": { "Type": "Pass", "End": true }
                } }
            ]
        }
    }
}"#;

/// Past branch 0's `Wait`: the only move the run needs to reach both the failure and, therefore, the
/// sweep.
const PAST_THE_LINGER: Duration = Duration::from_secs(6);

/// The bound every such run must reach its terminal within. A failure detector, not a delay.
const TERMINAL_WITHIN: Duration = Duration::from_secs(5);

#[tokio::test]
async fn a_swept_state_never_runs_its_catch() {
    let (client, log, execution) = virtual_run_from(SWEEP_FLOW, r#"{"n":1}"#, 1).await;

    // Park on the invocation before moving the clock: the sweep has to reach a *live* task, and an
    // advance issued any earlier would race the branch's own activation rather than follow it.
    await_log(&log, "the sibling branch's task to be invoked", |entries| {
        entries.iter().any(|entry| {
            matches!(
                &entry.payload,
                EntryPayload::Command(Command::ActivateTask(_))
            )
        })
    })
    .await;
    client.advance(PAST_THE_LINGER);

    let outcome = tokio::time::timeout(TERMINAL_WITHIN, client.wait_for_execution(&execution))
        .await
        .expect("the run reaches its terminal without real waiting on the deadline")
        .expect("the run reaches a terminal state");
    // The closing sweep of the same cascade follows the terminal ack, so let those appends land before
    // reading the log the assertions below walk.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let entries = log.entries();
    client.stop().await;

    assert_eq!(
        outcome.status,
        ExecutionStatus::Terminated(TerminationReason::Failed {
            error: ExecutionError::Runtime(RuntimeError::StateFailed {
                state: "Boom".to_string(),
                error: "BranchBoom".to_string(),
                output: Box::new(json!({"Error": "BranchBoom", "Cause": "nope"})),
            }),
        }),
        "the run ends on the sibling branch's failure"
    );

    // Finding this record at all is half the assertion: a state's teardown reaches only a `Running`
    // activity, so its presence is what proves the task was in flight when the sweep arrived.
    let swept = entries
        .iter()
        .find_map(|entry| match &entry.payload {
            EntryPayload::Event(Event::StateTerminated { activity })
                if activity.state_path.state_name() == "T" =>
            {
                Some(activity)
            }
            _ => None,
        })
        .expect("the swept branch's task state terminates");
    assert_eq!(
        swept.status,
        ActivityStatus::Terminated(TerminationReason::Cancelled),
        "a state an ancestor's teardown reached terminates as a sweep, never with the sibling's error"
    );

    assert!(
        !entries.iter().any(|entry| matches!(
            &entry.payload,
            EntryPayload::Event(Event::StateActivating { activity })
                if matches!(activity.state_path.state_name().as_str(), "Recovered" | "Done")
        )),
        "the swept task never ran its Catch: the catcher's route is never built, and the task's own \
         `Next` is never reached either"
    );
}
