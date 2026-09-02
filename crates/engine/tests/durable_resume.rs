//! End-to-end: a durable (Rocks) Engine resumes from its persisted `lastProcessedPosition` after a
//! restart.
//!
//! The NMC (no-Multiple-Crash) claim under test: when the StreamProcessor boots it read the log hard-coded
//! from position 1, so a restart re-dispatched every already-applied Command (duplicate appends). With
//! the apply-time watermark it must resume from `W + 1` and *not* re-append anything already applied.
//! See `docs/durable-execution-recovery-design.md` (single-node model).

mod common;

use std::path::Path;

use serde_json::Value;
use spica_engine::{EngineBuilder, EntryId, EntryPayload, FlowName, LogStream, ObjectName};
use spica_logstream::RocksLogStream;
use spica_scheduler::InMemoryScheduler;
use spica_storage::RocksStorage;

/// A single-terminal-state machine — one execution routes A → Succeed, settling deterministically.
const SM: &str = r#"{ "StartAt": "A", "States": { "A": { "Type": "Succeed" } } }"#;

/// A unique per-test path under the system temp dir, removed after the test.
fn temp_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("spica-durable-resume-{tag}-{}", ulid::Ulid::new()))
}

/// Count the durable entries in a freshly-opened `RocksLogStream`, closing it before returning.
///
/// RocksDB holds an exclusive directory lock while a handle is open, so the reader must be dropped
/// before the caller can `open` the same path again (for the next engine boot).
async fn log_len(path: &Path) -> usize {
    let log = RocksLogStream::<EntryPayload>::open(path).expect("open log to count");
    let mut n = 0usize;
    let mut from = EntryId::new(1);
    // Positions are contiguous, so probe sequentially until the first absent (past-the-end) position.
    while log.read(from).await.expect("read log entry").is_some() {
        n += 1;
        from = EntryId::new(from.get() + 1);
    }
    drop(log); // release the exclusive directory lock before the caller reopens the path.
    n
}

/// Boot a durable Engine over `log`/`storage` paths and run one anonymous execution to completion,
/// then shut it down cleanly. Because the terminal event's fold + watermark advance both land before
/// `wait_for_execution` resolves, returning here leaves a durable resume point `W` (cf. the accepted
/// at-least-once crash window in the design doc — a clean shutdown avoids it).
async fn run_one_execution(log_path: &Path, storage_path: &Path) {
    // Box the Rocks backends as the engine's trait objects. Each `run_one_execution` call opens its
    // own handles, so the caller can reuse the same paths across "restarts".
    let log = RocksLogStream::<EntryPayload>::open(log_path).expect("open log");
    let storage = RocksStorage::open(storage_path).expect("open storage");
    let engine = EngineBuilder::with_backends(Box::new(log), Box::new(storage))
        .with_scheduler(InMemoryScheduler::spawn())
        .start()
        .await
        .expect("engine boots");
    // A fresh anonymous flow each run keeps name-keyed rows (and their entry counts) from coupling
    // across restarts: every execution exercises the identical state machine path.
    let flow_name =
        FlowName::new(&format!("anon_{}", ulid::Ulid::new())).expect("ULID-suffixed name is valid");
    let flow_version = engine
        .create_flow(flow_name, SM)
        .await
        .expect("create flow");
    let execution_id = engine
        .start_for_revision(
            ObjectName::generated_with_suffix("resume", &ulid::Ulid::new().to_string())
                .expect("ULID-suffixed generated name is valid"),
            flow_version,
            Value::Null,
        )
        .await
        .expect("start execution");
    engine
        .wait_for_execution(&execution_id)
        .await
        .expect("execution completes");
    engine.stop().await; // controlled shutdown: drains the loop, drops the Rocks handles.
}

#[tokio::test]
async fn restart_resumes_from_last_processed_position_without_reappend() {
    let log_path = temp_path("log");
    let storage_path = temp_path("storage");
    let fresh_log = temp_path("fresh-log");
    let fresh_storage = temp_path("fresh-storage");

    // Phase 1: run one execution to completion on the durable backends, then record the log length.
    run_one_execution(&log_path, &storage_path).await;
    let n1 = log_len(&log_path).await;
    assert!(n1 > 0, "phase 1 must have appended entries");

    // Phase 2: restart on the SAME log + storage and run a second execution. If the resume watermark
    // is honored, this adds exactly one fresh execution's entries — no re-append of phase 1's
    // already-applied commands.
    run_one_execution(&log_path, &storage_path).await;
    let n2 = log_len(&log_path).await;

    // Reference: the per-execution entry delta on a fresh store. Log/appends assign the same sequence
    // of positions for the identical state machine + input, so this is the deterministic count a
    // single "clean" run contributes — regardless of where in the log it lands.
    run_one_execution(&fresh_log, &fresh_storage).await;
    let n_ref = log_len(&fresh_log).await;
    assert!(n_ref > 0, "a fresh run must append entries");

    // The core regression assertion: growth across the restart is exactly the new execution's entries.
    // A buggy restart that re-read from position 1 would re-dispatch phase 1's commands and grow the
    // log by (phase-1 entries + duplicate batches) — a strictly larger delta.
    assert_eq!(
        n2 - n1,
        n_ref,
        "restart appended more than the new execution's entries — already-applied commands were \
         re-dispatched (watermark not honored)"
    );

    let _ = std::fs::remove_dir_all(&log_path);
    let _ = std::fs::remove_dir_all(&storage_path);
    let _ = std::fs::remove_dir_all(&fresh_log);
    let _ = std::fs::remove_dir_all(&fresh_storage);
}
