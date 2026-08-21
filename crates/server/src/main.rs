//! `spica-server` — the spica workflow engine exposed as a gRPC service.
//!
//! A **single-node** gRPC server: it constructs one durable [`Engine`] (RocksDB log + storage),
//! starts its single long-lived StreamProcessor, and serves the two RPC services from the wire contract —
//! [`Workflow`](spica_proto::v1::workflow_server) (`CreateFlow`) and
//! [`Execution`](spica_proto::v1::execution_server) (`StartExecution` / `GetExecution` /
//! `StopExecution`). Each service's handlers live in its own module ([`workflow`], [`execution`]);
//! shared service state + helpers are in [`common`].
//!
//! The remote client (`spica` CLI) drives a run as **CreateFlow → StartExecution → poll
//! GetExecution**: `StartExecution` returns the execution id at birth and the client settles by
//! polling `GetExecution` (a non-blocking snapshot read), so there is no fire-and-forget or
//! blocking "Wait" RPC on the wire.

mod common;
mod execution;
mod workflow;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use spica_engine::{EngineBuilder, EntryPayload, LogStream, Scheduler, Storage, TaskService};
use spica_logstream::RocksLogStream;
use spica_proto::v1::execution_server::ExecutionServer;
use spica_proto::v1::workflow_server::WorkflowServer;
use spica_scheduler::InMemoryScheduler;
use spica_storage::RocksStorage;
use spica_task_service::InMemoryTaskService;

use crate::common::Svc;

/// Command-line flags for the server.
#[derive(Debug, Parser)]
#[command(
    name = "spica-server",
    about = "spica workflow engine as a gRPC service"
)]
struct Args {
    /// Address to bind and listen on.
    #[arg(long, default_value = "127.0.0.1:50051")]
    listen: String,
    /// Root directory holding the durable RocksDB log + storage state. Subdirectories `log/` and
    /// `storage/` are created beneath it on boot.
    #[arg(long, default_value = "./spica-data")]
    data: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Engine + server at info by default; RUST_LOG overrides.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let storage_dir = args.data.join("storage");
    let log_dir = args.data.join("log");
    // RocksDB backends won't open paths that don't exist — create them up front.
    std::fs::create_dir_all(&storage_dir).with_context(|| "create storage dir")?;
    std::fs::create_dir_all(&log_dir).with_context(|| "create log dir")?;

    // One durable engine backs every RPC. This binary owns the **assembly**: it opens the RocksDB
    // log + storage here (from the `spica-logstream` / `spica-storage` crates) and spawns the timer
    // scheduler (from `spica-scheduler`), injecting all three into the engine as type-erased trait
    // objects — `Engine::start` then consumes the unstarted builder (typestate: an unstarted engine
    // has no RPC-bound operation methods), boots the StreamProcessor, and returns the running `Engine`; its
    // single StreamProcessor replays the log into the storage projection on boot and runs for the process
    // lifetime (HTTP/2 keepalive keeps it alive). The running `Engine` is then wrapped in the `Arc`
    // shared by the two service handles.
    let log: Box<dyn LogStream<EntryPayload>> =
        Box::new(RocksLogStream::<EntryPayload>::open(&log_dir)?);
    let storage: Box<dyn Storage> = Box::new(RocksStorage::open(&storage_dir)?);
    let scheduler: Arc<dyn Scheduler> = InMemoryScheduler::spawn();
    // No worker is attached in this M1 server: a Task simply stays claimable/pending (Zeebe
    // semantics); if its state sets `TimeoutSeconds`, the engine's `TaskTimeout` backstop fails it
    // after that window. A production server would attach concrete `TaskHandler`s (see
    // `spica-task-service`).
    let task_service: Arc<dyn TaskService> = InMemoryTaskService::spawn(HashMap::new());
    let engine = EngineBuilder::with_backends(log, storage)
        .with_scheduler(scheduler)
        .with_task_service(task_service)
        .start()
        .await
        .context("start engine processor")?;
    tracing::info!(listen = %args.listen, "spica-server listening");

    let svc = Svc {
        engine: Arc::new(engine),
    };
    tonic::transport::Server::builder()
        .add_service(WorkflowServer::new(svc.clone()))
        .add_service(ExecutionServer::new(svc))
        .serve(args.listen.parse().context("parse listen address")?)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::Value;
    use spica_asl::StateMachine;
    use spica_engine::{
        EngineBuilder, EntryPayload, FlowName, LogStream, Scheduler, Storage, TaskService,
    };
    use spica_logstream::RocksLogStream;
    use spica_scheduler::InMemoryScheduler;
    use spica_storage::RocksStorage;
    use spica_task_service::InMemoryTaskService;

    /// A unique per-test path under the system temp dir, removed after the test.
    fn temp_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("spica-server-{tag}-{}", ulid::Ulid::new()))
    }

    /// End-to-end assembly test: open durable RocksDB backends, run a trivial `Pass` execution
    /// through them, and complete. This exercises the binary's exact construction path — opening
    /// the log + storage and injecting them via `EngineBuilder::with_backends` — plus the StreamProcessor
    /// driving the owned RocksStorage/RocksLogStream.
    #[tokio::test]
    async fn server_assembly_runs_durable_execution() {
        let log_dir = temp_path("log");
        let storage_dir = temp_path("storage");
        std::fs::create_dir_all(&log_dir).unwrap();
        std::fs::create_dir_all(&storage_dir).unwrap();

        let sm: StateMachine = serde_json::from_value(serde_json::json!({
            "StartAt": "P",
            "States": { "P": { "Type": "Pass", "End": true } }
        }))
        .unwrap();

        let log: Box<dyn LogStream<EntryPayload>> =
            Box::new(RocksLogStream::<EntryPayload>::open(&log_dir).unwrap());
        let storage: Box<dyn Storage> = Box::new(RocksStorage::open(&storage_dir).unwrap());
        let scheduler: std::sync::Arc<dyn Scheduler> = InMemoryScheduler::spawn();
        // No task worker is attached (as in the real server); see main().
        let task_service: std::sync::Arc<dyn TaskService> =
            InMemoryTaskService::spawn(HashMap::new());
        let engine = EngineBuilder::with_backends(log, storage)
            .with_scheduler(scheduler)
            .with_task_service(task_service)
            .start()
            .await
            .unwrap();
        let name = FlowName::new("test").unwrap();
        let definition = serde_json::to_string(&sm).unwrap();
        let flow_version_id = engine.create_flow(name, &definition).await.unwrap();
        let execution_id = engine
            .start_for_revision(flow_version_id, Value::Null)
            .await
            .unwrap();
        let result = engine.wait_for_execution(execution_id).await.unwrap();
        assert_eq!(result.output, Value::Null);

        // Drop the engine (releasing its DB handles) before removing the directories.
        drop(engine);
        let _ = std::fs::remove_dir_all(&log_dir);
        let _ = std::fs::remove_dir_all(&storage_dir);
    }
}
