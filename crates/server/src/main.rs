//! `spica-server` — the spica workflow engine exposed as a gRPC service.
//!
//! A **single-node** gRPC server: it constructs one durable [`Engine`] (RocksDB log + storage),
//! starts its single long-lived StreamProcessor, and serves the four RPC services from the wire
//! contract — [`Workflow`](spica_proto::v1::workflow_service_server) (`CreateFlow` / `ResolveFlowVersion`),
//! [`Execution`](spica_proto::v1::execution_service_server) (`StartExecution` / `StopExecution`),
//! [`Task`](spica_proto::v1::task_service_server) (`PollTasks` / `CompleteTask` / `FailTask` — the
//! out-of-process worker's claim/settle API), and [`Query`](spica_proto::v1::query_server)
//! (`GetObject` / `ListObjects` — the k8s-style read API over the projection). Each service's
//! handlers live in its own module ([`workflow`], [`execution`], [`task`], [`query`]); shared
//! service state + helpers are in [`common`].
//!
//! The remote client (`spica` CLI) drives a run as **CreateFlow → StartExecution → poll Query**: a
//! run is started (returning its id at birth) and settled by polling `Query.GetObject` on kind
//! `execution` — a non-blocking point-in-time read — so there is no fire-and-forget or blocking
//! "Wait" RPC on the wire, and no per-kind snapshot RPC.

mod common;
mod execution;
mod gateway;
mod query;
mod task;
mod workflow;

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use spica_engine::{EngineBuilder, EntryPayload, LogStream, Storage};
use spica_logstream::RocksLogStream;
use spica_proto::v1::execution_service_server::ExecutionServiceServer;
use spica_proto::v1::query_server::QueryServer;
use spica_proto::v1::task_service_server::TaskServiceServer;
use spica_proto::v1::workflow_service_server::WorkflowServiceServer;
use spica_scheduler::{InMemoryScheduler, Scheduler};
use spica_storage::RocksStorage;

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
    // shared by the four service handles.
    let log: Box<dyn LogStream<EntryPayload>> =
        Box::new(RocksLogStream::<EntryPayload>::open(&log_dir)?);
    let storage: Box<dyn Storage> = Box::new(RocksStorage::open(&storage_dir)?);
    let scheduler: Arc<dyn Scheduler> = InMemoryScheduler::spawn();
    // The application layer: an `AckHook` observer injected into the engine correlates awaited commands'
    // outcomes to their callers; the `Gateway` wraps engine + ack to expose the blocking request/response
    // API (the entry point future auth / backpressure run through). Timer scheduling is gateway-owned:
    // `build_observer` composes the ack correlation with a hook that re-derives physical timer arms from
    // durable events, and returns the engine slot the booted engine fills in for the sink
    // (see `spica_server::gateway`).
    let ack = Arc::new(gateway::AckHook::new());
    let (hook, engine_slot) = gateway::build_observer(ack.clone(), scheduler);
    let engine = EngineBuilder::with_backends(log, storage)
        .with_hook(hook)
        .start()
        .await
        .context("start engine processor")?;
    let engine = Arc::new(engine);
    *engine_slot.lock().await = Some(Arc::downgrade(&engine));
    tracing::info!(listen = %args.listen, "spica-server listening");

    let gateway = gateway::Gateway::new(engine.clone(), ack);
    let svc = Svc { engine, gateway };
    tonic::transport::Server::builder()
        .add_service(WorkflowServiceServer::new(svc.clone()))
        .add_service(ExecutionServiceServer::new(svc.clone()))
        .add_service(TaskServiceServer::new(svc.clone()))
        .add_service(QueryServer::new(svc))
        .serve(args.listen.parse().context("parse listen address")?)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use spica_asl::StateMachine;
    use spica_engine::{EngineBuilder, EntryPayload, FlowName, LogStream, Storage};
    use spica_logstream::RocksLogStream;
    use spica_scheduler::{InMemoryScheduler, Scheduler};
    use spica_storage::RocksStorage;

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
        let ack = std::sync::Arc::new(super::gateway::AckHook::new());
        let (hook, engine_slot) = super::gateway::build_observer(ack.clone(), scheduler);
        let engine = std::sync::Arc::new(
            EngineBuilder::with_backends(log, storage)
                .with_hook(hook)
                .start()
                .await
                .unwrap(),
        );
        *engine_slot.lock().await = Some(std::sync::Arc::downgrade(&engine));
        let gateway = super::gateway::Gateway::new(engine.clone(), ack);
        let name = FlowName::new("test").unwrap();
        let definition = serde_json::to_string(&sm).unwrap();
        let flow_version = gateway.create_flow(name, &definition).await.unwrap();
        let execution_id = gateway
            .start_for_revision(
                spica_engine::PlainName::new("exec")
                    .expect("static literal is a valid segment")
                    .generated_from_key(ulid::Ulid::new().0 as u64),
                flow_version,
                Value::Null,
            )
            .await
            .unwrap();
        let result = engine.wait_for_execution(&execution_id).await.unwrap();
        assert_eq!(result.output.unwrap_or(Value::Null), Value::Null);

        // Drop the engine (releasing its DB handles) before removing the directories.
        drop(engine);
        let _ = std::fs::remove_dir_all(&log_dir);
        let _ = std::fs::remove_dir_all(&storage_dir);
    }

    /// The `create_flow` pre-checks now live in the engine: both a malformed definition and a
    /// duplicate name are rejected with `InvalidDefinition` before anything is appended to the log.
    #[tokio::test]
    async fn create_flow_rejects_malformed_and_duplicate() {
        let log_dir = temp_path("log2");
        let storage_dir = temp_path("storage2");
        std::fs::create_dir_all(&log_dir).unwrap();
        std::fs::create_dir_all(&storage_dir).unwrap();

        let log: Box<dyn LogStream<EntryPayload>> =
            Box::new(RocksLogStream::<EntryPayload>::open(&log_dir).unwrap());
        let storage: Box<dyn Storage> = Box::new(RocksStorage::open(&storage_dir).unwrap());
        let scheduler: std::sync::Arc<dyn Scheduler> = InMemoryScheduler::spawn();
        let ack = std::sync::Arc::new(super::gateway::AckHook::new());
        let (hook, engine_slot) = super::gateway::build_observer(ack.clone(), scheduler);
        let engine = std::sync::Arc::new(
            EngineBuilder::with_backends(log, storage)
                .with_hook(hook)
                .start()
                .await
                .unwrap(),
        );
        *engine_slot.lock().await = Some(std::sync::Arc::downgrade(&engine));
        let gateway = super::gateway::Gateway::new(engine.clone(), ack);

        let name = FlowName::new("probe").unwrap();
        let ok_def = r#"{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}"#;

        // Malformed: does not parse as a StateMachine → InvalidDefinition before append.
        let err = gateway
            .create_flow(name.clone(), "{ not json")
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            spica_engine::ExecutionError::Runtime(spica_engine::RuntimeError::InvalidDefinition(_))
        ));

        // Duplicate: the first create succeeds; the second rejects before append.
        gateway.create_flow(name.clone(), ok_def).await.unwrap();
        let err = gateway.create_flow(name, ok_def).await.unwrap_err();
        assert!(matches!(
            err,
            spica_engine::ExecutionError::Runtime(spica_engine::RuntimeError::InvalidDefinition(_))
        ));

        drop(engine);
        let _ = std::fs::remove_dir_all(&log_dir);
        let _ = std::fs::remove_dir_all(&storage_dir);
    }
}
