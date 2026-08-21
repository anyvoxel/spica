//! `spica executions <verb>` (alias `exec`) — running and observing executions: start (non-blocking,
//! prints the id at birth), stop (issue an abort), get (one snapshot), wait (poll to settlement).
//! The args and handlers live together here; output/validation helpers come from `crate::util`.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use spica_proto::v1::{
    ExecutionState, GetExecutionRequest, StartExecutionRequest, StopExecutionRequest,
    execution_client::ExecutionClient, start_execution_request::Target,
};
use tonic::transport::Channel;

use crate::util::{print_json, printable_error, read_input, state_label, validate_flow_name};

/// `executions` subcommands.
#[derive(Subcommand)]
pub(crate) enum ExecutionsCmd {
    /// Start an execution; prints its id at birth (non-blocking).
    Start(ExecutionsStartArgs),
    /// Abort a running execution; non-blocking — settle is confirmed via `executions wait`.
    Stop(ExecutionsStopArgs),
    /// Read a single point-in-time status snapshot (non-blocking).
    Get(ExecutionsGetArgs),
    /// Poll an execution until it settles; print output (exit 0) or error (exit 1).
    Wait(ExecutionsWaitArgs),
}

#[derive(Args)]
#[command(group(
    clap::ArgGroup::new("target")
        .required(true)
        .multiple(false)
        .args(["flow_version_id", "name"])
))]
pub(crate) struct ExecutionsStartArgs {
    /// Address a concrete revision by its FlowVersionId (returned by `flows create`).
    #[arg(long)]
    pub(crate) flow_version_id: Option<String>,
    /// Address a revision by flow name; use --version for the ordinal (0 = latest).
    #[arg(long)]
    pub(crate) name: Option<String>,
    /// The ordinal version under --name; 0 = latest. Ignored with --flow-version-id.
    #[arg(long, default_value_t = 0)]
    pub(crate) version: u32,
    /// Path to the execution input (JSON); defaults to null if omitted.
    pub(crate) input: Option<PathBuf>,
}

#[derive(Args)]
pub(crate) struct ExecutionsStopArgs {
    /// The ExecutionId returned by `executions start` (the run to abort).
    #[arg(value_name = "EXECUTION_ID")]
    pub(crate) execution_id: String,
}

#[derive(Args)]
pub(crate) struct ExecutionsGetArgs {
    /// The ExecutionId returned by `executions start`.
    #[arg(value_name = "EXECUTION_ID")]
    pub(crate) execution_id: String,
}

#[derive(Args)]
pub(crate) struct ExecutionsWaitArgs {
    /// The ExecutionId returned by `executions start`.
    #[arg(value_name = "EXECUTION_ID")]
    pub(crate) execution_id: String,
    /// Poll interval in ms between GetExecution calls while the run is in flight.
    #[arg(long, default_value_t = 100)]
    pub(crate) poll_ms: u64,
}

/// Start an execution (addressed by explicit revision or name+version) and print its id at birth.
pub(crate) async fn start(
    execution: &mut ExecutionClient<Channel>,
    args: &ExecutionsStartArgs,
) -> Result<()> {
    // Exactly one of the group's fields is present (clap's ArgGroup enforces it).
    let target_builder = match (&args.flow_version_id, &args.name) {
        (Some(fvid), None) => Target::FlowVersionId(fvid.clone()),
        (None, Some(name)) => {
            validate_flow_name(name)?;
            Target::FlowName(name.clone())
        }
        // Unreachable: the group requires exactly one of the two.
        _ => unreachable!("clap ArgGroup requires exactly one of --flow-version-id / --name"),
    };
    let input = read_input(&args.input)?;
    let resp = execution
        .start_execution(StartExecutionRequest {
            target: Some(target_builder),
            version: args.version,
            input,
        })
        .await
        .context("StartExecution")?
        .into_inner();
    println!("{}", resp.execution_id);
    Ok(())
}

/// Issue an abort for an execution (non-blocking). Prints the echoed id, then points the user at
/// `executions wait` to confirm the run actually settles as TERMINATED.
pub(crate) async fn stop(
    execution: &mut ExecutionClient<Channel>,
    args: &ExecutionsStopArgs,
) -> Result<()> {
    let resp = execution
        .stop_execution(StopExecutionRequest {
            execution_id: args.execution_id.clone(),
        })
        .await
        .context("StopExecution")?
        .into_inner();
    println!(
        "termination requested for {}; confirm with 'spica executions wait {}'",
        resp.execution_id, resp.execution_id
    );
    Ok(())
}

/// Read a single point-in-time status snapshot and print its state (+ output/error if settled).
pub(crate) async fn get(
    execution: &mut ExecutionClient<Channel>,
    pretty: bool,
    args: &ExecutionsGetArgs,
) -> Result<()> {
    let snap = execution
        .get_execution(GetExecutionRequest {
            execution_id: args.execution_id.clone(),
        })
        .await
        .context("GetExecution")?
        .into_inner();
    let state = ExecutionState::try_from(snap.state).unwrap_or(ExecutionState::Unspecified);
    println!("state: {}", state_label(state));
    match state {
        ExecutionState::Completed => {
            let output: serde_json::Value =
                serde_json::from_slice(&snap.output).context("parsing completed output")?;
            print_json(&output, pretty);
        }
        ExecutionState::Terminated => {
            eprintln!(
                "error: {} {}",
                snap.error_name,
                printable_error(&snap.error_output)
            );
        }
        _ => {} // ACTIVE / NOT_FOUND: nothing further to show on a one-shot read.
    }
    Ok(())
}

/// Poll an execution until it settles: print output (exit 0) or surface the failure (exit 1).
pub(crate) async fn wait(
    execution: &mut ExecutionClient<Channel>,
    pretty: bool,
    args: &ExecutionsWaitArgs,
) -> Result<()> {
    loop {
        let snap = execution
            .get_execution(GetExecutionRequest {
                execution_id: args.execution_id.clone(),
            })
            .await
            .context("GetExecution")?
            .into_inner();
        match ExecutionState::try_from(snap.state) {
            Ok(ExecutionState::Completed) => {
                let output: serde_json::Value =
                    serde_json::from_slice(&snap.output).context("parsing completed output")?;
                print_json(&output, pretty);
                return Ok(());
            }
            Ok(ExecutionState::Terminated) => {
                bail!(
                    "execution terminated: {} {}",
                    snap.error_name,
                    printable_error(&snap.error_output)
                );
            }
            Ok(ExecutionState::NotFound) => {
                bail!(
                    "execution {}: no projection (never created or GC'd)",
                    args.execution_id
                );
            }
            // ACTIVE (or UNSPECIFIED) — still in flight; poll again after the interval.
            _ => tokio::time::sleep(Duration::from_millis(args.poll_ms)).await,
        }
    }
}
