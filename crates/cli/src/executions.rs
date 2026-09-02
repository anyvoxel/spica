//! `spica executions <verb>` (alias `exec`) — running and observing executions: start (non-blocking,
//! prints the name at birth), stop (issue an abort), get (one snapshot), wait (poll to settlement).
//! The args and handlers live together here; output/validation helpers come from `crate::util`.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use spica_client::{Client, ExecutionState, ObjectReference, StartExecution, Target};

use crate::util::{print_json, printable_error, read_input, state_label, validate_flow_name};

/// `executions` subcommands.
#[derive(Subcommand)]
pub(crate) enum ExecutionsCmd {
    /// Start an execution; prints its name at birth (non-blocking).
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
        .args(["flow_version", "name"])
))]
pub(crate) struct ExecutionsStartArgs {
    /// Address a concrete revision by its ObjectReference (`kind/name/uid`, as printed by
    /// `flows create`).
    #[arg(long)]
    pub(crate) flow_version: Option<String>,
    /// Address a revision by flow name; use --version for the ordinal (0 = latest).
    #[arg(long)]
    pub(crate) name: Option<String>,
    /// The ordinal version under --name; 0 = latest. Ignored with --flow-version.
    #[arg(long, default_value_t = 0)]
    pub(crate) version: u32,
    /// The execution's user-supplied name (required; charset `[A-Za-z0-9_]`, no `-`).
    #[arg(long)]
    pub(crate) execution_name: String,
    /// Path to the execution input (JSON); defaults to null if omitted.
    pub(crate) input: Option<PathBuf>,
}

#[derive(Args)]
pub(crate) struct ExecutionsStopArgs {
    /// The execution's user-supplied name (required) — the per-scope-unique run to abort.
    #[arg(long)]
    pub(crate) execution_name: String,
    /// Optional incarnation guard: only abort the named execution whose uid equals this.
    #[arg(long)]
    pub(crate) execution_uid: Option<String>,
}

#[derive(Args)]
pub(crate) struct ExecutionsGetArgs {
    /// The execution's user-supplied name (required) — the per-scope-unique run to snapshot.
    #[arg(long)]
    pub(crate) execution_name: String,
}

#[derive(Args)]
pub(crate) struct ExecutionsWaitArgs {
    /// The execution's user-supplied name (required) — the run to poll to settlement.
    #[arg(long)]
    pub(crate) execution_name: String,
    /// Poll interval in ms between GetExecution calls while the run is in flight.
    #[arg(long, default_value_t = 100)]
    pub(crate) poll_ms: u64,
}

/// Start an execution (addressed by explicit revision or name+version) and print its name at birth.
pub(crate) async fn start(client: &Client, args: &ExecutionsStartArgs) -> Result<()> {
    // Exactly one of the group's fields is present (clap's ArgGroup enforces it).
    let target = match (&args.flow_version, &args.name) {
        (Some(fv), None) => Target::ByVersion(
            fv.parse::<ObjectReference>().map_err(|e| {
                anyhow::anyhow!("malformed --flow-version (expected kind/name/uid, as printed by `flows create`): {e}")
            })?,
        ),
        (None, Some(name)) => {
            validate_flow_name(name)?;
            Target::ByName {
                name: name.clone(),
                version: args.version,
            }
        }
        // Unreachable: the group requires exactly one of the two.
        _ => unreachable!("clap ArgGroup requires exactly one of --flow-version / --name"),
    };
    let input = read_input(&args.input)?;
    let name = client
        .start_execution(StartExecution {
            target,
            input,
            name: args.execution_name.clone(),
        })
        .await
        .context("StartExecution")?;
    println!("{name}");
    Ok(())
}

/// Issue an abort for an execution by `name` (optionally guarded by `uid`) — non-blocking. Prints the
/// echoed name, then points the user at `executions wait` to confirm the run actually settles as
/// TERMINATED.
pub(crate) async fn stop(client: &Client, args: &ExecutionsStopArgs) -> Result<()> {
    let name = client
        .stop_execution(&args.execution_name, args.execution_uid.as_deref())
        .await
        .context("StopExecution")?;
    println!("termination requested for execution {name}");
    Ok(())
}

/// Read a single point-in-time status snapshot and print its state (+ output/error if settled).
pub(crate) async fn get(client: &Client, pretty: bool, args: &ExecutionsGetArgs) -> Result<()> {
    let snap = client
        .get_execution(&args.execution_name)
        .await
        .context("GetExecution")?;
    println!("state: {}", state_label(snap.state));
    match snap.state {
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
pub(crate) async fn wait(client: &Client, pretty: bool, args: &ExecutionsWaitArgs) -> Result<()> {
    loop {
        let snap = client
            .get_execution(&args.execution_name)
            .await
            .context("GetExecution")?;
        match snap.state {
            ExecutionState::Completed => {
                let output: serde_json::Value =
                    serde_json::from_slice(&snap.output).context("parsing completed output")?;
                print_json(&output, pretty);
                return Ok(());
            }
            ExecutionState::Terminated => {
                bail!(
                    "execution terminated: {} {}",
                    snap.error_name,
                    printable_error(&snap.error_output)
                );
            }
            ExecutionState::NotFound => {
                bail!(
                    "execution {}: no projection (never created or GC'd)",
                    args.execution_name
                );
            }
            // Still in flight; poll again after the interval.
            ExecutionState::Active => tokio::time::sleep(Duration::from_millis(args.poll_ms)).await,
        }
    }
}
