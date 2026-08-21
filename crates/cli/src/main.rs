//! The `spica` CLI — a **pure-remote** client for a `spica-server` workflow engine.
//!
//! The engine never runs in this process (it lives in `spica-server`, addressed by `--address`);
//! this binary only talks gRPC, as the wire contract prescribes. It is organized into
//! resource-scoped subcommands (kubectl-style): `flows <verb>` for definition versioning and
//! `executions <verb>` (alias `exec`) for running and observing executions. Each resource's args +
//! handlers live in its own module ([`flows`], [`executions`]); shared plumbing is in [`util`].
//!
//! ```text
//! spica [--address <endpoint>] [--pretty]
//!   flows
//!     create <DEFINITION> --name NAME            persist a new flow version -> FlowVersionId
//!     get <FLOW_NAME> [--version N]              resolve a name(+version) -> FlowVersionId
//!   executions (exec)
//!     start [--flow-version-id ID | --name NAME [--version N]] [INPUT]   start -> ExecutionId (at birth)
//!     stop <EXECUTION_ID>                          abort a running execution (non-blocking)
//!     get <EXECUTION_ID>                         one non-blocking status snapshot
//!     wait <EXECUTION_ID> [--poll-ms MS]         poll until terminal; exit 0 on success, 1 on failure
//! ```
//!
//! `flows create` persists a new immutable flow version (the name must not already exist).
//! `executions start` returns the id at **birth** (non-blocking); terminal state is observed by
//! `executions get` (one read) or `executions wait` (poll to settlement). `executions stop` issues an
//! abort (non-blocking) — confirm the run actually settles as `TERMINATED` with `executions wait`.
//! Completed output goes to stdout (compact JSON, or pretty with `--pretty`); a terminal failure
//! prints the ASL error name + output to stderr.

mod executions;
mod flows;
mod util;

use std::process::ExitCode;

use anyhow::Result;
use clap::{Parser, Subcommand};
use spica_proto::v1::execution_client::ExecutionClient;
use spica_proto::v1::workflow_client::WorkflowClient;

use crate::executions::ExecutionsCmd;
use crate::flows::FlowsCmd;
use crate::util::channel;

/// Global CLI shape: one `--address` (endpoint) + `--pretty`, then a resource-scoped subcommand.
#[derive(Parser)]
#[command(name = "spica", version, about = "Spica workflow engine remote client")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// The spica-server endpoint (gRPC target, e.g. `http://127.0.0.1:50051` or `127.0.0.1:50051`).
    #[arg(long, default_value = "http://127.0.0.1:50051", global = true)]
    address: String,

    /// Pretty-print the output JSON.
    #[arg(short = 'p', long, global = true)]
    pretty: bool,
}

/// The top-level command: which resource the invocation targets.
#[derive(Subcommand)]
enum Command {
    /// Manage flow versions.
    #[command(subcommand)]
    Flows(FlowsCmd),
    /// Manage and observe executions.
    #[command(subcommand, alias = "exec")]
    Executions(ExecutionsCmd),
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match dispatch(&cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e:#}");
            ExitCode::FAILURE
        }
    }
}

/// Route a parsed subcommand to its handler. One gRPC channel is dialed up front and cloned per
/// client, so whichever service a subcommand needs already shares the connection.
async fn dispatch(cli: &Cli) -> Result<()> {
    let channel = channel(&cli.address).await?;
    match &cli.command {
        Command::Flows(cmd) => {
            let mut workflow = WorkflowClient::new(channel);
            match cmd {
                FlowsCmd::Create(a) => flows::create(&mut workflow, a).await,
                FlowsCmd::Get(a) => flows::get(&mut workflow, a).await,
            }
        }
        Command::Executions(cmd) => {
            let mut execution = ExecutionClient::new(channel);
            match cmd {
                ExecutionsCmd::Start(a) => executions::start(&mut execution, a).await,
                ExecutionsCmd::Stop(a) => executions::stop(&mut execution, a).await,
                ExecutionsCmd::Get(a) => executions::get(&mut execution, cli.pretty, a).await,
                ExecutionsCmd::Wait(a) => executions::wait(&mut execution, cli.pretty, a).await,
            }
        }
    }
}
