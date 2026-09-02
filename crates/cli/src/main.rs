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
//!     create <DEFINITION> --name NAME            persist a new flow version -> ObjectReference
//!     get <FLOW_NAME> [--version N]              resolve a name(+version) -> ObjectReference
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

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use spica_client::Client;

use crate::executions::ExecutionsCmd;
use crate::flows::FlowsCmd;

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

/// Route a parsed subcommand to its handler. One `spica-client` dials the endpoint up front and
/// shares a single connection across every service.
async fn dispatch(cli: &Cli) -> Result<()> {
    let client = Client::connect(&cli.address)
        .await
        .context("connecting to spica-server (is it running?)")?;
    match &cli.command {
        Command::Flows(cmd) => match cmd {
            FlowsCmd::Create(a) => flows::create(&client, a).await,
            FlowsCmd::Get(a) => flows::get(&client, a).await,
        },
        Command::Executions(cmd) => match cmd {
            ExecutionsCmd::Start(a) => executions::start(&client, a).await,
            ExecutionsCmd::Stop(a) => executions::stop(&client, a).await,
            ExecutionsCmd::Get(a) => executions::get(&client, cli.pretty, a).await,
            ExecutionsCmd::Wait(a) => executions::wait(&client, cli.pretty, a).await,
        },
    }
}
