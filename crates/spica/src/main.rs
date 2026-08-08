//! The `spica` CLI: runs an ASL state machine definition against an input and prints the output.
//!
//! Usage: `spica <DEFINITION.json> [INPUT.json]`
//!
//! - `DEFINITION` — path to the ASL state machine (JSON).
//! - `INPUT` — path to the execution input (JSON); defaults to `null`.
//!
//! The execution output is printed to stdout (compact JSON, or pretty with `--pretty`). A
//! detailed execution trace is emitted to stderr via `tracing`; the level defaults to `info` and
//! can be overridden with `RUST_LOG` (e.g. `RUST_LOG=off` to silence, `RUST_LOG=debug` for more).
//!
//! Exit code: `0` on success, `1` on failure.

use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::Parser;
use serde_json::Value;
use spica_engine::{Engine, ExecutionError, StateMachine};
use tracing_subscriber::EnvFilter;

/// Execute an ASL state machine against an input.
#[derive(Parser)]
#[command(name = "spica", version, about)]
struct Cli {
    /// Path to the ASL state machine definition (JSON).
    definition: PathBuf,

    /// Path to the execution input (JSON). Defaults to null if omitted.
    input: Option<PathBuf>,

    /// Pretty-print the output JSON.
    #[arg(short = 'p', long)]
    pretty: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    init_tracing();

    match run(&cli).await {
        Ok(output) => {
            print_json(&output, cli.pretty);
            ExitCode::SUCCESS
        }
        Err(e) => {
            // Execution failures carry an ASL error name + output; surface both.
            if let Some(exec_err) = e.downcast_ref::<ExecutionError>() {
                eprintln!("execution failed: {exec_err}");
                eprintln!("error name: {}", exec_err.error_name());
                if let Some(output) = exec_err.error_output() {
                    eprint!("error output: ");
                    print_json(&output, cli.pretty);
                }
            } else {
                eprintln!("{e:#}");
            }
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: &Cli) -> Result<Value> {
    let definition = fs::read_to_string(&cli.definition)
        .with_context(|| format!("reading definition {}", cli.definition.display()))?;
    let state_machine: StateMachine = serde_json::from_str(&definition)
        .with_context(|| format!("parsing definition {}", cli.definition.display()))?;

    let input = match &cli.input {
        Some(path) => {
            let text = fs::read_to_string(path)
                .with_context(|| format!("reading input {}", path.display()))?;
            serde_json::from_str(&text)
                .with_context(|| format!("parsing input {}", path.display()))?
        }
        None => Value::Null,
    };

    Engine::start(state_machine, input)
        .await
        .map(|result| result.output)
        .map_err(Into::into)
}

/// Initialize `tracing` to stderr. Defaults to `spica_engine=info` (the execution trace); `RUST_LOG`
/// overrides (e.g. `off`, `debug`).
fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("spica_engine=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}

fn print_json(value: &Value, pretty: bool) {
    if pretty {
        serde_json::to_writer_pretty(std::io::stdout(), value).ok();
    } else {
        serde_json::to_writer(std::io::stdout(), value).ok();
    }
    println!();
}
