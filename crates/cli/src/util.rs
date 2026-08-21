//! Shared client-side plumbing for the `spica` CLI subcommands: connection handling, input reading,
//! and output / validation helpers. Kept here so the resource modules (`flows`, `executions`) and the
//! root `dispatch` don't duplicate it.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use spica_proto::v1::ExecutionState;
use tonic::transport::{Channel, Endpoint};

/// Dial a gRPC channel to the server; created once per invocation and cloned per service client.
pub(crate) async fn channel(address: &str) -> Result<Channel> {
    let endpoint = normalize_endpoint(address);
    Endpoint::from_shared(endpoint)
        .context("parsing --address")?
        .connect()
        .await
        .context("connecting to spica-server (is it running?)")
}

/// Read the execution input file into raw bytes; absent input defaults to JSON `null`.
pub(crate) fn read_input(path: &Option<PathBuf>) -> Result<Vec<u8>> {
    match path {
        Some(p) => Ok(fs::read(p).with_context(|| format!("reading input {}", p.display()))?),
        None => Ok(b"null".to_vec()),
    }
}

/// Accept either a bare host:port or a full `http://…` target; tonic endpoints require a scheme.
pub(crate) fn normalize_endpoint(address: &str) -> String {
    if address.contains("://") {
        address.to_string()
    } else {
        format!("http://{address}")
    }
}

/// The server FlowName charset — `[A-Za-z0-9_]`, 1..=64 — validated client-side for a friendlier
/// error than round-tripping a rejected request (the server re-checks authoritatively anyway).
pub(crate) fn validate_flow_name(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if !valid {
        bail!("invalid flow name {name:?}: must match [A-Za-z0-9_] (1..=64)");
    }
    Ok(())
}

/// Human-readable label for an execution state (for `executions get`'s status line).
pub(crate) fn state_label(state: ExecutionState) -> &'static str {
    match state {
        ExecutionState::Completed => "COMPLETED",
        ExecutionState::Terminated => "TERMINATED",
        ExecutionState::NotFound => "NOT_FOUND",
        _ => "ACTIVE",
    }
}

/// Render the error output bytes for stderr, trimming trailing whitespace when present.
pub(crate) fn printable_error(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        String::new()
    } else {
        String::from_utf8_lossy(bytes).trim().to_string()
    }
}

/// Write a JSON value to stdout (compact or pretty) followed by a newline.
pub(crate) fn print_json(value: &serde_json::Value, pretty: bool) {
    if pretty {
        serde_json::to_writer_pretty(std::io::stdout(), value).ok();
    } else {
        serde_json::to_writer(std::io::stdout(), value).ok();
    }
    println!();
}
