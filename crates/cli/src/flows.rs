//! `spica flows <verb>` — definition versioning: create a new immutable flow version, or resolve a
//! name (+ ordinal version) to its concrete version `ObjectReference`. Both are thin `spica-client`
//! calls that print the resulting reference; the args and handlers live together here.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use spica_client::Client;

use crate::util::validate_flow_name;

/// `flows` subcommands.
#[derive(Subcommand)]
pub(crate) enum FlowsCmd {
    /// Persist a new flow version from an ASL definition under a required, unique name.
    Create(FlowsCreateArgs),
    /// Resolve a flow name (+ optional version) to its concrete version reference.
    Get(FlowsGetArgs),
}

#[derive(Args)]
pub(crate) struct FlowsCreateArgs {
    /// Path to the ASL state machine definition (JSON).
    #[arg(value_name = "DEFINITION")]
    pub(crate) definition: PathBuf,
    /// The immutable flow name ([A-Za-z0-9_]_, 1..=64) to create under. Must not already exist.
    #[arg(long)]
    pub(crate) name: String,
}

#[derive(Args)]
pub(crate) struct FlowsGetArgs {
    /// The flow name to resolve (must already have been created).
    #[arg(value_name = "FLOW_NAME")]
    pub(crate) flow_name: String,
    /// The ordinal version; 0 (default) = latest.
    #[arg(long, default_value_t = 0)]
    pub(crate) version: u32,
}

/// Create a new flow version and print its created version's ObjectReference.
pub(crate) async fn create(client: &Client, args: &FlowsCreateArgs) -> Result<()> {
    validate_flow_name(&args.name)?;
    let definition = fs::read(&args.definition)
        .with_context(|| format!("reading definition {}", args.definition.display()))?;
    let flow_version = client
        .create_flow(&args.name, &definition)
        .await
        .context("CreateFlow")?;
    println!("{}", flow_version);
    Ok(())
}

/// Resolve a flow name (+ optional ordinal version) to its concrete ObjectReference and print it.
pub(crate) async fn get(client: &Client, args: &FlowsGetArgs) -> Result<()> {
    validate_flow_name(&args.flow_name)?;
    let flow_version = client
        .resolve_flow_version(&args.flow_name, args.version)
        .await
        .context("ResolveFlowVersion")?;
    println!("{}", flow_version);
    Ok(())
}
