//! Protobuf/gRPC wire contract for the spica workflow server.
//!
//! This crate is the single source of the `.proto` → Rust mapping: it compiles `proto/spica.proto`
//! via `tonic-build`/`prost-build` at build time and re-exports the generated code. Both the server
//! (`crates/server`, implements the service traits) and the CLI client (`crates/cli`, builds
//! clients against the generated stubs) depend on this crate so the two ends of the wire always see
//! the same compiled contract.

#![allow(clippy::derive_partial_eq_without_eq)]
// The prost+tonic build output is generated, not hand-written; keep clippy from flagging its style.
#![allow(clippy::all)]

/// The generated `spica.v1` package content (prost messages + tonic service/client/server traits).
/// prost-build emits the package's modules flat, so we wrap them in the versioned `v1` namespace.
pub mod v1 {
    tonic::include_proto!("spica.v1");
}
