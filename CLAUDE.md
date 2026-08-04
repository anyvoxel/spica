# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common commands

This repository is a small Rust workspace.

- Build the whole workspace:
  - `cargo build`
- Build a single crate:
  - `cargo build -p spica-asl`
  - `cargo build -p spica`
- Run all tests:
  - `cargo test`
- Run tests for the ASL library crate:
  - `cargo test -p spica-asl`
- Run a single test by name filter:
  - `cargo test -p spica-asl test_state_machine_minimal`
  - `cargo test -p spica-asl test_valid_resources_roundtrip`
- Run clippy across the workspace:
  - `cargo clippy --workspace --all-targets`
- Run the binary crate:
  - `cargo run -p spica`
- Build docs:
  - `cargo doc --workspace`

## Architecture overview

### Workspace shape

The root `Cargo.toml` defines a two-crate workspace:
- `crates/asl` → `spica-asl`, the main library crate
- `crates/spica` → `spica`, a binary crate that currently just depends on the library

`spica-asl` is where the real logic lives today. `spica` is currently a thin placeholder binary (`crates/spica/src/main.rs`).

### Core library model

`crates/asl/src/lib.rs` is the entry point for the ASL model. It:
- defines the top-level `StateMachine` type
- defines the tagged `State` enum
- re-exports the per-state modules and shared value types

The library models a **JSONata-only subset** of Amazon States Language. The crate-level docs in `crates/asl/src/lib.rs` are the best source for the intended scope: JSONata-based output/arguments/assign/condition handling is supported, while JSONPath-specific fields and broader Step Functions features are intentionally omitted.

### Module structure inside `spica-asl`

The library is organized primarily by ASL state type and supporting structures:
- state modules such as `task`, `choice`, `map`, `parallel`, `pass`, `wait`, `fail`, `succeed`
- supporting modules such as `branch`, `catch`, `retry`, `item_processor`, `assign`
- shared parsing/value helpers in `utils`

A useful mental model is:
- `lib.rs` defines the top-level machine/container types
- each state module owns the serde model for one ASL state category
- supporting modules hold reusable nested types referenced by multiple states
- `utils` contains cross-cutting parsing/value helpers such as `JsonataExpr`

### Serialization/deserialization style

This codebase is heavily serde-driven. Most of the library’s behavior is encoded in Rust data models plus custom `Deserialize` implementations or field-level `deserialize_with` helpers.

When changing parsing behavior, check both:
- the local module tests next to the type definitions
- the crate-level fixture tests in `crates/asl/src/lib.rs`

### Test strategy

Testing in `spica-asl` has two layers:

1. **Inline unit tests in each module**
   - These exercise the serde behavior and round-trip expectations for specific state types.

2. **Fixture-based corpus tests**
   - `crates/asl/tests/resources/valid/`
   - `crates/asl/tests/resources/invalid/`
   - The README for this corpus is at `crates/asl/tests/resources/README.md`.

The crate-level tests in `crates/asl/src/lib.rs` treat the fixture corpus as a contract:
- valid resources must parse and round-trip
- invalid resources are partitioned between definitions that must be serde-rejected and definitions that are intentionally still parseable under the current lenient model

If you change field validation or the modeled subset, update these fixture expectations carefully.

## Repository-specific notes

- The top-level `README.md` is minimal; rely more on the Rust source docs and tests than on repository docs.
- There are currently no repository-level GitHub workflows, Cursor rules, or Copilot instruction files to mirror here.
- Local Claude permissions in `.claude/settings.local.json` allow common Cargo commands, which matches the expected development workflow in this repo.
