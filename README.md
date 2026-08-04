# spica

Spica is a small Rust workspace centered on `spica-asl`, a library crate that models a JSONata-oriented subset of Amazon States Language (ASL). The repository also contains a thin `spica` binary crate that currently serves as a placeholder executable.

## Workspace layout

- `crates/asl` — `spica-asl`, the main library crate
- `crates/spica` — `spica`, a binary crate that depends on `spica-asl`

The library is the primary implementation surface today. It defines typed state-machine models for ASL states such as `Task`, `Choice`, `Map`, `Parallel`, `Pass`, `Wait`, `Fail`, and `Succeed`, along with supporting types like retries, catches, branches, and item processors.

## Scope of the ASL model

`spica-asl` intentionally models a **JSONata-only learning subset** of ASL. The crate-level documentation in `crates/asl/src/lib.rs` is the canonical statement of scope.

In particular:
- JSONata-based `Output`, `Arguments`, `Assign`, and `Condition` handling is modeled
- JSONPath-specific fields and broader Step Functions surface area are intentionally omitted
- the serde model is intentionally lenient in some places, so fixture-based tests are an important contract

## Common commands

Run these commands from the repository root.

### Build

- Build the whole workspace:
  - `cargo build`
- Build the ASL library only:
  - `cargo build -p spica-asl`
- Build the binary crate only:
  - `cargo build -p spica`

### Test

- Run the whole workspace test suite:
  - `cargo test`
- Run only the ASL library tests:
  - `cargo test -p spica-asl`
- Run a single test by name filter:
  - `cargo test -p spica-asl test_state_machine_minimal`
  - `cargo test -p spica-asl test_valid_resources_roundtrip`

### Lint and format

- Check formatting:
  - `cargo fmt --all --check`
- Run clippy across the workspace:
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`

### Run

- Run the binary crate:
  - `cargo run -p spica`

### Docs

- Build documentation:
  - `cargo doc --workspace`

## Test structure

The most important test coverage currently lives in `spica-asl`.

### Inline module tests

State modules and support modules contain inline unit tests that exercise serde behavior and round-trip expectations.

### Fixture corpus

The library also uses a fixture corpus under:
- `crates/asl/tests/resources/valid`
- `crates/asl/tests/resources/invalid`

The crate-level tests in `crates/asl/src/lib.rs` use this corpus to enforce parse and round-trip behavior. If you change the modeled subset or field validation rules, check those tests and fixture expectations carefully.

## Project status

This repository is still early-stage at the project level:
- `spica-asl` contains the meaningful implementation work
- `spica` is still a placeholder binary
- repository automation and contributor guidance are intentionally lightweight and focused on the current Rust/Cargo workflow

For contributor workflow details, see `CONTRIBUTING.md`. For Claude Code guidance, see `CLAUDE.md`. For broader AI-agent guidance, see `AGENTS.md`.
