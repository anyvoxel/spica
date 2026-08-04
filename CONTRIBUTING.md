# Contributing

Thanks for contributing to Spica.

## Development workflow

This repository is a Rust workspace. Run commands from the repository root.

### Build

- `cargo build`
- `cargo build -p spica-asl`
- `cargo build -p spica`

### Test

- `cargo test`
- `cargo test -p spica-asl`
- `cargo test -p spica-asl <test-name-substring>`

Examples:
- `cargo test -p spica-asl test_state_machine_minimal`
- `cargo test -p spica-asl test_valid_resources_roundtrip`

### Format and lint

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`

## Repository structure

- `crates/asl` contains `spica-asl`, the main library crate
- `crates/spica` contains `spica`, a thin binary crate

Keep new parsing and modeling logic in the library crate unless the change is specifically about CLI behavior.

## Testing expectations

`spica-asl` relies on two levels of tests:

1. inline unit tests in the state/support modules
2. fixture-driven tests rooted in `crates/asl/tests/resources/`

The fixture corpus is a contract for parse behavior and round-trip behavior. If you change serde behavior, validation boundaries, or the supported ASL subset, review the fixture-driven tests in `crates/asl/src/lib.rs` and update fixtures or expectations deliberately.

## Pull requests

Before opening a PR, run:

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test`

When writing the PR description, mention:
- what changed
- which crate(s) were affected
- whether fixture expectations changed
- what commands you ran to verify the change
