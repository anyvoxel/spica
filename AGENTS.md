# AGENTS.md

Guidance for AI coding agents working in this repository.

## Repository shape

This is a Rust workspace with two crates:
- `crates/asl` → `spica-asl`, the main library crate
- `crates/spica` → `spica`, a thin binary crate

The meaningful implementation work is in `spica-asl`. Prefer changing the library crate over the binary crate unless the task is explicitly about the executable.

## Modeling scope

`spica-asl` models a JSONata-only subset of Amazon States Language. Use the crate-level docs in `crates/asl/src/lib.rs` as the source of truth for the intended scope. Do not add JSONPath-oriented or broader Step Functions features without checking whether the repository is intentionally omitting them.

## Development commands

Use the Cargo workflow from the repository root:
- `cargo build`
- `cargo test`
- `cargo test -p spica-asl <test-name-substring>`
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo run -p spica`

## Testing model

When changing serde behavior or validation:
- inspect the inline unit tests next to the affected type definitions
- inspect the fixture-driven tests in `crates/asl/src/lib.rs`
- treat `crates/asl/tests/resources/valid` and `crates/asl/tests/resources/invalid` as part of the public behavior contract

## Code organization guidance

- `crates/asl/src/lib.rs` defines top-level machine/container types and re-exports submodules
- state modules (`task`, `choice`, `map`, `parallel`, `pass`, `wait`, `fail`, `succeed`) own the serde model for each state category
- support modules (`assign`, `branch`, `catch`, `retry`, `item_processor`, `utils`) hold reusable nested types and parsing helpers

## Documentation guidance

Do not describe `crates/spica/src/main.rs` as a real CLI unless the code has been implemented to support that claim. The current binary crate is only a placeholder.
