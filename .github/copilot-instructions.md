# Copilot instructions for this repository

This repository is a Rust workspace with two crates:
- `crates/asl` (`spica-asl`) is the main library crate
- `crates/spica` (`spica`) is a thin binary crate and is currently a placeholder

Prefer putting real logic in `spica-asl` unless a task is specifically about the executable.

`spica-asl` models a JSONata-only subset of Amazon States Language. Use `crates/asl/src/lib.rs` as the source of truth for scope. Avoid introducing JSONPath-oriented or broader Step Functions surface area unless the task explicitly calls for expanding the modeled subset.

Before considering a change complete, run the standard root-level workflow when relevant:
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test`

If parsing or validation behavior changes, review both inline module tests and the fixture corpus under `crates/asl/tests/resources/`.
