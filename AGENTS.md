# AGENTS.md

Guidance for AI coding agents working in this repository.

# Mandatory requirements

1. After finish the work which changed source code, You **MUST** ensure **cargo test**、**cargo clippy** and **cargo fmt --check** successfully pass.
2. When a user asks you to implement a feature, refactor code, or fix an issue, you must first propose a specific technical solution and wait for their confirmation before proceeding with the implementation. The proposed solution should be concise and include a one-sentence summary, 3-5 key changes, a compatibility plan, and a testing and validation strategy.
3. Implement logging strategically to ensure critical system paths are fully observable at runtime, specifically by recording entries when significant conditional branches are executed, state transitions occur, or errors are caught. Logs should provide sufficient contextual information for effective debugging and tracing without generating noise from trivial operations, thereby maintaining a clear signal-to-noise ratio that supports production diagnostics rather than merely documenting code execution flow.
4. Restrict comments exclusively to complex algorithms, pivotal functions, and essential data structures where they explain the underlying design rationale or non-obvious trade-offs behind the implementation, strictly avoiding any description of what the code literally does or historical justifications for removed approaches. Each comment must be concise and focused solely on the "why" behind current decisions, never exceeding 120 words, and should never serve as a changelog or an explanation of discarded alternatives; if deeper context is needed, it belongs in external documentation rather than inline code annotations.
5. Use TODO comments to document features that are currently unimplemented or planned for future development.