# AGENTS.md

Guidance for AI coding agents working in this repository.

# Mandatory requirements

1. After finish the work which changed source code, You **MUST** ensure **cargo test**、**cargo clippy** and **cargo fmt --check** successfully pass.
2. When a user asks you to implement a feature, refactor code, or fix an issue, you must first propose a specific technical solution and wait for their confirmation before proceeding with the implementation. The proposed solution should be concise and include a one-sentence summary, 3-5 key changes, a compatibility plan, and a testing and validation strategy.
3. When implementing code, you should include detailed comments and logging. For logging, ensure that critical paths in the system are appropriately logged (e.g., when entering a significant new if branch). As for comments, they should be applied to complex algorithms, important functions, or structs. The primary goal of comments is to explain why the code is implemented a certain way, rather than how it is implemented.
4. Use TODO comments to document features that are currently unimplemented or planned for future development.