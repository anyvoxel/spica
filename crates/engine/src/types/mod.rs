//! Re-export shim: the pure type vocabulary now lives in `spica-engine-types` (see that crate's docs
//! for why it is a crate of its own). The module path is preserved so the engine's internal
//! `crate::types::…` imports, and the public `spica_engine::types::…` / re-exported paths, keep
//! resolving unchanged.

pub use spica_engine_types::types::*;
