//! Re-export shim: the storage contract (the three traits, their projection row types, and the
//! thread flow-version resolver) now lives in `spica-engine-types` — see that crate's docs for why the
//! contract is a crate of its own. The module path is preserved so the engine's internal
//! `crate::storage::…` imports and the public `spica_engine::storage::…` paths keep resolving.

pub use spica_engine_types::storage::*;
