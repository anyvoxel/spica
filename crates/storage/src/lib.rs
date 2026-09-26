//! `spica-storage` — the persistent projection backends for the [`Storage`] trait.
//!
//! This crate holds the **implementations** of the CCES storage seam: the durable [`RocksStorage`]
//! (the production credit), the in-process [`InMemoryStorage`] (tests / synchronous runs), and the
//! canonical byte [`KeyBuilder`] / [`Scope`] encoding the durable store keys its rows by.
//!
//! The [`Storage`] *contract* — the trait plus the projection row types it names
//! ([`Execution`]/[`Activity`]/[`Task`]/[`Timer`]) — lives in `spica-engine-types`, the leaf crate both
//! this crate and `spica-engine` implement or consume it from. Neither backend crate depends on the
//! other: a binary (`spica-server`) wires them together by constructing an implementation here and
//! injecting it into the engine as a `Box<dyn Storage>`. The engine never fabricates concrete storage
//! itself (`EngineBuilder::with_backends` in `spica-engine`) — that is the assembly responsibility of
//! whatever process boots it.

mod key;
mod memory;
mod rocks;

pub use key::{KeyBuilder, Kind, Scope};
pub use memory::InMemoryStorage;
pub use rocks::RocksStorage;

// Forward the contract this crate implements, so a caller that only injects a concrete backend can
// name the `Storage` trait (for the `Box<dyn Storage>` it hands to the engine) without importing
// `spica-engine-types` at that site. The engine re-exports the same trait, and it is the *same* trait.
pub use spica_engine_types::Storage;
