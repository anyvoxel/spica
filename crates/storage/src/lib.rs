//! `spica-storage` — the persistent projection backends for the `spica-engine` [`Storage`] trait.
//!
//! This crate holds the **implementations** of the CCES storage seam: the durable [`RocksStorage`]
//! (the production credit), the in-process [`InMemoryStorage`] (tests / synchronous runs), and the
//! canonical byte [`KeyBuilder`] / [`Scope`] encoding the durable store keys its rows by.
//!
//! The [`Storage`] *contract* — the trait plus the projection row types it names
//! ([`Execution`]/[`Activity`]/[`Task`]/[`Timer`]) — lives in `spica-engine`, the crate that consumes
//! it. This crate therefore **depends on** `spica-engine` rather than the other way around, which
//! keeps the dependency acyclic: `spica-storage → spica-engine`, with a binary (`spica-server`)
//! wiring the two together by constructing an implementation here and injecting it into the engine
//! as a `Box<dyn Storage>`. The engine never fabricates concrete storage itself (see
//! [`EngineBuilder::with_backends`](spica_engine::EngineBuilder::with_backends)) — that is the
//! assembly responsibility of whatever process boots it.

mod key;
mod memory;
mod rocks;

pub use key::{KeyBuilder, Scope};
pub use memory::InMemoryStorage;
pub use rocks::RocksStorage;

// Forward the contract this crate implements, so a caller that only injects a concrete backend can
// name the `Storage` trait (for the `Box<dyn Storage>` it hands to the engine) without importing
// `spica-engine` at that site. The engine re-exports the same trait at its root.
pub use spica_engine::Storage;
