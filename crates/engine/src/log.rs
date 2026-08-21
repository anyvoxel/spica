//! Re-exports the engine's log seam: the payload-agnostic [`spica_logstream`] primitives plus the
//! engine's concrete [`Entry`](crate::entry::Entry) / [`EntryPayload`](crate::entry::EntryPayload).
//!
//! This module is a thin shim so engine internals (and the public re-export in `lib.rs`) can
//! continue to say `use crate::log::{Entry, LogStream, …}` unchanged — the actual append-only log
//! generic lives in the separate `spica-logstream` crate.

pub use crate::entry::{Entry, EntryPayload};
pub use spica_logstream::{InMemoryLogStream, LogStream, RocksLogStream, Timestamp};
