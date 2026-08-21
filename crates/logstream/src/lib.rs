//! `spica-logstream` — the durable, ordered append log of Causal Command Event Sourcing.
//!
//! This crate is the **generic** low-level CCES seam: the append-only [`LogStream`] of [`Entry`]s,
//! along with its in-memory ([`InMemoryLogStream`]) and RocksDB-backed ([`RocksLogStream`])
//! implementations, plus the envelope types they share ([`Entry`], [`Timestamp`]) and the position
//! ids ([`EntryId`], [`StreamId`]).
//!
//! It is deliberately **agnostic to the entry payload**: [`Entry<P>`](Entry) and
//! [`LogStream<P>`](LogStream) are generic over `P`, which the embedding engine fills with its own
//! domain record (a `Command` | `Event` | `Reject` union). This keeps the log itself reusable —
//! e.g. a distributed, shared, partitioned log for a cluster — without dragging the engine's domain
//! types into it. Concrete instantiations appear in `spica-engine` (which type-aliases
//! `Entry = spica_logstream::Entry<EntryPayload>` and holds a `Box<dyn LogStream<EntryPayload>>`).

mod entry;
mod error;
mod id;
mod logstream;
mod memory;
mod rocks;
mod timestamp;

/// A single record's envelope + payload ([`crate::entry`]).
pub use self::entry::Entry;
/// The log's own error type ([`crate::error`]).
pub use self::error::LogError;
/// The append-only log abstraction + position/stream ids.
pub use self::id::{EntryId, StreamId};
pub use self::logstream::LogStream;
/// An in-process, in-memory [`LogStream`].
pub use self::memory::InMemoryLogStream;
/// A durable RocksDB-backed [`LogStream`].
pub use self::rocks::RocksLogStream;
/// A wall-clock timestamp (via [`crate::timestamp`]).
pub use self::timestamp::Timestamp;
