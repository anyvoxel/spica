//! Re-exports the engine's log seam: the payload-agnostic [`spica_logstream`] primitives plus the
//! engine's concrete [`Entry`](crate::types::entry::Entry) / [`EntryPayload`](crate::types::entry::EntryPayload).
//!
//! This module is a thin shim so engine internals (and the public re-export in `lib.rs`) can
//! continue to say `use crate::log::{Entry, LogStream, …}` unchanged — the actual append-only log
//! generic lives in the separate `spica-logstream` crate.

use std::pin::Pin;

use tokio_stream::Stream;

pub use crate::types::entry::{Entry, EntryPayload};
use crate::types::id::{EntryId, StreamId};
pub use spica_logstream::{InMemoryLogStream, LogStream, RocksLogStream, Timestamp};

/// A [`LogStream`] adapter that closes every atomic append with a [`Noop`](EntryPayload::Noop)
/// unless it is already Noop-terminated.
///
/// This is a **defensive guard**, not the causal-batch terminator. A Command's real batch
/// terminator — the one carrying the producing Command's position as `cause_id` and the one the
/// leader advances its resume watermark to — is still appended by the StreamProcessor when it writes
/// the batch. The wrapper only guarantees the weaker invariant that *no append is left
/// unterminated*: a bare Command appended without a follow-up batch in the same write (e.g. worker /
/// gateway-initiated commands via `append_command`, `cancel_execution`) gains an inert trailing
/// `Noop` (`cause_id: None`). Such a `Noop` terminates nothing and never advances the watermark;
/// readers treat it as a lone no-op marker. The cost — a spurious `Noop` per bare Command — is
/// accepted so a forgotten terminator can never quietly leave an append unclosed.
// Inner `Box<dyn LogStream>` carries no `Debug`, so the wrapper isn't derived; this name-only impl
// keeps it printable for `debug!` traces.
pub struct NoopTerminatedLogStream {
    inner: Box<dyn LogStream<EntryPayload>>,
}
impl std::fmt::Debug for NoopTerminatedLogStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NoopTerminatedLogStream")
    }
}

impl NoopTerminatedLogStream {
    pub fn new(inner: Box<dyn LogStream<EntryPayload>>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl LogStream<EntryPayload> for NoopTerminatedLogStream {
    fn stream_id(&self) -> StreamId {
        self.inner.stream_id()
    }

    async fn append(&self, entries: Vec<Entry>) -> Result<EntryId, spica_logstream::LogError> {
        let closed = if matches!(entries.last().map(|e| &e.payload), Some(EntryPayload::Noop)) {
            entries
        } else {
            let mut entries = entries;
            entries.push(Entry {
                // Placeholders the inner log stamps on append; the orphan Noop's `cause_id` is
                // deliberately None — it terminates no batch, so there is no producing Command to
                // link it to.
                stream_id: StreamId::nil(),
                entry_id: EntryId::nil(),
                cause_id: None,
                timestamp: Timestamp::now(),
                payload: EntryPayload::Noop,
            });
            entries
        };
        self.inner.append(closed).await
    }

    async fn read(&self, entry_id: EntryId) -> Result<Option<Entry>, spica_logstream::LogError> {
        self.inner.read(entry_id).await
    }

    fn stream_read(&self, from: EntryId) -> Pin<Box<dyn Stream<Item = Entry> + Send + 'static>> {
        self.inner.stream_read(from)
    }
}
