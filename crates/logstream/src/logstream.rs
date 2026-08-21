use std::pin::Pin;

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use tokio_stream::Stream;

use crate::entry::Entry;
use crate::error::LogError;
use crate::id::{EntryId, StreamId};

/// An append-only, ordered, consumable log of [`Entry`]s.
///
/// The LogStream is the durable source of truth. A StreamProcessor reads entries in order: Commands are
/// dispatched to handlers (which produce more entries, appended atomically); Events are applied to
/// a projection (a rebuildable fold of the stream). A distributed deployment provides a persistent,
/// shared, partitioned implementation; the in-memory and RocksDB variants live in this crate.
///
/// The payload type is generic over `P`: a log is a log regardless of what it carries, so the
/// generic seam keeps this crate free of any embedding engine's domain types.
#[async_trait]
#[auto_impl::auto_impl(Box)]
pub trait LogStream<P>: Send + Sync
where
    P: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// This log's own stable [`StreamId`]. A LogStream is a single stream, so its identity is a
    /// property of the log itself — never a per-entry or per-execution value chosen by the caller.
    /// Every entry this log appends carries this id (see [`append`](Self::append)); the id is stable
    /// across a reopen (a durable log persists it), so a reader can rely on it identifying the log
    /// regardless of when it reads. Callers never select it; they read it off the log when needed.
    fn stream_id(&self) -> StreamId;

    /// Atomically append a batch of entries (the Events + subsequent Commands produced by one
    /// Command).
    ///
    /// The stream **assigns each entry's `entry_id` itself** — entry positions come from the log's
    /// own monotonic counter, not from the caller. The caller's entries carry an `entry_id` field
    /// but it is a placeholder the log overwrites (writers can set it to
    /// [`EntryId::nil()`](EntryId::nil()), the all-zero "unset" sentinel). This makes any number of
    /// concurrent writers to one stream naturally contiguous: allocation happens under the log's
    /// critical section, so there is no caller-run id source to race.
    ///
    /// The log likewise stamps each entry's `stream_id` with its own
    /// ([`stream_id()`](Self::stream_id)): the caller's value is a placeholder that is overwritten,
    /// so "same logstream → same stream_id" is guaranteed by the log itself, not by callers
    /// remembering to agree on an id.
    ///
    /// On success the whole batch is appended; on failure nothing is (all-or-nothing). `cause_id`
    /// is copied through unchanged — it is the causal link to the producing Command and does not
    /// depend on the assigned position.
    ///
    /// Returns the **last** `entry_id` of the appended batch — the stream's new high-water mark
    /// (BookKeeper `lastAddConfirmed`). Batch positions are contiguous, so the batch spans
    /// `[last - entries.len() + 1, last]`; with a known length the caller can derive the first
    /// position, and the next writer of this stream continues at `last + 1`.
    async fn append(&self, entries: Vec<Entry<P>>) -> Result<EntryId, LogError>;

    /// Read the entry at `entry_id`, or `None` if no such entry has been written (i.e. `entry_id`
    /// is past the end of the stream).
    ///
    /// Reading is **position-based and `&self`**: the consumer tracks its own read position
    /// (advancing `entry_id` by 1 each read), so multiple consumers can read the same stream
    /// independently and a consumer can resume from any `entry_id` (e.g. a checkpoint + 1 after a
    /// crash). The log holds no per-consumer cursor.
    async fn read(&self, entry_id: EntryId) -> Result<Option<Entry<P>>, LogError>;

    /// Stream entries starting at `from`, **tailing** the stream: yields `from`, `from+1`, … and,
    /// once caught up to the current end, waits for newly appended entries and continues yielding
    /// them. Like [`read`](Self::read), this is position-based and `&self`; the returned stream is
    /// `'static` (it owns a handle to the log) so it can be moved to a task and multiple consumers
    /// can tail the same stream independently.
    ///
    /// `from` may be [`EntryId::nil()`](EntryId::nil()) — the "unset" sentinel — which is treated
    /// as "start reading from the first entry in the stream" (the stream's initial position 1).
    /// This lets a consumer express "give me everything from the beginning" without knowing the
    /// first position in advance.
    ///
    /// The stream ends (`None`) only when the log is closed (no more appends can happen).
    fn stream_read(&self, from: EntryId) -> Pin<Box<dyn Stream<Item = Entry<P>> + Send + 'static>>;
}
