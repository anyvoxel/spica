use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_stream::stream;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_stream::Stream;

use crate::command::Command;
use crate::error::ExecutionError;
use crate::event::Event;
use crate::id::{EntryId, StreamId};

/// A wall-clock timestamp recording when a log [`Entry`] was created, as milliseconds since
/// `UNIX_EPOCH`.
///
/// This is **audit metadata, not decision input**: the decider/handlers never read it, and replay
/// uses the recorded value rather than regenerating it, so it does not affect the determinism of
/// the decision logic. It is also **not used for ordering** — ordering is by [`Entry::entry_id`],
/// mirroring BookKeeper (which has no timestamp field) and DistributedLog (where the transaction
/// id is app metadata, not the sort key). A distributed LogStream may re-stamp entries with a
/// hybrid logical clock (HLC) instead of the producer's wall clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp(u64);

impl Timestamp {
    /// The current wall-clock time.
    pub fn now() -> Self {
        Self(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        )
    }

    /// Construct from a millisecond count since `UNIX_EPOCH` (mainly for tests).
    pub fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    /// Parse an ASL `Wait.Timestamp` — an RFC3339 / ISO 8601 timestamp (e.g.
    /// `2016-03-14T01:59:00Z`, or `+08:00`-style offsets) — into an absolute [`Timestamp`]. Returns
    /// `None` if `s` is not a valid RFC3339 timestamp. A relative `Seconds` is normalized to an
    /// absolute deadline at activation; an absolute `Timestamp` is used as-is (this parser converts
    /// the wall-clock instant, including its offset, into epoch milliseconds).
    pub fn from_rfc3339(s: &str) -> Option<Self> {
        use time::format_description::well_known::Rfc3339;
        let dt = time::OffsetDateTime::parse(s, &Rfc3339).ok()?;
        // `dt.unix_timestamp()` is i64 seconds; `dt.millisecond()` is a u8 (0-999) that fits u64.
        let ms = u64::try_from(dt.unix_timestamp()).ok()?.checked_mul(1000)?;
        let frac = u64::from(dt.millisecond());
        Some(Self(ms.saturating_add(frac)))
    }

    /// Milliseconds since `UNIX_EPOCH`.
    pub fn as_millis(self) -> u64 {
        self.0
    }

    /// Adds a `Duration` to this timestamp, returning `None` on overflow. Used to normalize a
    /// relative ASL `Seconds`/`TimeoutSeconds` into an absolute deadline at activation
    /// (`now + seconds`).
    pub fn checked_add(self, d: Duration) -> Option<Self> {
        self.0.checked_add(d.as_millis() as u64).map(Self)
    }

    /// The wall-clock gap from `earlier` to `self` (i.e. `self - earlier`), floored at zero. Used to
    /// turn a persisted absolute `deadline` into a `DelayQueue` wait for a timer still ahead. Returns
    /// zero if `earlier` is at/after `self` — a deadline already passed fires immediately.
    pub fn saturating_duration_since(self, earlier: Timestamp) -> Duration {
        Duration::from_millis(self.0.saturating_sub(earlier.0))
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The application payload of a log [`Entry`] — the record body (BookKeeper `data` /
/// DistributedLog `payload`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EntryPayload {
    Command(Command),
    Event(Event),
}

/// A single record in an execution's log (the "stream"). Envelope metadata is separated from the
/// payload, mirroring Apache BookKeeper's `LedgerEntry` (ledgerId + entryId + payload) and
/// DistributedLog's `LogRecord` (sequence + txid + payload).
///
/// - `stream_id` — the stream this entry belongs to. A stream may contain entries from multiple
///   executions; the entry's payload identifies which execution it belongs to. [BookKeeper ledgerId]
/// - `entry_id` — monotonic position within the stream, assigned by the writer (Processor) on
///   append. This is the authoritative ordering and identity; wall-clock `timestamp` is NOT used
///   for ordering. [BookKeeper entryId / DistributedLog sequenceId]
/// - `cause_id` — causal link to the entry that produced this one (None for the root
///   `StartExecution`). [CCES-specific; BookKeeper/DistributedLog have no causal field]
/// - `timestamp` — app-assigned wall-clock timestamp, audit metadata like DistributedLog's
///   transaction id; not used for ordering or decisions.
/// - `payload` — the record body. [BookKeeper data / DistributedLog payload]
///
/// Deferred (distributed stage): `lastAddConfirmed` (durability watermark), an auth/MAC field for
/// integrity, log segmentation (DistributedLog LSSN), and an opaque-bytes payload form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    /// The stream this entry belongs to (a stream may hold multiple executions).
    pub stream_id: StreamId,
    /// Monotonic position within the stream — the authoritative ordering and identity.
    pub entry_id: EntryId,
    /// Causal link to the producing entry (None for the root `StartExecution`).
    pub cause_id: Option<EntryId>,
    /// App-assigned wall-clock timestamp (audit metadata; not used for ordering/decisions).
    pub timestamp: Timestamp,
    /// The record body.
    pub payload: EntryPayload,
}

/// An append-only, ordered, consumable log of [`Entry`]s — Commands and Events.
///
/// The LogStream is the durable source of truth. The Processor reads entries in order: Commands
/// are dispatched to handlers (which produce more entries, appended atomically); Events are
/// applied to [`Storage`](crate::Storage). A distributed deployment provides a persistent,
/// shared, partitioned implementation; M1 uses [`InMemoryLogStream`].
#[async_trait]
pub trait LogStream: Send {
    /// Atomically append a batch of entries (the Events + subsequent Commands produced by one
    /// Command).
    ///
    /// The stream **assigns each entry's `entry_id` itself** — entry positions come from the log's
    /// own monotonic counter, not from the caller. The caller's entries carry an `entry_id` field
    /// but it is a placeholder the log overwrites (writers can set it to [`EntryId`](crate::EntryId)::nil(),
    /// the all-zero "unset" sentinel). This makes any number of concurrent writers to one stream
    /// naturally contiguous: allocation
    /// happens under the log's critical section, so there is no caller-run `IdSource` to race (the
    /// previous writer-assigned scheme validated contiguity and could only *reject* a batch on
    /// interleaving — see the original TODO this replaced).
    ///
    /// On success the whole batch is appended; on failure nothing is (all-or-nothing).
    /// `cause_id` is copied through unchanged — it is the causal link to the producing Command and
    /// does not depend on the assigned position.
    ///
    /// Returns the **last** `entry_id` of the appended batch — the stream's new high-water mark
    /// (BookKeeper `lastAddConfirmed`). Batch positions are contiguous, so the batch spans
    /// `[last - entries.len() + 1, last]`; with a known length the caller can derive the first
    /// position, and the next writer of this stream continues at `last + 1`.
    async fn append(&self, entries: Vec<Entry>) -> Result<EntryId, ExecutionError>;

    /// Read the entry at `entry_id`, or `None` if no such entry has been written (i.e. `entry_id`
    /// is past the end of the stream).
    ///
    /// Reading is **position-based and `&self`**: the consumer tracks its own read position
    /// (advancing `entry_id` by 1 each read), so multiple consumers can read the same stream
    /// independently and a consumer can resume from any `entry_id` (e.g. a checkpoint + 1 after a
    /// crash). The log holds no per-consumer cursor.
    async fn read(&self, entry_id: EntryId) -> Result<Option<Entry>, ExecutionError>;

    /// Stream entries starting at `from`, **tailing** the stream: yields `from`, `from+1`, … and,
    /// once caught up to the current end, waits for newly appended entries and continues yielding
    /// them. Like [`read`](Self::read), this is position-based and `&self`; the returned stream is
    /// `'static` (it owns a handle to the log) so it can be moved to a task and multiple consumers
    /// can tail the same stream independently.
    ///
    /// `from` may be [`EntryId`](crate::EntryId)::nil() — the "unset" sentinel — which is treated as
    /// "start reading from the first entry in the stream" (the stream's initial position 1). This
    /// lets a consumer express "give me everything from the beginning" without knowing the first
    /// position in advance.
    ///
    /// The stream ends (`None`) only when the log is closed (no more appends can happen).
    fn stream_read(&self, from: EntryId) -> Pin<Box<dyn Stream<Item = Entry> + Send + 'static>>;
}

/// In-process, in-memory LogStream used by M1's `Engine::start`.
///
/// Entries are held in an `Arc<Mutex<Inner>>` shared with [`stream_read`](LogStream::stream_read)
/// consumers, and a `watch::Sender` notifies tailing readers when new entries are appended.
pub struct InMemoryLogStream {
    inner: Arc<Mutex<Inner>>,
    /// Notifies tailing readers after each append; carries the new entry count.
    watch: watch::Sender<u64>,
}

struct Inner {
    entries: Vec<Entry>,
    /// The next expected `entry_id` (the position after the last appended entry). Starts at 1.
    next_entry_id: i64,
}

impl InMemoryLogStream {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                entries: Vec::new(),
                next_entry_id: 1,
            })),
            watch: watch::channel(0).0,
        }
    }

    /// All entries appended so far (for inspection in tests/debugging).
    pub fn entries(&self) -> Vec<Entry> {
        self.inner
            .lock()
            .expect("inner mutex poisoned")
            .entries
            .clone()
    }
}

impl Default for InMemoryLogStream {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LogStream for InMemoryLogStream {
    async fn append(&self, mut entries: Vec<Entry>) -> Result<EntryId, ExecutionError> {
        // Assign each entry's `entry_id` from the stream's own counter, under the lock so concurrent
        // writers get contiguous, non-overlapping positions (all-or-nothing per batch).
        let (new_len, last_id) = {
            let mut inner = self.inner.lock().expect("inner mutex poisoned");
            let mut id = inner.next_entry_id;
            for entry in &mut entries {
                entry.entry_id = EntryId(id);
                id += 1;
            }
            inner.next_entry_id = id;
            inner.entries.extend(entries);
            // `id` is one past the last assigned position; decrement to get the batch's high-water mark.
            let last_id = id - 1;
            (inner.entries.len() as u64, EntryId(last_id))
        };
        // Notify tailing readers that new entries are available.
        let _ = self.watch.send(new_len);
        Ok(last_id)
    }

    async fn read(&self, entry_id: EntryId) -> Result<Option<Entry>, ExecutionError> {
        let inner = self.inner.lock().expect("inner mutex poisoned");
        let idx = entry_id
            .get()
            .checked_sub(1)
            .and_then(|i| usize::try_from(i).ok());
        Ok(idx.and_then(|i| inner.entries.get(i).cloned()))
    }

    fn stream_read(&self, from: EntryId) -> Pin<Box<dyn Stream<Item = Entry> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        let mut rx = self.watch.subscribe();
        Box::pin(stream! {
            // `nil()` is the "unset" sentinel — normalize it to the first real position (1) so a
            // consumer can ask for "everything from the beginning" without knowing position 1 in
            // advance. Any other value is used as the inclusive starting position.
            let mut next = if from == EntryId::nil() { EntryId::new(1) } else { from };
            loop {
                // Yield every currently-available entry from `next`.
                loop {
                    let entry = {
                        let inner = inner.lock().expect("inner mutex poisoned");
                        let idx = next.get().checked_sub(1).and_then(|i| usize::try_from(i).ok());
                        idx.and_then(|i| inner.entries.get(i).cloned())
                    };
                    match entry {
                        Some(e) => {
                            next = EntryId(next.get() + 1);
                            yield e;
                        }
                        None => break,
                    }
                }
                // Caught up: wait for the next append. `rx.changed()` returns immediately if a
                // change already happened since subscribe/last change (no lost wakeup).
                if rx.changed().await.is_err() {
                    return; // watch sender dropped — log closed, no more entries.
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{EntryId, ExecutionId, StreamId};
    use serde_json::Value;

    /// Builds an entry with an un-stamped (placeholder) `entry_id` — the log assigns the real
    /// position at append time, so writers never set it. `nil()` is the 1-based stream's "unset"
    /// sentinel (never a valid position).
    fn entry(_entry_id: u64) -> Entry {
        Entry {
            stream_id: StreamId(1),
            entry_id: EntryId::nil(),
            cause_id: None,
            timestamp: Timestamp::from_millis(0),
            payload: EntryPayload::Event(Event::ExecutionCreated {
                id: ExecutionId::nil(),
                input: Value::Null,
            }),
        }
    }

    #[tokio::test]
    async fn append_assigns_contiguous_entry_ids() {
        let log = InMemoryLogStream::new();
        // A multi-entry batch gets sequential ids starting at 1.
        let last = log.append(vec![entry(0), entry(0)]).await.unwrap();
        // `append` returns the high-water mark: the batch's last assigned position.
        assert_eq!(last, EntryId(2));
        let got: Vec<i64> = log.entries().iter().map(|e| e.entry_id.get()).collect();
        assert_eq!(got, vec![1, 2]);
        // A later append continues from the stream's counter and reports its own high-water mark.
        let last = log.append(vec![entry(0)]).await.unwrap();
        assert_eq!(last, EntryId(3));
        let got: Vec<i64> = log.entries().iter().map(|e| e.entry_id.get()).collect();
        assert_eq!(got, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn append_overwrites_caller_entry_ids() {
        let log = InMemoryLogStream::new();
        // Callers hand in arbitrary placeholders; the log stamps real contiguous positions.
        log.append(vec![entry(99), entry(7)]).await.unwrap();
        let got: Vec<i64> = log.entries().iter().map(|e| e.entry_id.get()).collect();
        assert_eq!(got, vec![1, 2]);
    }

    #[tokio::test]
    async fn read_returns_entry_at_position() {
        let log = InMemoryLogStream::new();
        log.append(vec![entry(1), entry(2)]).await.unwrap();
        assert_eq!(
            log.read(EntryId(1)).await.unwrap().unwrap().entry_id,
            EntryId(1)
        );
        assert_eq!(
            log.read(EntryId(2)).await.unwrap().unwrap().entry_id,
            EntryId(2)
        );
        // past end -> None
        assert!(log.read(EntryId(3)).await.unwrap().is_none());
        // the "unset" sentinel (-1, outside the valid 1-based space) -> None
        assert!(log.read(EntryId::nil()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn stream_read_begins_at_first_entry_when_from_is_nil() {
        use tokio_stream::StreamExt;
        let log = InMemoryLogStream::new();
        log.append(vec![entry(1), entry(2)]).await.unwrap();

        // The "unset" sentinel reads the whole prefix from the very first entry.
        let mut s = log.stream_read(EntryId::nil());
        assert_eq!(s.next().await.unwrap().entry_id, EntryId(1));
        assert_eq!(s.next().await.unwrap().entry_id, EntryId(2));
    }

    #[tokio::test]
    async fn stream_read_yields_existing_then_tails_new() {
        use tokio::sync::oneshot;
        use tokio_stream::StreamExt;

        let log = InMemoryLogStream::new();
        log.append(vec![entry(1), entry(2)]).await.unwrap();

        let mut s = log.stream_read(EntryId(1));
        // Existing entries yield immediately.
        assert_eq!(s.next().await.unwrap().entry_id, EntryId(1));
        assert_eq!(s.next().await.unwrap().entry_id, EntryId(2));

        // Now caught up. Spawn a consumer that waits for entry 3 while we append it.
        let (tx, rx) = oneshot::channel();
        let mut s3 = log.stream_read(EntryId(3));
        let handle = tokio::spawn(async move {
            let _ = tx.send(()); // signal: about to wait for entry 3
            s3.next().await.unwrap().entry_id
        });
        rx.await.unwrap(); // consumer is now waiting on the stream
        log.append(vec![entry(3)]).await.unwrap(); // wakes the tailing consumer
        assert_eq!(handle.await.unwrap(), EntryId(3));
    }
}
