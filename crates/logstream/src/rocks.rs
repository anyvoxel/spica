//! A durable [`LogStream`] backed by [RocksDB].
//!
//! [`InMemoryLogStream`](crate::InMemoryLogStream) keeps everything in RAM and is rebuilt from
//! scratch each boot. This type is the same [`LogStream`] contract, but persists entries to RocksDB
//! so a restarted process can resume where it left off — the durability half of the failover story
//! (application/recovery lives in the engine's StreamProcessor).
//!
//! Why an embedded storage engine rather than hand-written files: RocksDB gives the three properties
//! a hand-rolled append log would have to re-derive at great cost and risk:
//!
//! - **Atomic batch append** — a whole batch is one [`WriteBatch`], committed and fsync'd as a unit,
//!   so a crash never leaves a partially-applied batch (the `LogStream::append` all-or-nothing
//!   contract).
//! - **Crash recovery** — RocksDB replays its WAL on open; an un-committed (not-yet-fsynced) batch is
//!   discarded automatically, and a committed one survives. No manual trailing-frame truncation, no
//!   half-frame detection.
//! - **Stable point reads + ordered scans** — entries keyed by position give O(1) `read` and cheap
//!   sequential iteration for tailing.
//!
//! Both [`Entry`] and its payload implement [`serde::Serialize`], so values are stored as their JSON
//! encoding — no new codec, and the persisted form is debuggable by hand.
//!
//! Keys are namespaced inside a single column family via a leading byte:
//!
//! - entry key = `0x00` followed by the 8-byte big-endian [`EntryId`] position (so a forward iterator
//!   yields positions in strictly ascending order),
//! - meta key = `0xFE` holding the 8-byte big-endian *next* position to assign.
//!
//! The `next` position lives both in that meta key (persisted, so a reopen continues at the right
//! place) and cached under the write lock (so `append` doesn't re-read it from the DB each time);
//! the two are kept in step because the meta write is part of the same `WriteBatch`.
//!
//! [RocksDB]: https://rocksdb.org

use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_stream::stream;
use rocksdb::{DB, Direction, IteratorMode, WriteBatch, WriteOptions};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::watch;
use tokio_stream::Stream;

use crate::entry::Entry;
use crate::error::LogError;
use crate::id::{EntryId, StreamId};
use crate::logstream::LogStream;

/// Entry-key prefix byte: real entry keys are `0x00 ‖ BE(position)`, so they sort before the meta
/// key (`0xFE`) and a `From(position)` iterator stops naturally at the meta key.
const ENTRY_PREFIX: u8 = 0x00;

/// Meta-key prefix: value is the 8-byte big-endian *next* EntryId position to assign. Lives after
/// every entry key (`0xFE > 0x00`), so it never collides with an entry and never appears in a
/// tailing iterator that filters on `ENTRY_PREFIX`.
const META_NEXT_ID: &[u8] = b"\xFE";

/// Meta-key prefix for this log's own stable [`StreamId`]: the 8-byte big-endian stream id. Lives
/// just before the `next` key (`0xFD < 0xFE`) and after every entry key, so it too never collides
/// with an entry and never appears in a tailing iterator. Persisted so a reopen of the same log
/// keeps the same id — the "same logstream → same stream_id" invariant survives a restart.
const META_STREAM_ID: &[u8] = b"\xFD";

/// Build the 9-byte RocksDB key for the entry at 1-based `position`.
fn entry_key(position: i64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = ENTRY_PREFIX;
    key[1..9].copy_from_slice(&position.to_be_bytes());
    key
}

/// Decode the 8-byte big-endian next-position stored under [`META_NEXT_ID`].
fn decode_next(bytes: &[u8]) -> Result<i64, LogError> {
    let raw = bytes
        .try_into()
        .map_err(|_| LogError::Corrupt("meta next-id length".to_string()))?;
    Ok(i64::from_be_bytes(raw))
}

/// Decode the 8-byte big-endian stream id stored under [`META_STREAM_ID`]. Stream ids are 1-based
/// and always valid (never `nil`); a corrupt length is surfaced as a corrupt store.
fn decode_stream_id(bytes: &[u8]) -> Result<StreamId, LogError> {
    let raw = bytes
        .try_into()
        .map_err(|_| LogError::Corrupt("meta stream-id length".to_string()))?;
    Ok(StreamId(i64::from_be_bytes(raw)))
}

/// Shared state behind an `Arc` so tailing readers can hold a handle to the same store.
struct Inner {
    db: DB,
    /// Serializes `append` so positions are assigned contiguously and the meta `next` cache stays in
    /// step with what was just written. Reads (`get` / iterator) go straight to the DB and need no
    /// lock. The lock is a `std::sync::Mutex` because the critical section is pure synchronous
    /// RocksDB I/O — never held across an `.await`.
    state: Mutex<WriterState>,
}

struct WriterState {
    /// Next 1-based position to assign. Cached copy of the persisted [`META_NEXT_ID`] value,
    /// refreshed under the write lock so `append` avoids a DB read per batch.
    next_id: i64,
}

/// A [`LogStream`] whose entries are durably stored in RocksDB.
pub struct RocksLogStream<P> {
    inner: Arc<Inner>,
    /// Notifies tailing readers after each append (carries the stream's total entry count, matching
    /// [`InMemoryLogStream`](crate::InMemoryLogStream)). Not persisted — it only tracks the *live*
    /// tail; readers re-scan the DB for committed entries each poll regardless.
    watch: watch::Sender<u64>,
    /// This log's own stable identity, bootstrapped from the persisted [`META_STREAM_ID`] on open
    /// (1 for a fresh store) and stamped onto every appended entry. Shared by all entries on this
    /// log — a log is one stream.
    stream_id: StreamId,
    /// The payload type is never used in the struct body, but pins this concrete `RocksLogStream<P>`
    /// instantiation so `P`'s `Serialize`/`Deserialize` bounds are carried where the log is rooted.
    _payload: std::marker::PhantomData<fn() -> P>,
}

impl<P> RocksLogStream<P>
where
    P: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Open (creating if needed) the store rooted at `path`. Committed batches present in the DB's
    /// WAL are recovered automatically; a partially-fsynced trailing batch is discarded by RocksDB.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LogError> {
        let db = DB::open_default(path).map_err(|e| LogError::Open(e.to_string()))?;
        // Bootstrap `next` from the persisted meta key if present; a fresh store starts at position 1.
        let next_id = match db
            .get(META_NEXT_ID)
            .map_err(|e| LogError::Read(e.to_string()))?
        {
            Some(bytes) => decode_next(&bytes)?,
            None => 1,
        };
        // Bootstrap this log's own stable stream id from the persisted meta key if present; a fresh
        // store gets stream id 1. Persisting it means a reopen of the same log (same path) keeps the
        // same id, preserving "same logstream → same stream_id" across a restart.
        let stream_id = match db
            .get(META_STREAM_ID)
            .map_err(|e| LogError::Read(e.to_string()))?
        {
            Some(bytes) => decode_stream_id(&bytes)?,
            None => StreamId(1),
        };
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                state: Mutex::new(WriterState { next_id }),
            }),
            watch: watch::channel(0).0,
            stream_id,
            _payload: std::marker::PhantomData,
        })
    }
}

#[async_trait::async_trait]
impl<P> LogStream<P> for RocksLogStream<P>
where
    P: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    /// Atomically append a batch as one RocksDB `WriteBatch`, fsync'd (`sync` write option) before
    /// returning — so a successful `append` means the whole batch is durable, and a crash mid-write
    /// leaves nothing (all-or-nothing). Assigns contiguous 1-based positions from the store's
    /// `next`, updating both the cached and persisted copies of `next` in the same batch.
    async fn append(&self, mut entries: Vec<Entry<P>>) -> Result<EntryId, LogError> {
        let mut state = self.inner.state.lock().expect("state mutex poisoned");
        if entries.is_empty() {
            // Nothing to durably commit; report the stream's current high-water mark.
            return Ok(EntryId(state.next_id - 1));
        }

        let mut batch = WriteBatch::default();
        let mut position = state.next_id;
        for entry in &mut entries {
            // Stamp both ids the same way `InMemoryLogStream` does — the log owns `entry_id`
            // (position) and `stream_id` (its own stable identity): every entry this log appends
            // carries the same stream id, so callers never pick a stream.
            entry.stream_id = self.stream_id;
            entry.entry_id = EntryId(position);
            let value =
                serde_json::to_vec(entry).map_err(|e| LogError::Serialize(e.to_string()))?;
            batch.put(entry_key(position), value);
            position += 1;
        }
        // Persist the new `next` atomically with the entries, so a reopen continues exactly here.
        batch.put(META_NEXT_ID, position.to_be_bytes());

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(true); // fsync the WAL before returning → Ok means durable.
        self.inner
            .db
            .write_opt(batch, &write_opts)
            .map_err(|e| LogError::Write(e.to_string()))?;

        state.next_id = position;
        let last = position - 1;
        let _ = self.watch.send(last as u64);
        Ok(EntryId(last))
    }

    /// Read the entry at 1-based `entry_id`, or `None` past the end of the stream (or the `nil`
    /// sentinel). A direct point read on the position key.
    async fn read(&self, entry_id: EntryId) -> Result<Option<Entry<P>>, LogError> {
        let position = entry_id.get();
        if position < 1 {
            return Ok(None);
        }
        match self
            .inner
            .db
            .get(entry_key(position))
            .map_err(|e| LogError::Read(e.to_string()))?
        {
            Some(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| LogError::Deserialize(e.to_string())),
            None => Ok(None),
        }
    }

    /// Stream entries starting at `from`, tailing the log: reads every committed entry from `from`
    /// forward via a RocksDB iterator (batches are yielded in one sweep), then waits on the live
    /// `watch` for the next append and repeats. A `nil()` `from` starts at position 1.
    fn stream_read(&self, from: EntryId) -> Pin<Box<dyn Stream<Item = Entry<P>> + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        let mut rx = self.watch.subscribe();
        Box::pin(stream! {
            let mut next = if from == EntryId::nil() { 1 } else { from.get() };
            loop {
                // Sweep all currently-committed entries from `next` in one iterator pass (stops at
                // the meta key, which doesn't carry the entry prefix).
                let start = entry_key(next);
                let committed: Vec<Entry<P>> = inner
                    .db
                    .iterator(IteratorMode::From(&start, Direction::Forward))
                    .filter_map(Result::ok)
                    .filter(|(key, _)| key[0] == ENTRY_PREFIX)
                    .filter_map(|(_, value)| serde_json::from_slice(&value).ok())
                    .collect();
                if committed.is_empty() {
                    // Caught up: block until the next append (or the store is dropped → end stream).
                    if rx.changed().await.is_err() {
                        return;
                    }
                    continue;
                }
                for entry in committed {
                    next += 1;
                    yield entry;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StreamId, Timestamp};

    /// A minimal payload for the logstream's own tests.
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Payload(u64);

    /// Build a payload-simple entry with an un-stamped (placeholder) `entry_id`.
    fn entry(p: u64) -> Entry<Payload> {
        Entry {
            stream_id: StreamId(1),
            entry_id: EntryId::nil(),
            cause_id: None,
            timestamp: Timestamp::from_millis(0),
            payload: Payload(p),
        }
    }

    /// A unique per-test store path under the system temp dir, removed after the test.
    fn temp_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("spica-rocks-{tag}-{}", ulid::Ulid::new()))
    }

    #[tokio::test]
    async fn appends_stamp_contiguous_positions_and_returns_high_water() {
        let path = temp_path("high-water");
        {
            let log = RocksLogStream::<Payload>::open(&path).unwrap();
            let last = log.append(vec![entry(0), entry(0)]).await.unwrap();
            assert_eq!(last, EntryId(2));
            assert_eq!(
                log.read(EntryId(1)).await.unwrap().unwrap().entry_id,
                EntryId(1)
            );
            assert_eq!(
                log.read(EntryId(2)).await.unwrap().unwrap().entry_id,
                EntryId(2)
            );
            assert!(log.read(EntryId(3)).await.unwrap().is_none());
            // later appends continue from the committed counter
            let last = log.append(vec![entry(0)]).await.unwrap();
            assert_eq!(last, EntryId(3));
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn committed_batches_survive_reopen() {
        let path = temp_path("reopen");
        {
            let log = RocksLogStream::<Payload>::open(&path).unwrap();
            log.append(vec![entry(0), entry(0)]).await.unwrap();
            log.append(vec![entry(0)]).await.unwrap();
        }
        // DB is dropped and reopened: a persisted log must resume at position 4, not restart at 1.
        {
            let log = RocksLogStream::<Payload>::open(&path).unwrap();
            assert_eq!(
                log.read(EntryId(1)).await.unwrap().unwrap().entry_id,
                EntryId(1)
            );
            assert_eq!(
                log.read(EntryId(3)).await.unwrap().unwrap().entry_id,
                EntryId(3)
            );
            let last = log.append(vec![entry(0)]).await.unwrap();
            assert_eq!(last, EntryId(4));
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn stream_read_tails_new_appends() {
        use tokio_stream::StreamExt;

        let path = temp_path("tail");
        {
            let log = RocksLogStream::<Payload>::open(&path).unwrap();
            log.append(vec![entry(0), entry(0)]).await.unwrap();

            let mut s = log.stream_read(EntryId::new(1));
            // committed entries yield immediately, in order
            assert_eq!(s.next().await.unwrap().entry_id, EntryId(1));
            assert_eq!(s.next().await.unwrap().entry_id, EntryId(2));

            // caught up: move the tailing stream into a task, then append entry 3. The reader either
            // already sees it on next poll (committed before it waited) or is woken by the watch.
            let handle = tokio::spawn(async move { s.next().await.unwrap().entry_id });
            log.append(vec![entry(0)]).await.unwrap(); // wakes the tailing reader
            assert_eq!(handle.await.unwrap(), EntryId(3));
        }
        let _ = std::fs::remove_dir_all(&path);
    }
}
