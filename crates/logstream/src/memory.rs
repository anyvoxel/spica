//! An in-process, in-memory [`LogStream`] — the transient sibling of [`RocksLogStream`](crate::RocksLogStream).

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_stream::stream;
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::watch;
use tokio_stream::Stream;

use crate::entry::Entry;
use crate::error::LogError;
use crate::id::{EntryId, StreamId};
use crate::logstream::LogStream;

/// In-process, in-memory LogStream.
///
/// Entries are held in an `Arc<Mutex<Inner>>` shared with [`stream_read`](LogStream::stream_read)
/// consumers, and a `watch::Sender` notifies tailing readers when new entries are appended.
///
/// Every entry lives in RAM and the whole store is rebuilt from scratch each boot. Use it for tests
/// and single-process flows; use [`RocksLogStream`](crate::RocksLogStream) when a restarted process
/// must resume where it left off.
pub struct InMemoryLogStream<P> {
    inner: Arc<Mutex<Inner<P>>>,
    /// Notifies tailing readers after each append; carries the new entry count.
    watch: watch::Sender<u64>,
    /// This log's own identity, stamped onto every appended entry. An in-memory log is rebuilt from
    /// scratch each boot and never persists, so a fixed id (1) is the natural choice: within a
    /// process it is constant for the log's whole life.
    stream_id: StreamId,
}

struct Inner<P> {
    entries: Vec<Entry<P>>,
    /// The next expected `entry_id` (the position after the last appended entry). Starts at 1.
    next_entry_id: i64,
}

impl<P> InMemoryLogStream<P>
where
    P: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                entries: Vec::new(),
                next_entry_id: 1,
            })),
            watch: watch::channel(0).0,
            stream_id: StreamId(1),
        }
    }

    /// All entries appended so far (for inspection in tests/debugging).
    pub fn entries(&self) -> Vec<Entry<P>> {
        self.inner
            .lock()
            .expect("inner mutex poisoned")
            .entries
            .clone()
    }
}

impl<P> Default for InMemoryLogStream<P>
where
    P: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<P> LogStream<P> for InMemoryLogStream<P>
where
    P: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    async fn append(&self, mut entries: Vec<Entry<P>>) -> Result<EntryId, LogError> {
        // Assign each entry's `entry_id` from the stream's own counter, under the lock so concurrent
        // writers get contiguous, non-overlapping positions (all-or-nothing per batch). Each entry
        // also carries this log's own `stream_id` (stamped, never caller-chosen) — the log is one
        // stream, so every entry it appends shares the id.
        let (new_len, last_id) = {
            let mut inner = self.inner.lock().expect("inner mutex poisoned");
            let mut id = inner.next_entry_id;
            for entry in &mut entries {
                entry.stream_id = self.stream_id;
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

    async fn read(&self, entry_id: EntryId) -> Result<Option<Entry<P>>, LogError> {
        let inner = self.inner.lock().expect("inner mutex poisoned");
        let idx = entry_id
            .get()
            .checked_sub(1)
            .and_then(|i| usize::try_from(i).ok());
        Ok(idx.and_then(|i| inner.entries.get(i).cloned()))
    }

    fn stream_read(&self, from: EntryId) -> Pin<Box<dyn Stream<Item = Entry<P>> + Send + 'static>> {
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
    use crate::Timestamp;

    /// A minimal payload for the logstream's own tests — the crate is payload-agnostic, so a plain
    /// wrapper stands in for whatever an embedding engine would carry.
    #[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
    struct Payload(u64);

    /// Builds an entry with an un-stamped (placeholder) `entry_id` — the log assigns the real
    /// position at append time, so writers never set it. `nil()` is the 1-based stream's "unset"
    /// sentinel (never a valid position).
    fn entry(p: u64) -> Entry<Payload> {
        Entry {
            stream_id: crate::StreamId(1),
            entry_id: EntryId::nil(),
            cause_id: None,
            timestamp: Timestamp::from_millis(0),
            payload: Payload(p),
        }
    }

    async fn entries_after_append(log: &InMemoryLogStream<Payload>) -> Vec<i64> {
        log.entries().iter().map(|e| e.entry_id.get()).collect()
    }

    #[tokio::test]
    async fn append_assigns_contiguous_entry_ids() {
        let log = InMemoryLogStream::<Payload>::new();
        // A multi-entry batch gets sequential ids starting at 1.
        let last = log.append(vec![entry(0), entry(0)]).await.unwrap();
        // `append` returns the high-water mark: the batch's last assigned position.
        assert_eq!(last, EntryId(2));
        assert_eq!(entries_after_append(&log).await, vec![1, 2]);
        // A later append continues from the stream's counter and reports its own high-water mark.
        let last = log.append(vec![entry(0)]).await.unwrap();
        assert_eq!(last, EntryId(3));
        assert_eq!(entries_after_append(&log).await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn append_overwrites_caller_entry_ids() {
        let log = InMemoryLogStream::<Payload>::new();
        // Callers hand in arbitrary placeholders; the log stamps real contiguous positions.
        log.append(vec![entry(99), entry(7)]).await.unwrap();
        assert_eq!(entries_after_append(&log).await, vec![1, 2]);
    }

    #[tokio::test]
    async fn read_returns_entry_at_position() {
        let log = InMemoryLogStream::<Payload>::new();
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
        let log = InMemoryLogStream::<Payload>::new();
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

        let log = InMemoryLogStream::<Payload>::new();
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
