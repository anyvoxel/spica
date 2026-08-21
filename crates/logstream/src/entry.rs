use serde::{Deserialize, Serialize};

use crate::Timestamp;
use crate::id::{EntryId, StreamId};

/// A single record in a stream (the "log"). Envelope metadata is separated from the payload,
/// mirroring Apache BookKeeper's `LedgerEntry` (ledgerId + entryId + payload) and DistributedLog's
/// `LogRecord` (sequence + txid + payload).
///
/// - `stream_id` — the stream this entry belongs to. A stream may contain entries from multiple
///   executions; the payload identifies which execution it belongs to. [BookKeeper ledgerId]
/// - `entry_id` — monotonic position within the stream, assigned by the writer on append. This is
///   the authoritative ordering and identity; wall-clock `timestamp` is NOT used for ordering.
///   [BookKeeper entryId / DistributedLog sequenceId]
/// - `cause_id` — causal link to the entry that produced this one (None for the root). [CCES;
///   BookKeeper/DistributedLog have no causal field]
/// - `timestamp` — app-assigned wall-clock timestamp, audit metadata; not used for ordering.
/// - `payload` — the record body, generic over the embedding engine's record type. [BookKeeper data]
///
/// Deferred (distributed stage): `lastAddConfirmed` (durability watermark), an auth/MAC field for
/// integrity, log segmentation (DistributedLog LSSN), and an opaque-bytes payload form.
///
/// The single-node continuation model removes the need for a record-level marker: the StreamProcessor's
/// persisted `last_processed_position` watermark (see `docs/durable-execution-recovery-design.md`)
/// lets a restart resume from `W + 1`, skipping already-applied Commands by position. A per-record
/// "already processed" flag — as Zeebe's `LogAppendEntry.ofProcessed` (`shouldSkipProcessing()`)
/// stamps — is deliberately **not** adopted here: it would add per-record bookkeeping for a problem
/// the position watermark already solves for the single-node case.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entry<P> {
    /// The stream this entry belongs to (a stream may hold multiple executions).
    pub stream_id: StreamId,
    /// Monotonic position within the stream — the authoritative ordering and identity.
    pub entry_id: EntryId,
    /// Causal link to the producing entry (None for the root).
    pub cause_id: Option<EntryId>,
    /// App-assigned wall-clock timestamp (audit metadata; not used for ordering/decisions).
    pub timestamp: Timestamp,
    /// The record body — the embedding engine's payload (e.g. `Command | Event | Reject`).
    pub payload: P,
}
