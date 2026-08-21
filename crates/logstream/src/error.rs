use thiserror::Error;

/// A failure produced by a [`LogStream`](crate::LogStream) implementation — a low-level I/O /
/// codec fault confined to the log itself.
///
/// This is the generic log's own error, deliberately **independent of any embedding engine's error
/// type**. An engine that integrates a log maps it onto its own error surface (e.g. `spica-engine`
/// implements `From<LogError> for ExecutionError`, so `?` on a log call just works). `LogError`
/// variants are folk-taxonomic over the failure modes the two shipped implementations can hit:
/// opening the store, a committed/un-committed `Read`/`Write`, and `(de)serializing` an entry.
/// There is no "logical" failure variant — violations of append-ordering are the caller's contract
/// to uphold, not the log's to detect.
#[derive(Debug, Error)]
pub enum LogError {
    /// Failed to open/create the durable store (e.g. RocksDB) at its path.
    #[error("log open: {0}")]
    Open(String),
    /// A point read from the log failed.
    #[error("log read: {0}")]
    Read(String),
    /// An append (`WriteBatch` commit) failed.
    #[error("log write: {0}")]
    Write(String),
    /// An entry failed to serialize at append time.
    #[error("log serialize: {0}")]
    Serialize(String),
    /// A stored entry failed to deserialize at read time (corrupt/foreign payload).
    #[error("log deserialize: {0}")]
    Deserialize(String),
    /// On-disk metadata is structurally invalid (e.g. a malformed next-position value).
    #[error("log corrupt: {0}")]
    Corrupt(String),
}
