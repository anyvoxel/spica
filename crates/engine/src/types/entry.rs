//! The engine-domain log record: the payload carried by a [`LogStream`](spica_logstream::LogStream)
//! entry, plus a concrete alias for the envelope the engine uses.

use serde::{Deserialize, Serialize};

use crate::types::command::Command;
use crate::types::event::Event;
use crate::types::reject::Reject;

/// The application payload of a log [`Entry`] — the record body (BookKeeper `data` /
/// DistributedLog `payload`).
///
/// This is the engine's concrete payload type for the generic
/// [`spica_logstream::Entry<P>`](spica_logstream::Entry) — the seam that lets the logstream crate
/// stay free of engine domain types.
///
/// `Event` is intentionally the large arm here after the lifecycle-record redesign: several event
/// variants now carry a full [`crate::Activity`] so a follower / recovered leader can rebuild
/// the same domain entity from the stream alone. Boxing only the `Event` arm would shrink this enum
/// but would also add pervasive heap indirection on the engine's hottest path (append, replay,
/// dispatch, tests) without changing the persisted semantics. We therefore keep the payload inline
/// and document the size trade-off explicitly.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EntryPayload {
    Command(Command),
    Event(Event),
    /// A command that was refrained from applying — a parallel record to [`Event`](crate::Event),
    /// carrying its own `request_id` so the StreamProcessor can awake the awaiting caller directly (see
    /// [`Reject`]). A rejection is the engine's `COMMAND_REJECTION` analogue, not an event subtype.
    Reject(Reject),
    /// The terminator of one **atomic append batch** (see `docs/durable-execution-recovery-design.md`).
    /// The [`StreamProcessor`](crate::StreamProcessor) appends a `Noop` as the **last** entry of
    /// every non-empty batch it writes for a dispatched Command, so a reader can tell — from the log
    /// alone, without peeking ahead — that a causal batch is complete and safe to fold/commit. Its
    /// `cause_id` (on the envelope) is the producing Command's position, giving the batch a stable,
    /// GTID-like identity.
    ///
    /// The leader applies a batch **eagerly** at production time, so by the time its read-back
    /// reaches this `Noop` the batch is already folded and this is a no-op for it. The marker is
    /// consumed by readers that apply on read-back (a future follower rounds off its atomic apply
    /// here) and stands as the log's explicit, self-describing roll of committed batches.
    Noop,
}

/// The concrete log envelope the engine reads and writes: the generic
/// [`spica_logstream::Entry`] specialized to this crate's [`EntryPayload`]. Kept as a type alias so
/// every engine call site (and the public re-export `spica_engine::Entry`) sees a concrete, named
/// type rather than a generic instantiation.
pub type Entry = spica_logstream::Entry<EntryPayload>;
