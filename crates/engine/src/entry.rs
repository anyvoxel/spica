//! The engine-domain log record: the payload carried by a [`LogStream`](spica_logstream::LogStream)
//! entry, plus a concrete alias for the envelope the engine uses.

use serde::{Deserialize, Serialize};

use crate::command::Command;
use crate::event::Event;
use crate::reject::Reject;

/// The application payload of a log [`Entry`] — the record body (BookKeeper `data` /
/// DistributedLog `payload`).
///
/// This is the engine's concrete payload type for the generic
/// [`spica_logstream::Entry<P>`](spica_logstream::Entry) — the seam that lets the logstream crate
/// stay free of engine domain types.
///
/// `Event` is intentionally the large arm here after the lifecycle-record redesign: several event
/// variants now carry a full [`crate::ActivityValue`] so a follower / recovered leader can rebuild
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
}

/// The concrete log envelope the engine reads and writes: the generic
/// [`spica_logstream::Entry`] specialized to this crate's [`EntryPayload`]. Kept as a type alias so
/// every engine call site (and the public re-export `spica_engine::Entry`) sees a concrete, named
/// type rather than a generic instantiation.
pub type Entry = spica_logstream::Entry<EntryPayload>;
