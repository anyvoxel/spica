use serde::{Deserialize, Serialize};

use crate::types::id::RequestId;

/// A command that **cannot be honored** — syntactic/stateful preconditions for applying it are not
/// met — carried as a record **parallel to** [`Event`](crate::Event), not a subtype of it.
///
/// This is the CCES analogue of Zeebe's `COMMAND_REJECTION` record (a `recordType` sibling of
/// `COMMAND`/`EVENT`): the command itself was well-formed, but the engine refuses to apply it. Where
/// an [`Event`](crate::Event) records an accepted command's outcome, a `Reject` records a refused
/// one — so **every client-originated awaiting command produces exactly one response entry**
/// (either an `Event` or a `Reject`), never a silent handler `return` with no entry at all.
///
/// A `Reject` carries its own `request_id` (the echoing correlate of the rejected command's id), so
/// the StreamProcessor can awake the awaiting caller directly as it reads the record — no projection to
/// fold, no deferred side-effect correlation (unlike `Event`s, whose identity is only recoverable
/// from the applied entity).
///
/// Do not confuse a rejection with an execution failure (`TerminateExecution{Failed}`): a
/// rejection answers a *command request it will not apply* (e.g. a malformed definition, a
/// duplicate name); execution failure is a *runtime outcome of an accepted execution*. The two live
/// on different layers and neither subsumes the other.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reject {
    /// The opaque per-request id echoed from the rejected `Command`, routing this record back to the
    /// caller that awaits that command's acknowledgement (Zeebe's `requestId → future` model).
    pub request_id: RequestId,
    /// A stable, machine-readable classification of *why* the command was refused — the engine's
    /// `RejectionType` taxonomy (see there). Matchable by callers to distinguish a duplicate create
    /// (`AlreadyExists`) from a malformed payload (`InvalidArgument`), etc.
    pub rejection_type: RejectionType,
    /// A human-readable explanation of the rejection, mirroring Zeebe's `rejectionReason`. Intended
    /// for surfacing to an operator or end-user, not for programmatic branching (use `rejection_type`).
    pub rejection_reason: String,
}

/// The stable classification of *why* a command was rejected — the engine's analogue of Zeebe's
/// SBE `RejectionType` enum (NULL_VAL aside). Each variant names a distinct way a well-formed
/// command can fail to apply, so callers can branch on semantics rather than parsing free-text
/// reasons. Future enrichment (e.g. an `ExceededBatchRecordSize`) can be added as new variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectionType {
    /// The command is not applicable in the current context, but the state was not *wrong* — there
    /// was simply nothing for it to do. (Zeebe `NOT_APPLICABLE`.)
    NotApplicable,
    /// The command payload is structurally invalid — e.g. a `CreateFlow` whose definition does not
    /// parse as a `StateMachine`. (Zeebe `INVALID_ARGUMENT`.)
    InvalidArgument,
    /// The command cannot be applied because the target is in the wrong state. (Zeebe `INVALID_STATE`.)
    InvalidState,
    /// The command references an entity that does not exist. (Zeebe `NOT_FOUND`.)
    NotFound,
    /// The command's preconditions conflict with current state — e.g. racing concurrent appends.
    /// (Zeebe `STATE_CONFLICT`.)
    StateConflict,
    /// An entity the command would create already exists — e.g. a `CreateFlow` on a duplicate name.
    /// (Zeebe `ALREADY_EXISTS`.)
    AlreadyExists,
    /// A generic internal failure prevented the command from being applied. (Zeebe `PROCESSING_ERROR`).
    ProcessingError,
}

impl std::fmt::Display for RejectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            RejectionType::NotApplicable => "NOT_APPLICABLE",
            RejectionType::InvalidArgument => "INVALID_ARGUMENT",
            RejectionType::InvalidState => "INVALID_STATE",
            RejectionType::NotFound => "NOT_FOUND",
            RejectionType::StateConflict => "STATE_CONFLICT",
            RejectionType::AlreadyExists => "ALREADY_EXISTS",
            RejectionType::ProcessingError => "PROCESSING_ERROR",
        };
        write!(f, "{s}")
    }
}

impl std::fmt::Display for Reject {
    /// The human-readable form of a rejection, used when the engine surfaces this record embedded in
    /// its [`ExecutionError`](crate::ExecutionError) facade (`ExecutionError::Rejected`). Combines the
    /// stable classification and the reason; callers wanting to branch on semantics should match
    /// [`RejectionType`] instead.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.rejection_type, self.rejection_reason)
    }
}
