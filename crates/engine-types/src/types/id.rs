use derive_more::{From, Into};
use serde::{Deserialize, Serialize};

use crate::types::meta::PlainName;

// Entry positions (`EntryId`) and stream identity (`StreamId`) are intrinsic to the append-only
// log, so they live in the payload-agnostic `spica-logstream` crate and are re-exported here
// unchanged — every engine caller keeps the same identifiers, and the two are guaranteed to be the
// exact types the log produces/consumes.
pub use spica_logstream::{EntryId, StreamId};

/// Defines a ULID-backed identifier newtype (e.g. [`RequestId`]) with
/// the conventional derives and constructors, collapsing the per-type boilerplate into one macro.
///
/// The `#[doc = $doc]` attribute carries the type's own documentation. `From`/`Into` (derive_more)
/// give the standard newtype conversions — `From<Ulid>` wraps a raw ULID, `Into<Ulid>` unwraps back
/// to it — and `new()`/`nil()`/`Default`/`Display` are the uniform id surface every such newtype
/// shares. Only the type name and doc differ, so both are macro parameters (the `i64`-backed
/// position ids `EntryId`/`StreamId` live in the `spica-logstream` crate instead).
macro_rules! ulid_id_type {
    ($name:ident, $doc:expr) => {
        #[doc = $doc]
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            Serialize,
            Deserialize,
            From,
            Into,
        )]
        pub struct $name(pub ulid::Ulid);

        impl $name {
            /// Mints a fresh, globally-unique id.
            pub fn new() -> Self {
                Self(ulid::Ulid::new())
            }

            /// The all-zero id: a `Discriminant` placeholder when registering handler/applier tables
            /// (only the variant discriminant matters, never the payload) or an "unknown / not yet
            /// materialized" sentinel. Never persisted as a real id.
            pub fn nil() -> Self {
                Self(ulid::Ulid::nil())
            }
        }

        impl Default for $name {
            /// The default id is the `nil()` sentinel (all-zero / "unknown"), not a freshly-minted
            /// one — matching how an id defaults in storage/registration contexts where it means
            /// "not yet materialized".
            fn default() -> Self {
                Self::nil()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }
    };
}

/// A user-supplied, **immutable** name identifying a logical flow — the identity under which
/// [`Flow`](crate::Flow)s are addressed (creating the same name again appends a new
/// [`FlowVersion`](crate::FlowVersion) to the same flow). A **type alias of [`PlainName`]**: a flow
/// name obeys exactly the same rules (4..=64 ASCII alnum/`_`, no `-`) and shares its validation,
/// serde and `as_str`, so there is no duplicate newtype.
pub type FlowName = PlainName;

ulid_id_type!(
    RequestId,
    "Identifies one client request awaiting its acknowledgement — Zeebe's `requestId`. An opaque, \
     never-reused id minted per operation (e.g. `Engine::create_flow` or `Engine::start_for_revision`), \
     carried on the initiating Command and on the outcome Event that resolves it, so the StreamProcessor can \
     route the acknowledgement back to exactly that caller. It is deliberately **not** a target entity \
     id: a single flow or execution (addressed by a raw `ulid::Ulid`) can be the target of many concurrent \
     requests, so acks are correlated per-*request*, never per-entity, to avoid aliasing them."
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ulid_ids_convert_from_raw_ulid_and_roundtrip() {
        // Each ULID id type wraps a raw Ulid; `From` builds it and the reverse `From`/`Into`
        // unwraps it back (the `get()`-free way to access the underlying Ulid).
        let raw = ulid::Ulid::new();
        let request: RequestId = raw.into();
        let back_request: ulid::Ulid = request.into();
        assert_eq!(back_request, raw);
    }

    #[test]
    fn flow_name_validates_and_roundtrips() {
        // Valid names.
        let n = FlowName::new("checkout_flow").unwrap();
        assert_eq!(n.as_str(), "checkout_flow");
        assert_eq!(n.to_string(), "checkout_flow");
        assert!(FlowName::new("abcd").is_ok()); // 4-char floor is inclusive
        assert!(FlowName::new("OrderFlow23").is_ok());
        assert!(FlowName::new(&"x".repeat(64)).is_ok());

        // Invalid names.
        assert!(FlowName::new("").is_err()); // empty
        assert!(FlowName::new("a").is_err()); // too short (< 4 chars)
        assert!(FlowName::new("abc").is_err()); // too short (< 4 chars)
        assert!(FlowName::new(&"x".repeat(65)).is_err()); // too long
        assert!(FlowName::new("_checkout").is_err()); // leading underscore
        assert!(FlowName::new("1checkout").is_ok()); // leading digit allowed
        assert!(FlowName::new("checkout-flow").is_err()); // hyphen banned
        assert!(FlowName::new("checkout flow").is_err()); // whitespace banned
        assert!(FlowName::new("checkout/flow").is_err()); // slash banned
        assert!(FlowName::new("中文").is_err()); // non-ASCII

        // Case-sensitive: distinct values.
        assert_ne!(
            FlowName::new("Checkout").unwrap(),
            FlowName::new("checkout").unwrap()
        );
    }
}
