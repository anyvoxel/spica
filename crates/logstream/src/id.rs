use serde::{Deserialize, Serialize};

/// Defines an `i64`-backed numeric identifier newtype with the conventional derives and the
/// uniform `new`/`nil`/`get`/`Display` surface. `nil()` is the "unset / not yet materialized"
/// sentinel `-1`, deliberately **outside** the valid 1-based position space, so an un-stamped id is
/// unambiguously distinguishable from a real one.
macro_rules! id_type {
    ($name:ident, $doc:expr) => {
        #[doc = $doc]
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        pub struct $name(pub i64);

        impl $name {
            pub fn new(v: i64) -> Self {
                Self(v)
            }

            /// The "unset / not yet materialized" sentinel: `-1`, which lies **outside** the valid
            /// 1-based position space (`1, 2, 3, …`). Because it is not a representable position,
            /// any consumer can unambiguously tell an un-stamped id from a real one — unlike an
            /// all-zero sentinel, which for a `u64` position is itself a valid-looking value. Never
            /// persisted as a real position.
            pub fn nil() -> Self {
                Self(-1)
            }

            pub fn get(self) -> i64 {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }
    };
}

id_type!(
    StreamId,
    "Identifies a log stream; a stream may contain entries from multiple executions."
);

id_type!(
    EntryId,
    "Identifies a log entry — its monotonic position within a stream (BookKeeper entryId)."
);
