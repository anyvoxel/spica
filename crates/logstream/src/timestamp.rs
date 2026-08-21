use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// A wall-clock timestamp recording when a log [`Entry`](crate::Entry) was created, as
/// milliseconds since `UNIX_EPOCH`.
///
/// This is **audit metadata, not decision input**: the engine's handlers never read it, and replay
/// uses the recorded value rather than regenerating it, so it does not affect the determinism of
/// the decision logic. It is also **not used for ordering** — ordering is by
/// [`Entry::entry_id`](crate::Entry), mirroring BookKeeper (which has no timestamp field) and
/// DistributedLog (where the transaction id is app metadata, not the sort key). A distributed
/// LogStream may re-stamp entries with a hybrid logical clock (HLC) instead of the producer's wall
/// clock.
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

    /// Parse an RFC3339 / ISO 8601 timestamp (e.g. `2016-03-14T01:59:00Z`, or `+08:00`-style
    /// offsets) into an absolute [`Timestamp`]. Returns `None` if `s` is not a valid RFC3339
    /// timestamp.
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

    /// Adds a `Duration` to this timestamp, returning `None` on overflow.
    pub fn checked_add(self, d: Duration) -> Option<Self> {
        self.0.checked_add(d.as_millis() as u64).map(Self)
    }

    /// The wall-clock gap from `earlier` to `self` (i.e. `self - earlier`), floored at zero.
    pub fn saturating_duration_since(self, earlier: Timestamp) -> Duration {
        Duration::from_millis(self.0.saturating_sub(earlier.0))
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
