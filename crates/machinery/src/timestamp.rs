use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// A wall-clock timestamp recording when a log entry was created, as milliseconds since
/// `UNIX_EPOCH`.
///
/// This is **audit metadata, not decision input** when used on a log entry: the engine's handlers
/// never read the entry's stamp, and replay uses the recorded value rather than regenerating it, so
/// it does not affect the determinism of the decision logic. It is also **not used for ordering** —
/// ordering is by entry id, mirroring BookKeeper (which has no timestamp field) and DistributedLog
/// (where the transaction id is app metadata, not the sort key). A distributed log may re-stamp
/// entries with a hybrid logical clock (HLC) instead of the producer's wall clock.
///
/// Beyond the log seam, `Timestamp` is also the engine's shared **domain time primitive**
/// (`ObjectMeta` timestamps, deadlines, leases, retry backoff), which is exactly why it lives in
/// [`crate`] — the low-level leaf every crate shares — rather than in the log crate that happens to
/// stamp entries with it.
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
        // `as_millis()` is a `u128`, so narrowing it with `as u64` would *fold it modulo 2^64*:
        // a span the clock cannot hold would come back as a made-up nearby instant rather than as
        // overflow. `try_from` keeps the promise the signature already makes, for both operands —
        // the span has to be representable, and the sum has to fit.
        let millis = u64::try_from(d.as_millis()).ok()?;
        self.0.checked_add(millis).map(Self)
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::Timestamp;

    #[test]
    fn an_ordinary_span_adds() {
        assert_eq!(
            Timestamp::from_millis(1_000).checked_add(Duration::from_secs(2)),
            Some(Timestamp::from_millis(3_000))
        );
    }

    /// The two operands' ranges meet at `u64::MAX` milliseconds, so a span that is itself perfectly
    /// representable can still overflow the sum — the boundary is the sum, not either side.
    #[test]
    fn a_representable_span_can_still_overflow_the_sum() {
        let end = Timestamp::from_millis(u64::MAX);
        assert_eq!(end.checked_add(Duration::from_millis(0)), Some(end));
        assert_eq!(end.checked_add(Duration::from_millis(1)), None);
    }

    /// A span the millisecond count cannot hold is overflow, not a wrap: narrowing the `u128` from
    /// `as_millis()` folds it modulo 2^64, so a caller asking for something the clock cannot express
    /// would be handed a made-up nearby instant as a *successful* deadline instead of `None`. This
    /// span is a little over half a billion years; it folds to 384 ms, which is what makes the
    /// difference between "refused" and "granted at `now + 384ms`" invisible to the caller.
    #[test]
    fn a_span_the_millisecond_count_cannot_hold_is_overflow() {
        let absurd = Duration::from_secs(u64::MAX / 1000 + 1);
        assert_eq!(Timestamp::from_millis(1_000).checked_add(absurd), None);
    }
}
