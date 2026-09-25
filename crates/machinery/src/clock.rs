//! The engine's **time source**, injectable so the wall clock is not a hidden input.
//!
//! Every deadline, lease, and backoff gate the engine decides on, and every stamp it writes, is
//! derived from a reading of *now* — which made "testing a `Wait` of 60 seconds" mean waiting 60
//! seconds, and made two runs of the same definition produce different logs. [`Clock`] turns that
//! reading into an injected dependency: production keeps [`SystemClock`] (the wall clock), while a
//! test substitutes [`ManualClock`] and advances time itself, so a run's timing becomes as
//! deterministic as its ids and a timeout can be exercised without being waited out.
//!
//! The trait is deliberately *synchronous and read-only* (one `now()`): machinery stays a leaf crate
//! with no runtime dependency, and *waiting* — which does need a runtime — belongs to whoever drives
//! the wait (the scheduler), not to the notion of "what time is it".

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::timestamp::Timestamp;

/// The engine's source of "now". Injected at assembly so the wall clock is replaceable rather than
/// ambient: a reading taken through this seam is the *only* legitimate way the engine learns the
/// time, which is what makes time a controllable input instead of an untestable side effect.
pub trait Clock: Send + Sync {
    /// The current time, as seen by this clock.
    fn now(&self) -> Timestamp;
}

/// The wall clock — the production [`Clock`], and the default when none is injected.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp::now()
    }
}

/// A clock the caller sets. Time does not move on its own: [`advance`](Self::advance) /
/// [`set`](Self::set) are the only things that change the reading, so a test drives a `Wait` expiry,
/// a lease lapse, or a retry backoff to its boundary exactly — no sleeping, and no slack for the
/// machine's speed to widen or narrow.
///
/// The reading is a plain atomic, so the engine's tasks read it without coordination; advancing it
/// is *not* a schedule event, and a timer armed against it fires only once its owner re-evaluates the
/// pending deadlines (see `spica_scheduler`).
#[derive(Debug, Default)]
pub struct ManualClock {
    millis: AtomicU64,
}

impl ManualClock {
    /// A clock reading `at` (and staying there until advanced).
    pub fn new(at: Timestamp) -> Self {
        Self {
            millis: AtomicU64::new(at.as_millis()),
        }
    }

    /// Move the reading forward by `by`, returning the new reading. Advancing is monotonic by
    /// construction — the engine's causal reading of time (a stamp never precedes its cause) holds
    /// without the caller having to preserve it.
    pub fn advance(&self, by: Duration) -> Timestamp {
        let next = Timestamp::from_millis(
            self.millis
                .load(Ordering::SeqCst)
                .saturating_add(by.as_millis() as u64),
        );
        self.set(next);
        next
    }

    /// Jump the reading to `at` (for driving time to an absolute instant, e.g. an RFC3339 deadline).
    pub fn set(&self, at: Timestamp) {
        self.millis.store(at.as_millis(), Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(self.millis.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_manual_clock_only_moves_when_moved() {
        let clock = ManualClock::new(Timestamp::from_millis(1_000));

        assert_eq!(clock.now(), Timestamp::from_millis(1_000));
        assert_eq!(clock.now(), clock.now(), "reading is side-effect free");

        assert_eq!(
            clock.advance(Duration::from_millis(500)),
            Timestamp::from_millis(1_500)
        );
        assert_eq!(clock.now(), Timestamp::from_millis(1_500));

        clock.set(Timestamp::from_millis(42));
        assert_eq!(clock.now(), Timestamp::from_millis(42));
    }

    #[test]
    fn the_system_clock_reads_the_wall_clock() {
        let clock = SystemClock;
        let before = Timestamp::now();
        let read = clock.now();

        assert!(read >= before, "the wall clock does not run backwards");
    }
}
