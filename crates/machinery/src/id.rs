//! The engine's **identity source**, injectable so a run's ids are not a hidden input.
//!
//! Identity is the other half of what makes two runs of one definition differ: with the injected
//! [`Clock`](crate::Clock) the stamps agree, but the `uid` of every flow, version, execution, thread,
//! activity, task and timer is still minted fresh — so the log of the same definition never reads the
//! same twice. [`IdGenerator`] turns that minting into an injected dependency: production keeps
//! [`SystemIdGenerator`] (fresh ULIDs), while a test substitutes [`CountingIdGenerator`] and gets the
//! same ids on every run, which is what lets a test pin a whole entry chain literally instead of
//! masking identities out of it.
//!
//! The trait is deliberately *synchronous and single-method*, for the same reason as [`Clock`]:
//! machinery stays a leaf crate with no runtime dependency. Minting needs no runtime — only a counter
//! or a source of randomness — and a generator that did need one (say, an id service) can still
//! implement this synchronously by caching.

use std::sync::atomic::{AtomicU64, Ordering};

use ulid::Ulid;

/// The engine's source of fresh identities. Injected at assembly so id minting is a controlled input
/// rather than an ambient side effect: an id minted through this seam is the *only* way the engine
/// names a newly created object.
pub trait IdGenerator: Send + Sync {
    /// A fresh, never-before-minted id.
    fn next_ulid(&self) -> Ulid;
}

/// Fresh random ULIDs — the production [`IdGenerator`], and the default when none is injected.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemIdGenerator;

impl IdGenerator for SystemIdGenerator {
    fn next_ulid(&self) -> Ulid {
        Ulid::new()
    }
}

/// A generator the caller can predict: the ids are `1, 2, 3, …`, in mint order, so a run repeats
/// itself exactly and an expected id can be written down.
///
/// A counting ULID's 48-bit time prefix is zero (i.e. 1970) rather than "now". Nothing in the
/// workspace reads that prefix — ULIDs here are opaque identity, not a timestamp — and the prefix is
/// constant within a generator, so the mint order is the order these ids sort in. The ids stay
/// valid, distinct and never-reused, which is all the engine asks of them.
///
/// Two engines booted over one store must be given disjoint ranges (see [`Self::starting_at`]);
/// otherwise each mints the same ids and their objects collide.
#[derive(Debug)]
pub struct CountingIdGenerator {
    next: AtomicU64,
}

impl CountingIdGenerator {
    /// A generator whose first id is `1`.
    pub fn new() -> Self {
        Self::starting_at(1)
    }

    /// A generator whose first id is `first` — for a caller booting several engines over one store,
    /// each needing identities the others will not mint.
    pub fn starting_at(first: u64) -> Self {
        Self {
            next: AtomicU64::new(first),
        }
    }
}

impl Default for CountingIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl IdGenerator for CountingIdGenerator {
    fn next_ulid(&self) -> Ulid {
        Ulid::from(u128::from(self.next.fetch_add(1, Ordering::SeqCst)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    /// The id a counting generator mints at `n`.
    fn at(n: u64) -> Ulid {
        Ulid::from(u128::from(n))
    }

    #[test]
    fn a_counting_generator_is_predictable_and_monotonic() {
        let ids = CountingIdGenerator::new();

        assert_eq!(ids.next_ulid(), at(1));
        assert_eq!(ids.next_ulid(), at(2));
        // The ids sort in mint order — the property a generator derived from the wall clock loses.
        assert!(at(2) > at(1));

        assert_eq!(CountingIdGenerator::starting_at(100).next_ulid(), at(100));
    }

    #[test]
    fn ids_are_distinct_under_concurrent_minting() {
        let ids = Arc::new(CountingIdGenerator::new());
        let minted = Arc::new(std::sync::Mutex::new(Vec::new()));

        let threads: Vec<_> = (0..8)
            .map(|_| {
                let ids = Arc::clone(&ids);
                let minted = Arc::clone(&minted);
                thread::spawn(move || {
                    let batch: Vec<Ulid> = (0..64).map(|_| ids.next_ulid()).collect();
                    minted.lock().expect("not poisoned").extend(batch);
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("every minting thread completes");
        }

        let minted = minted.lock().expect("not poisoned");
        let distinct: std::collections::HashSet<Ulid> = minted.iter().copied().collect();
        assert_eq!(distinct.len(), minted.len(), "a minted id is never reused");
    }

    #[test]
    fn the_system_generator_mints_fresh_ids() {
        let ids = SystemIdGenerator;

        assert_ne!(ids.next_ulid(), ids.next_ulid());
    }
}
