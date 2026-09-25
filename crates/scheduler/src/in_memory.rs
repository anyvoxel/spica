//! The M1 in-memory implementation of the [`Scheduler`] contract.
//!
//! The armed timers are recorded in the stream as durable facts (`TimerActivated`); this runtime is
//! the *physical* time side effect driven by those facts. It receives `schedule`/`cancel` calls from
//! the consumer (which re-derives them from the durable timer events), tracks the pending timers as
//! reference → absolute deadline, and on expiry pushes the resumption command
//! (`Command::TriggerTimer`) back to the engine through the injected [`TimerSink`] — never writing to
//! the log itself, so the engine keeps its write/validation boundary.
//!
//! Deadlines are compared against an injected [`Clock`] rather than against elapsed real time: the
//! loop waits out the earliest deadline's *remaining* duration and then re-checks the clock, so with
//! the wall clock it behaves exactly like a timer wheel, while a caller that owns the clock (and
//! moves it) can drive expiries without waiting — see [`Scheduler::tick`].

use std::collections::HashMap;
use std::sync::Arc;

use spica_engine::{ObjectReference, Timestamp};
use spica_machinery::{Clock, SystemClock};
use tokio::sync::mpsc;

use crate::{Scheduler, TimerSink};

/// A message the consumer pushes into the scheduler's inbox.
enum SchedulerInput {
    /// Arm a timer to fire `TriggerTimer` at the absolute `deadline`.
    Schedule {
        timer: ObjectReference,
        deadline: Timestamp,
    },
    /// Cancel a previously-armed timer (a `TimerCancelled` event was applied).
    Cancel { timer: ObjectReference },
    /// The clock was moved; re-evaluate what is due (see [`Scheduler::tick`]). Carried on the inbox
    /// rather than a bare wake primitive so it cannot be lost to a select race — a lost wake would
    /// hang a manually-advanced test.
    Wake,
}

/// Apply one inbox message to the armed set. A re-arm for the same reference replaces the prior
/// deadline (defensive; arms are unique).
fn apply(input: SchedulerInput, armed: &mut HashMap<ObjectReference, Timestamp>) {
    match input {
        SchedulerInput::Schedule { timer, deadline } => {
            armed.insert(timer, deadline);
        }
        SchedulerInput::Cancel { timer } => {
            armed.remove(&timer);
        }
        SchedulerInput::Wake => {}
    }
}

/// The M1 in-process [`Scheduler`]: a single long-lived loop that arms/cancels timers off an inbox
/// and, on expiry, calls the consumer-injected [`TimerSink`] to append the `TriggerTimer` resumption
/// command.
///
/// The **push is into the engine**, not a pull the engine performs: the consumer injects an
/// `Arc<dyn TimerSink>` via [`Scheduler::attach_sink`] after construction, and this impl calls
/// `sink.trigger` on expiry. That keeps the contract a single injectable value
/// (`Arc<dyn Scheduler>`), exactly the shape the storage split established — and mirrors what a
/// distributed executor (a service that already owns its own transport) would look like behind the
/// same trait.
pub struct InMemoryScheduler {
    /// Inbox for `schedule`/`cancel`/`wake`, produced by the consumer.
    tx: mpsc::UnboundedSender<SchedulerInput>,
    /// The consumer-injected write entry, called on expiry. `None` until the consumer attaches it —
    /// which happens *before* any timer is armed, so a fire with no sink is a wiring bug (logged and
    /// skipped rather than writing to the log raw, which the engine owns).
    sink: Arc<std::sync::RwLock<Option<Arc<dyn TimerSink>>>>,
}

impl InMemoryScheduler {
    /// Spawn the scheduler loop against the wall clock — the production default. See
    /// [`Self::spawn_with_clock`] for the loop's contract.
    pub fn spawn() -> Arc<Self> {
        Self::spawn_with_clock(Arc::new(SystemClock))
    }

    /// Spawn the scheduler loop and return an `Arc`-wrapped scheduler, deciding expiry from `clock`.
    ///
    /// The loop's lifetime tracks `self`: once this `Arc` is dropped (the engine is done with the
    /// scheduler), the inbox closes and the loop drains and exits — no task leaks past the engine's
    /// use. The engine injects its [`TimerSink`] afterwards via [`Scheduler::attach_sink`] (called
    /// from `start()` before any timer is armed). Requires a Tokio runtime context (the loop runs on
    /// `tokio::spawn`), which the engine's [`tokio::main`] / `#[tokio::test]` provides.
    pub fn spawn_with_clock(clock: Arc<dyn Clock>) -> Arc<Self> {
        let (tx, mut input_rx) = mpsc::unbounded_channel();
        // Shared sink slot: one handle stays in `Arc<Self>` for `attach_sink`, a clone goes to the
        // loop so it can call `trigger` on expiry.
        let sink: Arc<std::sync::RwLock<Option<Arc<dyn TimerSink>>>> =
            Arc::new(std::sync::RwLock::new(None));
        let loop_sink = Arc::clone(&sink);
        let loop_clock = Arc::clone(&clock);
        tokio::spawn(async move {
            // Armed timers as reference -> deadline. A flat map rather than a delay queue: the queue
            // would key its waiting on real elapsed time, which is precisely what an injected clock
            // must be able to disagree with. The scan below is over a handful of live timers.
            let mut armed: HashMap<ObjectReference, Timestamp> = HashMap::new();
            loop {
                // Drain the inbox first, so a cancel that raced its own deadline is honoured before
                // the fire below rather than after it.
                let mut closed = false;
                loop {
                    match input_rx.try_recv() {
                        Ok(input) => apply(input, &mut armed),
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            closed = true;
                            break;
                        }
                    }
                }
                if closed {
                    // All handles dropped — the engine is done with this scheduler; stop the loop.
                    break;
                }

                // Fire everything the clock says is due. Cloning the references out first keeps the
                // map unborrowed while each trigger is awaited.
                let now = loop_clock.now();
                let due: Vec<ObjectReference> = armed
                    .iter()
                    .filter(|(_, deadline)| **deadline <= now)
                    .map(|(timer, _)| timer.clone())
                    .collect();
                for timer in due {
                    armed.remove(&timer);
                    // Push the fired timer's resumption into the engine's controlled write entry
                    // (the engine validates + appends the `TriggerTimer`). Clone the sink out of the
                    // lock to drop the guard before awaiting — the engine appends on its own log,
                    // which must not be blocked by this scheduler's read lock.
                    let sink = loop_sink
                        .read()
                        .expect("scheduler sink lock is not poisoned")
                        .clone();
                    match sink {
                        Some(sink) => sink.trigger(&timer).await,
                        None => {
                            // The engine should always attach a sink before arming a timer; a fire
                            // here means the wiring is broken. We must NOT write to the log raw (the
                            // engine owns that path), so this fire is dropped.
                            tracing::warn!(
                                timer = %timer,
                                "timer fired before the engine attached a sink; trigger dropped"
                            );
                        }
                    }
                }

                // Sleep out the earliest remaining deadline's remaining duration (nothing armed means
                // nothing to wait for — the inbox is then the only wake-up). Re-checking the clock
                // after the sleep is what makes this correct under either clock: a real sleep lands
                // at or after its deadline, and a manually-advanced clock is met by the `Wake`.
                let next = armed.values().copied().min();
                match next {
                    Some(deadline) => {
                        tokio::select! {
                            input = input_rx.recv() => {
                                let Some(input) = input else { break };
                                apply(input, &mut armed);
                            }
                            _ = tokio::time::sleep(deadline.saturating_duration_since(loop_clock.now())) => {}
                        }
                    }
                    None => match input_rx.recv().await {
                        Some(input) => apply(input, &mut armed),
                        None => break,
                    },
                }
            }
        });
        Arc::new(Self { tx, sink })
    }
}

#[async_trait::async_trait]
impl Scheduler for InMemoryScheduler {
    fn attach_sink(&self, sink: Arc<dyn TimerSink>) {
        // Replacing any prior sink (defensive; the engine attaches exactly once at boot).
        *self
            .sink
            .write()
            .expect("scheduler sink lock is not poisoned") = Some(sink);
    }

    fn schedule(&self, timer: &ObjectReference, deadline: Timestamp) {
        let _ = self.tx.send(SchedulerInput::Schedule {
            timer: timer.clone(),
            deadline,
        });
    }

    fn cancel(&self, timer: &ObjectReference) {
        let _ = self.tx.send(SchedulerInput::Cancel {
            timer: timer.clone(),
        });
    }

    fn tick(&self) {
        let _ = self.tx.send(SchedulerInput::Wake);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spica_engine::{ObjectKind, PlainName};
    use spica_machinery::ManualClock;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::time::sleep;
    use ulid::Ulid;

    fn timer() -> ObjectReference {
        let uid = Ulid::new();
        ObjectReference::new(
            ObjectKind::Timer,
            PlainName::new("child")
                .expect("static literal is a valid segment")
                .generated_from_key(uid.0 as u64),
            uid,
        )
    }

    /// An absolute deadline `from` now by `offset`.
    fn deadline_after(offset: Duration) -> Timestamp {
        // `now + offset` can't overflow at these test scales; `checked_add` keeps the arithmetic
        // explicit (the type has no `Add` impl — future timestamps are built via checked add).
        Timestamp::now()
            .checked_add(offset)
            .expect("now + a small offset never overflows")
    }

    /// A test [`TimerSink`] that records the triggers it receives into a shared `Vec`, so tests can
    /// observe the expiry callback without a real log. Mirrors how the engine connects its sink.
    #[derive(Clone, Default)]
    struct RecordingSink {
        triggered: Arc<Mutex<Vec<ObjectReference>>>,
    }

    impl RecordingSink {
        fn new() -> Self {
            Self::default()
        }
        /// Snapshot of every timer this sink has been asked to trigger.
        fn snaps(&self) -> Vec<ObjectReference> {
            self.triggered.lock().unwrap().clone()
        }

        /// Wait (bounded) for `count` triggers to land — the loop runs on a background task, so a
        /// manual-clock test still has to let that task run before it can assert.
        async fn wait_for(&self, count: usize) -> Vec<ObjectReference> {
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    let fired = self.snaps();
                    if fired.len() >= count {
                        return fired;
                    }
                    sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("the expected triggers reach the sink")
        }
    }

    #[async_trait::async_trait]
    impl TimerSink for RecordingSink {
        async fn trigger(&self, timer: &ObjectReference) {
            self.triggered.lock().unwrap().push(timer.clone());
        }
    }

    /// A window in which a scheduler keyed on *real* elapsed time would still be waiting: the
    /// virtual advance below skips minutes, so this real pause cannot let a real-time implementation
    /// fire — it only gives the loop room to have behaved wrongly. A wrong implementation fails
    /// here; a right one is silent by construction.
    const NOT_ENOUGH_REAL_TIME: Duration = Duration::from_millis(20);

    #[tokio::test]
    async fn fires_trigger_timer_after_deadline() {
        let s = InMemoryScheduler::spawn();
        let sink = RecordingSink::new();
        s.attach_sink(Arc::new(sink.clone()));

        let timer = timer();
        s.schedule(&timer, deadline_after(Duration::from_millis(30)));

        // Poll the recording sink until the trigger lands (it arrives on a background loop).
        let fired = sink.wait_for(1).await;
        assert_eq!(fired, vec![timer]);
    }

    #[tokio::test]
    async fn cancel_suppresses_a_pending_timer() {
        let s = InMemoryScheduler::spawn();
        let sink = RecordingSink::new();
        s.attach_sink(Arc::new(sink.clone()));

        let timer = timer();
        s.schedule(&timer, deadline_after(Duration::from_millis(15)));
        s.cancel(&timer);

        // Allow enough time that a leaked arm would definitely have fired, then assert silence.
        sleep(Duration::from_millis(60)).await;
        assert!(
            sink.snaps().is_empty(),
            "a cancelled timer must not reach the sink"
        );
    }

    #[tokio::test]
    async fn a_clock_advance_fires_a_timer_without_waiting_for_it() {
        // The whole point of the injected clock: a 60-second deadline fires after a virtual advance,
        // in no real time at all.
        let clock = Arc::new(ManualClock::new(Timestamp::from_millis(1_000)));
        let s = InMemoryScheduler::spawn_with_clock(Arc::clone(&clock) as Arc<dyn Clock>);
        let sink = RecordingSink::new();
        s.attach_sink(Arc::new(sink.clone()));

        let timer = timer();
        let deadline = clock
            .now()
            .checked_add(Duration::from_secs(60))
            .expect("a minute from the epoch never overflows");
        s.schedule(&timer, deadline);

        // Not due yet: half the wait passes, and the timer stays armed.
        clock.advance(Duration::from_secs(30));
        s.tick();
        sleep(NOT_ENOUGH_REAL_TIME).await;
        assert!(
            sink.snaps().is_empty(),
            "a timer must not fire before the clock reaches its deadline"
        );

        // The rest of the wait, in one step, with no real waiting.
        clock.advance(Duration::from_secs(30));
        s.tick();
        assert_eq!(sink.wait_for(1).await, vec![timer]);
    }

    #[tokio::test]
    async fn one_advance_fires_every_deadline_it_passed() {
        let clock = Arc::new(ManualClock::new(Timestamp::from_millis(1_000)));
        let s = InMemoryScheduler::spawn_with_clock(Arc::clone(&clock) as Arc<dyn Clock>);
        let sink = RecordingSink::new();
        s.attach_sink(Arc::new(sink.clone()));

        let (first, second, beyond) = (timer(), timer(), timer());
        let at = |secs: u64| {
            clock
                .now()
                .checked_add(Duration::from_secs(secs))
                .expect("a small offset never overflows")
        };
        s.schedule(&first, at(10));
        s.schedule(&second, at(20));
        s.schedule(&beyond, at(120));

        // Jump past two of the three: both fire, and the third stays armed.
        clock.advance(Duration::from_secs(30));
        s.tick();
        let fired: HashSet<ObjectReference> = sink.wait_for(2).await.into_iter().collect();
        assert_eq!(
            fired,
            HashSet::from([first, second]),
            "every deadline the advance passed fires exactly once"
        );

        sleep(NOT_ENOUGH_REAL_TIME).await;
        assert_eq!(
            sink.snaps().len(),
            2,
            "a deadline the advance did not reach stays armed"
        );
    }

    #[tokio::test]
    async fn a_cancel_that_raced_its_deadline_suppresses_the_fire() {
        let clock = Arc::new(ManualClock::new(Timestamp::from_millis(1_000)));
        let s = InMemoryScheduler::spawn_with_clock(Arc::clone(&clock) as Arc<dyn Clock>);
        let sink = RecordingSink::new();
        s.attach_sink(Arc::new(sink.clone()));

        let timer = timer();
        let deadline = clock
            .now()
            .checked_add(Duration::from_secs(60))
            .expect("a minute from the epoch never overflows");
        s.schedule(&timer, deadline);
        // Cancelled in the same instant the clock reaches the deadline: the cancel was queued first,
        // so the loop must honour it instead of firing.
        s.cancel(&timer);
        clock.advance(Duration::from_secs(60));
        s.tick();

        sleep(NOT_ENOUGH_REAL_TIME).await;
        assert!(
            sink.snaps().is_empty(),
            "a cancel queued before the deadline must beat the fire"
        );
    }
}
