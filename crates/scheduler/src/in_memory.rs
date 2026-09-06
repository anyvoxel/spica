//! The M1 in-memory implementation of the [`Scheduler`] contract.
//!
//! The armed timers are recorded in the stream as durable facts (`TimerActivated`); this runtime is
//! the *physical* wall-clock side effect driven by those facts. It receives `schedule`/`cancel`
//! calls from the consumer (which re-derives them from the durable timer events), tracks pending
//! timers in a single long-lived [`DelayQueue`], and on expiry pushes the resumption command
//! (`Command::TriggerTimer`) back to the engine through the injected [`TimerSink`] — never writing to
//! the log itself, so the engine keeps its write/validation boundary.

use std::collections::HashMap;
use std::sync::Arc;

use spica_engine::{ObjectReference, Timestamp};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_util::time::DelayQueue;
use tokio_util::time::delay_queue::Key;

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
}

/// Envelope context a fired timer needs, captured when it was armed.
#[derive(Debug, Clone)]
struct PendingTimer {
    timer: ObjectReference,
}

/// The M1 in-process [`Scheduler`]: a single long-lived [`DelayQueue`] loop that arms/cancels
/// timers off an inbox and, on expiry, calls the consumer-injected [`TimerSink`] to append the
/// `TriggerTimer` resumption command.
///
/// The **push is into the engine**, not a pull the engine performs: the consumer injects an
/// `Arc<dyn TimerSink>` via [`Scheduler::attach_sink`] after construction, and this impl calls
/// `sink.trigger` on expiry. That keeps the contract a single injectable value
/// (`Arc<dyn Scheduler>`), exactly the shape the storage split established — and mirrors what a
/// distributed executor (a service that already owns its own transport) would look like behind the
/// same trait.
pub struct InMemoryScheduler {
    /// Inbox for `schedule`/`cancel`, produced by the consumer.
    tx: mpsc::UnboundedSender<SchedulerInput>,
    /// The consumer-injected write entry, called on expiry. `None` until the consumer attaches it —
    /// which happens *before* any timer is armed, so a fire with no sink is a wiring bug (logged and
    /// skipped rather than writing to the log raw, which the engine owns).
    sink: Arc<std::sync::RwLock<Option<Arc<dyn TimerSink>>>>,
}

impl InMemoryScheduler {
    /// Spawn the scheduler loop and return an `Arc`-wrapped scheduler.
    ///
    /// The loop's lifetime tracks `self`: once this `Arc` is dropped (the engine is done with the
    /// scheduler), the inbox closes and the loop drains and exits — no task leaks past the engine's
    /// use. The engine injects its [`TimerSink`] afterwards via [`Scheduler::attach_sink`] (called
    /// from `start()` before any timer is armed). Requires a Tokio runtime context (the loop runs on
    /// `tokio::spawn`), which the engine's [`tokio::main`] / `#[tokio::test]` provides.
    pub fn spawn() -> Arc<Self> {
        let (tx, mut input_rx) = mpsc::unbounded_channel();
        // Shared sink slot: one handle stays in `Arc<Self>` for `attach_sink`, a clone goes to the
        // loop so it can call `trigger` on expiry.
        let sink: Arc<std::sync::RwLock<Option<Arc<dyn TimerSink>>>> =
            Arc::new(std::sync::RwLock::new(None));
        let loop_sink = Arc::clone(&sink);
        tokio::spawn(async move {
            let mut queue: DelayQueue<PendingTimer> = DelayQueue::new();
            // timer-reference -> DelayQueue key, for cancelling a pending timer by reference.
            let mut by_id: HashMap<ObjectReference, Key> = HashMap::new();
            loop {
                tokio::select! {
                    maybe = input_rx.recv() => {
                        let Some(input) = maybe else {
                            // All handles dropped — the engine is done with this scheduler; stop the
                            // loop.
                            break;
                        };
                        match input {
                            SchedulerInput::Schedule { timer, deadline } => {
                                // Replace any prior arm for the same reference (defensive; arms are
                                // unique).
                                if let Some(old) = by_id.remove(&timer) {
                                    queue.remove(&old);
                                }
                                // Derive the wait from the persisted absolute deadline; a deadline
                                // already in the past fires immediately (saturating to zero).
                                let wait = deadline.saturating_duration_since(Timestamp::now());
                                // Clone the reference into the queue entry; the original is the
                                // cancellation key recorded in `by_id` (ObjectReference, unlike the
                                // former Copy TimerId, is not Copy).
                                let key = queue.insert(
                                    PendingTimer { timer: timer.clone() },
                                    wait,
                                );
                                by_id.insert(timer, key);
                            }
                            SchedulerInput::Cancel { timer } => {
                                if let Some(key) = by_id.remove(&timer) {
                                    queue.remove(&key);
                                }
                            }
                        }
                    }
                    Some(expiration) = queue.next() => {
                        let PendingTimer { timer } = expiration.into_inner();
                        by_id.remove(&timer);
                        // Push the fired timer's resumption into the engine's controlled write entry
                        // (the engine validates + appends the `TriggerTimer`). Clone the sink out of
                        // the lock to drop the guard before awaiting — the engine appends on its own
                        // log, which must not be blocked by this scheduler's read lock.
                        let sink = loop_sink
                            .read()
                            .expect("scheduler sink lock is not poisoned")
                            .clone();
                        match sink {
                            Some(sink) => sink.trigger(&timer).await,
                            None => {
                                // The engine should always attach a sink before arming a timer; a
                                // fire here means the wiring is broken. We must NOT write to the log
                                // raw (the engine owns that path), so this fire is dropped.
                                tracing::warn!(
                                    timer = %timer,
                                    "timer fired before the engine attached a sink; trigger dropped"
                                );
                            }
                        }
                    }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use spica_engine::{ObjectKind, PlainName};
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
    }

    #[async_trait::async_trait]
    impl TimerSink for RecordingSink {
        async fn trigger(&self, timer: &ObjectReference) {
            self.triggered.lock().unwrap().push(timer.clone());
        }
    }

    #[tokio::test]
    async fn fires_trigger_timer_after_deadline() {
        let s = InMemoryScheduler::spawn();
        let sink = RecordingSink::new();
        s.attach_sink(Arc::new(sink.clone()));

        let timer = timer();
        s.schedule(&timer, deadline_after(Duration::from_millis(30)));

        // Poll the recording sink until the trigger lands (it arrives on a background loop).
        let fired = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(t) = sink.snaps().into_iter().next() {
                    return t;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a fired timer reaches the sink");
        assert_eq!(fired, timer);
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
}
