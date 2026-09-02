//! The M1 in-memory implementation of [`Scheduler`](spica_engine::Scheduler).
//!
//! The armed timers are recorded in the stream as durable facts (`TimerActivated`); this runtime is
//! the *physical* wall-clock side effect driven by those facts. It receives `schedule`/`cancel`
//! calls from the engine's run loop, tracks pending timers in a single long-lived [`DelayQueue`],
//! and on expiry pushes the resumption command (`Command::TriggerTimer`) back to the engine through
//! the [`TimerSink`](spica_engine::TimerSink) the engine injects at boot — never writing to the log
//! itself, so the engine keeps its write/validation boundary.

use std::collections::HashMap;
use std::sync::Arc;

use spica_engine::{EntryId, ObjectReference, Scheduler, TimerSink, Timestamp};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_util::time::DelayQueue;
use tokio_util::time::delay_queue::Key;

/// A message the engine pushes into the scheduler's inbox.
enum SchedulerInput {
    /// Arm a timer to fire `TriggerTimer` at the absolute `deadline`.
    Schedule {
        timer: ObjectReference,
        deadline: Timestamp,
        cause_id: EntryId,
    },
    /// Cancel a previously-armed timer (a `TimerCancelled` event was applied).
    Cancel { timer: ObjectReference },
}

/// Envelope context a fired timer needs, captured when it was armed.
#[derive(Debug, Clone)]
struct PendingTimer {
    timer: ObjectReference,
    /// Causal link back to the `TimerActivated` entry that armed it.
    cause_id: EntryId,
}

/// The M1 in-process [`Scheduler`]: a single long-lived [`DelayQueue`] loop that arms/cancels
/// timers off an inbox and, on expiry, calls the engine-injected [`TimerSink`] to append the
/// `TriggerTimer` resumption command.
///
/// The **push is into the engine**, not a pull the engine performs: the engine injects an
/// `Arc<dyn spica_engine::TimerSink>` via [`Scheduler::attach_sink`] at boot, and this impl calls
/// `sink.trigger` on expiry. That keeps the contract a single injectable value
/// (`Arc<dyn spica_engine::Scheduler>`), exactly the shape the storage split established — and
/// mirrors what a distributed executor (a service that already owns its own transport) would look
/// like behind the same trait.
pub struct InMemoryScheduler {
    /// Inbox for `schedule`/`cancel`, produced by the engine clones.
    tx: mpsc::UnboundedSender<SchedulerInput>,
    /// The engine-injected write entry, called on expiry. `None` until the engine boots — but the
    /// engine attaches it *before* the run loop arms any timer, so a fire with no sink is a wiring
    /// bug (logged and skipped rather than writing to the log raw, which the engine owns).
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
                            SchedulerInput::Schedule { timer, deadline, cause_id } => {
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
                                    PendingTimer { timer: timer.clone(), cause_id },
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
                        let PendingTimer { timer, cause_id } = expiration.into_inner();
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
                            Some(sink) => sink.trigger(&timer, cause_id).await,
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

    fn schedule(&self, timer: &ObjectReference, deadline: Timestamp, cause_id: EntryId) {
        let _ = self.tx.send(SchedulerInput::Schedule {
            timer: timer.clone(),
            deadline,
            cause_id,
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
    use spica_engine::ObjectKind;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::time::sleep;
    use ulid::Ulid;

    fn timer() -> ObjectReference {
        ObjectReference::for_uid(ObjectKind::Timer, Ulid::new())
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
        triggered: Arc<Mutex<Vec<(ObjectReference, EntryId)>>>,
    }

    impl RecordingSink {
        fn new() -> Self {
            Self::default()
        }
        /// Snapshot of every `(timer, cause_id)` this sink has been asked to trigger.
        fn snaps(&self) -> Vec<(ObjectReference, EntryId)> {
            self.triggered.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl TimerSink for RecordingSink {
        async fn trigger(&self, timer: &ObjectReference, cause_id: EntryId) {
            self.triggered
                .lock()
                .unwrap()
                .push((timer.clone(), cause_id));
        }
    }

    #[tokio::test]
    async fn fires_trigger_timer_after_deadline() {
        let s = InMemoryScheduler::spawn();
        let sink = RecordingSink::new();
        s.attach_sink(Arc::new(sink.clone()));

        let timer = timer();
        let cause = EntryId::new(1);
        s.schedule(&timer, deadline_after(Duration::from_millis(30)), cause);

        // Poll the recording sink until the trigger lands (it arrives on a background loop).
        let fired = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some((t, c)) = sink.snaps().into_iter().next() {
                    return (t, c);
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a fired timer reaches the sink");
        assert_eq!(fired.1, cause);
        assert_eq!(fired.0, timer);
    }

    #[tokio::test]
    async fn cancel_suppresses_a_pending_timer() {
        let s = InMemoryScheduler::spawn();
        let sink = RecordingSink::new();
        s.attach_sink(Arc::new(sink.clone()));

        let timer = timer();
        s.schedule(
            &timer,
            deadline_after(Duration::from_millis(15)),
            EntryId::new(2),
        );
        s.cancel(&timer);

        // Allow enough time that a leaked arm would definitely have fired, then assert silence.
        sleep(Duration::from_millis(60)).await;
        assert!(
            sink.snaps().is_empty(),
            "a cancelled timer must not reach the sink"
        );
    }
}
