//! Timer scheduling, decoupled from the serial dispatch loop.
//!
//! Arming a timer (`TimerActivated`) is recorded in the stream as a durable fact. The actual
//! *physical* timing is a side effect driven by that fact, not by the command dispatcher: instead
//! of the Processor special-casing `ActivateTimer` (which previously forced an in-dispatch
//! `sleep`/spawn), the run loop feeds `TimerActivated`/`TimerCancelled` events into
//! [`SchedulerHandle`]. The scheduler owns a single long-lived [`DelayQueue`] and, on expiry,
//! routes a `CompleteTimer` *back to the run loop* so the log keeps a single writer.
//!
//! Keeping the timer loop separate from dispatch means an external `TerminateExecution` (or another
//! timer) flows through the stream while a `Wait`'s timer is pending — the earlier deadlock where an
//! inline `sleep` blocked the whole stream can't recur.
//!
//! The `CompleteTimer` the scheduler emits is routed back to the run loop (the single writer) and
//! appended there; the log assigns its `entry_id` at append time (positions no longer come from a
//! shared `IdSource`), so the re-enveloping write needs no counter bookkeeping.

use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::time::DelayQueue;
use tokio_util::time::delay_queue::Key;

use crate::command::Command;
use crate::id::{EntryId, StreamId, TimerId};
use crate::log::Timestamp;

/// Envelope context a fired timer needs, captured when it was armed.
#[derive(Debug, Clone)]
struct PendingTimer {
    timer: TimerId,
    /// Stream the `CompleteTimer` belongs to (same as the `TimerActivated` entry's stream).
    stream_id: StreamId,
    /// Causal link back to the `TimerActivated` entry that armed it.
    cause_id: EntryId,
}

/// A message the run loop pushes into the scheduler's inbox.
enum SchedulerInput {
    /// Arm a timer to fire `CompleteTimer` at the absolute `deadline`.
    Schedule {
        timer: TimerId,
        deadline: Timestamp,
        stream_id: StreamId,
        cause_id: EntryId,
    },
    /// Cancel a previously-armed timer (a `TimerCancelled` event was applied).
    Cancel { timer: TimerId },
}

/// Clonable handle to a running scheduler loop. Each clone shares the same inbox; when every clone
/// is dropped the inbox closes and the loop exits (so no task leaks when the run loop ends).
#[derive(Clone)]
pub struct SchedulerHandle {
    tx: mpsc::UnboundedSender<SchedulerInput>,
}

impl SchedulerHandle {
    /// Spawn the scheduler loop, returning a handle. Loop exits when all handles are dropped.
    pub fn spawn(
        fire_tx: mpsc::UnboundedSender<(StreamId, EntryId, Command)>,
    ) -> (Self, JoinHandle<()>) {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(async move {
            let mut queue: DelayQueue<PendingTimer> = DelayQueue::new();
            // timer-id -> DelayQueue key, for cancelling a pending timer by id.
            let mut by_id: HashMap<TimerId, Key> = HashMap::new();

            loop {
                tokio::select! {
                    maybe = rx.recv() => {
                        let Some(input) = maybe else {
                            // All handles dropped — the run loop is done; stop the loop.
                            break;
                        };
                        match input {
                            SchedulerInput::Schedule { timer, deadline, stream_id, cause_id } => {
                                // Replace any prior arm for the same id (defensive; arms are unique).
                                if let Some(old) = by_id.remove(&timer) {
                                    queue.remove(&old);
                                }
                                // Derive the wait from the persisted absolute deadline; a deadline
                                // already in the past fires immediately (saturating to zero).
                                let wait = deadline.saturating_duration_since(Timestamp::now());
                                let key = queue.insert(
                                    PendingTimer { timer, stream_id, cause_id },
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
                        let PendingTimer { timer, stream_id, cause_id } = expiration.into_inner();
                        by_id.remove(&timer);
                        // Route the fired timer's completion back to the run loop (the single
                        // writer), which appends it with a freshly allocated entry_id.
                        let _ = fire_tx.send((stream_id, cause_id, Command::CompleteTimer { timer }));
                    }
                }
            }
        });
        let scheduler = SchedulerHandle { tx };
        (scheduler, handle)
    }

    /// Arm `timer` to fire `CompleteTimer` at the absolute `deadline`.
    pub fn schedule(
        &self,
        timer: TimerId,
        deadline: Timestamp,
        stream_id: StreamId,
        cause_id: EntryId,
    ) {
        let _ = self.tx.send(SchedulerInput::Schedule {
            timer,
            deadline,
            stream_id,
            cause_id,
        });
    }

    /// Cancel a pending `timer`.
    pub fn cancel(&self, timer: TimerId) {
        let _ = self.tx.send(SchedulerInput::Cancel { timer });
    }
}
