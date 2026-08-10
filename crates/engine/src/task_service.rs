//! External-resource task execution, decoupled from the serial dispatch loop.
//!
//! Invoking a `Task` state's `Resource` is recorded in the stream as the durable fact
//! `Event::TaskActivated`. The physical call is a side effect driven by that fact, not by the
//! command dispatcher: instead of the Processor special-casing `ActivateTask` (which would force an
//! in-dispatch external await), the run loop feeds `TaskActivated` events into [`TaskServiceHandle`];
//! the service dispatches on the `resource` name to a caller-registered [`TaskHandler`], awaits the
//! external call, and routes the outcome — the `Command::CompleteTask` — *back to the run loop* so
//! the log keeps a single writer.
//!
//! This is the direct analogue of [`crate::scheduler::SchedulerHandle`]: where a timer has a
//! deterministic wall-clock deadline owned by the scheduler, a task has a user-defined handler over
//! which the engine has no internal control. Both return a resumption command to the run loop over
//! a channel; both keep the stream strictly ordered while the side effect runs in the background.
//!
//! Each invocation is spawned on its own `tokio::spawn` rather than parked in a queue: there is no
//! deadline to order by, and the causal ordering of *settlements* (not invocations) is what the
//! stream needs — the run loop assigns each `CompleteTask` its entry position when it appends. The
//! spawned call is deliberately **not** aborted on cancel: `TaskCancelled` marks the task
//! `Cancelled` in storage, and the `CompleteTaskHandler` treats a settle for a non-`Active` task as
//! an idempotent no-op — exactly the race guard a `CompleteTimer` after a `CancelTimer` already
//! uses. Aborting a `!Send`/foreign handler mid-await is also not something the engine should
//! attempt.
//!
//! # TODO(M2) — timeouts / heartbeats / retries
//! The service currently awaits `TaskHandler::run` to completion with no deadline. A later
//! milestone must enforce `TaskState.timeout_seconds`/`heartbeat_seconds`
//! (`TaskTimeoutSeconds`/`TaskHeartbeatSeconds`, parsed in `crates/asl/src/task.rs`) as
//! deadline-driven `States.Timeout` / `States.HeartbeatTimeout` terminations, and drive `Retry`
//! re-arming — each routed back to the run loop as a `CompleteTask { Err(…) }` exactly like a
//! handler failure today (see `handlers/states/task.rs`'s TODO and `handlers/complete_task.rs`).
//! A `TaskHandler` may be `Retry`-eligible based on which `ExecutionError` it returns.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::error;

use crate::command::Command;
use crate::error::ExecutionError;
use crate::id::{EntryId, StreamId, TaskId};

/// A caller-registered handler for one `Resource` URI. The engine dispatches a `Task` state's
/// invocation to the handler registered under the state's `Resource`, awaits its `run`, and routes
/// the returned `Result` back to the run loop as `Command::CompleteTask`.
///
/// `Ok(output)` resumes the owning state (its `complete`); `Err(error)` terminates it with the
/// failure — mirroring the `result` carried on [`Command::CompleteTask`].
#[async_trait::async_trait]
pub trait TaskHandler: Send + Sync {
    /// Execute the external call for `resource` with the projected `arguments`. Return `Ok` on
    /// success (the value becoming the task's output / the owning state's result) or `Err` on
    /// failure (the state terminates with that error).
    async fn run(&self, resource: &str, arguments: &Value) -> Result<Value, ExecutionError>;
}

/// A message the run loop pushes into the task service's inbox.
enum TaskInput {
    /// Invoke the handler registered for `resource` with `arguments`, returning the outcome as
    /// `Command::CompleteTask` (enveloped with the `TaskActivated` entry's stream/cause identity).
    Invoke {
        task: TaskId,
        resource: String,
        arguments: Value,
        stream_id: StreamId,
        cause_id: EntryId,
    },
}

/// Clonable handle to a running task-service loop. Each clone shares the same inbox; when every
/// clone is dropped the inbox closes and the loop exits (so no task leaks when the run loop ends).
#[derive(Clone)]
pub struct TaskServiceHandle {
    tx: mpsc::UnboundedSender<TaskInput>,
}

impl TaskServiceHandle {
    /// Spawn the task-service loop, returning a handle. Loop exits when all handles are dropped.
    pub fn spawn(
        handlers: HashMap<String, Arc<dyn TaskHandler>>,
        fire_tx: mpsc::UnboundedSender<(StreamId, EntryId, Command)>,
    ) -> (Self, JoinHandle<()>) {
        // The handler map is moved into the loop so no per-invocation lock is needed; the service
        // is a single loop reading from a channel (mirroring the scheduler's single DelayQueue
        // loop), so a plain HashMap suffices.
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    maybe = rx.recv() => {
                        let Some(input) = maybe else {
                            // All handles dropped — the run loop is done; stop the loop.
                            break;
                        };
                        match input {
                            TaskInput::Invoke { task, resource, arguments, stream_id, cause_id } => {
                                let handler = match handlers.get(&resource) {
                                    Some(h) => Arc::clone(h),
                                    None => {
                                        // No handler registered for this Resource — the task can
                                        // never settle successfully. Route a definitive failure
                                        // through the single writer so the owning state terminates
                                        // (and the task row is swept) rather than hanging.
                                        error!(resource = %resource, task = %task,
                                            "no TaskHandler registered for Resource");
                                        let _ = fire_tx.send((stream_id, cause_id, Command::CompleteTask {
                                            task,
                                            result: Err(ExecutionError::InvalidDefinition(
                                                format!("no TaskHandler registered for Resource '{resource}'"),
                                            )),
                                        }));
                                        continue;
                                    }
                                };
                                // Spawn the external call off the serial loop so the service stays
                                // responsive while the handler awaits; the eventual `CompleteTask`
                                // is routed back to the single writer. Clone the feed per
                                // invocation (the loop may spawn many).
                                let task_id = task;
                                let resource_c = resource.clone();
                                let args = arguments.clone();
                                let fire_tx_c = fire_tx.clone();
                                tokio::spawn(async move {
                                    let result = handler.run(&resource_c, &args).await;
                                    let _ = fire_tx_c.send((stream_id, cause_id, Command::CompleteTask {
                                        task: task_id,
                                        result,
                                    }));
                                });
                            }
                        }
                    }
                }
            }
        });
        let service = TaskServiceHandle { tx };
        (service, handle)
    }

    /// Invoke the handler registered for `resource`, routing its outcome back as `CompleteTask`.
    pub fn invoke(
        &self,
        task: TaskId,
        resource: String,
        arguments: Value,
        stream_id: StreamId,
        cause_id: EntryId,
    ) {
        let _ = self.tx.send(TaskInput::Invoke {
            task,
            resource,
            arguments,
            stream_id,
            cause_id,
        });
    }
}
