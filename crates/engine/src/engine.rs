use serde_json::Value;
use spica_asl::StateMachine;

use crate::command::Command;
use crate::error::ExecutionError;
use crate::id::{ExecutionId, IdSource, StreamId};
use crate::log::{Entry, EntryPayload, LogStream, Timestamp};
use crate::processor::Processor;

/// Executes ASL state machines via the CCES architecture (Causal Command Event Sourcing).
///
/// `Engine::submit` appends a `StartExecution` command to the log and returns the `ExecutionId`.
/// The caller then runs `Processor::run` (indefinite service loop) and separately tails the
/// [`LogStream`](crate::LogStream) to observe when the target execution reaches a terminal state.
pub struct Engine;

impl Engine {
    pub fn new() -> Self {
        Engine
    }

    /// Appends a `CreateExecution` command to `logstream` and returns the new `ExecutionId` plus the
    /// `StreamId` the execution's entries live on. Keep the `StreamId`: it is the address a later
    /// [`Engine::terminate`] needs to append to the same stream.
    ///
    /// Non-blocking — the caller is responsible for running the `Processor` and observing completion
    /// (e.g. by tailing the stream for `ExecutionCompleted` / `ExecutionTerminated`).
    pub async fn submit(
        input: Value,
        logstream: &impl LogStream,
        ids: &mut IdSource,
    ) -> Result<(ExecutionId, StreamId), ExecutionError> {
        let stream_id = ids.next_stream();
        let execution_id = ExecutionId::new();
        logstream
            .append(vec![Entry {
                stream_id,
                entry_id: crate::id::EntryId::nil(), // placeholder — the log assigns the position.
                cause_id: None,
                timestamp: Timestamp::now(),
                payload: EntryPayload::Command(Command::CreateExecution {
                    id: execution_id,
                    input,
                }),
            }])
            .await?;
        Ok((execution_id, stream_id))
    }

    /// Aborts a running execution by appending `TerminateExecution{Cancelled}` to `stream_id` (the
    /// same stream returned by [`Engine::submit`]). The Processor drives the unwind:
    /// `ExecutionTerminating`, then child cleanup, then `ExecutionTerminated{Cancelled}`.
    pub async fn terminate(
        execution_id: ExecutionId,
        stream_id: StreamId,
        logstream: &impl LogStream,
        ids: &mut IdSource,
    ) -> Result<(), ExecutionError> {
        let _ = ids; // retained for API symmetry; entry positions are log-assigned.
        logstream
            .append(vec![Entry {
                stream_id,
                entry_id: crate::id::EntryId::nil(), // placeholder — the log assigns the position.
                cause_id: None,
                timestamp: Timestamp::now(),
                payload: EntryPayload::Command(Command::TerminateExecution {
                    id: execution_id,
                    reason: crate::command::TerminationReason::Cancelled,
                }),
            }])
            .await?;
        Ok(())
    }

    /// Convenience: submits `sm` with `input`, runs the `Processor` on a spawned task, and tails
    /// the stream until the execution reaches a terminal state. Returns the result.
    ///
    /// This is an M1 convenience for tests and the CLI. In a real service, the caller would
    /// `submit`, spawn `Processor::run` once, and tail the stream for many executions.
    pub async fn start(
        sm: StateMachine,
        input: Value,
    ) -> Result<crate::result::ExecutionResult, ExecutionError> {
        use std::sync::Arc;

        use tokio_stream::StreamExt;

        use crate::event::Event;
        use crate::id::EntryId;
        use crate::log::InMemoryLogStream;
        use crate::result::ExecutionResult;
        use crate::storage::InMemoryStorage;

        let logstream = Arc::new(InMemoryLogStream::new());
        let mut ids = IdSource::new();
        let (execution_id, _stream) = Engine::submit(input, &*logstream, &mut ids).await?;

        let mut processor = Processor::new(sm, execution_id);
        let logstream_clone = Arc::clone(&logstream);
        let handle = tokio::spawn(async move {
            let mut storage = InMemoryStorage::new();
            processor.run(&*logstream_clone, &mut storage).await
        });

        let mut stream = logstream.stream_read(EntryId::new(1));
        while let Some(entry) = stream.next().await {
            if let EntryPayload::Event(event) = &entry.payload {
                match event {
                    Event::ExecutionCompleted { id, output } if *id == execution_id => {
                        handle.abort();
                        return Ok(ExecutionResult {
                            output: output.clone(),
                        });
                    }
                    Event::ExecutionTerminated { id, reason } if *id == execution_id => {
                        handle.abort();
                        return Err(reason.to_execution_error());
                    }
                    _ => {}
                }
            }
        }
        handle.abort();
        Err(ExecutionError::InvalidDefinition(
            "execution stalled (stream closed before terminal state)".to_string(),
        ))
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}
