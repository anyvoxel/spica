//! Diagnostic: run one full ASL state machine covering every State type and dump the **entire
//! logstream** (all Commands, Events, Noops, Rejections, in log order) to a file, so the concrete
//! engine trace can be inspected.
//!
//! Run with: `cargo run -p spica-engine --example dump_events`
//!
//! The engine's durable log is the single source of truth for the whole trace, so the dump reads
//! the whole log back using a shared wrapper (the builder consumes a `Box<dyn LogStream>`, so the
//! wrapper keeps our own `Arc` handle for inspection). Order, batch boundaries, and cause→effect
//! links are the real log facts — nothing is reconstructed or re-synthesized.
//!
//! `Succeed` and `Fail` are both terminal, so a single successful run cannot reach both. We therefore
//! run two executions in one engine session: a happy path covering Pass/Choice/Wait/Task/Parallel/
//! Map/Succeed, and a short Pass→Fail run to capture the terminate cascade. Notable: the two-phase
//! Activate/Complete model that this branch decided to keep means every synchronous state (Pass,
//! Choice, Succeed) contributes *two* causal batches — one per `CompleteState` command — exactly as
//! the design retains.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use spica_asl::StateMachine;
use spica_client::worker::{
    ClaimedTask, InMemoryTaskService, TaskApi as WorkerTaskApi, TaskApiError, TaskFailure,
    TaskHandler, TaskService,
};
use spica_engine::{
    EngineBuilder, Entry, EntryId, EntryPayload, ExecutionError, ExecutionResult, FlowName,
    InMemoryLogStream, LogStream, ObjectName, RuntimeError, StreamId,
};
use spica_scheduler::InMemoryScheduler;
use spica_storage::InMemoryStorage;
use tokio_util::sync::CancellationToken;

/// A `LogStream` wrapper that carries its own `Arc` handle so the log can be read back for the dump:
/// the builder consumes a `Box<dyn LogStream>`, but we hand it a clone of this shared wrapper and keep
/// `arc` for inspection (as an `InMemoryLogStream`), giving access to its `entries()` after the run.
struct SharedLog(Arc<tokio::sync::Mutex<InMemoryLogStream<EntryPayload>>>);

#[async_trait]
impl LogStream<EntryPayload> for SharedLog {
    fn stream_id(&self) -> StreamId {
        self.0.try_lock().unwrap().stream_id()
    }
    async fn append(&self, entries: Vec<Entry>) -> Result<EntryId, spica_logstream::LogError> {
        self.0.lock().await.append(entries).await
    }
    async fn read(&self, e: EntryId) -> Result<Option<Entry>, spica_logstream::LogError> {
        self.0.lock().await.read(e).await
    }
    fn stream_read(
        &self,
        from: EntryId,
    ) -> std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Entry> + Send + 'static>> {
        self.0.try_lock().unwrap().stream_read(from)
    }
}

/// The engine-side half of the worker boundary inside this example: presents the engine's inbound
/// [`spica_engine::TaskApi`] as the worker-facing [`spica_client::worker::TaskApi`] so an
/// [`InMemoryTaskService`] completes Tasks against the running engine. (Mirrors `tests/common`.)
struct EngineTaskApi {
    inner: Arc<dyn spica_engine::TaskApi>,
}

impl EngineTaskApi {
    fn new(inner: Arc<dyn spica_engine::TaskApi>) -> Self {
        Self { inner }
    }
    fn task_name(s: &str, op: &str) -> Result<spica_engine::ObjectName, TaskApiError> {
        spica_engine::ObjectName::from_parsed(s)
            .map_err(|_| TaskApiError(format!("{op}: invalid task name: {s:?}")))
    }
    fn request_id(s: &str, op: &str) -> Result<spica_engine::RequestId, TaskApiError> {
        s.parse::<ulid::Ulid>()
            .map(spica_engine::RequestId::from)
            .map_err(|_| TaskApiError(format!("{op}: invalid request_id ULID: {s:?}")))
    }
}

#[async_trait]
impl WorkerTaskApi for EngineTaskApi {
    async fn poll_tasks(
        &self,
        worker_id: &str,
        resource: &str,
        max_tasks: usize,
        lease_seconds: u64,
    ) -> Result<Vec<ClaimedTask>, TaskApiError> {
        let tasks = self
            .inner
            .poll_tasks(worker_id, resource, max_tasks, lease_seconds)
            .await
            .map_err(|e| TaskApiError(e.to_string()))?;
        Ok(tasks
            .into_iter()
            .map(|t| ClaimedTask {
                task_name: t.task.as_str().to_string(),
                resource: t.resource,
                arguments: t.arguments,
            })
            .collect())
    }
    async fn complete(
        &self,
        worker_id: &str,
        task_name: &str,
        request_id: &str,
        output: Value,
    ) -> Result<(), TaskApiError> {
        let name = Self::task_name(task_name, "CompleteTask")?;
        let request_id = Self::request_id(request_id, "CompleteTask")?;
        self.inner
            .complete(worker_id, name, request_id, output)
            .await
            .map_err(|e| TaskApiError(e.to_string()))
    }
    async fn fail(
        &self,
        worker_id: &str,
        task_name: &str,
        error: TaskFailure,
    ) -> Result<(), TaskApiError> {
        let name = Self::task_name(task_name, "FailTask")?;
        let TaskFailure { error_name, output } = error;
        let exec_err = ExecutionError::Runtime(RuntimeError::StateFailed {
            state: String::new(),
            error: error_name,
            output: Box::new(output),
        });
        self.inner
            .fail(worker_id, name, exec_err)
            .await
            .map_err(|e| TaskApiError(e.to_string()))
    }
}

/// A `TaskHandler` that echoes its arguments back as the task's output.
#[derive(Default)]
struct EchoHandler;

#[async_trait]
impl TaskHandler for EchoHandler {
    async fn run(&self, _resource: &str, arguments: &Value) -> Result<Value, TaskFailure> {
        Ok(arguments.clone())
    }
}

fn anonymous_name() -> FlowName {
    FlowName::new(&format!("anon_{}", ulid::Ulid::new()))
        .expect("a ULID-suffixed anonymous name always satisfies FlowName's charset")
}

fn execution_name() -> ObjectName {
    ObjectName::plain(&format!("run_{}", ulid::Ulid::new()))
        .expect("a ULID-suffixed user name is always valid")
}

fn parse_sm(s: &str) -> StateMachine {
    serde_json::from_str(s).expect("state machine fixture must parse")
}

/// Render one log [`Entry`] — its envelope plus its full payload (Command / Event / Noop / Reject)
/// as pretty JSON, so the whole logstream is visible in real log order. The envelope fields are the
/// durable log facts: the causal `entry_id` (batch position), the `cause_id` (the producing
/// command), the `stream_id`, and the frozen `timestamp`.
fn entry_line(entry: &Entry) -> String {
    let (kind, body) = match &entry.payload {
        EntryPayload::Command(c) => (
            "COMMAND",
            serde_json::to_string_pretty(c).unwrap_or_else(|_| "<unserializable>".into()),
        ),
        EntryPayload::Event(e) => (
            "EVENT  ",
            serde_json::to_string_pretty(e).unwrap_or_else(|_| "<unserializable>".into()),
        ),
        EntryPayload::Noop => ("NOOP   ", "{}".to_string()),
        EntryPayload::Reject(r) => (
            "REJECT ",
            serde_json::to_string_pretty(r).unwrap_or_else(|_| "<unserializable>".into()),
        ),
    };
    let body = body.replace('\n', "\n        ");
    let cause = entry
        .cause_id
        .map(|c| c.to_string())
        .unwrap_or_else(|| "-".to_string());
    format!(
        "[{kind} entry {} cause {cause} stream {}]\n        {body}\n",
        entry.entry_id, entry.stream_id
    )
}

/// The happy-path machine: a Pass, a Choice (JSONata condition), a Wait (1s timer), a Task (worker),
/// a Parallel (two branches), a Map (over an items array), then a terminal Succeed — every
/// non-terminal-reaching State type in one run.
const HAPPY_SM: &str = r#"{
  "StartAt": "Pass0",
  "States": {
    "Pass0": {
      "Type": "Pass",
      "Assign": { "pv": "{% $states.input.greet %}", "map_items": "{% $states.input.items %}" },
      "Next": "Choice"
    },
    "Choice": {
      "Type": "Choice",
      "Choices": [
        { "Condition": "{% $states.input.greet = 'hi' %}", "Next": "Wait" }
      ],
      "Default": "Wait"
    },
    "Wait": { "Type": "Wait", "Seconds": 1, "Next": "Task" },
    "Task": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Arguments": { "echo": "{% $states.input.greet %}" },
      "Next": "Parallel"
    },
    "Parallel": {
      "Type": "Parallel",
      "Next": "Map",
      "Branches": [
        { "StartAt": "A0", "States": { "A0": { "Type": "Pass", "Next": "A1" }, "A1": { "Type": "Succeed", "Output": { "branch": "A" } } } },
        { "StartAt": "B0", "States": { "B0": { "Type": "Pass", "Next": "B1" }, "B1": { "Type": "Succeed", "Output": { "branch": "B" } } } }
      ]
    },
    "Map": {
      "Type": "Map",
      "Next": "Succeed",
      "Items": "{% $map_items %}",
      "ItemProcessor": {
        "StartAt": "I",
        "States": { "I": { "Type": "Pass", "Output": "{% $states.input %}", "End": true } }
      }
    },
    "Succeed": { "Type": "Succeed" }
  }
}"#;

/// A short Pass→Fail run, captured separately because `Fail` is a terminal state and cannot coexist
/// with the `Succeed` that ends the happy path.
const FAIL_SM: &str = r#"{
  "StartAt": "P",
  "States": {
    "P": { "Type": "Pass", "Next": "F" },
    "F": { "Type": "Fail", "Error": "DemoFail", "Cause": "deliberate termination" }
  }
}"#;

#[tokio::main]
async fn main() {
    let out_path = "/Users/bytedance/workspace/github.com/anyvoxel/spica/events_dump.json";
    let mut dump = String::new();

    dump.push_str("=== spica engine logstream dump ===\n");
    dump.push_str(
        "Every log entry in real log order: Commands (read + dispatched by the leader), the\n",
    );
    dump.push_str(
        "Events they produced, the terminating Noop that commits each causal batch, and any\n",
    );
    dump.push_str(
        "Rejections — with the envelope fields (entry/cause/stream) that stitch cause→effect.\n",
    );
    dump.push_str("Note the retained two-phase Activate/Complete model: every synchronous state\n");
    dump.push_str(
        "(Pass/Choice/Succeed) spans TWO causal batches (Activate, then Complete). Succeed and\n",
    );
    dump.push_str("Fail are both terminal, so Fail is shown in a separate run.\n\n");

    run_and_dump(
        "RUN 1 — happy path (Pass, Choice, Wait, Task, Parallel, Map, Succeed)",
        HAPPY_SM,
        json!({ "greet": "hi", "items": [ { "n": 1 }, { "n": 2 } ] }),
        &mut dump,
    )
    .await;

    dump.push_str("\n\n");
    run_and_dump(
        "RUN 2 — terminal Fail (Pass -> Fail)",
        FAIL_SM,
        Value::Null,
        &mut dump,
    )
    .await;

    std::fs::write(out_path, &dump).expect("write event dump to file");
    println!("wrote event dump to {out_path}");
    eprintln!("{dump}");
}

/// Build an engine backed by a shared (readable) log + a worker, run one machine, wait for its
/// outcome, and append every log Event to `dump` under `title`.
async fn run_and_dump(title: &str, sm_def: &str, input: Value, dump: &mut String) {
    dump.push_str(&format!("== {title} ==\n"));
    dump.push_str(&format!(
        "state machine input: {}\n\n",
        serde_json::to_string_pretty(&input).unwrap()
    ));

    // Build the shared log + storage + scheduler, and the engine over them.
    let log = Arc::new(tokio::sync::Mutex::new(
        InMemoryLogStream::<EntryPayload>::new(),
    ));
    let engine = EngineBuilder::with_backends(
        Box::new(SharedLog(log.clone())),
        Box::new(InMemoryStorage::new()),
    )
    .with_scheduler(InMemoryScheduler::spawn())
    .start()
    .await
    .expect("start engine");

    // Boot a worker against the engine's inbound TaskApi so the Task state completes.
    let cancel = CancellationToken::new();
    let worker = {
        let api = Arc::new(EngineTaskApi::new(engine.task_api()));
        let cancel = cancel.clone();
        let handlers: HashMap<String, Arc<dyn TaskHandler>> = HashMap::from([(
            "arn:aws:states:::lambda:invoke".to_string(),
            Arc::new(EchoHandler) as Arc<dyn TaskHandler>,
        )]);
        let service = InMemoryTaskService::spawn(handlers);
        tokio::spawn(async move { service.run(api, cancel).await })
    };

    let sm = parse_sm(sm_def);
    let definition = serde_json::to_string(&sm).unwrap();
    let flow_version = engine
        .create_flow(anonymous_name(), &definition)
        .await
        .expect("create flow");
    let execution = engine
        .start_for_revision(execution_name(), flow_version, input)
        .await
        .expect("start execution");

    // Await the terminal outcome (Completed => Ok; a Fail run => Err with the StateFailed reason).
    let outcome = engine.wait_for_execution(&execution).await;
    match &outcome {
        Ok(ExecutionResult { output }) => {
            dump.push_str(&format!("run output: {output}\n\n"));
        }
        Err(e) => {
            dump.push_str(&format!("run terminated with error: {e}\n\n"));
        }
    }

    cancel.cancel();
    let _ = worker.await;

    // Dump the entire log: every Command (which the leader read and dispatched), the produced
    // Events, the terminating Noop that commits each causal batch, and any Rejections — all in real
    // log order, with the envelope fields that stitch cause→effect together.
    let entries = log.lock().await.entries();
    for entry in &entries {
        dump.push_str(&entry_line(entry));
    }
    drop(engine);
}
