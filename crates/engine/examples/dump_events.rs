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
use spica_client::worker::{InMemoryTaskService, TaskFailure, TaskHandler, TaskService};
use spica_engine::{
    EngineBuilder, Entry, EntryId, EntryPayload, ExecutionStatus, InMemoryLogStream, LogStream,
    StreamId,
};
use spica_storage::InMemoryStorage;
use tokio_util::sync::CancellationToken;

// The blocking convenience client (create_flow / start_for_revision + ack correlation) now lives with
// the consumer, not the engine; the example reuses the integration suite's `tests/common` LocalClient.
#[path = "../tests/common/mod.rs"]
mod common;

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

/// A `TaskHandler` that echoes its arguments back as the task's output.
#[derive(Default)]
struct EchoHandler;

#[async_trait]
impl TaskHandler for EchoHandler {
    async fn run(&self, _resource: &str, arguments: &Value) -> Result<Value, TaskFailure> {
        Ok(arguments.clone())
    }
}

fn parse_sm(s: &str) -> StateMachine {
    serde_json::from_str(s).expect("state machine fixture must parse")
}

/// One log [`Entry`] rendered as a JSON node: the durable envelope (`entry_id`/`cause_id`/`stream_id`
/// /`timestamp`) plus the full payload. `children` is filled by [`attach_children`].
fn entry_node(entry: &Entry) -> Value {
    json!({
        "entry_id": entry.entry_id.to_string(),
        "cause_id": entry.cause_id.map(|c| c.to_string()),
        "stream_id": entry.stream_id.to_string(),
        "timestamp": serde_json::to_value(entry.timestamp).unwrap_or(Value::Null),
        "payload": serde_json::to_value(&entry.payload).unwrap_or(Value::Null),
        "children": [],
    })
}

/// Recursively render one node and graft its children — the entries whose `cause_id` names this
/// `entry_id`. A Command's produced Events *and* its batch-terminating Noop hang directly under it,
/// so the whole causal batch reads as one subtree.
fn attach_children(
    entry_id: EntryId,
    by_id: &HashMap<EntryId, &Entry>,
    children_of: &HashMap<EntryId, Vec<EntryId>>,
) -> Value {
    let entry = by_id[&entry_id];
    let kids = children_of
        .get(&entry_id)
        .map(|ids| {
            ids.iter()
                .map(|id| attach_children(*id, by_id, children_of))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut node = entry_node(entry);
    node["children"] = json!(kids);
    node
}

/// The whole log as one tree rooted at cause: each no-cause Command is a root, and every later entry
/// is a child of the entry that produced it (its `cause_id`). Real log order is preserved because
/// every parent precedes its children, and children are attached in order.
fn build_tree(entries: &[Entry]) -> Value {
    let by_id: HashMap<EntryId, &Entry> = entries.iter().map(|e| (e.entry_id, e)).collect();
    let mut children_of: HashMap<EntryId, Vec<EntryId>> = HashMap::new();
    for entry in entries {
        if let Some(cause) = &entry.cause_id {
            children_of.entry(*cause).or_default().push(entry.entry_id);
        }
    }
    let roots: Vec<Value> = entries
        .iter()
        .filter(|e| e.cause_id.is_none())
        .map(|e| attach_children(e.entry_id, &by_id, &children_of))
        .collect();
    json!({ "roots": roots })
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
    // Each run becomes one JSON object whose `log` is a tree rooted at cause: a no-cause Command is a
    // root, and every entry hangs as a child of the entry that produced it (its `cause_id`).
    let runs = vec![
        run_tree(
            "RUN 1 — happy path (Pass, Choice, Wait, Task, Parallel, Map, Succeed)",
            HAPPY_SM,
            json!({ "greet": "hi", "items": [ { "n": 1 }, { "n": 2 } ] }),
        )
        .await,
        run_tree("RUN 2 — terminal Fail (Pass -> Fail)", FAIL_SM, Value::Null).await,
    ];
    let out = json!({ "runs": runs });
    std::fs::write(
        out_path,
        serde_json::to_string_pretty(&out).expect("the dump serializes to JSON"),
    )
    .expect("write event dump to file");
    println!("wrote event dump to {out_path}");
}

/// Build an engine backed by a shared (readable) log + a worker, run one machine, wait for its
/// outcome, and return a JSON object carrying the run's input, outcome, and its whole log as a
/// cause-rooted entry tree.
async fn run_tree(title: &str, sm_def: &str, input: Value) -> Value {
    // Build the shared log + storage + scheduler, and the engine over them.
    let log = Arc::new(tokio::sync::Mutex::new(
        InMemoryLogStream::<EntryPayload>::new(),
    ));
    let engine = common::LocalClient::start(EngineBuilder::with_backends(
        Box::new(SharedLog(log.clone())),
        Box::new(InMemoryStorage::new()),
    ))
    .await
    .expect("start engine");

    // Boot a worker against the engine's inbound TaskApi so the Task state completes.
    let cancel = CancellationToken::new();
    let worker = {
        let api = Arc::new(common::EngineTaskApi::new(Arc::new(engine.clone())));
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
        .create_flow(common::anonymous_name(), &definition)
        .await
        .expect("create flow");
    let execution = engine
        .start_for_revision(common::execution_name(), flow_version, input.clone())
        .await
        .expect("start execution");

    // Await the terminal snapshot; settling (Completed vs Terminated) is read off the returned value.
    let result = match engine.wait_for_execution(&execution).await {
        Ok(exec) => match exec.status {
            ExecutionStatus::Completed => {
                json!({ "output": exec.output.unwrap_or(serde_json::Value::Null) })
            }
            ExecutionStatus::Terminated(reason) => {
                json!({ "error": reason.to_execution_error().to_string() })
            }
            // wait_for_execution only returns once terminal; a live non-terminal arm is unreachable.
            other => json!({ "status": format!("{:?}", other) }),
        },
        Err(e) => json!({ "error": e.to_string() }),
    };

    cancel.cancel();
    let _ = worker.await;

    // The durable log is the single source of truth for the whole trace; render it as a tree.
    let log_val = build_tree(&log.lock().await.entries());
    drop(engine);
    json!({ "title": title, "input": input, "result": result, "log": log_val })
}
