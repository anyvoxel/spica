//! Integration tests for the engine's read facade behind the k8s-style Query API — `get_object`
//! (one object of any kind by name) and `list_objects` (a kind, paged by `limit` + continue-token),
//! both straight from the current storage projection. These exercise the expose/decode layer the
//! Server's Query service maps onto the wire.

use serde_json::Value;
use spica_asl::StateMachine;
use spica_engine::{FlowName, ObjectKind, ObjectName, QueryObject};

mod common;

/// A trivial one-state machine that completes immediately — the M1 default for "runs and settles".
fn sm() -> StateMachine {
    serde_json::from_value(serde_json::json!({
        "StartAt": "P",
        "States": { "P": { "Type": "Pass", "End": true } }
    }))
    .unwrap()
}

/// `get_object` round-trips the persisted rows of each exercised kind back out as its typed
/// `QueryObject`, keyed by the addressing `name` the caller supplied.
#[tokio::test]
async fn get_object_reads_flow_version_and_execution_by_name() {
    let engine = common::LocalClient::start(common::in_memory_builder())
        .await
        .unwrap();
    let definition = serde_json::to_string(&sm()).unwrap();

    // A created flow yields Flow + FlowVersion rows (its first version's generated `{name}-{v}` name).
    let flow_version = engine
        .create_flow(FlowName::new("lookup_flow").unwrap(), &definition)
        .await
        .unwrap();
    let flow = engine
        .get_object(
            ObjectKind::Flow,
            &ObjectName::from_parsed("lookup_flow").unwrap(),
        )
        .await
        .unwrap()
        .expect("flow row should exist");
    assert!(matches!(flow, QueryObject::Flow(f) if f.meta.name.as_str() == "lookup_flow"));
    let version = engine
        .get_object(ObjectKind::FlowVersion, &flow_version.name)
        .await
        .unwrap()
        .expect("first flow version row should exist");
    assert!(matches!(version, QueryObject::FlowVersion(_)));

    // Start + settle; the execution row is then addressable by its user-supplied name.
    let exec_name = common::execution_name();
    let execution_id = engine
        .start_for_revision(exec_name.clone(), flow_version, Value::Null)
        .await
        .unwrap();
    engine.wait_for_execution(&execution_id).await.unwrap();
    let execution = engine
        .get_object(ObjectKind::Execution, &exec_name)
        .await
        .unwrap()
        .expect("execution row should exist");
    assert!(matches!(execution, QueryObject::Execution(_)));

    // A name that matches no row — of a kind we *did* exercise — reads as None, not an error.
    let missing = engine
        .get_object(
            ObjectKind::Execution,
            &ObjectName::from_parsed("no_such_run").unwrap(),
        )
        .await
        .unwrap();
    assert!(missing.is_none());

    engine.stop().await;
}

/// A produced run leaves descendant rows (Activity, Thread) that a kind `ListObjects` returns; the
/// point is that a kind we never wrote to directly is reachable through the same generic read.
#[tokio::test]
async fn list_objects_reads_descendant_kinds_of_a_completed_run() {
    let engine = common::LocalClient::start(common::in_memory_builder())
        .await
        .unwrap();
    let definition = serde_json::to_string(&sm()).unwrap();
    let flow_version = engine
        .create_flow(FlowName::new("desc_flow").unwrap(), &definition)
        .await
        .unwrap();
    let execution_id = engine
        .start_for_revision(common::execution_name(), flow_version, Value::Null)
        .await
        .unwrap();
    engine.wait_for_execution(&execution_id).await.unwrap();

    // The completed Pass leaves at least one Activity row (the executed state).
    let activities = engine
        .list_objects(ObjectKind::Activity, 100, None)
        .await
        .unwrap();
    assert!(
        !activities.objects.is_empty(),
        "a completed run should have produced at least one activity"
    );
    // The scope's threads are reachable too; the exact count is not asserted (a plain Pass may elide
    // a Thread row — only that the kind is listable without error).
    let _threads = engine
        .list_objects(ObjectKind::Thread, 100, None)
        .await
        .unwrap();
    let _timers = engine
        .list_objects(ObjectKind::Timer, 100, None)
        .await
        .unwrap();
    let _tasks = engine
        .list_objects(ObjectKind::Task, 100, None)
        .await
        .unwrap();

    engine.stop().await;
}

/// `list_objects` pages a kind in storage-key order: `limit` rows per page, an opaque continue-token
/// to resume the next, and `None` once the last page is exhausted — exactly the k8s semantics.
#[tokio::test]
async fn list_objects_paginates_flows_with_limit_and_continue_token() {
    let engine = common::LocalClient::start(common::in_memory_builder())
        .await
        .unwrap();
    let definition = serde_json::to_string(&sm()).unwrap();
    // Distinct names in deterministic byte order so the collected page can be compared exactly.
    let names = ["aaaa", "bbbb", "cccc", "dddd"];
    for n in names {
        engine
            .create_flow(FlowName::new(n).unwrap(), &definition)
            .await
            .unwrap();
    }

    let mut seen = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = engine
            .list_objects(ObjectKind::Flow, 2, token.as_deref())
            .await
            .unwrap();
        assert!(page.objects.len() <= 2, "a page never exceeds its limit");
        for o in &page.objects {
            seen.push(o.name().as_str());
        }
        match page.continue_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    assert_eq!(seen, names, "all flows returned once, in storage order");

    engine.stop().await;
}
