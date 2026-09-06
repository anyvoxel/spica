//! The engine-side read facade for the k8s-style Query API: GET one object of any kind by name, or
//! LIST a kind with limit + continue-token pagination, both read straight from the current Storage
//! projection.
//!
//! The engine stays proto-agnostic: it exposes the persisted rows as typed [`QueryObject`]s, and the
//! server maps each onto its wire mirror (the `spica_proto` Query service). Keying and paging live
//! here (kind + addressing name; storage-key order), so the wire never leaks storage layout.

use crate::storage::{ActivityRecord, ExecutionRecord, TaskRecord, ThreadRecord, TimerRecord};
use crate::types::error::ExecutionError;
use crate::types::flow::Flow;
use crate::types::flow_version::FlowVersion;
use crate::types::meta::{ObjectKind, ObjectName, ObjectReference};

/// The persisted projection of one object of any kind, in its typed record form — the value behind a
/// `GetObject`/`ListObjects` read. Each variant names the entity kind it mirrors, so `kind()` is a
/// match, not a scan.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryObject {
    Flow(Flow),
    FlowVersion(FlowVersion),
    Execution(ExecutionRecord),
    Thread(ThreadRecord),
    Activity(ActivityRecord),
    Timer(TimerRecord),
    Task(TaskRecord),
}

impl QueryObject {
    pub fn kind(&self) -> ObjectKind {
        match self {
            QueryObject::Flow(_) => ObjectKind::Flow,
            QueryObject::FlowVersion(_) => ObjectKind::FlowVersion,
            QueryObject::Execution(_) => ObjectKind::Execution,
            QueryObject::Thread(_) => ObjectKind::Thread,
            QueryObject::Activity(_) => ObjectKind::Activity,
            QueryObject::Timer(_) => ObjectKind::Timer,
            QueryObject::Task(_) => ObjectKind::Task,
        }
    }

    /// The object's addressing name (`meta.name`), the primary key its row is stored under.
    pub fn name(&self) -> &ObjectName {
        match self {
            QueryObject::Flow(f) => &f.meta.name,
            QueryObject::FlowVersion(v) => &v.meta.name,
            QueryObject::Execution(e) => &e.value.meta.name,
            QueryObject::Thread(t) => &t.value.meta.name,
            QueryObject::Activity(a) => &a.value.meta.name,
            QueryObject::Timer(t) => &t.value.meta.name,
            QueryObject::Task(t) => &t.value.meta.name,
        }
    }
}

/// One page of a `ListObjects` read: the objects plus the opaque `continue_token` to resume the next
/// page (the last page carries `None`). The token is the previous page's last addressing name — a
/// stable, storage-order resume point with no server-side cursor state.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryListPage {
    pub objects: Vec<QueryObject>,
    pub continue_token: Option<String>,
}

impl QueryListPage {
    /// The page's objects, one per returned row.
    pub fn into_objects(self) -> Vec<QueryObject> {
        self.objects
    }
}

/// Build the reference a point `get_*` storage read resolves by from a bare `(kind, name)` — the
/// reads key rows by the addressing `name` alone, so a nil uid is a benign placeholder that never
/// matches (the returned row carries the real uid back).
pub(crate) fn ref_for(kind: ObjectKind, name: &ObjectName) -> ObjectReference {
    ObjectReference::new(kind, name.clone(), ulid::Ulid::nil())
}

/// List `kind`'s rows in storage-key order through the `Storage::list_kind` scan, deserializing each
/// row's raw bytes into its typed record. `limit + 1` rows are fetched to detect whether a further
/// page exists; the token then names the last row actually emitted.
pub(crate) async fn list_kind(
    storage: &dyn crate::Storage,
    kind: ObjectKind,
    limit: usize,
    continue_token: Option<&str>,
) -> Result<QueryListPage, ExecutionError> {
    let start_after = match continue_token {
        Some(t) if !t.is_empty() => Some(ObjectName::from_parsed(t).map_err(|e| {
            ExecutionError::Runtime(crate::types::error::RuntimeError::InvalidDefinition(
                format!("invalid continue_token {t:?}: {e}"),
            ))
        })?),
        _ => None,
    };
    // Ask for one more than the page size; a limit+1-th row proves another page exists.
    let rows = storage
        .list_kind(kind, start_after.as_ref(), limit.saturating_add(1))
        .await?;
    let more = rows.len() > limit;
    let rows = rows.into_iter().take(limit).collect::<Vec<_>>();
    let objects = rows
        .into_iter()
        .map(|(name, bytes)| decode(kind, name, &bytes))
        .collect::<Result<Vec<_>, _>>()?;
    let continue_token = match more {
        true => objects.last().map(|o| o.name().as_str()),
        false => None,
    };
    Ok(QueryListPage {
        objects,
        continue_token,
    })
}

/// Deserialize one `list_kind` row's raw JSON bytes into its kind's typed record.
fn decode(kind: ObjectKind, name: ObjectName, bytes: &[u8]) -> Result<QueryObject, ExecutionError> {
    use crate::types::error::RuntimeError;
    let bad = || {
        ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
            "Query decode of {} {name:?} failed",
            kind.as_str()
        )))
    };
    match kind {
        ObjectKind::Flow => serde_json::from_slice::<Flow>(bytes)
            .map(QueryObject::Flow)
            .map_err(|_| bad()),
        ObjectKind::FlowVersion => serde_json::from_slice::<FlowVersion>(bytes)
            .map(QueryObject::FlowVersion)
            .map_err(|_| bad()),
        ObjectKind::Execution => serde_json::from_slice::<ExecutionRecord>(bytes)
            .map(QueryObject::Execution)
            .map_err(|_| bad()),
        ObjectKind::Thread => serde_json::from_slice::<ThreadRecord>(bytes)
            .map(QueryObject::Thread)
            .map_err(|_| bad()),
        ObjectKind::Activity => serde_json::from_slice::<ActivityRecord>(bytes)
            .map(QueryObject::Activity)
            .map_err(|_| bad()),
        ObjectKind::Timer => serde_json::from_slice::<TimerRecord>(bytes)
            .map(QueryObject::Timer)
            .map_err(|_| bad()),
        ObjectKind::Task => serde_json::from_slice::<TaskRecord>(bytes)
            .map(QueryObject::Task)
            .map_err(|_| bad()),
    }
}
