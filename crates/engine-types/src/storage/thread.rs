use std::collections::HashSet;
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::storage::ReadonlyStorageTxn;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::meta::ObjectReference;
use crate::types::thread::Thread;
use crate::types::variables::Variables;
use spica_machinery::Timestamp;

/// The storage projection row of a [`Thread`] — one scoped sub-run (`Parallel` branch / `Map` item).
///
/// Mirrors [`ExecutionRecord`](crate::storage::ExecutionRecord) exactly: the canonical `Thread`
/// domain entity reconstructed from the stream, wrapped so projection-only bookkeeping
/// (`variables`, `active_children`, timing facts) stays off the event-carried entity value. A thread
/// is a self-contained sub-run, so it owns its own variable scope and in-flight children, exactly
/// like a top-level execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadRecord {
    /// The canonical thread domain value reconstructed from the event stream. Not `#[serde(flatten)]`
    /// for the same reason as `ExecutionRecord`: `Thread`'s domain timing lives in `meta`, which
    /// would collide with this row's entry-timestamp fields at the same JSON level.
    pub value: Thread,
    /// The thread's current variable scope (see `ExecutionRecord::variables`).
    pub variables: Variables,
    /// Owned nodes still in flight (active activities / timers / child threads). A completing or
    /// terminating thread waits for this projection-only set to drain before its terminal `ed`.
    pub active_children: HashSet<ObjectReference>,
    /// Birth entry moment (`ThreadCreated`), projection-derived — never a local `now()`.
    pub created_at: Timestamp,
    /// Latest applied entry's timestamp that touched this row.
    pub updated_at: Timestamp,
}

impl ThreadRecord {
    pub fn value(&self) -> Thread {
        self.value.clone()
    }

    pub fn from_value(value: Thread, active_children: HashSet<ObjectReference>) -> Self {
        Self {
            value,
            variables: Variables::new(),
            active_children,
            created_at: Timestamp::from_millis(0),
            updated_at: Timestamp::from_millis(0),
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.value.is_terminal()
    }

    /// Stamp a fresh row's birth entry moment (`ThreadCreated`): `created_at == updated_at == at`.
    pub fn born(&mut self, at: Timestamp) {
        self.created_at = at;
        self.updated_at = at;
    }

    /// Record a row write at `at` (a mutation applier): advances `updated_at`, leaves `created_at`.
    pub fn with_update_at(&mut self, at: Timestamp) {
        self.updated_at = at;
    }
}

impl Deref for ThreadRecord {
    type Target = Thread;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for ThreadRecord {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/// Resolve the **immutable flow version** a thread's states bind to (its machine definition), derived
/// from the thread's root `execution`: the whole tree shares one definition, so a thread never stores
/// its own copy (see [`Thread`]). Returns an `InvalidDefinition` error if the owning execution is
/// gone, which can only mean the tree is being torn down.
pub async fn resolve_thread_flow_version<S: ReadonlyStorageTxn + ?Sized>(
    storage: &S,
    thread: &ThreadRecord,
) -> Result<ObjectReference, ExecutionError> {
    let exec = storage
        .get_execution(&thread.value.execution)
        .await?
        .ok_or_else(|| {
            ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                "thread {} lost its owning execution {}",
                thread.value.reference(),
                thread.value.execution
            )))
        })?;
    Ok(exec.value.flow_version.clone())
}
