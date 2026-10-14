use crate::storage::ReadonlyStorageTxn;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::flow_version::FlowVersionKind;
use crate::types::meta::ObjectRef;
use crate::types::thread::Thread;

/// Resolve the **immutable flow version** a thread's states bind to (its machine definition), derived
/// from the thread's root `execution`: the whole tree shares one definition, so a thread never stores
/// its own copy (see [`Thread`]). Returns an `InvalidDefinition` error if the owning execution is
/// gone, which can only mean the tree is being torn down.
pub async fn resolve_thread_flow_version<S: ReadonlyStorageTxn + ?Sized>(
    storage: &S,
    thread: &Thread,
) -> Result<ObjectRef<FlowVersionKind>, ExecutionError> {
    let exec = storage
        .get_execution(&thread.execution)
        .await?
        .ok_or_else(|| {
            ExecutionError::Runtime(RuntimeError::InvalidDefinition(format!(
                "thread {} lost its owning execution {}",
                thread.meta.raw_object_ref(),
                thread.execution
            )))
        })?;
    Ok(exec.flow_version.clone())
}
