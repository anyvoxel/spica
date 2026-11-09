//! `StateTransitioned` event projection: folds the `Event::StateTransitioned` into Storage.

use crate::ApplierContext;
use crate::types::error::ExecutionError;
use crate::types::event::StateTransitioned;

/// `StateTransitioned` is a pure "routing resolved" marker; the hop (`ActivateState` /
/// `CompleteExecution`) is carried by the following `Command`, so there is nothing to fold. No-op,
/// mirroring `StateActivated`.
#[derive(Default)]
pub(crate) struct StateTransitionedApplier;
impl StateTransitionedApplier {
    pub(crate) async fn apply(
        &self,
        _ctx: &mut ApplierContext<'_>,
        _event: &StateTransitioned,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}
