//! `StateTransitioned` event projection: folds the `Event::StateTransitioned` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::ActivityId;

/// `StateTransitioned` is a pure "routing resolved" marker; the hop (`ActivateState` /
/// `CompleteExecution`) is carried by the following `Command`, so there is nothing to fold. No-op,
/// mirroring `StateActivated`.
#[derive(Default)]
pub(crate) struct StateTransitionedApplier;
#[async_trait]
impl EventApplier for StateTransitionedApplier {
    fn event(&self) -> Event {
        Event::StateTransitioned {
            activity: ActivityId::nil(),
            next: String::new(),
            output: Default::default(),
        }
    }

    async fn apply(
        &self,
        _ctx: &mut ApplierContext<'_>,
        _event: &Event,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}
