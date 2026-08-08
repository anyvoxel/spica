//! `StateActivated` event projection: folds the `Event::StateActivated` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::ActivityId;

/// `StateActivated` is a pure phase marker; there is no data to fold, so it's a no-op.
#[derive(Default)]
pub(crate) struct StateActivatedApplier;
#[async_trait]
impl EventApplier for StateActivatedApplier {
    fn event(&self) -> Event {
        Event::StateActivated {
            activity: ActivityId::nil(),
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
