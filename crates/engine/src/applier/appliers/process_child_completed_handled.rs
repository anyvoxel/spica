//! `ProcessChildCompletedHandled` event projection: folds the `Event::ProcessChildCompletedHandled`
//! into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::types::meta::ObjectReference;

/// `ProcessChildCompletedHandled` is a pure "command handled" marker: it confirms a no-op
/// `ProcessChildCompleted` was received, and since that no-op projection changed no state there is
/// nothing to fold — the event's entire purpose is the durable, causally-tied receipt on the stream
/// so the watermark advances. No-op, mirroring `StateTransitioned`.
#[derive(Default)]
pub(crate) struct ProcessChildCompletedHandledApplier;
#[async_trait]
impl EventApplier for ProcessChildCompletedHandledApplier {
    fn event(&self) -> Event {
        Event::ProcessChildCompletedHandled {
            owner: ObjectReference::nil(),
            child: ObjectReference::nil(),
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
