//! `StateCompleting` event projection: folds the `Event::StateCompleting` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::ActivityId;
use crate::storage::Status;

#[derive(Default)]
pub(crate) struct StateCompletingApplier;
#[async_trait]
impl EventApplier for StateCompletingApplier {
    fn event(&self) -> Event {
        Event::StateCompleting {
            activity: ActivityId::nil(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::StateCompleting { activity } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut act) = ctx.storage.get_activity(*activity).await? {
            act.status = Status::Completing;
            ctx.storage.put_activity(act).await?;
        }
        Ok(())
    }
}
