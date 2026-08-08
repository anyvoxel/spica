//! `StateTerminating` event projection: folds the `Event::StateTerminating` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::ActivityId;
use crate::storage::Status;

#[derive(Default)]
pub(crate) struct StateTerminatingApplier;
#[async_trait]
impl EventApplier for StateTerminatingApplier {
    fn event(&self) -> Event {
        Event::StateTerminating {
            activity: ActivityId::nil(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::StateTerminating { activity } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut act) = ctx.storage.get_activity(*activity).await? {
            act.status = Status::Terminating;
            ctx.storage.put_activity(act).await?;
        }
        Ok(())
    }
}
