//! `StateTerminating` event projection: folds the `Event::StateTerminating` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::ActivityId;
use crate::storage::ActivityStatus;

#[derive(Default)]
pub(crate) struct StateTerminatingApplier;
#[async_trait]
impl EventApplier for StateTerminatingApplier {
    fn event(&self) -> Event {
        Event::StateTerminating {
            activity: ActivityId::nil(),
            reason: crate::command::TerminationReason::Cancelled,
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::StateTerminating { activity, reason } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut act) = ctx.storage.get_activity(*activity).await? {
            act.status = ActivityStatus::Terminating(reason.clone());
            ctx.storage.put_activity(act).await?;
        }
        Ok(())
    }
}
