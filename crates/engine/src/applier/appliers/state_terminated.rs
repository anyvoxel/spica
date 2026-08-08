//! `StateTerminated` event projection: folds the `Event::StateTerminated` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{ActivityId, NodeId};
use crate::storage::Status;

#[derive(Default)]
pub(crate) struct StateTerminatedApplier;
#[async_trait]
impl EventApplier for StateTerminatedApplier {
    fn event(&self) -> Event {
        Event::StateTerminated {
            activity: ActivityId::nil(),
            reason: crate::command::TerminationReason::Cancelled,
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::StateTerminated { activity, reason } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut act) = ctx.storage.get_activity(*activity).await? {
            let parent = act.parent;
            act.status = Status::Terminated(reason.clone());
            act.reason = Some(reason.clone());
            act.pending_reason = None;
            ctx.storage.put_activity(act).await?;
            ctx.storage
                .remove_child(parent, NodeId::Activity(*activity))
                .await?;
            if let NodeId::Execution(exec_id) = parent
                && let Some(mut exec) = ctx.storage.get_execution(exec_id).await?
            {
                exec.current_activity = None;
                ctx.storage.put_execution(exec).await?;
            }
        }
        Ok(())
    }
}
