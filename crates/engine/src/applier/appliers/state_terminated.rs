//! `StateTerminated` event projection: folds the `Event::StateTerminated` activity value into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{
    ActivityState, ActivityStatus, ActivityValue, ApplierContext, EventApplier, RetryState,
};

use crate::id::{ActivityId, ExecutionId, NodeId};

#[derive(Default)]
pub(crate) struct StateTerminatedApplier;
#[async_trait]
impl EventApplier for StateTerminatedApplier {
    fn event(&self) -> Event {
        Event::StateTerminated {
            activity: ActivityValue {
                id: ActivityId::nil(),
                execution: ExecutionId::nil(),
                root_execution: ExecutionId::nil(),
                parent: NodeId::Execution(ExecutionId::nil()),
                state_path: jsonptr::PointerBuf::new(),
                status: ActivityStatus::Terminated(crate::command::TerminationReason::Cancelled),
                raw_input: Default::default(),
                input: Default::default(),
                raw_output: None,
                activity_state: ActivityState::Leaf,
                retry_state: RetryState::default(),
                output: None,
            },
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::StateTerminated { activity } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(act) = ctx.storage.get_activity(activity.id).await? {
            let parent = act.value.parent;
            // An update, not a birth: carry the row's `created_at` over and stamp `updated_at`.
            let mut row =
                crate::storage::Activity::from_value(activity.clone(), act.active_children);
            row.created_at = act.created_at;
            row.updated_at = ctx.timestamp;
            ctx.storage.put_activity(row).await?;
            ctx.storage
                .remove_child(parent, NodeId::Activity(activity.id))
                .await?;
            if let NodeId::Execution(exec_id) = parent
                && let Some(mut exec) = ctx.storage.get_execution(exec_id).await?
            {
                // Mirror `StateCompleted`: once the owned activity terminates, the execution-level
                // projection cursor must be cleared so the unwind sees no stale "current" state.
                exec.current_activity = None;
                exec.touch(ctx.timestamp);
                ctx.storage.put_execution(exec).await?;
            }
        }
        Ok(())
    }
}
