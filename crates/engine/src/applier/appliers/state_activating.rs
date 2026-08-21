//! `StateActivating` event projection: folds the `Event::StateActivating` activity value into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{
    ActivityState, ActivityStatus, ActivityValue, ApplierContext, EventApplier, RetryState,
};

use crate::id::{ActivityId, ExecutionId, NodeId};
use crate::storage::Activity;

#[derive(Default)]
pub(crate) struct StateActivatingApplier;
#[async_trait]
impl EventApplier for StateActivatingApplier {
    fn event(&self) -> Event {
        Event::StateActivating {
            activity: ActivityValue {
                id: ActivityId::nil(),
                execution: ExecutionId::nil(),
                root_execution: ExecutionId::nil(),
                parent: NodeId::Execution(ExecutionId::nil()),
                state_path: jsonptr::PointerBuf::new(),
                status: ActivityStatus::Running,
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
        let Event::StateActivating { activity } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        // `StateActivating` is the creation moment of the projection row: the event already carries
        // the canonical domain entity, and storage only adds its projection-only `active_children`
        // bookkeeping alongside it.
        let mut row = Activity::from_value(activity.clone(), std::collections::HashSet::new());
        // Birth: `created_at`/`updated_at` stamped with the `StateActivating` entry's moment.
        row.born(ctx.timestamp);
        ctx.storage.put_activity(row).await?;
        ctx.storage
            .add_child(activity.parent, NodeId::Activity(activity.id))
            .await?;
        if let Some(mut exec) = ctx.storage.get_execution(activity.execution).await? {
            // Track the currently active state only in the projection row. The event-carried
            // execution value stays focused on durable domain facts, while this storage-only cursor
            // keeps the single-active-state invariant easy to observe and debug.
            exec.current_activity = Some(activity.id);
            // Newest activation also touches the owning execution's update time.
            exec.touch(ctx.timestamp);
            ctx.storage.put_execution(exec).await?;
        }
        Ok(())
    }
}
