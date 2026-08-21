//! `StateActivated` event projection: folds the `Event::StateActivated` activity value onto Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{
    ActivityState, ActivityStatus, ActivityValue, ApplierContext, EventApplier, RetryState,
};

use crate::id::{ActivityId, ExecutionId, NodeId};

#[derive(Default)]
pub(crate) struct StateActivatedApplier;
#[async_trait]
impl EventApplier for StateActivatedApplier {
    fn event(&self) -> Event {
        Event::StateActivated {
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
        let Event::StateActivated { activity } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        let Some(act) = ctx.storage.get_activity(activity.id).await? else {
            return Ok(());
        };
        // Activation mutates only the domain value itself (processed input and, for Map, its
        // activation product). The independently-maintained `active_children` set is preserved; the
        // row's `created_at` is carried over (this is an update, not a birth) and `updated_at` is
        // stamped with this entry's moment.
        let mut row = crate::storage::Activity::from_value(activity.clone(), act.active_children);
        row.created_at = act.created_at;
        row.updated_at = ctx.timestamp;
        ctx.storage.put_activity(row).await?;
        Ok(())
    }
}
