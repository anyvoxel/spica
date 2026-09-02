//! `StateCompleted` event projection: folds the `Event::StateCompleted` activity value into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{Activity, ActivityState, ActivityStatus, ApplierContext, EventApplier, RetryState};

use crate::types::meta::ObjectKind;

#[derive(Default)]
pub(crate) struct StateCompletedApplier;
#[async_trait]
impl EventApplier for StateCompletedApplier {
    fn event(&self) -> Event {
        Event::StateCompleted {
            activity: Activity {
                execution: crate::types::meta::ObjectReference::nil(),
                state_path: jsonptr::PointerBuf::new(),
                status: ActivityStatus::Completed,
                raw_input: Default::default(),
                input: Default::default(),
                raw_output: None,
                activity_state: ActivityState::Leaf,
                retry_state: RetryState::default(),
                output: None,
                meta: crate::types::meta::ObjectMeta::born_placeholder(
                    crate::types::meta::ObjectKind::Activity,
                    crate::types::id::ActivityId::nil().into(),
                    crate::log::Timestamp::from_millis(0),
                ),
            },
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::StateCompleted { activity } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(act) = ctx.storage.get_activity(&activity.reference()).await? {
            let parent = act
                .value
                .meta
                .owner
                .clone()
                .expect("an owned activity has an owner");
            // An update, not a birth: carry the row's `created_at` over and stamp `updated_at`.
            let mut row =
                crate::storage::ActivityRecord::from_value(activity.clone(), act.active_children);
            row.created_at = act.created_at;
            row.updated_at = ctx.timestamp;
            ctx.storage.put_activity(row).await?;
            ctx.storage
                .remove_child(parent.clone(), activity.reference())
                .await?;
            if parent.kind == ObjectKind::Execution
                && let Some(mut exec) = ctx.storage.get_execution(&parent).await?
            {
                // Clear the projection-only active cursor as soon as the owned activity reaches its
                // terminal `ed`, so later activation/termination logic never treats a finished state
                // as still in flight.
                exec.current_activity = None;
                exec.touch(ctx.timestamp);
                ctx.storage.put_execution(exec).await?;
            }
        }
        Ok(())
    }
}
