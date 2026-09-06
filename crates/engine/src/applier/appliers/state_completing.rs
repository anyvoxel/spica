//! `StateCompleting` event projection: folds the `Event::StateCompleting` activity value into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{Activity, ActivityStatus, ApplierContext, EventApplier};

#[derive(Default)]
pub(crate) struct StateCompletingApplier;
#[async_trait]
impl EventApplier for StateCompletingApplier {
    fn event(&self) -> Event {
        Event::StateCompleting {
            activity: Activity {
                execution: crate::types::meta::ObjectReference::nil(),
                state_path: jsonptr::PointerBuf::new(),
                status: ActivityStatus::Completing,
                raw_input: Default::default(),
                input: None,
                raw_output: None,
                activity_state: None,
                retry_state: None,
                output: None,
                meta: crate::types::meta::ObjectMeta::builder(
                    crate::types::meta::ObjectKind::Activity,
                    crate::types::id::ActivityId::nil().into(),
                )
                .at(crate::log::Timestamp::from_millis(0))
                .build(),
            },
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
        if let Some(act) = ctx.storage.get_activity(&activity.reference()).await? {
            // An update, not a birth: carry the row's `created_at` over and stamp `updated_at`.
            let mut row =
                crate::storage::ActivityRecord::from_value(activity.clone(), act.active_children);
            row.created_at = act.created_at;
            row.updated_at = ctx.timestamp;
            ctx.storage.put_activity(row).await?;
        }
        Ok(())
    }
}
