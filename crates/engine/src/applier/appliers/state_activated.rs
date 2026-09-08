//! `StateActivated` event projection: folds the `Event::StateActivated` activity value onto Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{Activity, ActivityStatus, ApplierContext, EventApplier};

#[derive(Default)]
pub(crate) struct StateActivatedApplier;
#[async_trait]
impl EventApplier for StateActivatedApplier {
    fn event(&self) -> Event {
        Event::StateActivated {
            activity: Activity {
                execution: crate::types::meta::ObjectReference::nil(),
                state_path: jsonptr::PointerBuf::new(),
                status: ActivityStatus::Running,
                raw_input: Default::default(),
                input: None,
                raw_output: None,
                activity_state: None,
                retry_state: None,
                output: None,
                meta: crate::types::meta::ObjectMeta::builder(
                    crate::types::meta::ObjectKind::Activity,
                    ulid::Ulid::nil(),
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
        let Event::StateActivated { activity } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        let Some(act) = ctx.storage.get_activity(&activity.reference()).await? else {
            return Ok(());
        };
        // Activation mutates only the domain value itself (processed input and, for Map, its
        // activation product). The independently-maintained `active_children` set is preserved; the
        // row's `created_at` is carried over (this is an update, not a birth) and `updated_at` is
        // stamped with this entry's moment.
        let mut row =
            crate::storage::ActivityRecord::from_value(activity.clone(), act.active_children);
        row.created_at = act.created_at;
        row.updated_at = ctx.timestamp;
        ctx.storage.put_activity(row).await?;
        Ok(())
    }
}
