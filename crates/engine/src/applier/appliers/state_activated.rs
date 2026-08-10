//! `StateActivated` event projection: folds the `Event::StateActivated` activation product onto the
//! activity row.
//!
//! `StateActivated` carries the state's *activation product* — state the activate step computed that
//! must be reconstructible from the event stream alone (see the [`Event::StateActivated`] docs). For
//! a `Map` state this is the iteration plan (`plan: Some(MapActivityState)`); the applier materializes it
//! into the activity's `activity_state` (`ActivityState::Map`) so the later replenish rounds (driven by
//! `child_completed`) can read the items/total/cap. For every non-container state the plan is `None`,
//! so the fold is a no-op — the activity already carries all it needs.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::ActivityId;

#[derive(Default)]
pub(crate) struct StateActivatedApplier;
#[async_trait]
impl EventApplier for StateActivatedApplier {
    fn event(&self) -> Event {
        Event::StateActivated {
            activity: ActivityId::nil(),
            input: Default::default(),
            plan: None,
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::StateActivated {
            activity,
            input,
            plan,
        } = event
        else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        let Some(mut act) = ctx.storage.get_activity(*activity).await? else {
            return Ok(());
        };
        // Record the state's processed input — the value the activate step preprocessed the raw input
        // into (or the raw input itself when no preprocessing happened). Inspector/auditor reads it
        // directly instead of re-running the projection.
        act.input = input.clone();
        // Only a Map's activation carries a plan; a `None` plan means there is nothing to fold — the
        // activity row from `StateActivating` is already complete (`map_progress` stays `None`).
        // A Map's activation product: materialize the iteration plan into the activity's state
        // repository so the replenish loop's `child_completed` can read items/total/cap without
        // re-deriving them. `activity_state` started as `ActivityState::Leaf` (from `StateActivating`).
        if let Some(plan) = plan {
            act.activity_state = crate::storage::ActivityState::Map(plan.clone());
        }
        ctx.storage.put_activity(act).await?;
        Ok(())
    }
}
