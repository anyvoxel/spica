//! `StateActivating` event projection: folds the `Event::StateActivating` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{ActivityId, ExecutionId, NodeId};
use crate::storage::ActivityStatus;

#[derive(Default)]
pub(crate) struct StateActivatingApplier;
#[async_trait]
impl EventApplier for StateActivatingApplier {
    fn event(&self) -> Event {
        Event::StateActivating {
            execution: ExecutionId::nil(),
            activity: ActivityId::nil(),
            state_path: jsonptr::PointerBuf::new(),
            input: Default::default(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::StateActivating {
            execution,
            activity,
            state_path,
            input,
        } = event
        else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        // The leaf state name is derived from the full `state_path` (`/states/<name>` → `<name>`),
        // so the two never drift apart.
        let state_name = crate::handlers::state_name_from_path(state_path.as_ptr());
        ctx.storage
            .put_activity(crate::storage::Activity {
                id: *activity,
                parent: NodeId::Execution(*execution),
                state_path: state_path.clone(),
                status: ActivityStatus::Running,
                // Initially the processed input equals the raw input: preprocessing (e.g. projecting
                // a Task's `Arguments`) has not run yet — `StateActivated` overwrites `input` once a
                // state's activate step finishes its input preprocessing.
                raw_input: input.clone(),
                input: input.clone(),
                raw_output: None,
                // A state enters with no state-specific repository yet. A container state's
                // `Leaf` is replaced by its `ActivityState::Parallel`/`Map` when its fan-out/plan is
                // recorded (`ParallelBranchSpawned` / `StateActivated`).
                activity_state: crate::storage::ActivityState::Leaf,
                // Retry bookkeeping starts empty: no retry has been scheduled yet, so both the total
                // count and every per-retrier counter begin at 0.
                retry_state: crate::storage::RetryState::default(),
                output: None,
                active_children: std::collections::HashSet::new(),
            })
            .await?;
        ctx.storage
            .add_child(NodeId::Execution(*execution), NodeId::Activity(*activity))
            .await?;
        if let Some(mut exec) = ctx.storage.get_execution(*execution).await? {
            exec.current_state = Some(state_name.clone());
            exec.current_activity = Some(*activity);
            ctx.storage.put_execution(exec).await?;
        }
        Ok(())
    }
}
