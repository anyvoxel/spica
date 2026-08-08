//! `StateActivating` event projection: folds the `Event::StateActivating` into Storage.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{ActivityId, ExecutionId, NodeId};
use crate::storage::Status;

#[derive(Default)]
pub(crate) struct StateActivatingApplier;
#[async_trait]
impl EventApplier for StateActivatingApplier {
    fn event(&self) -> Event {
        Event::StateActivating {
            execution: ExecutionId::nil(),
            activity: ActivityId::nil(),
            state: String::new(),
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
            state,
            input,
        } = event
        else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        ctx.storage
            .put_activity(crate::storage::Activity {
                id: *activity,
                parent: NodeId::Execution(*execution),
                state: state.clone(),
                status: Status::Running,
                input: input.clone(),
                output: None,
                reason: None,
                pending_output: None,
                pending_reason: None,
                active_children: std::collections::HashSet::new(),
            })
            .await?;
        ctx.storage
            .add_child(NodeId::Execution(*execution), NodeId::Activity(*activity))
            .await?;
        if let Some(mut exec) = ctx.storage.get_execution(*execution).await? {
            exec.current_state = Some(state.clone());
            exec.current_activity = Some(*activity);
            ctx.storage.put_execution(exec).await?;
        }
        Ok(())
    }
}
