//! `ParallelBranchSpawned` event projection: records a Parallel branch's child execution under its
//! branch index, so the Parallel activity can aggregate branch outputs in order at convergence.

use async_trait::async_trait;

use crate::error::ExecutionError;
use crate::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::id::{ActivityId, ExecutionId};

#[derive(Default)]
pub(crate) struct ParallelBranchSpawnedApplier;
#[async_trait]
impl EventApplier for ParallelBranchSpawnedApplier {
    fn event(&self) -> Event {
        Event::ParallelBranchSpawned {
            activity: ActivityId::nil(),
            index: 0,
            execution: ExecutionId::nil(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::ParallelBranchSpawned {
            activity,
            index,
            execution,
        } = event
        else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut act) = ctx.storage.get_activity(*activity).await? {
            // `ParallelBranchSpawned` drives both `Parallel` branches and `Map` items (the same
            // `SpawnBranch` path). Record the child under its index in the activity's state-specific
            // repository, dispatching on the variant already there: an existing `Map` repository
            // collects item children; anything else is a `Parallel` (entered as `Leaf`, upgraded on
            // the first spawn).
            match &mut act.activity_state {
                crate::storage::ActivityState::Map(progress) => {
                    progress.children.insert(*index, *execution);
                }
                _ => {
                    let progress = match &mut act.activity_state {
                        crate::storage::ActivityState::Parallel(progress) => progress,
                        _ => {
                            act.activity_state =
                                crate::storage::ActivityState::Parallel(Default::default());
                            let crate::storage::ActivityState::Parallel(progress) =
                                &mut act.activity_state
                            else {
                                unreachable!()
                            };
                            progress
                        }
                    };
                    progress.branches.insert(*index, *execution);
                }
            }
            ctx.storage.put_activity(act).await?;
        }
        Ok(())
    }
}
