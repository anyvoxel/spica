//! `StateChildSettled` event projection: folds the `Event::StateChildSettled` into Storage.

use crate::ApplierContext;
use crate::types::activity::ActivityState;
use crate::types::error::ExecutionError;
use crate::types::event::StateChildSettled;

/// The child's own terminal event already folded its outcome and its ownership edge; what is left to
/// fold is the **progress** of the fan-out that consumed the settle. It has to be recorded rather
/// than derived: the hand-off is deferred, so every pending settle for a `Parallel`/`Map` is
/// dispatched after its child's row reads terminal, and none of them could tell from the children how
/// many settles came before. Appends are deduplicated, so a replayed or repeated hand-off cannot make
/// the activity converge early.
#[derive(Default)]
pub(crate) struct StateChildSettledApplier;
impl StateChildSettledApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &StateChildSettled,
    ) -> Result<(), ExecutionError> {
        let Some(mut activity) = ctx.storage.get_activity(&event.activity).await? else {
            // A settle whose activity is gone leaves nothing to fold; the hand-off that carried it is
            // refused by the activity element, so there is no progress to record here.
            return Ok(());
        };
        match activity.activity_state.as_mut() {
            Some(ActivityState::Parallel(progress)) => {
                if !progress.settled.contains(&event.child) {
                    progress.settled.push(event.child.clone());
                }
            }
            Some(ActivityState::Map(progress)) => {
                if !progress.settled.contains(&event.child) {
                    progress.settled.push(event.child.clone());
                }
            }
            // A settle only ever reaches the two states that own children, so any other repository
            // here is a log that disagrees with the element dispatch — nothing to fold.
            _ => return Ok(()),
        }
        ctx.storage.put_activity(activity).await?;
        Ok(())
    }
}
