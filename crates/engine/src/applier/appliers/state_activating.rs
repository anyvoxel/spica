//! `StateActivating` event projection: folds the `Event::StateActivating` activity value into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{Activity, ActivityStatus, ApplierContext, EventApplier};

use crate::storage::ActivityRecord;

#[derive(Default)]
pub(crate) struct StateActivatingApplier;
#[async_trait]
impl EventApplier for StateActivatingApplier {
    fn event(&self) -> Event {
        Event::StateActivating {
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
        let Event::StateActivating { activity } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        // `StateActivating` is the creation moment of the projection row: the event already carries
        // the canonical domain entity, and storage only adds its projection-only `active_children`
        // bookkeeping alongside it.
        let mut row =
            ActivityRecord::from_value(activity.clone(), std::collections::HashSet::new());
        // Birth: `created_at`/`updated_at` stamped with the `StateActivating` entry's moment.
        row.born(ctx.timestamp);
        ctx.storage.put_activity(row).await?;
        super::bump_generated_seq(ctx.storage, &activity.reference().name).await?;
        ctx.storage
            .add_child(
                activity
                    .meta
                    .owner
                    .clone()
                    .expect("an owned activity has an owner"),
                activity.reference(),
            )
            .await?;
        // Advance the owning scope's projection-only `current_activity` cursor. The scope that owns
        // this activity is `activity.meta.owner` (an `Execution` for a top-level activity, a `Thread`
        // for one inside a `Parallel` branch / `Map` item) — *not* `activity.execution`, which is only
        // the shared top-level anchor and would wrongly move a deeply-nested activity's cursor onto
        // the root. Matching on the owner's kind updates whichever record actually holds the cursor.
        let ownership = activity
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        let uid = activity.reference().uid;
        match ownership.kind {
            crate::types::meta::ObjectKind::Execution => {
                if let Some(mut exec) = ctx.storage.get_execution(&ownership).await? {
                    exec.current_activity = Some(uid.into());
                    exec.with_update_at(ctx.timestamp);
                    ctx.storage.put_execution(exec).await?;
                }
            }
            crate::types::meta::ObjectKind::Thread => {
                if let Some(mut thread) = ctx.storage.get_thread(&ownership).await? {
                    thread.current_activity = Some(uid.into());
                    thread.with_update_at(ctx.timestamp);
                    ctx.storage.put_thread(thread).await?;
                }
            }
            _ => {} // a non-scope owner resolves to nothing (silent) — no cursor to move.
        }
        Ok(())
    }
}
