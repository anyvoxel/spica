//! `ThreadTerminated` event projection: folds the `Event::ThreadTerminated` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier, Thread, ThreadStatus};

#[derive(Default)]
pub(crate) struct ThreadTerminatedApplier;
#[async_trait]
impl EventApplier for ThreadTerminatedApplier {
    fn event(&self) -> Event {
        Event::ThreadTerminated {
            thread: Thread {
                execution: crate::types::meta::ObjectReference::nil(),
                state_path: jsonptr::PointerBuf::new(),
                index: 0,
                status: ThreadStatus::Terminated(
                    crate::types::command::TerminationReason::Cancelled,
                ),
                input: Default::default(),
                output: None,
                meta: crate::types::meta::ObjectMeta::builder(
                    crate::types::meta::ObjectKind::Thread,
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
        let Event::ThreadTerminated { thread } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut row) = ctx.storage.get_thread(&thread.reference()).await? {
            row.status = thread.status.clone();
            row.value.meta.updated_at = thread.meta.updated_at;
            // Termination also clears the projection-only active cursor; no state remains current
            // once the thread itself has reached a terminal abnormal finish.
            row.current_activity = None;
            let parent = row.value.meta.owner.clone();
            row.with_update_at(ctx.timestamp);
            ctx.storage.put_thread(row).await?;
            if let Some(owner) = parent {
                ctx.storage.remove_child(owner, thread.reference()).await?;
            }
        }
        Ok(())
    }
}
