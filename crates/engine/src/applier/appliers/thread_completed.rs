//! `ThreadCompleted` event projection: folds the `Event::ThreadCompleted` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::meta::ObjectReference;
use crate::{ApplierContext, EventApplier, Thread, ThreadStatus};

#[derive(Default)]
pub(crate) struct ThreadCompletedApplier;
#[async_trait]
impl EventApplier for ThreadCompletedApplier {
    fn event(&self) -> Event {
        Event::ThreadCompleted {
            thread: Thread {
                execution: ObjectReference::nil(),
                state_path: jsonptr::PointerBuf::new(),
                index: 0,
                status: ThreadStatus::Completed,
                input: Default::default(),
                output: Some(Default::default()),
                meta: crate::types::meta::ObjectMeta::born_placeholder(
                    crate::types::meta::ObjectKind::Thread,
                    ulid::Ulid::nil(),
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
        let Event::ThreadCompleted { thread } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut row) = ctx.storage.get_thread(&thread.reference()).await? {
            row.status = ThreadStatus::Completed;
            row.output = thread.output.clone();
            // Keep the projected domain `updated_at` in step with the event's (handler-stamped).
            row.value.meta.updated_at = thread.meta.updated_at;
            // A terminal thread cannot still hold an in-flight state activation cursor.
            row.current_activity = None;
            let parent = row.value.meta.owner.clone();
            row.touch(ctx.timestamp);
            ctx.storage.put_thread(row).await?;
            if let Some(owner) = parent {
                ctx.storage.remove_child(owner, thread.reference()).await?;
            }
        }
        Ok(())
    }
}
