//! `ThreadCompleting` event projection: folds the `Event::ThreadCompleting` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::meta::ObjectReference;
use crate::{ApplierContext, EventApplier, Thread, ThreadStatus};

#[derive(Default)]
pub(crate) struct ThreadCompletingApplier;
#[async_trait]
impl EventApplier for ThreadCompletingApplier {
    fn event(&self) -> Event {
        Event::ThreadCompleting {
            thread: Thread {
                execution: ObjectReference::nil(),
                state_path: jsonptr::PointerBuf::new(),
                index: 0,
                status: ThreadStatus::Completing,
                input: Default::default(),
                output: Some(Default::default()),
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
        let Event::ThreadCompleting { thread } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut row) = ctx.storage.get_thread(&thread.reference()).await? {
            row.status = ThreadStatus::Completing;
            row.output = thread.output.clone();
            // Keep the projected domain `updated_at` in step with the event's (handler-stamped).
            row.value.meta.updated_at = thread.meta.updated_at;
            row.with_update_at(ctx.timestamp);
            ctx.storage.put_thread(row).await?;
        }
        Ok(())
    }
}
