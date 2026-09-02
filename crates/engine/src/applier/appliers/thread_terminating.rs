//! `ThreadTerminating` event projection: folds the `Event::ThreadTerminating` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier, Thread, ThreadStatus};

#[derive(Default)]
pub(crate) struct ThreadTerminatingApplier;
#[async_trait]
impl EventApplier for ThreadTerminatingApplier {
    fn event(&self) -> Event {
        Event::ThreadTerminating {
            thread: Thread {
                execution: crate::types::meta::ObjectReference::nil(),
                state_path: jsonptr::PointerBuf::new(),
                index: 0,
                status: ThreadStatus::Terminating(
                    crate::types::command::TerminationReason::Cancelled,
                ),
                input: Default::default(),
                output: None,
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
        let Event::ThreadTerminating { thread } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        if let Some(mut row) = ctx.storage.get_thread(&thread.reference()).await? {
            row.status = thread.status.clone();
            row.value.meta.updated_at = thread.meta.updated_at;
            row.touch(ctx.timestamp);
            ctx.storage.put_thread(row).await?;
        }
        Ok(())
    }
}
