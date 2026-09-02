//! `TaskCompleted` event projection: folds the `Event::TaskCompleted` into Storage.

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::{ApplierContext, EventApplier};

use crate::types::meta::ObjectKind;
use crate::{RetryState, Task, TaskStatus};

#[derive(Default)]
pub(crate) struct TaskCompletedApplier;
#[async_trait]
impl EventApplier for TaskCompletedApplier {
    fn event(&self) -> Event {
        Event::TaskCompleted {
            request_id: crate::types::id::RequestId::nil(),
            task: Task {
                execution: crate::types::meta::ObjectReference::nil(),
                resource: String::new(),
                arguments: Default::default(),
                status: TaskStatus::Completed,
                deadline: None,
                worker_id: None,
                lease_until: None,
                retry_plan: vec![],
                retry_state: RetryState::default(),
                meta: crate::types::meta::ObjectMeta::placeholder_with_times(
                    crate::types::meta::ObjectKind::Task,
                    ulid::Ulid::nil(),
                    crate::log::Timestamp::from_millis(0),
                    crate::log::Timestamp::from_millis(0),
                ),
            },
            output: Default::default(),
        }
    }

    async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        event: &Event,
    ) -> Result<(), ExecutionError> {
        let Event::TaskCompleted {
            request_id: _,
            task,
            output,
        } = event
        else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        // Mark the task Completed and drain it from its owning activity. The task's returned payload
        // is folded into the activity's `raw_output`: it is the state's raw result before the
        // complete step's `Output` projection, distinct from the immutable processed input recorded
        // during `StateActivated`.
        if let Some(mut t) = ctx.storage.get_task(&task.reference()).await? {
            let parent = t
                .meta
                .owner
                .clone()
                .expect("an owned task always has an owner");
            t.status = TaskStatus::Completed;
            // Sync the domain value's transition stamp from the event (the row's own `updated_at`
            // is the entry timestamp via `touch`, a separate concept).
            t.value.meta.touch(task.meta.updated_at);
            t.touch(ctx.timestamp);
            ctx.storage.put_task(t).await?;
            if parent.kind == ObjectKind::Activity
                && let Some(mut act) = ctx.storage.get_activity(&parent).await?
            {
                act.value.raw_output = Some(output.clone());
                act.touch(ctx.timestamp);
                ctx.storage.put_activity(act).await?;
            }
            ctx.storage.remove_child(parent, task.reference()).await?;
        }
        Ok(())
    }
}
