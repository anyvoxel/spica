use crate::TaskStatus;
use crate::handler::{Collector, HandlerContext};
use crate::types::event::Event;
use crate::types::meta::ObjectReference;

/// Handles `CancelTask`: an in-flight `Task` is cancelled because its owning activity/execution is
/// being torn down. Emits only `TaskCancelled`, which marks the task `Cancelled` in storage and
/// drains it from its parent. The physical work on the worker is deliberately left running (the
/// worker owns cancellation of its own call); a later `CompleteTask` for
/// this task is swallowed by the `CompleteTaskHandler`'s non-`Running` guard — exactly the race
/// guard a `CancelTimer` + late `TriggerTimer` already uses.
#[derive(Default)]
pub struct CancelTaskHandler;

impl CancelTaskHandler {
    pub(crate) async fn handle(
        &self,
        task: &ObjectReference,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) {
        let Some(task_value) = ctx
            .storage
            .get_task(task)
            .await
            .ok()
            .flatten()
            .map(|task| task.value())
        else {
            return;
        };
        out.append_event(Event::TaskCancelled {
            task: crate::Task {
                status: TaskStatus::Cancelled,
                // Stamp the cancel moment; `created_at` is carried forward by the explicit `meta`
                // reading `task_value.meta.created_at` (the `..task_value` spread still fills the
                // remaining fields).
                meta: crate::types::meta::ObjectMeta::builder(
                    crate::types::meta::ObjectKind::Task,
                    task_value.meta.uid,
                )
                .timestamps(task_value.meta.created_at, crate::log::Timestamp::now())
                .build(),
                ..task_value
            },
        })
        .await;
    }
}
