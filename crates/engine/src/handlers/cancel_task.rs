use crate::TaskStatus;
use crate::handler::{Collector, HandlerContext};
use crate::types::event::Event;
use crate::types::meta::ObjectReference;

/// Handles `CancelTask`: an in-flight `Task` is cancelled because its owning activity/execution is
/// being torn down. Emits `TaskCancelled`, which marks the task `Cancelled` in storage and drains it
/// from its parent; the physical work on the worker is deliberately left running (the worker owns
/// cancellation of its own call), and a later `CompleteTask` for this task is swallowed by the
/// `CompleteTaskHandler`'s non-`Running` guard — exactly the race guard a `CancelTimer` + late
/// `TriggerTimer` already uses.
///
/// After recording the terminal state, runs the inline child-settled reaction: a cancel is the
/// sweep's own last move for a `Task` child, so [`child_completed::child_settled`] is what lets the
/// `Terminating` owner that issued the sweep converge in the same batch. Without it the owner waits
/// forever for a child that is already gone.
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
        let owner = task_value.meta.owner.clone().expect("a live task is owned");
        out.append_event(Event::TaskCancelled {
            task: crate::Task {
                status: TaskStatus::Cancelled,
                // The task's own meta with only the cancel moment moved. Rebuilding it from the kind
                // and uid alone drops its `name` and `owner` (the builder falls back to a generated
                // `child-N`), and a cancel that renames its task is one its parent can no longer
                // match against the child it owns — so the task never drains and the teardown stalls.
                meta: {
                    let mut m = task_value.meta.clone();
                    m.with_update_at(ctx.now());
                    m
                },
                ..task_value
            },
        })
        .await;
        super::child_completed::child_settled(ctx, out, owner, task.clone()).await;
    }
}
