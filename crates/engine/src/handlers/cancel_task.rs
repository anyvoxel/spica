use crate::handler::{Collector, HandlerContext};
use crate::handlers::container::{ActivityContainer, Container};
use crate::types::event::Event;
use crate::types::meta::ObjectReference;

/// Handles `CancelTask`: an in-flight `Task` is cancelled because its owning activity/execution is
/// being torn down. Emits `TaskCancelled`, which marks the task `Cancelled` in storage and drains it
/// from its parent; the physical work on the worker is deliberately left running (the worker owns
/// cancellation of its own call), and a later `CompleteTask` for this task is swallowed by the
/// `CompleteTaskHandler`'s non-`Running` guard — exactly the race guard a `CancelTimer` + late
/// `TriggerTimer` already uses.
///
/// The owning activity's container is resolved *before* the cancel is written, because this settle is
/// the sweep's own last move for a `Task` child and it is what lets the `Terminating` owner that
/// issued the sweep converge in the same batch. An owner that is already gone makes the cancel
/// pointless, so it is reported rather than written against a row nothing can drain.
#[derive(Default)]
pub struct CancelTaskHandler;

impl CancelTaskHandler {
    pub(crate) async fn handle(
        &self,
        task: &ObjectReference,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) {
        let Some(mut task_value) = ctx
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
        let Some(container) = ActivityContainer::open(ctx.storage, owner.clone()).await else {
            tracing::warn!(
                task = %task,
                owner = %owner,
                "cancel settle has no live owning activity; cancel dropped"
            );
            return;
        };
        task_value.cancel(ctx.now());
        out.append_event(Event::TaskCancelled { task: task_value })
            .await;
        container.after_child_terminated(ctx, out, task).await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use serde_json::json;
    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_storage::InMemoryStorage;

    use super::CancelTaskHandler;
    use crate::StatePath;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, OverlaySink};
    use crate::handlers::dispatch::build_state_handlers;
    use crate::storage::{ActivityRecord, Storage, TaskRecord};
    use crate::types::command::Command;
    use crate::types::event::Event;
    use crate::types::id::EntryId;
    use crate::types::meta::{ObjectKind, ObjectMeta, ObjectName, ObjectReference};
    use crate::working::WorkingState;
    use crate::{
        Activity, ActivityStatus, EntryPayload, Task, TaskStatus, TerminationReason, Timestamp,
    };

    /// The instant every stamp reads: one `ManualClock` reading serves the seeded rows' meta and the
    /// collector's envelopes alike.
    fn at() -> Timestamp {
        Timestamp::from_millis(1_000)
    }

    fn reference(kind: ObjectKind, name: &str, uid: u64) -> ObjectReference {
        ObjectReference::new(
            kind,
            ObjectName::from_parsed(name).expect("a static literal is a valid object name"),
            ulid::Ulid::from(u128::from(uid)),
        )
    }

    fn activity_ref() -> ObjectReference {
        reference(ObjectKind::Activity, "execution-0", 90)
    }

    /// The cancel target. Its name is load-bearing: the cancelled task keeps its own meta, and a
    /// renamed task is one its owner can no longer match against the child it owns.
    fn task_ref() -> ObjectReference {
        reference(ObjectKind::Task, "execution-0", 91)
    }

    /// The live task the sweep cancels, owned by [`activity_ref`] and leased to `w1`.
    fn seeded_task() -> TaskRecord {
        let value = Task {
            meta: ObjectMeta::builder(ObjectKind::Task, task_ref().uid)
                .name(task_ref().name)
                .at(at())
                .build()
                .with_owner(activity_ref()),
            execution: reference(ObjectKind::Execution, "execution", 70),
            resource: "service-a".to_string(),
            arguments: json!({ "in": 1 }),
            status: TaskStatus::Running,
            deadline: None,
            worker_id: Some("w1".to_string()),
            lease_expires_at: Some(Timestamp::from_millis(2_000)),
            retry_plan: Vec::new(),
            retry_state: Default::default(),
        };
        let mut row = TaskRecord::from_value(value);
        row.born(at());
        row
    }

    /// The owning activity, with the task as its one live child so the drain the cancel triggers is
    /// observable: the container may only converge an owner that has nothing left in flight.
    fn seeded_activity(
        status: ActivityStatus,
        children: HashSet<ObjectReference>,
    ) -> ActivityRecord {
        let mut path = jsonptr::PointerBuf::new();
        path.push_back("States");
        path.push_back("P");
        let activity = Activity {
            meta: ObjectMeta::builder(ObjectKind::Activity, activity_ref().uid)
                .name(activity_ref().name)
                .at(at())
                .build()
                .with_owner(reference(ObjectKind::Thread, "execution-1", 80)),
            execution: reference(ObjectKind::Execution, "execution", 70),
            state_path: StatePath::from(path),
            status,
            raw_input: json!({ "in": 1 }),
            input: Some(json!({ "in": 1 })),
            raw_output: None,
            activity_state: None,
            retry_state: None,
            output: None,
        };
        let mut row = ActivityRecord::from_value(activity, children);
        row.born(at());
        row
    }

    /// Drive one `CancelTask` over a working overlay seeded with whichever rows are wanted — the
    /// leader's shape, so the assertions read the batch the handler would commit, not a bare intent.
    async fn cancel(
        task: Option<TaskRecord>,
        activity: Option<ActivityRecord>,
    ) -> Vec<EntryPayload> {
        let mut store = InMemoryStorage::new();
        if let Some(task) = task {
            store
                .put_task(task)
                .await
                .expect("the in-memory store seeds a task row");
        }
        if let Some(act) = activity {
            store
                .put_activity(act)
                .await
                .expect("the in-memory store seeds an activity row");
        }

        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(at()));
        let ids: Arc<dyn IdGenerator> = Arc::new(CountingIdGenerator::new());
        let work = WorkingState::new(store.begin_txn().expect("the in-memory store begins a txn"));
        let mut out = Collector::new(
            EntryId::new(1),
            Some(OverlaySink::new(&work)),
            clock.clone(),
            ids.clone(),
        );
        let mut env = EvalEnv::new();
        let mut definitions = HashMap::new();
        let state_handlers = build_state_handlers();
        let mut ctx = HandlerContext {
            env: &mut env,
            storage: &work,
            clock,
            ids,
            definitions: &mut definitions,
            state_handlers: &state_handlers,
        };
        CancelTaskHandler
            .handle(&task_ref(), &mut ctx, &mut out)
            .await;
        out.into_entries()
            .into_iter()
            .map(|entry| entry.payload)
            .collect()
    }

    /// A cancelled task keeps its own identity — the reference its owner matches children by — and
    /// carries the cancel moment rather than the row's older stamp.
    #[tokio::test]
    async fn a_cancelled_task_keeps_its_own_name_and_owner() {
        let chain = cancel(
            Some(seeded_task()),
            Some(seeded_activity(
                ActivityStatus::Running,
                HashSet::from([task_ref()]),
            )),
        )
        .await;
        let EntryPayload::Event(Event::TaskCancelled { task }) = &chain[0] else {
            panic!("a cancelled task emits TaskCancelled first: {chain:?}");
        };
        assert_eq!(task.status, TaskStatus::Cancelled);
        assert_eq!(task.meta.name, task_ref().name);
        assert_eq!(task.meta.owner.as_ref(), Some(&activity_ref()));
        assert_eq!(task.meta.updated_at, at());
    }

    /// The whole point of handing the settle to the owner: the cancel drains the task from its
    /// `Terminating` owner *within the same batch*, so a sweep whose last child was this task
    /// converges instead of waiting forever for a child that is already gone.
    #[tokio::test]
    async fn a_cancelled_task_converges_its_drained_terminating_owner() {
        let chain = cancel(
            Some(seeded_task()),
            Some(seeded_activity(
                ActivityStatus::Terminating(TerminationReason::Cancelled),
                HashSet::from([task_ref()]),
            )),
        )
        .await;
        assert!(
            matches!(chain[0], EntryPayload::Event(Event::TaskCancelled { .. })),
            "the cancel is recorded before its owner is advanced: {chain:?}"
        );
        assert_eq!(
            chain.last(),
            Some(&EntryPayload::Command(Command::ContinueTerminate {
                owner: activity_ref(),
            })),
            "a drained terminating owner must be advanced: {chain:?}"
        );
    }

    /// A `Running` owner being torn down is the anomalous case — whatever is finishing it owns the
    /// next move, so the settle only records the cancel.
    #[tokio::test]
    async fn a_cancelled_task_leaves_a_running_owner_alone() {
        let chain = cancel(
            Some(seeded_task()),
            Some(seeded_activity(
                ActivityStatus::Running,
                HashSet::from([task_ref()]),
            )),
        )
        .await;
        assert_eq!(chain.len(), 1, "no owner reaction is asked for: {chain:?}");
    }

    /// An owner that is already gone is caught at container resolution, *before* the cancel is
    /// written: a `TaskCancelled` against a row nothing can drain would only orphan the task in a
    /// terminal state its owner can never observe.
    #[tokio::test]
    async fn a_task_whose_owner_is_gone_is_not_cancelled() {
        let chain = cancel(Some(seeded_task()), None).await;
        assert!(chain.is_empty(), "nothing may be written: {chain:?}");
    }

    /// A cancel for a task that was never written is a no-op, like every other guard on this path.
    #[tokio::test]
    async fn a_missing_task_emits_nothing() {
        let chain = cancel(
            None,
            Some(seeded_activity(
                ActivityStatus::Running,
                HashSet::from([task_ref()]),
            )),
        )
        .await;
        assert!(chain.is_empty(), "nothing may be written: {chain:?}");
    }
}
