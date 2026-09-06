//! `ThreadCreated` event projection: folds the `Event::ThreadCreated` into Storage.

use std::collections::HashSet;

use async_trait::async_trait;

use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectReference};
use crate::{ActivityState, ApplierContext, EventApplier, Thread, ThreadStatus};

#[derive(Default)]
pub(crate) struct ThreadCreatedApplier;
#[async_trait]
impl EventApplier for ThreadCreatedApplier {
    fn event(&self) -> Event {
        Event::ThreadCreated {
            thread: Thread {
                execution: ObjectReference::nil(),
                state_path: jsonptr::PointerBuf::new(),
                index: 0,
                status: ThreadStatus::Running,
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
        let Event::ThreadCreated { thread, .. } = event else {
            unreachable!(
                "event dispatch guarantees the applier receives its own variant; got {event:?}"
            );
        };
        let mut row = crate::storage::ThreadRecord::from_value(thread.clone(), HashSet::new());
        // Birth: the row's `created_at`/`updated_at` are stamped with the `ThreadCreated` entry's
        // moment (deterministic across replicas — see `ApplierContext::timestamp`).
        row.born(ctx.timestamp);
        // The child Thread inherits its **enclosing scope's** current variable snapshot, so a
        // branch/item starts already seeing the variables in scope where it was spawned (a thread's
        // variable scope is projection-only, see `Thread`'s doc — so the seed happens here, not on
        // the domain event). Addressed structurally: the thread's `meta.owner` is its container
        // Activity, whose own owner is the enclosing scope (a top-level `Execution` or a fan-out
        // `Thread`); siblings spawned in the same fan-out batch all inherit the same parent snapshot.
        if let Some(owner) = thread.meta.owner.clone()
            && let Ok(Some(act)) = ctx.storage.get_activity(&owner).await
            && let Some(scope) = act.value.meta.owner.clone()
        {
            match scope.kind {
                ObjectKind::Execution => {
                    if let Some(exec) = ctx.storage.get_execution(&scope).await? {
                        row.variables = exec.variables;
                    }
                }
                ObjectKind::Thread => {
                    if let Some(parent) = ctx.storage.get_thread(&scope).await? {
                        row.variables = parent.variables;
                    }
                }
                // A non-scope owner kind carries no variable store; nothing to seed.
                _ => {}
            }
        }
        ctx.storage.put_thread(row).await?;
        super::bump_generated_seq(ctx.storage, &thread.reference().name).await?;
        // A thread is always a fan-out child, owned by its container Activity. It is added to that
        // owner's `active_children` so the owner drains (Completing/Terminating) waits on it via the
        // shared cascade, and the inline drain reaction notices it as in-flight.
        if let Some(owner) = thread.meta.owner.clone() {
            ctx.storage
                .add_child(owner.clone(), thread.reference())
                .await?;
            // Fold the thread's own `index` into the container's ordered fan-out map, so the owning
            // `Parallel`/`Map` aggregates branch/item outputs in declaration order at its
            // convergence. Dispatching on the container's existing `ActivityState`: an existing
            // `Map` repository collects item children; anything else is a `Parallel` (entered as
            // `None`, materialized on the first spawn). Because the ordinal is part of the Thread's
            // own identity, this projection needs no separate fan-out event.
            if let Some(mut act) = ctx.storage.get_activity(&owner).await? {
                match &mut act.value.activity_state {
                    Some(ActivityState::Map(progress)) => {
                        progress.children.insert(thread.index, thread.reference());
                    }
                    Some(ActivityState::Parallel(progress)) => {
                        progress.branches.insert(thread.index, thread.reference());
                    }
                    None => {
                        let ActivityState::Parallel(progress) = act
                            .value
                            .activity_state
                            .insert(ActivityState::Parallel(Default::default()))
                        else {
                            unreachable!()
                        };
                        progress.branches.insert(thread.index, thread.reference());
                    }
                }
                act.with_update_at(ctx.timestamp);
                ctx.storage.put_activity(act).await?;
            }
        }
        Ok(())
    }
}
