//! `ThreadCreated` event projection: folds the `Event::ThreadCreated` into Storage.

use std::collections::HashSet;

use crate::types::error::ExecutionError;
use crate::types::meta::ErasedOwner;
use crate::{ActivityState, ApplierContext, Thread, ThreadOwner};

#[derive(Default)]
pub(crate) struct ThreadCreatedApplier;
impl ThreadCreatedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        thread: &Thread,
    ) -> Result<(), ExecutionError> {
        let mut row = crate::storage::ThreadRecord::from_value(thread.clone(), HashSet::new());
        // Birth: the row's `created_at`/`updated_at` are stamped with the `ThreadCreated` entry's
        // moment (deterministic across replicas — see `ApplierContext::timestamp`).
        row.born(ctx.timestamp);
        // The child Thread inherits its **enclosing scope's** current variable snapshot, so a
        // branch/item starts already seeing the variables in scope where it was spawned (a thread's
        // variable scope is projection-only, see `Thread`'s doc — so the seed happens here, not on
        // the domain event). Addressed structurally: a fan-out thread's `meta.owner` is its container
        // Activity, whose own owner is the enclosing scope — always a `Thread` (the activity slot's
        // own type), never an `Execution` directly — so only the `Activity` variant has a scope to
        // inherit from. Siblings spawned in the same fan-out batch all inherit the same parent
        // snapshot.
        if let ThreadOwner::Activity(owner) = &thread.meta.owner
            && let Ok(Some(act)) = ctx.storage.get_activity(owner.erased()).await
            && let Some(parent) = ctx
                .storage
                .get_thread(act.value.meta.owner.erased())
                .await?
        {
            row.variables = parent.variables;
        }
        ctx.storage.put_thread(row).await?;
        super::bump_generated_seq(ctx.storage, &thread.meta.reference().name).await?;
        // A thread joins its owner's `active_children` so the owner drains (Completing/Terminating)
        // waits on it via the shared cascade, and the inline drain reaction notices it as in-flight.
        // Either owner holds that edge: a fan-out thread's container Activity, and a root thread's
        // top-level Execution, whose own drain waits on the root thread running its machine.
        let owner = thread.meta.owner.clone();
        ctx.storage
            .add_child(owner.erased().clone(), thread.meta.reference())
            .await?;
        // Fold the thread's own `index` into the container's ordered fan-out map, so the owning
        // `Parallel`/`Map` aggregates branch/item outputs in declaration order at its convergence.
        // Dispatching on the container's existing `ActivityState`: an existing `Map` repository
        // collects item children; anything else is a `Parallel` (entered as `None`, materialized on
        // the first spawn). Because the ordinal is part of the Thread's own identity, this projection
        // needs no separate fan-out event. Only an Activity aggregates a fan-out, so a root thread —
        // whose index is a fixed placeholder (see `Thread::index`) — has nothing to fold.
        let ThreadOwner::Activity(owner) = owner else {
            return Ok(());
        };
        if let Some(mut act) = ctx.storage.get_activity(owner.erased()).await? {
            match &mut act.value.activity_state {
                Some(ActivityState::Map(progress)) => {
                    progress
                        .children
                        .insert(thread.index, thread.meta.reference());
                }
                Some(ActivityState::Parallel(progress)) => {
                    progress
                        .branches
                        .insert(thread.index, thread.meta.reference());
                }
                // A `Wait` owns no fan-out — its only child is its resume timer — so no thread is
                // ever spawned under one and there is nothing to fold. Leaving the row untouched
                // keeps this projection free of a write for an owner it can never apply to.
                Some(ActivityState::Wait(_)) => return Ok(()),
                None => {
                    let ActivityState::Parallel(progress) = act
                        .value
                        .activity_state
                        .insert(ActivityState::Parallel(Default::default()))
                    else {
                        unreachable!()
                    };
                    progress
                        .branches
                        .insert(thread.index, thread.meta.reference());
                }
            }
            act.with_update_at(ctx.timestamp);
            ctx.storage.put_activity(act).await?;
        }
        Ok(())
    }
}
