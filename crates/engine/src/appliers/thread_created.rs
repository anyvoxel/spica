//! `ThreadCreated` event projection: folds the `Event::ThreadCreated` into Storage.

use crate::types::error::ExecutionError;
use crate::types::meta::HasRawObjectRef;
use crate::{ActivityState, ApplierContext, Thread, ThreadOwner};

#[derive(Default)]
pub(crate) struct ThreadCreatedApplier;
impl ThreadCreatedApplier {
    pub(crate) async fn apply(
        &self,
        ctx: &mut ApplierContext<'_>,
        thread: &Thread,
    ) -> Result<(), ExecutionError> {
        ctx.storage.put_thread(thread.clone()).await?;
        // The child Thread inherits its **enclosing scope's** current variable snapshot, so a
        // branch/item starts already seeing the variables in scope where it was spawned (a thread's
        // variable scope is derived state kept beside the row, see `Storage::get_variables` — so the
        // seed happens here, not on the domain event, whose value carries no scope). Addressed
        // structurally: a fan-out thread's `meta.owner` is its container Activity, whose own owner is
        // the enclosing scope — always a `Thread` (the activity slot's own type), never an
        // `Execution` directly — so only the `Activity` variant has a scope to inherit from, and an
        // absent parent scope reads as empty. Siblings spawned in the same fan-out batch all inherit
        // the same parent snapshot.
        if let ThreadOwner::Activity(owner) = &thread.meta.owner
            && let Ok(Some(act)) = ctx.storage.get_activity(owner).await
        {
            let scope = ctx.storage.get_variables(&act.meta.owner).await?;
            ctx.storage
                .put_variables(&thread.meta.object_ref(), scope)
                .await?;
        }
        super::bump_generated_seq(ctx.storage, &thread.meta.name).await?;
        // A thread becomes its owner's child so the owner's drain (Completing/Terminating) waits on
        // it via the shared cascade, and the inline drain reaction notices it as in-flight. This is an
        // ownership edge — a cell of its own, independent of the owner's row. Either owner holds that
        // edge: a fan-out thread's container Activity, and a root thread's top-level Execution, whose
        // own drain waits on the root thread running its machine.
        let owner = thread.meta.owner.clone();
        ctx.storage
            .add_child(
                owner.as_raw_object_ref().clone(),
                thread.meta.raw_object_ref(),
            )
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
        if let Some(mut act) = ctx.storage.get_activity(&owner).await? {
            match &mut act.activity_state {
                Some(ActivityState::Map(progress)) => {
                    progress
                        .children
                        .insert(thread.index, thread.meta.object_ref());
                }
                Some(ActivityState::Parallel(progress)) => {
                    progress
                        .branches
                        .insert(thread.index, thread.meta.object_ref());
                }
                // A `Wait` owns no fan-out — its resume instant is a field on its own row — so no thread is
                // ever spawned under one and there is nothing to fold. Leaving the row untouched
                // keeps this projection free of a write for an owner it can never apply to.
                Some(ActivityState::Wait(_)) => return Ok(()),
                None => {
                    let ActivityState::Parallel(progress) = act
                        .activity_state
                        .insert(ActivityState::Parallel(Default::default()))
                    else {
                        unreachable!()
                    };
                    progress
                        .branches
                        .insert(thread.index, thread.meta.object_ref());
                }
            }
            // The event carries no activity value, so this fold's own write is stamped from the
            // entry's frozen moment — deterministic on replay, never a local clock.
            act.meta.with_update_at(ctx.timestamp);
            ctx.storage.put_activity(act).await?;
        }
        Ok(())
    }
}
