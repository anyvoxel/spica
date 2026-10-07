//! The **container side** of a child settling: what an owner does when one of its owned nodes reaches
//! a terminal state.
//!
//! A settling node names only its owner. What that settle *means* is the owner's business, so the
//! reaction lives here rather than in the settling node's own handler — a `Task`'s handler used to
//! know that its parent was an `Activity` and that the parent therefore needed a `CompleteState`. Now
//! the child hands its settle to its owner's [`Container`] and the owner decides.
//!
//! The owner's own kind picks the impl at the call site, so one container exists per kind that owns
//! children: [`ActivityContainer`] (a state's in-flight task or timer), [`ThreadContainer`] (the
//! state activities that run in one scope) and [`ExecutionContainer`] (a run's own scope children).

mod activity;
mod execution;
mod thread;

pub(crate) use activity::ActivityContainer;
pub(crate) use execution::ExecutionContainer;
pub(crate) use thread::ThreadContainer;

use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::storage::ReadonlyStorageTxn;
use crate::types::meta::{ObjectKindMarker, ObjectRef, RawObjectRef};

/// A node that owns children: the reaction point for one of them settling. The implementor is chosen
/// from the owner's *kind* at the call site (a timer's owner is an `Activity` or the `Execution`, a
/// root thread's is the `Execution`, …), so the concrete type is known statically and this is never
/// used as `dyn`.
pub(crate) trait Container {
    /// The kind of owner this container owns children for — the type of [`Self::open`]'s parameter,
    /// so an owner of another kind is unrepresentable rather than a case to fold into the answer.
    type Owner: ObjectKindMarker;

    /// Resolve the container that owns `owner`'s children, refused when the owner's row is missing.
    ///
    /// The caller runs this *before* producing the settle's effects, so an ownerless settle is caught
    /// while it can still be answered — a `Reject` for a worker-reported settle — rather than
    /// discovered as a silent no-op after the terminal event is already on the log. That is the same
    /// answer at every such call site, so the seam produces it: `Rejected(NotFound)` names the gone
    /// owner, and the call site is a plain `?`.
    ///
    /// A read that **faults** stays a fault ([`ProcessingError::Unexpected`]), so a store that hiccups
    /// is never mistaken for an owner that is gone — the leader keeps the retry, exactly as it does for
    /// the hooks' own re-reads.
    ///
    /// A handful of callers must **not** fail on a gone owner: their settle is already a fact in this
    /// same batch (a timer's fire, a node's own terminal), so a refusal here would drop the batch and
    /// lose it. They match out the `Rejected` arm and answer the absence themselves.
    ///
    /// This read is the *existence* check only. The hooks re-read the row: the settle's own batch
    /// rewrites it (draining the child, folding the payload), so the row read here is stale by the
    /// time a hook decides on it.
    async fn open(
        storage: &dyn ReadonlyStorageTxn,
        owner: ObjectRef<Self::Owner>,
    ) -> Result<Self, ProcessingError>
    where
        Self: Sized;

    /// An owned `child` reached a **success** terminal. Fired on *every* settled child, not only the
    /// last one: a container with open slots (a `Map`'s `MaxConcurrency`) decides per settle whether
    /// to replenish, converge or fail, so the hook cannot assume it is the final settle.
    ///
    /// Fallible because the hook re-reads its own row: a read that **faults** is returned, so the leader
    /// owns the retry, and an owner that is *gone* is refused rather than answered with a silent no-op —
    /// the settle's own terminal event is already in this attempt's batch, so succeeding without a
    /// reaction would leave the log saying the child settled with nothing to explain what followed.
    async fn after_child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError>;

    /// An owned `child` reached an **abnormal** terminal (cancelled, terminated, timed out). The
    /// reason is deliberately not carried: a container that is *finishing* already holds its own
    /// `Terminating(reason)` on its row — the single source of truth for why *it* is going down —
    /// and one still `Running` reads it off the settled child's row, the same place the success hook
    /// reads that child's output. Fallible for `after_child_completed`'s reason.
    async fn after_child_terminated(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError>;
}
