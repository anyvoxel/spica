use serde_json::Value;

use super::{ActivityElementBehavior, Container};
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::storage::ReadonlyStorageTxn;
use crate::types::activity::ActivityKind;
use crate::types::command::{
    ActivateState, Command, CompleteThread, SpawnThread, TerminateState, TerminateThread,
    TerminationReason,
};
use crate::types::event::Event;
use crate::types::meta::{HasRawObjectRef, ObjectKind, ObjectRef, RawObjectRef, ThreadOwner};
use crate::types::thread::{ThreadKind, ThreadStatus};
use crate::{ActivityStatus, RejectionType};

use super::ExecutionElementBehavior;

/// The behavior of a `Thread` — one scope's activity states, and everything it does as a child of its
/// owner or the owner of its own children.
///
/// Two faces of one element, gathered here from where they were split before: the **container** side
/// (what a thread does when one of its own activity children settles) and the **command** side (the
/// `TerminateThread`/`CompleteThread` verbs that open its finish, and the `Continue*` hops that advance
/// each). A thread that is *finished* hands its settle to its owner's `Container` — a run closes, or a
/// `Parallel`/`Map` converges — so this element is also where a thread's result is relayed up.
pub(crate) struct ThreadElementBehavior {
    thread: ObjectRef<ThreadKind>,
}

impl Container for ThreadElementBehavior {
    type Owner = ThreadKind;

    async fn open(
        storage: &dyn ReadonlyStorageTxn,
        owner: ObjectRef<Self::Owner>,
    ) -> Result<Self, ProcessingError> {
        // The thread is the seam's own existence check (see `Container::open`): a thread that is gone is
        // refused here, uniformly for every caller whose settle would have nothing to land on.
        if storage.get_thread(&owner).await?.is_none() {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("thread_element: thread {owner} is gone"),
            ));
        }
        Ok(Self { thread: owner })
    }

    // Both hooks decide on one pair: *how the child settled* (which hook) and *where the thread
    // stands* (its status). A thread's child is always an `Activity` (see `ThreadElementBehavior`), so
    // the child's kind adds no choice — the table is the same in both hooks, only the `Running` arm
    // differs, since a thread still running has to be told what the settle *means* while one already
    // finishing only has to be advanced. Whether the thread has *drained* is not checked here: a
    // `Continue*` cell names the thread this settle advances, and that command's own handler takes down
    // anything still attached and stops short of advancing, so the settle that drains the thread last is
    // the one that advances it.

    async fn after_child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        // The thread was resolved *before* the settle's own terminal event was written, and this re-read
        // runs in that same batch — so a row that is gone here is the lifecycle disagreeing with the log
        // rather than a settle to ignore. Refused, never no-op'ed: the terminal is already on this batch,
        // and a silent success would leave it unexplained. A fault is the leader's to retry.
        let Some(thread) = ctx.storage.get_thread(&self.thread).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "thread_element: thread {} is gone from under its settled child",
                    self.thread
                ),
            ));
        };
        match &thread.value.status {
            // The scope's own success: a `Succeed` is a thread's only natural end, so the thread
            // completes on the settled child's output — read off that child's own row, the single source
            // of truth, rather than carried on the call. A terminal that projects no value folds as no
            // `output` at all, hence the `Null` fallback: a null-returning terminal still completes its
            // thread, where a none-event would strand it.
            ThreadStatus::Running => {
                let output = ctx
                    .storage
                    .get_activity(&child.clone().typed::<ActivityKind>())
                    .await?
                    .map(|act| act.value.output.clone().unwrap_or(Value::Null))
                    .unwrap_or(Value::Null);
                out.append_command(Command::CompleteThread(CompleteThread {
                    thread: self.thread.clone(),
                    output,
                }));
                Ok(())
            }
            // Already finishing: this settle advances the thread, whether it was a `Completing`
            // thread's bystander landing or a `Terminating` one's swept child.
            ThreadStatus::Completing => {
                out.append_command(Command::ContinueComplete {
                    owner: self.thread.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            ThreadStatus::Terminating(_) => {
                out.append_command(Command::ContinueTerminate {
                    owner: self.thread.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            // A terminal thread has no child left to settle: the row and the lifecycle disagree, and the
            // batch is refused rather than advanced.
            status => Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "thread_element: thread {} settled a completed child {child} while {status:?}",
                    self.thread
                ),
            )),
        }
    }

    /// A state activity under this thread reached an abnormal terminal (a leaf `Fail`, or a teardown's
    /// `Cancelled` sweep) — the same table as the success hook, so the thread is taken down with the
    /// reason its child failed with while it is still `Running`, and advanced when it is already
    /// finishing.
    async fn after_child_terminated(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        let Some(thread) = ctx.storage.get_thread(&self.thread).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "thread_element: thread {} is gone from under its settled child",
                    self.thread
                ),
            ));
        };
        match &thread.value.status {
            // The thread is the scope the failed state ran in, so it goes down with the same reason —
            // read off the settled child's own row, the single source of truth, rather than carried on
            // the call. An abnormal hook whose child is not in an abnormal terminal, or whose row is
            // already gone, has no reason to propagate: a running thread is left alone rather than taken
            // down with a reason this container would have to invent.
            ThreadStatus::Running => {
                let reason = match ctx
                    .storage
                    .get_activity(&child.clone().typed::<ActivityKind>())
                    .await?
                {
                    Some(act) => match &act.value.status {
                        ActivityStatus::Terminated(reason) => Some(reason.clone()),
                        _ => None,
                    },
                    None => None,
                };
                let Some(reason) = reason else {
                    tracing::debug!(
                        thread = %self.thread,
                        child = %child,
                        "settled child carries no termination reason; no reaction"
                    );
                    return Ok(());
                };
                out.append_command(Command::TerminateThread(TerminateThread {
                    thread: self.thread.clone(),
                    reason,
                }));
                Ok(())
            }
            ThreadStatus::Completing => {
                out.append_command(Command::ContinueComplete {
                    owner: self.thread.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            ThreadStatus::Terminating(_) => {
                out.append_command(Command::ContinueTerminate {
                    owner: self.thread.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            status => Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "thread_element: thread {} settled a terminated child {child} while {status:?}",
                    self.thread
                ),
            )),
        }
    }
}

impl ThreadElementBehavior {
    /// Handles `TerminateThread`: begins the abnormal finish of a fan-out `Thread` with `reason`.
    /// Resolved against **thread** storage: a `Thread` is only terminated internally by its owning
    /// container Activity (a `Parallel`/`Map` sweep tearing down a branch/item), never by the external
    /// name-addressed root terminate. Emits `ThreadTerminating`; a thread with nothing left to sweep lands
    /// `ThreadTerminated` in this same batch and hands the settle to its owner's `Container`, while one
    /// whose child is still live sweeps it and finishes when that child settles back. A thread's only child
    /// kind is `Activity`, so a nested fan-out is unwound one layer down — by the `TerminateState` that
    /// child receives — rather than by a thread sweeping a thread.
    pub(crate) async fn terminate(
        p: &TerminateThread,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let TerminateThread { thread, reason } = p;
        // Addressed by kind: `TerminateThread` is only ever dispatched for a `Thread`, so the row is
        // read directly. Nothing ever removes a row, and a thread row is written by the batch that
        // creates it — before anything could name it in a sweep — so a miss is the log and the
        // projection disagreeing (a forged command, or a corrupt store) rather than a thread that
        // outlived its teardown. A fault reading the row is neither, and is returned so the leader can
        // retry it.
        let Some(thread_row) = ctx.storage.get_thread(thread).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("terminate_thread: thread {thread} does not exist; termination refused"),
            ));
        };
        let thread_ref = thread_row.meta.raw_object_ref();
        // The whole precondition — only a `Running` thread has a teardown to begin — lives in the
        // transition (`mark_terminating`), so no call site can honor part of it and forget the rest.
        //
        // A thread that is already finishing or terminal is *expected* to be swept twice — one branch
        // thread is reachable from its container's own sweep and from the run-level cascade, so two of
        // them can tear the same branch down. Refused rather than dropped, so the durable log records
        // that this sweep was a duplicate rather than leaving a bogus one indistinguishable from an
        // absorbed one.
        let mut terminating_thread = thread_row.value();
        if let Err(why) = terminating_thread.mark_terminating(reason.clone(), ctx.now()) {
            tracing::warn!(
                thread = %thread_ref,
                status = ?thread_row.value.status,
                reason = ?reason,
                "termination arrived for a thread that is already past Running; refused: {why}"
            );
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("terminate_thread: thread {thread_ref} cannot be terminated: {why}"),
            ));
        }
        out.append_event(Event::ThreadTerminating {
            thread: terminating_thread,
        })
        .await;

        // A thread's abnormal finish reaches its owner only **once the thread itself has cleaned up**
        // — an empty child set below, or the last sweep settle later — never at the moment its own
        // teardown opens. Reaching earlier (as the run-level relay here once did) starts a parent's
        // teardown while this thread still holds children, so the parent sweeps a subtree that is
        // already unwinding and every command it issues lands against a row past `Running`.
        let children = thread_row.active_children.clone();
        if children.is_empty() {
            let mut terminated_thread = thread_row.value();
            terminated_thread.status = ThreadStatus::Terminated(reason.clone());
            terminated_thread.meta.with_update_at(ctx.now());

            // Which parent the settled thread reports to is decided by the *type* of its owner — a
            // `ThreadOwner` sum, so each variant names its own container — never by comparing a runtime
            // kind. The container is resolved *before* `ThreadTerminated`: an ownerless settle is
            // refused while it can still be answered, rather than discovered as a no-op once the
            // terminal event is already on the log. What the settle *means* is the owner's business: a
            // root thread landing here is what starts its run's own teardown.
            match thread_row.value.meta.owner.clone() {
                ThreadOwner::Execution(execution) => {
                    let container =
                        ExecutionElementBehavior::open(ctx.storage, execution.clone()).await?;
                    out.append_event(Event::ThreadTerminated {
                        thread: terminated_thread,
                    })
                    .await;
                    container
                        .after_child_terminated(ctx, out, &thread_ref)
                        .await?;
                }
                // A fan-out thread is owned by its container Activity; the hook runs the settle so the
                // `Parallel`/`Map` converges through the state's own decision.
                ThreadOwner::Activity(activity) => {
                    let container =
                        ActivityElementBehavior::open(ctx.storage, activity.clone()).await?;
                    out.append_event(Event::ThreadTerminated {
                        thread: terminated_thread,
                    })
                    .await;
                    container
                        .after_child_terminated(ctx, out, &thread_ref)
                        .await?;
                }
            }
            return Ok(());
        }

        // A thread's only child kind is `Activity`: `ActivityKind::OwnedBy` is `ObjectRef<ThreadKind>`
        // and so is the one edge that can parent an object to a thread, while `ThreadOwner` and
        // `TimerOwner` admit no thread at all and an `Execution`/`Task` is never owned by one. A nested
        // fan-out therefore reaches its teardown through the `TerminateState` below — that activity's
        // own sweep reaps its child threads, timers and tasks — never thread-sweeping-thread.
        //
        // Any other kind in `active_children` is thus an engine invariant broken — a corrupt projection
        // or a forged child edge. Refuse to guess and fail loud, rather than leave the teardown
        // deferred on a child no sweep can reap.
        for child in children.iter().cloned() {
            match child.kind {
                ObjectKind::Activity => {
                    out.append_command(Command::TerminateState(TerminateState {
                        activity: child.typed::<ActivityKind>(),
                        // Swept, not failing: this thread is going down, and its children are being
                        // taken with it — so they carry `Cancelled` rather than this thread's own
                        // `reason`. Passing the reason down would put the original error on a
                        // bystander's row and, for a state with a `Catch`, let it route a failure that
                        // was never its own.
                        reason: TerminationReason::Cancelled,
                    }));
                }
                other => panic!("engine regression: a Thread owns no {other:?} child"),
            }
        }
        tracing::debug!(
            thread = %thread_ref,
            children = children.len(),
            "thread terminating deferred: waiting on owned children"
        );

        Ok(())
    }

    /// Handles `CompleteThread`: begins the success finish of a fan-out `Thread` (a `Parallel` branch's
    /// or a `Map` item's terminal `Succeed`/`End` reached). Emits `ThreadCompleting`, which fixes its
    /// output on its row; nothing else can be pending — a thread owns activities only, and the activity
    /// relaying this completion has already drained — so `ThreadCompleted` follows in the same batch and
    /// the settle is handed to the owner's `Container`, which lets the owning container Activity
    /// converge once its last branch/item drains (or closes the run, for a root thread).
    ///
    /// A `Thread` is completed **internally** by its own terminal hop (via `emit_transition`), never by
    /// an external caller.
    pub(crate) async fn complete(
        p: &CompleteThread,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let CompleteThread { thread, output } = p;
        // Addressed by kind: `CompleteThread` is only ever dispatched for a `Thread`, so the row is
        // read directly. Nothing ever removes a row, and a thread row is written by the batch that
        // creates it — before anything could complete it — so a miss is the log and the projection
        // disagreeing (a forged command, or a corrupt store) rather than a thread that outlived its own
        // finish. A fault reading the row is neither, and is returned so the leader can retry it.
        let Some(thread_row) = ctx.storage.get_thread(thread).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("complete_thread: thread {thread} does not exist; completion refused"),
            ));
        };
        let thread_ref = thread_row.meta.raw_object_ref();
        // A thread that is already finishing or terminal: the success finish's intent is already
        // satisfied. The duplicate is a race rather than a bug — a branch thread reaching its terminal
        // hop drafts this command while its container's own sweep drafts `TerminateThread` for the same
        // thread, and the leader dispatches in append order, so whichever lands second finds the thread
        // past `Running`. Refused rather than dropped, so the durable log records that this finish was a
        // duplicate rather than leaving it indistinguishable from one that applied.
        if !thread_row.value.status.is_running() {
            let phase = thread_row.value.status.phase();
            tracing::warn!(
                thread = %thread_ref,
                status = ?thread_row.value.status,
                "completion arrived for a thread that is already past Running; refused"
            );
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "complete_thread: thread {thread_ref} is already {phase}; completion refused"
                ),
            ));
        }

        let mut completing_thread = thread_row.value();
        completing_thread.status = ThreadStatus::Completing;
        completing_thread.output = Some(output.clone());
        // A new lifecycle transition — advance the domain `updated_at` (stemmed at event
        // construction, not from Entry metadata); `created_at` is carried forward unchanged.
        completing_thread.meta.with_update_at(ctx.now());
        out.append_event(Event::ThreadCompleting {
            thread: completing_thread,
        })
        .await;

        // A thread owns **activities only** — a deadline belongs to the activity that armed it
        // (`TimerOwner` has no `Thread` variant) — and the activity relaying this completion has
        // already drained, so an empty set is the ordinary case: the thread finishes here and hands its
        // result up to its owner.
        if thread_row.active_children.is_empty() {
            let mut completed_thread = thread_row.value();
            completed_thread.status = ThreadStatus::Completed;
            completed_thread.output = Some(output.clone());
            completed_thread.meta.with_update_at(ctx.now());

            // Which parent the settled thread reports to is decided by the *type* of its owner — a
            // `ThreadOwner` sum, so each variant names its own container — never by comparing a
            // runtime kind. The container is resolved *before* `ThreadCompleted`: an ownerless settle
            // is refused while it can still be answered, rather than discovered as a no-op once the
            // terminal event is already on the log. The hook then lets the owner decide what the
            // settle *means* (a run closes; a `Parallel`/`Map` converges).
            match thread_row.value.meta.owner.clone() {
                ThreadOwner::Execution(execution) => {
                    let container =
                        ExecutionElementBehavior::open(ctx.storage, execution.clone()).await?;
                    out.append_event(Event::ThreadCompleted {
                        thread: completed_thread,
                    })
                    .await;
                    container
                        .after_child_completed(ctx, out, &thread_ref)
                        .await?;
                }
                // A fan-out thread is owned by its container Activity; the hook runs the settle so the
                // parallel/map converges (or replenishes) through the state's own decision.
                ThreadOwner::Activity(activity) => {
                    let container =
                        ActivityElementBehavior::open(ctx.storage, activity.clone()).await?;
                    out.append_event(Event::ThreadCompleted {
                        thread: completed_thread,
                    })
                    .await;
                    container
                        .after_child_completed(ctx, out, &thread_ref)
                        .await?;
                }
            }
            return Ok(());
        }

        // Anything still attached is the row's `active_children` disagreeing with the lifecycle rather
        // than a child to wait on: there is no deadline here to cancel, and an activity a thread could
        // still be waiting on is the very one relaying this completion. Refused rather than closed
        // over, and a refusal strands nothing — the child's own settle drains the thread.
        Err(ProcessingError::Rejected(
            RejectionType::InvalidState,
            format!(
                "complete_thread: thread {thread_ref} still owns {} live child(ren); completion refused",
                thread_row.active_children.len()
            ),
        ))
    }

    /// The **failure** side of the deferred drain-continuation for a thread: settles a thread that a
    /// settled child drained, emitting its `ThreadTerminated` and handing the settle to its owner's
    /// `Container` — the same reaction point the inline `terminate` uses — so a root thread that drains
    /// *here*, after the last of its own children settled, starts its run's teardown exactly as one that
    /// had nothing to wait for does.
    pub(crate) async fn continue_terminate(
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        node: ObjectRef<ThreadKind>,
    ) -> Result<(), ProcessingError> {
        // A read fault is the dispatch's — returned, so the leader owns retry/refusal. A missing row is
        // a real refusal: the Continue names the thread a settled child drained, so a gone row means the
        // command's own precondition failed rather than "already closed".
        let Some(thread) = ctx.storage.get_thread(&node).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("continue_terminate: thread {node} not found"),
            ));
        };
        // The hop lands only on a `Terminating` thread (the reason was fixed when the teardown began);
        // a thread that moved under the read above is refused like the misses above.
        let ThreadStatus::Terminating(reason) = &thread.status else {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "continue_terminate: thread {node} is not terminating (status {:?})",
                    thread.status
                ),
            ));
        };
        if !thread.active_children.is_empty() {
            // Unreachable by construction — a Continue is issued only for a drained node. A thread
            // holds `Activity` children and nothing else (`ActivityKind::OwnedBy` is
            // `ObjectRef<ThreadKind>`, and it is the one edge that can parent an object to a thread), so
            // any other kind here is the row disagreeing with the lifecycle rather than a child to reap.
            for child in &thread.active_children {
                match child.kind {
                    ObjectKind::Activity => {
                        out.append_command(Command::TerminateState(TerminateState {
                            activity: child.clone().typed::<ActivityKind>(),
                            reason: TerminationReason::Cancelled,
                        }))
                    }
                    kind => panic!(
                        "continue_terminate: thread {node} owns a live {kind} child {child}; a \
                         thread's children are only Activities"
                    ),
                }
            }
            return Ok(());
        }
        let mut terminated_thread = thread.value();
        terminated_thread.status = ThreadStatus::Terminated(reason.clone());
        terminated_thread.meta.with_update_at(ctx.now());
        out.append_event(Event::ThreadTerminated {
            thread: terminated_thread,
        })
        .await;
        // The drained thread hands its settle to its owner's `Container` — the same reaction point the
        // inline path uses (see `terminate`'s command handler) — so a root thread that drains
        // *here*, after the last of its own children settled, starts its run's teardown exactly as one
        // that had nothing to wait for does. A kind-routed dispatch would not: it has no arm for
        // a still-`Running` run, which is where that teardown has to come from.
        let child = node.into_raw_object_ref();
        match thread.value.meta.owner.clone() {
            ThreadOwner::Execution(execution) => {
                match ExecutionElementBehavior::open(ctx.storage, execution).await {
                    Ok(container) => container.after_child_terminated(ctx, out, &child).await?,
                    // The settle's own terminal is already on this batch: refusing a gone owner here
                    // would drop it and leave the node unfinished, so the rail is simply absent.
                    Err(ProcessingError::Rejected(..)) => {}
                    Err(err) => return Err(err),
                }
            }
            ThreadOwner::Activity(activity) => {
                match ActivityElementBehavior::open(ctx.storage, activity).await {
                    Ok(container) => container.after_child_terminated(ctx, out, &child).await?,
                    Err(ProcessingError::Rejected(..)) => {}
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(())
    }

    /// The **success** side of the deferred drain-continuation for a thread: settles a thread that a
    /// settled child drained, emitting its `ThreadCompleted` and handing the settle to its owner's
    /// `Container` — the owner's *type* names the container, so a root thread closes its run and a
    /// fan-out thread converges its activity.
    pub(crate) async fn continue_complete(
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        node: ObjectRef<ThreadKind>,
    ) -> Result<(), ProcessingError> {
        // A read fault is the dispatch's — returned, so the leader owns retry/refusal. A missing row is
        // a real refusal: the Continue names the thread a settled child drained, so a gone row means the
        // command's own precondition failed rather than "already closed".
        let Some(thread) = ctx.storage.get_thread(&node).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("continue_complete: thread {node} not found"),
            ));
        };
        // The hop lands only on a `Completing` thread — one that a settled child drained (see
        // `child_completed`). Any other status means the row moved under the read above.
        if !matches!(thread.status, ThreadStatus::Completing) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "continue_complete: thread {node} is not completing (status {:?})",
                    thread.status
                ),
            ));
        }
        if !thread.active_children.is_empty() {
            // Unreachable by construction — a Continue is issued only for a drained node. A thread
            // holds `Activity` children and nothing else, so any other kind here is the row disagreeing
            // with the lifecycle rather than a child to take down.
            for child in &thread.active_children {
                match child.kind {
                    ObjectKind::Activity => {
                        out.append_command(Command::TerminateState(TerminateState {
                            activity: child.clone().typed::<ActivityKind>(),
                            reason: TerminationReason::Cancelled,
                        }))
                    }
                    kind => panic!(
                        "continue_complete: thread {node} owns a live {kind} child {child}; a thread's \
                         children are only Activities"
                    ),
                }
            }
            return Ok(());
        }
        let output = thread.output.clone().unwrap_or_default();
        let mut completed_thread = thread.value();
        completed_thread.status = ThreadStatus::Completed;
        completed_thread.output = Some(output);
        completed_thread.meta.with_update_at(ctx.now());
        out.append_event(Event::ThreadCompleted {
            thread: completed_thread,
        })
        .await;
        // The settled thread hands its settle to its owner's `Container` — the owner's *type* names the
        // container, so a root thread closes its run and a fan-out thread converges its activity.
        let child = node.into_raw_object_ref();
        match thread.value.meta.owner.clone() {
            ThreadOwner::Execution(execution) => {
                match ExecutionElementBehavior::open(ctx.storage, execution).await {
                    Ok(container) => container.after_child_completed(ctx, out, &child).await?,
                    // The settle's own terminal is already on this batch: refusing a gone owner here
                    // would drop it and leave the node unfinished, so the rail is simply absent.
                    Err(ProcessingError::Rejected(..)) => {}
                    Err(err) => return Err(err),
                }
            }
            ThreadOwner::Activity(activity) => {
                match ActivityElementBehavior::open(ctx.storage, activity).await {
                    Ok(container) => container.after_child_completed(ctx, out, &child).await?,
                    Err(ProcessingError::Rejected(..)) => {}
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(())
    }

    /// Handles `SpawnThread`: fans out one branch of a `Parallel` state — or one item of a `Map` state —
    /// into a new child **Thread** (a scoped sub-run, distinct from a top-level `Execution`).
    ///
    /// The child is projected by a `ThreadCreated{parent, execution, state_path}` (which wires it
    /// into its owner's `active_children` via the applier), then entered at its `StartAt` state via
    /// `ActivateState`. From there it runs as a self-contained sub-state-machine — its internal hops
    /// happen entirely within the thread, and its terminal hop runs the inline child-settled reaction
    /// back to `parent`, so the owning `Parallel`/`Map` activity only learns the child settled
    /// when the whole branch/item finishes. This is the flat (non-recursive) fan-out: the thread is
    /// stored as one row with a `state_path` into the shared machine, never copying branch state.
    ///
    /// For a `Map`, `index` carries the **item index** and `start_at`/`input` are the item
    /// processor's `StartAt` and the item's input value — the same command shape, reused verbatim for
    /// map items via the identical `index`→`children` mapping.
    ///
    /// TODO(command-design): the rename from `SpawnBranch` resolved the naming concern — `SpawnThread`
    /// now matches the `Thread` entity it creates. The remaining concern is the payload: it still does
    /// not preserve the higher-level source context (container kind, whether the index is a branch or
    /// item index, and any future fan-out metadata), only how to start the child. Extend the payload
    /// before more fan-out modes or source-specific behavior are added.
    pub(crate) async fn spawn(
        p: &SpawnThread,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let SpawnThread {
            owner,
            execution,
            state_path,
            index,
            start_at,
            input,
        } = p;

        // A fan-out is always emitted while its container is `Running` — both `after_activated` sites
        // activate it in the same batch, and the `Map` replenish runs only on the `Running` arm — and the
        // leader dispatches in append order, so a command that could stop the container is always
        // appended *after* this one. An owner that is missing, or present but no longer `Running`, is
        // therefore the log and the projection disagreeing rather than a command that arrived late:
        // refused, not dropped silently. A fault reading the row is neither, and is returned so the
        // leader can retry it.
        let owner_activity = match ctx.storage.get_activity(owner).await? {
            Some(a) => a,
            None => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("spawn_thread: owner activity {owner} not found"),
                ));
            }
        };
        if !owner_activity.status.is_running() {
            tracing::warn!(
                owner = %owner,
                status = ?owner_activity.status,
                "fan-out against a container that is no longer Running; refused"
            );
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "spawn_thread: owner activity {owner} is {}, not Running; fan-out refused",
                    owner_activity.status.phase()
                ),
            ));
        }

        // The child Thread runs against the *same* machine version as its owner: the owning
        // activity's parent is the owning **scope** (a top-level `Execution` or a fan-out `Thread`),
        // and the whole tree binds to one definition (that of the top-level `Execution`). The child
        // does **not** re-record `flow_version` (derived via its `execution` later, see `Thread`) —
        // it only inherits the tree's top-level anchor verbatim, so its states resolve against the
        // same shared machine doc.
        let scope_ref = owner_activity.value.meta.owner.clone();
        // Resolve the owning thread to inherit the tree's top-level anchor (`execution`): the child
        // Thread shares the owner's tree, so the anchor is taken verbatim while the Thread itself is
        // the child's `owner`. The slot admits only a `Thread`, so the row is read directly.
        //
        // Nothing ever removes a row, and an activity's owner slot is written in the batch that births
        // it — with a thread created in that same batch. An activity that is here, naming a thread that
        // is not, is therefore the log and the projection disagreeing, not a fan-out that arrived
        // late: refused, like the two arms above.
        let Some(owner_thread) = ctx.storage.get_thread(&scope_ref).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!(
                    "spawn_thread: owner activity {owner} names thread {scope_ref}, which does not exist; fan-out refused"
                ),
            ));
        };
        let root_execution = owner_thread.value.execution.clone();

        // Mint the child's uid and its stable RawObjectRef up front: the child `Thread` row is
        // keyed by that reference, and the sibling `ActivateState` entry must name the same run
        // before the `ThreadCreated` applier builds the entity.
        let uid: ulid::Ulid = ctx.mint();
        // Name the thread as a child of its owning execution (the #3/#11/#13 convention, applied to
        // threads): the generated name's plain base is the owning execution's name, inherited
        // verbatim through every nesting level — so a branch thread still names its root run — and
        // its suffix is a random tail via `PlainName::to_generated`, decoupled from the thread's own
        // `uid`. Not the opaque `child-<uid>` placeholder. Minted once and reused for the reference
        // and the serialized `meta.name`, so the storage row key (`thread.meta.raw_object_ref()`) matches
        // the sibling `ActivateState` owner.
        let thread_name = execution
            .name()
            .base()
            .generated_from_key(out.next_generated_seq().await);
        let reference =
            crate::types::meta::ObjectRef::<crate::ThreadKind>::new(thread_name.clone(), uid);
        tracing::debug!(child = ?reference, owner = ?owner, "spawning child thread from fan-out command");

        // Root the child in the owning tree: `parent` links it to the Parallel activity (whose
        // `active_children` the applier populates, so the Parallel drains only once every branch
        // settles); `execution` inherits the top-level run's id verbatim (the flat query
        // anchor shared by the whole tree); `state_path` lets the child resolve its own branch
        // states without querying its parent or the root — it already names the branch's `States`
        // table within the single shared machine document.
        //
        // A thread's `state_path` is its defining property — it always descends into the shared
        // machine — so the command must carry it. `SpawnThread` is only issued by the container
        // states (Parallel/Map), which always compute the branch/item pointer.
        let branch_states = state_path
            .clone()
            .expect("a spawned thread always receives its state_path from the container");
        out.append_event(Event::ThreadCreated {
            thread: crate::Thread {
                execution: execution.clone(),
                state_path: branch_states.clone(),
                // The entry point inside that table travels with the thread too, so the entity names
                // its whole sub-run (which table it runs, and where in it it starts) rather than
                // leaving the entry recoverable only from the `ActivateState` emitted just below.
                start_at: start_at.clone(),
                // The thread records its own ordinal (branch/item index in declaration order); the
                // container's ordered fan-out map is projected from this by the `ThreadCreated`
                // applier, so the value lives here on the entity as its identity.
                index: *index,
                status: crate::ThreadStatus::Running,
                input: input.clone(),
                output: None,
                // Birth: `created_at == now` (fan-out moment). The owner is the SpawnThread command's
                // `parent` Parallel/Map activity — its checked conversion above is what makes the
                // `Activity` variant the only one a fan-out thread can carry.
                meta: crate::types::meta::ObjectMeta::builder(uid)
                    .name(thread_name)
                    .at(ctx.now())
                    .with_owner(crate::types::meta::ThreadOwner::Activity(owner.clone())),
            },
        })
        .await;
        // Enter the branch at its `StartAt` state. The child's path = the thread's branch/item
        // `state_path` (its enclosing `States` table) extended by the start state's name, so the
        // carried path locates the state self-containedly. `owner` is the new Thread (the branch's
        // immediate scope); `execution` is the inherited top-level anchor.
        out.append_command(Command::ActivateState(ActivateState {
            execution: root_execution,
            owner: reference,
            state_path: branch_states.state(start_at),
            input: input.clone(),
        }));

        Ok(())
    }
}

#[cfg(test)]
mod settle_tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use serde_json::{Value, json};
    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_storage::InMemoryStorage;
    use spica_testing::MockReadonlyStorageTxn;

    use super::{Container, ThreadElementBehavior};
    use crate::RejectionType;
    use crate::StatePath;
    use crate::StorageError;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, OverlaySink, ProcessingError};
    use crate::handlers::build_state_handlers;
    use crate::handlers::fixtures::{at, object_ref};
    use crate::storage::{ActivityRecord, ReadonlyStorageTxn, Storage, ThreadRecord};
    use crate::types::activity::{ActivityKind, ActivityStatus};
    use crate::types::command::{Command, CompleteThread, TerminateThread, TerminationReason};
    use crate::types::execution::ExecutionKind;
    use crate::types::id::EntryId;
    use crate::types::meta::{HasRawObjectRef, ObjectMeta, ObjectRef, RawObjectRef, ThreadOwner};
    use crate::types::thread::{Thread, ThreadKind, ThreadStatus};
    use crate::types::timer::TimerKind;
    use crate::working::WorkingState;
    use crate::{Activity, EntryPayload};

    fn thread_ref() -> ObjectRef<ThreadKind> {
        object_ref("execution-1", 80)
    }

    fn execution_ref() -> ObjectRef<ExecutionKind> {
        object_ref("execution", 70)
    }

    /// The settled child handed to the container — a one-state activity under the thread.
    fn child_ref() -> ObjectRef<ActivityKind> {
        object_ref("execution-0", 90)
    }

    fn state_path() -> StatePath {
        let mut p = jsonptr::PointerBuf::new();
        p.push_back("States");
        p.push_back("P");
        StatePath::from(p)
    }

    /// A seeded thread row with `children` live children still attached (any ref is fine here — the
    /// drain decision reads the set, not the members).
    fn seeded_thread(status: ThreadStatus, children: usize) -> ThreadRecord {
        let thread = Thread {
            meta: ObjectMeta::builder(thread_ref().uid())
                .name(thread_ref().name().clone())
                .at(at())
                .with_owner(ThreadOwner::Execution(execution_ref())),
            execution: execution_ref(),
            state_path: state_path(),
            start_at: "P".to_string(),
            index: 0,
            status,
            input: json!({ "in": 1 }),
            output: None,
        };
        let live = (0..children)
            .map(|n| object_ref::<TimerKind>("deadline", 200 + n as u64).into_raw_object_ref())
            .collect::<HashSet<_>>();
        let mut row = ThreadRecord::from_value(thread, live);
        row.born(at());
        row
    }

    /// A settled child activity row owned by [`thread_ref`]-shaped owner slot — the row the
    /// terminated hook reads the failure's reason back off of.
    fn seeded_child(status: ActivityStatus) -> ActivityRecord {
        let activity = Activity {
            meta: ObjectMeta::builder(child_ref().uid())
                .name(child_ref().name().clone())
                .at(at())
                .with_owner(thread_ref()),
            execution: execution_ref(),
            state_path: state_path(),
            status,
            raw_input: json!({}),
            input: None,
            raw_output: None,
            output: None,
            activity_state: None,
            retry_state: None,
        };
        let mut row = ActivityRecord::from_value(activity, HashSet::new());
        row.born(at());
        row
    }

    /// Drive one hook over a working overlay seeded with the thread and (optionally) its settled
    /// child — the leader's shape, so the assertion reads the command the container emitted.
    async fn drive(
        thread: ThreadRecord,
        child: Option<ActivityRecord>,
        terminated: bool,
    ) -> Vec<EntryPayload> {
        let mut store = InMemoryStorage::new();
        store
            .put_thread(thread)
            .await
            .expect("the in-memory store seeds a thread row");
        if let Some(child) = child {
            store
                .put_activity(child)
                .await
                .expect("the in-memory store seeds the child activity row");
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
        let container = ThreadElementBehavior::open(&work, thread_ref())
            .await
            .expect("the seeded thread resolves its container");
        {
            let mut ctx = HandlerContext {
                env: &mut env,
                storage: &work,
                clock,
                ids,
                definitions: &mut definitions,
                state_handlers: &state_handlers,
            };
            if terminated {
                container
                    .after_child_terminated(&mut ctx, &mut out, &child_ref().into_raw_object_ref())
                    .await
                    .expect("the hook relays the settle");
            } else {
                container
                    .after_child_completed(&mut ctx, &mut out, &child_ref().into_raw_object_ref())
                    .await
                    .expect("the hook relays the settle");
            }
        }
        out.into_entries()
            .into_iter()
            .map(|entry| entry.payload)
            .collect()
    }

    /// Drive the **completed** hook against a mock store answering the one owner read, with the container
    /// built directly — the shape a vanished owner needs, since `open` refuses that same missing row
    /// before any hook could be reached.
    async fn hook_over(
        store: &dyn ReadonlyStorageTxn,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(at()));
        let ids: Arc<dyn IdGenerator> = Arc::new(CountingIdGenerator::new());
        let mut out = Collector::new(EntryId::new(1), None, clock.clone(), ids.clone());
        let mut env = EvalEnv::new();
        let mut definitions = HashMap::new();
        let state_handlers = build_state_handlers();
        let container = ThreadElementBehavior {
            thread: thread_ref(),
        };
        let mut ctx = HandlerContext {
            env: &mut env,
            storage: store,
            clock,
            ids,
            definitions: &mut definitions,
            state_handlers: &state_handlers,
        };
        container
            .after_child_completed(&mut ctx, &mut out, child)
            .await
    }

    /// A settle with no live owner is refused up front: `open` names the gone owner, so a caller whose
    /// settle would have nothing to land on answers the absence before it writes its terminal.
    #[tokio::test]
    async fn a_missing_owner_has_no_container() {
        let store = InMemoryStorage::new();
        let work = WorkingState::new(store.begin_txn().expect("the in-memory store begins a txn"));
        let Err(ProcessingError::Rejected(ty, reason)) =
            ThreadElementBehavior::open(&work, thread_ref()).await
        else {
            panic!("an open that did not read a missing row");
        };
        assert_eq!(ty, RejectionType::NotFound);
        assert!(
            reason.contains("is gone"),
            "the refusal names the gone owner: {reason}"
        );
    }

    /// A read that **faults** stays a fault, never a missing owner (see `ActivityElementBehavior`'s sibling).
    #[tokio::test]
    async fn a_faulted_owner_read_is_not_a_missing_owner() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_thread()
            .times(1)
            .return_once(|_| Err(StorageError::Backend("injected storage fault".to_string())));
        assert!(
            matches!(
                ThreadElementBehavior::open(&store, thread_ref()).await,
                Err(ProcessingError::Unexpected(_))
            ),
            "a fault must surface as Unexpected, not read as a missing row"
        );
    }

    /// A terminated state under a still-`Running` thread is the scope's own failure: the thread is the
    /// state's scope, so it is taken down with the state's reason.
    #[tokio::test]
    async fn a_terminated_child_terminates_a_running_thread() {
        let reason = TerminationReason::Cancelled;
        let chain = drive(
            seeded_thread(ThreadStatus::Running, 0),
            Some(seeded_child(ActivityStatus::Terminated(reason.clone()))),
            true,
        )
        .await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::TerminateThread(
                TerminateThread {
                    thread: thread_ref(),
                    reason,
                }
            ))]
        );
    }

    /// A settled child that is *not* an abnormal terminal (a `Completed` state) does not take the
    /// scope down — a thread's normal advance is not its end.
    #[tokio::test]
    async fn a_completed_child_reaches_a_running_thread_without_terminating() {
        let chain = drive(
            seeded_thread(ThreadStatus::Running, 0),
            Some(seeded_child(ActivityStatus::Completed)),
            true,
        )
        .await;
        assert!(
            chain.is_empty(),
            "a completed child must not terminate its scope: {chain:?}"
        );
    }

    /// A thread gone from under its settled child refuses the settle instead of no-op'ing it: the
    /// terminal is already on this attempt's batch, so a silent success would leave the log saying the
    /// child settled with nothing after it.
    #[tokio::test]
    async fn a_vanished_thread_refuses_the_settle() {
        let mut store = MockReadonlyStorageTxn::new();
        store.expect_get_thread().times(1).return_once(|_| Ok(None));
        let result = hook_over(&store, child_ref().as_raw_object_ref()).await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a vanished thread must refuse the settle: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("is gone from under its settled child"),
            "the refusal names what it could not relay to: {reason}"
        );
    }

    /// A settled child under a thread that is already terminal has nothing to mean — the row and the
    /// lifecycle disagree, so the batch is refused rather than advanced.
    #[tokio::test]
    async fn a_settled_child_under_a_terminal_thread_is_refused() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_thread()
            .times(1)
            .return_once(|_| Ok(Some(seeded_thread(ThreadStatus::Completed, 0))));
        let result = hook_over(&store, child_ref().as_raw_object_ref()).await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a settled child under a terminal thread must be refused: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("while Completed"),
            "the refusal names the status the thread was found in: {reason}"
        );
    }

    /// A settled child row that is already gone leaves the running thread alone — nothing to read a
    /// reason from.
    #[tokio::test]
    async fn a_terminated_child_with_no_readable_row_does_nothing() {
        let chain = drive(seeded_thread(ThreadStatus::Running, 0), None, true).await;
        assert!(
            chain.is_empty(),
            "a missing child must not terminate the scope: {chain:?}"
        );
    }

    /// A completed state under a still-`Running` thread is the scope's own success — a `Succeed` is a
    /// thread's only natural end — so the thread completes with the child's output.
    #[tokio::test]
    async fn a_completed_child_completes_a_running_thread() {
        let mut child = seeded_child(ActivityStatus::Completed);
        child.value.output = Some(json!({ "done": 1.0 }));
        let chain = drive(seeded_thread(ThreadStatus::Running, 0), Some(child), false).await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::CompleteThread(
                CompleteThread {
                    thread: thread_ref(),
                    output: json!({ "done": 1.0 }),
                }
            ))]
        );
    }

    /// A `Succeed` that projects no `Output` (input `Null`) completes the running thread with a
    /// `Null` result. The terminal's success is the thread's end regardless of whether it carried a
    /// value — a null-rejecting re-read must not strand it (durable `Succeed` resume regression).
    #[tokio::test]
    async fn a_completed_child_without_output_completes_the_running_thread_with_null() {
        let chain = drive(
            seeded_thread(ThreadStatus::Running, 0),
            Some(seeded_child(ActivityStatus::Completed)),
            false,
        )
        .await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::CompleteThread(
                CompleteThread {
                    thread: thread_ref(),
                    output: Value::Null,
                }
            ))]
        );
    }

    /// A `Completing` thread with a child still in flight still advances: the Continue names the thread
    /// this settle belongs to, and its own handler takes down whatever is left and stops short of
    /// advancing — so the settle that drains the thread last is the one that completes it.
    #[tokio::test]
    async fn a_settled_child_advances_a_completing_thread_before_the_drain() {
        let chain = drive(
            seeded_thread(ThreadStatus::Completing, 1),
            Some(seeded_child(ActivityStatus::Completed)),
            false,
        )
        .await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueComplete {
                owner: thread_ref().into_raw_object_ref(),
            })]
        );
    }

    /// A terminated child advances a `Terminating` owner into its ContinueTerminate — child still in
    /// flight or not: the Continue names the thread this settle belongs to, and its own handler takes
    /// down whatever is left and stops short of advancing, so the settle that drains the thread last is
    /// the one that terminates it.
    #[tokio::test]
    async fn a_terminated_child_advances_a_terminating_thread_before_the_drain() {
        let chain = drive(
            seeded_thread(ThreadStatus::Terminating(TerminationReason::Cancelled), 1),
            Some(seeded_child(ActivityStatus::Terminated(
                TerminationReason::Cancelled,
            ))),
            true,
        )
        .await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueTerminate {
                owner: thread_ref().into_raw_object_ref(),
            })]
        );
    }

    /// A *completed* child under a `Terminating` thread advances that teardown just the same: the
    /// thread's direction was fixed when the teardown began, and a bystander's settle does not reopen it.
    #[tokio::test]
    async fn a_completed_child_advances_a_terminating_thread() {
        let chain = drive(
            seeded_thread(ThreadStatus::Terminating(TerminationReason::Cancelled), 0),
            Some(seeded_child(ActivityStatus::Completed)),
            false,
        )
        .await;
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueTerminate {
                owner: thread_ref().into_raw_object_ref(),
            })]
        );
    }
}
