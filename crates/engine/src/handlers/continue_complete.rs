//! Handles `Command::ContinueComplete`: the **success** side of the deferred drain-continuation.
//!
//! A settled child no longer drains its owner inline (the old recursive cascade in `child_completed`);
//! the one-hop reactor instead issues one `ContinueComplete` per drained-and-finishing **success** owner,
//! dispatched on a **later round** (Zeebe's `COMPLETE_ELEMENT` decoupling). This handler settles the
//! owner by its own kind and, on success, hands the settled owner up to *its* owner via that owner's
//! [`Container`] — exactly one hop, never recursion, so stack depth is independent of owner-chain depth.
//!
//! The node is dispatched by kind to its own [`Completion`] impl, and every impl shares one shape:
//! a node that still holds children cannot advance, so each child is taken down by its own kind's verb;
//! a drained node advances to `Completed` and hands the settle up to its owner.
//! An **activity**'s advance runs its state's `after_completing` rather than a kind-local close, so the
//! projection, the `Next`/`End` routing and the relay the deferred `complete` step owes happen there —
//! the state emits the terminal *and* the `CompleteThread`/`ActivateState` that carries it up (see
//! [`ActivityCompletion`]).
//!
//! The failure side (`Terminate`) lives in [`continue_terminate`](super::continue_terminate); the two are
//! split by direction so each file carries only its own per-kind terminal logic, mirroring
//! `complete_state` / `terminate_state`.

use crate::RejectionType;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::handlers::container::{ActivityContainer, Container, ExecutionContainer};
use crate::types::activity::ActivityKind;
use crate::types::command::{Command, TerminateState, TerminateThread, TerminationReason};
use crate::types::error::ExecutionError;
use crate::types::event::Event;
use crate::types::execution::ExecutionKind;
use crate::types::meta::{
    HasRawObjectRef, ObjectKind, ObjectKindMarker, ObjectRef, RawObjectRef, ThreadOwner,
};
use crate::types::task::TaskKind;
use crate::types::thread::{ThreadKind, ThreadStatus};
use crate::types::timer::TimerKind;
use spica_asl::State;

/// Handles `Command::ContinueComplete`: dispatches the drained-and-finishing `owner` to its own kind's
/// [`Completion`] impl, which emits the owner's success terminal and hands the settle up its owner
/// chain on this round — one hop.
#[derive(Default)]
pub struct ContinueCompleteHandler;

impl ContinueCompleteHandler {
    pub(crate) async fn handle(
        &self,
        owner: &RawObjectRef,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        match owner.kind {
            ObjectKind::Activity => {
                ActivityCompletion::handle(ctx, out, owner.clone().typed::<ActivityKind>()).await
            }
            ObjectKind::Thread => {
                ThreadCompletion::handle(ctx, out, owner.clone().typed::<ThreadKind>()).await
            }
            ObjectKind::Execution => {
                ExecutionCompletion::handle(ctx, out, owner.clone().typed::<ExecutionKind>()).await
            }
            // A Flow / FlowVersion / Timer / Task is never a `ContinueComplete` owner: the only
            // issuers are the three containers' settle hooks, which address an Activity, a Thread or
            // an Execution, so another kind here is a dispatch fault, not a command.
            kind => panic!(
                "continue_complete: {kind} is never a Continue owner; only an Activity, a Thread or an Execution defers"
            ),
        }
    }
}

/// A finishing node that a `ContinueComplete` names, settled by its own kind. The impls share one shape:
/// a node that still holds children has them taken down so they can settle and drain it; a drained node
/// advances to `Completed` and hands its settle to its owner — through the owner's [`Container`], or,
/// for an activity, through its own state's `after_completing`.
trait Completion {
    /// The node kind this impl settles — the slot's own type, so a `ContinueComplete` for another kind
    /// is unrepresentable rather than a case to fold in.
    type Kind: ObjectKindMarker;

    async fn handle(
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        node: ObjectRef<Self::Kind>,
    ) -> Result<(), ProcessingError>;
}

struct ActivityCompletion;

impl Completion for ActivityCompletion {
    type Kind = ActivityKind;

    async fn handle(
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        node: ObjectRef<ActivityKind>,
    ) -> Result<(), ProcessingError> {
        use crate::ActivityStatus;
        // A read fault is the dispatch's — returned, so the leader owns retry/refusal. A missing row is
        // a real refusal: the Continue names the node a settled child drained, so a gone row means the
        // command's own precondition failed rather than "already closed".
        let Some(act) = ctx.storage.get_activity(&node).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("continue_complete: activity {node} not found"),
            ));
        };
        // The hop lands only on a `Completing` node — one that a settled child drained (see
        // `ActivityContainer::after_child_completed`). Any other status means the row
        // moved under the read above, leaving this command nothing to apply.
        if !matches!(act.value.status, ActivityStatus::Completing) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "continue_complete: activity {node} is not completing (status {:?})",
                    act.value.status
                ),
            ));
        }
        if !act.active_children.is_empty() {
            // Unreachable by construction — a Continue is issued only for a drained node — so this is
            // the row moving under the read. Each child is taken down by its own verb with `Cancelled`:
            // the node is being flushed, not failing, so a bystander must not carry a failure a state's
            // own `Catch` could route.
            for child in &act.active_children {
                match child.kind {
                    ObjectKind::Thread => {
                        out.append_command(Command::TerminateThread(TerminateThread {
                            thread: child.clone().typed::<ThreadKind>(),
                            reason: TerminationReason::Cancelled,
                        }))
                    }
                    ObjectKind::Task => out.append_command(Command::CancelTask {
                        task: child.clone().typed::<TaskKind>(),
                    }),
                    ObjectKind::Timer => out.append_command(Command::CancelTimer {
                        timer: child.clone().typed::<TimerKind>(),
                    }),
                    kind => panic!(
                        "continue_complete: activity {node} owns a live {kind} child {child}; an \
                         activity's children are only a fan-out Thread, a Task or a Timer"
                    ),
                }
            }
            return Ok(());
        }
        // A container state defers its terminal to this hop, so the drained node finishes through its
        // state's `after_completing` — the projection, the `Next`/`End` routing and the relay to the
        // owning thread the deferred `complete` step owes. The three resolution misses below are refused
        // exactly as `activate_state` refuses the same ones: a state that cannot be resolved cannot be
        // finished at all, so nothing is closed generically in its place.
        let activity_value = act.value();
        let owner = activity_value.meta.owner.clone();
        // An activity's owner slot admits only a `Thread`, so the row is read directly. A fault reading
        // it is the dispatch's, and is returned for the leader to retry.
        let thread = match ctx.storage.get_thread(&owner).await? {
            Some(thread) => thread,
            None => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!(
                        "continue_complete: owning thread {owner} of activity {node} does not exist"
                    ),
                ));
            }
        };
        let machine = match ctx.machine_for_thread(&thread).await {
            Ok(machine) => machine,
            // `machine_for_thread` mixes a missing definition (domain) with the storage fault
            // underneath it, so its `Infra` is split out rather than refused as the thread's.
            Err(ExecutionError::Infra(e)) => return Err(ProcessingError::Unexpected(e.into())),
            Err(e) => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("continue_complete: thread {owner} cannot resolve its machine: {e}"),
                ));
            }
        };
        // The state to finish is the one this activity names — its own `state_path` locates the
        // definition inside the machine the owning thread binds to.
        let state_def = match machine.state_at(&activity_value.state_path) {
            Ok(state_def) => state_def,
            Err(e) => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!(
                        "continue_complete: state {:?} is not defined by the machine that thread {owner} binds to: {e}",
                        activity_value.state_path
                    ),
                ));
            }
        };
        // Only a state that owns children can defer: `Parallel`/`Map` leave their fan-out running and
        // finish on the last settle, a `Task` its invocation. Every other state (`Wait`, `Pass`,
        // `Choice`, `Succeed`, `Fail`) closes inline in its `after_completing`, so one arriving here is
        // an engine regression, not a flow.
        assert!(
            matches!(
                state_def,
                State::Parallel(_) | State::Map(_) | State::Task(_)
            ),
            "continue_complete: activity {node} deferred on state {state_def:?}, which owns no children"
        );
        let handler = ctx
            .state_handlers
            .create(state_def)
            .expect("state type has no registered handler: engine regression, not a flow error");
        let variables = thread.variables.clone();
        // The state's finish owns the terminal *and* the relay: every deferring state emits its own
        // `StateCompleted` plus the `CompleteThread`/`ActivateState` that carries the settle up, so this
        // hop adds neither — a second `CompleteThread` for the thread would only meet
        // `complete_thread`'s own guard.
        handler
            .after_completing(ctx, out, &activity_value, &variables)
            .await?;
        Ok(())
    }
}

struct ThreadCompletion;

impl Completion for ThreadCompletion {
    type Kind = ThreadKind;

    async fn handle(
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
                match ExecutionContainer::open(ctx.storage, execution).await {
                    Ok(container) => container.after_child_completed(ctx, out, &child).await?,
                    // The settle's own terminal is already on this batch: refusing a gone owner here
                    // would drop it and leave the node unfinished, so the rail is simply absent.
                    Err(ProcessingError::Rejected(..)) => {}
                    Err(err) => return Err(err),
                }
            }
            ThreadOwner::Activity(activity) => {
                match ActivityContainer::open(ctx.storage, activity).await {
                    Ok(container) => container.after_child_completed(ctx, out, &child).await?,
                    Err(ProcessingError::Rejected(..)) => {}
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(())
    }
}

struct ExecutionCompletion;

impl Completion for ExecutionCompletion {
    type Kind = ExecutionKind;

    async fn handle(
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        node: ObjectRef<ExecutionKind>,
    ) -> Result<(), ProcessingError> {
        use crate::ExecutionStatus;
        // A read fault is the dispatch's — returned, so the leader owns retry/refusal. A missing row is
        // a real refusal: the Continue names the run a settled child drained, so a gone row means the
        // command's own precondition failed rather than "already closed".
        let Some(exec) = ctx.storage.get_execution(&node).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("continue_complete: execution {node} not found"),
            ));
        };
        // The hop lands only on a `Completing` run — one that a settled child drained (see
        // `child_completed`). Any other status means the row moved under the read above.
        if !matches!(exec.status, ExecutionStatus::Completing) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "continue_complete: execution {node} is not completing (status {:?})",
                    exec.status
                ),
            ));
        }
        if !exec.active_children.is_empty() {
            // A run's container does not wait for its own drain — both of its children can settle under
            // one teardown — so an early Continue reaches here with a child still attached. It is
            // absorbed: the sweep re-issues what is still in flight and this hop stops short of
            // advancing, so the Continue that lands on the drained run is the one that completes it. A
            // success finish keeps only the deadlines it owns (`complete_execution` cancels them and
            // waits for the drain), so any other kind here is the row disagreeing with the lifecycle.
            for child in &exec.active_children {
                match child.kind {
                    ObjectKind::Timer => out.append_command(Command::CancelTimer {
                        timer: child.clone().typed::<TimerKind>(),
                    }),
                    kind => panic!(
                        "continue_complete: execution {node} owns a live {kind} child {child}; a \
                         completing run only keeps its own timers"
                    ),
                }
            }
            return Ok(());
        }
        // The row already carries what the finish fixed (`output` was written when it entered
        // `Completing`), so the terminal advances the stored value rather than re-deriving one.
        let mut completed_execution = exec.value();
        // The arm is the transition's own precondition, so this can only decline if the row moved
        // under the read above.
        if let Err(reason) = completed_execution.mark_completed(ctx.now()) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("continue_complete: execution {node} cannot be completed: {reason}"),
            ));
        }
        out.append_event(Event::ExecutionCompleted {
            execution: completed_execution,
        })
        .await;
        // Nothing to relay: a run is the root of its object tree, so it has no owner to settle up to.
        Ok(())
    }
}
