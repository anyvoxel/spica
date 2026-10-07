//! Handles `Command::ContinueTerminate`: the **failure** side of the deferred drain-continuation.
//!
//! A settled child no longer drains its owner inline (the old recursive cascade in `child_completed`);
//! the one-hop reactor instead issues one `ContinueTerminate` per drained-and-finishing **failure** owner,
//! dispatched on a **later round** (Zeebe's `COMPLETE_ELEMENT` decoupling). This handler settles the
//! owner by its own kind and hands the settled owner up to *its* owner — exactly one hop, never
//! recursion, so stack depth is independent of owner-chain depth.
//!
//! It mirrors the success side ([`continue_complete`](super::continue_complete)): the node is dispatched
//! by kind to its own [`Termination`] impl, and every impl shares one shape — a node that still holds
//! children cannot advance, so each child is taken down by its own kind's verb; a drained node emits its
//! `Terminated` and hands the settle up. An **activity**'s advance runs its state's `after_terminating`,
//! so the terminal and the relay the deferred `terminate` step owes happen there (see
//! [`ActivityTermination`]). The failure reason is never re-derived: it was fixed when the teardown began
//! (`Terminating(reason)` on the row), so each kind's terminal only advances that stored value.

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

/// Handles `Command::ContinueTerminate`: dispatches the drained-and-finishing `owner` to its own kind's
/// [`Termination`] impl, which emits the owner's failure terminal and hands the settle up its owner
/// chain on this round — one hop.
#[derive(Default)]
pub struct ContinueTerminateHandler;

impl ContinueTerminateHandler {
    pub(crate) async fn handle(
        &self,
        owner: &RawObjectRef,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        match owner.kind {
            ObjectKind::Activity => {
                ActivityTermination::handle(ctx, out, owner.clone().typed::<ActivityKind>()).await
            }
            ObjectKind::Thread => {
                ThreadTermination::handle(ctx, out, owner.clone().typed::<ThreadKind>()).await
            }
            ObjectKind::Execution => {
                ExecutionTermination::handle(ctx, out, owner.clone().typed::<ExecutionKind>()).await
            }
            // A Flow / FlowVersion / Timer / Task is never a `ContinueTerminate` owner: the only
            // issuers are the three containers' settle hooks, which address an Activity, a Thread or
            // an Execution, so another kind here is a dispatch fault, not a command.
            kind => panic!(
                "continue_terminate: {kind} is never a Continue owner; only an Activity, a Thread or an Execution defers"
            ),
        }
    }
}

/// A finishing node that a `ContinueTerminate` names, settled by its own kind. The impls share one
/// shape: a node that still holds children has them taken down so they can settle and drain it; a
/// drained node emits its terminal and hands its settle to its owner.
trait Termination {
    /// The node kind this impl settles — the slot's own type, so a `ContinueTerminate` for another kind
    /// is unrepresentable rather than a case to fold in.
    type Kind: ObjectKindMarker;

    async fn handle(
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        node: ObjectRef<Self::Kind>,
    ) -> Result<(), ProcessingError>;
}

struct ActivityTermination;

impl Termination for ActivityTermination {
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
                format!("continue_terminate: activity {node} not found"),
            ));
        };
        // The hop lands only on a `Terminating` activity — one that a settled child drained (see
        // `ActivityContainer::after_child_completed`). Any other status means the row
        // moved under the read above; the reason itself stays on the row, so nothing re-derives it.
        if !matches!(act.value.status, ActivityStatus::Terminating(_)) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "continue_terminate: activity {node} is not terminating (status {:?})",
                    act.value.status
                ),
            ));
        }
        if !act.active_children.is_empty() {
            // Unreachable by construction — a Continue is issued only for a drained node — so this is
            // the row moving under the read. Each child is taken down by its own verb with `Cancelled`:
            // the node is being flushed, not failing, so a bystander must not carry the reason its
            // ancestor died of and, with a `Catch`, route a failure that was never its own (the same
            // rule `terminate_thread` applies).
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
                        "continue_terminate: activity {node} owns a live {kind} child {child}; an \
                         activity's children are only a fan-out Thread, a Task or a Timer"
                    ),
                }
            }
            return Ok(());
        }
        let activity_value = act.value();
        // A container state defers its terminal to this hop, so the drained node finishes through its
        // state's `after_terminating` — the terminal *and* the relay to the owning thread that the
        // deferred `terminate` step owes, exactly as the success side runs `after_completing`. The
        // resolution misses below are refused the same way `activate_state` refuses them: a state that
        // cannot be resolved cannot be finished at all, so nothing is closed generically in its place.
        let owner = activity_value.meta.owner.clone();
        // An activity's owner slot admits only a `Thread`, so the row is read directly. A fault reading
        // it is the dispatch's, and is returned for the leader to retry.
        let thread = match ctx.storage.get_thread(&owner).await? {
            Some(thread) => thread,
            None => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!(
                        "continue_terminate: owning thread {owner} of activity {node} does not exist"
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
                    format!("continue_terminate: thread {owner} cannot resolve its machine: {e}"),
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
                        "continue_terminate: state {:?} is not defined by the machine that thread {owner} binds to: {e}",
                        activity_value.state_path
                    ),
                ));
            }
        };
        // Only a state that owns children can defer: a `Task` leaves its invocation and its
        // `TimeoutSeconds` timer to be cancelled, a `Wait` its deadline, a `Parallel`/`Map` its
        // fan-out. Every other state closes inline in its `after_terminating` — so a drained one
        // arriving here is an engine regression, not a flow.
        assert!(
            matches!(
                state_def,
                State::Task(_) | State::Wait(_) | State::Parallel(_) | State::Map(_)
            ),
            "continue_terminate: activity {node} deferred on state {state_def:?}, which owns no children"
        );
        let handler = ctx
            .state_handlers
            .create(state_def)
            .expect("state type has no registered handler: engine regression, not a flow error");
        let variables = thread.variables.clone();
        // The state's drained arm owns the terminal *and* the relay: it re-reads its own children,
        // finds them gone, and emits `StateTerminated` plus the owner-container relay that carries the
        // settle up, so this hop adds neither.
        handler
            .after_terminating(ctx, out, &activity_value, &variables)
            .await?;
        Ok(())
    }
}

struct ThreadTermination;

impl Termination for ThreadTermination {
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
        // inline path uses (see `terminate_thread`'s command handler) — so a root thread that drains
        // *here*, after the last of its own children settled, starts its run's teardown exactly as one
        // that had nothing to wait for does. A kind-routed dispatch would not: it has no arm for
        // a still-`Running` run, which is where that teardown has to come from.
        let child = node.into_raw_object_ref();
        match thread.value.meta.owner.clone() {
            ThreadOwner::Execution(execution) => {
                match ExecutionContainer::open(ctx.storage, execution).await {
                    Ok(container) => container.after_child_terminated(ctx, out, &child).await?,
                    // The settle's own terminal is already on this batch: refusing a gone owner here
                    // would drop it and leave the node unfinished, so the rail is simply absent.
                    Err(ProcessingError::Rejected(..)) => {}
                    Err(err) => return Err(err),
                }
            }
            ThreadOwner::Activity(activity) => {
                match ActivityContainer::open(ctx.storage, activity).await {
                    Ok(container) => container.after_child_terminated(ctx, out, &child).await?,
                    Err(ProcessingError::Rejected(..)) => {}
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(())
    }
}

struct ExecutionTermination;

impl Termination for ExecutionTermination {
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
                format!("continue_terminate: execution {node} not found"),
            ));
        };
        // The hop lands only on a `Terminating` run; any other status means the row moved under the
        // read above.
        if !matches!(exec.status, ExecutionStatus::Terminating(_)) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "continue_terminate: execution {node} is not terminating (status {:?})",
                    exec.status
                ),
            ));
        }
        if !exec.active_children.is_empty() {
            // A run's container does not wait for its own drain — a teardown takes down both of a run's
            // children at once, so each of their settles advances it — which means an earlier Continue
            // reaches here with the other child still attached. It is absorbed: the sweep below takes
            // that child down again and this hop stops short of advancing, so only the Continue that
            // lands on the drained run terminates it. A run's direct children are exactly its root
            // thread and its own timers, so any other kind here is the row disagreeing with the
            // lifecycle; both sweepable kinds carry `Cancelled`, for the reason the `Terminating`
            // activity's arm gives.
            for child in &exec.active_children {
                match child.kind {
                    ObjectKind::Timer => out.append_command(Command::CancelTimer {
                        timer: child.clone().typed::<TimerKind>(),
                    }),
                    ObjectKind::Thread => {
                        out.append_command(Command::TerminateThread(TerminateThread {
                            thread: child.clone().typed::<ThreadKind>(),
                            reason: TerminationReason::Cancelled,
                        }))
                    }
                    kind => panic!(
                        "continue_terminate: execution {node} owns a live {kind} child {child}; a \
                         run's children are only its root Thread and its own timers"
                    ),
                }
            }
            return Ok(());
        }
        // The reason was written when the teardown began (`mark_terminating`), so the terminal advances
        // the stored value rather than re-deriving one.
        let mut terminated_execution = exec.value();
        // The arm is the transition's own precondition, so this can only decline if the row moved
        // under the read above.
        if let Err(reason) = terminated_execution.mark_terminated(ctx.now()) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!("continue_terminate: execution {node} cannot be terminated: {reason}"),
            ));
        }
        out.append_event(Event::ExecutionTerminated {
            execution: terminated_execution,
        })
        .await;
        // Nothing to relay: a run is the root of its object tree, so it has no owner to settle up to.
        Ok(())
    }
}
