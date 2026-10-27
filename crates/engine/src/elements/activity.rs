use serde_json::Value;
use spica_asl::State;

use super::Container;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::storage::ReadonlyStorageTxn;
use crate::types::activity::{Activity, ActivityKind};
use crate::types::command::{
    Command, CompleteState, TerminateState, TerminateThread, TerminationReason,
};
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::meta::{HasRawObjectRef, ObjectKind, ObjectRef, RawObjectRef};
use crate::types::task::TaskKind;
use crate::types::thread::{Thread, ThreadKind};
use crate::{ActivityStatus, RejectionType, Timestamp};

/// The `Activity` element — one state's live instance inside a thread: what it does when its own
/// children settle, and what its own deadline's fire and cancellation mean. The deadline stands beside
/// the settle relation rather than inside it: a `Wait`'s resume instant and a `Task`'s
/// `TimeoutSeconds` are acted on by the activity itself rather than relayed as a settled child.
pub(crate) struct ActivityElementBehavior {
    activity: ObjectRef<ActivityKind>,
}

impl ActivityElementBehavior {
    /// Hand the settle to the state that fronts this activity — its own
    /// [`StateHandler::child_completed`](crate::elements::state_handler::StateHandler::child_completed),
    /// the one place a per-state reaction lives.
    async fn hand_to_state(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        act: &Activity,
        thread: &Thread,
        state_def: &State,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        let activity = self.activity.clone();
        let activity_value = act.clone();
        // The scope's variables are derived state in their own `_index` cell (see
        // `Storage::get_variables`), read once here as what the state's reaction evaluates against.
        let variables = ctx.storage.get_variables(&thread.meta.object_ref()).await?;
        // Every `State` variant has a registered factory (see `build_state_handlers`), so a miss here is
        // an engine regression — fail loud rather than leave the activity stuck `Running`.
        let handler = ctx
            .state_handlers
            .create(state_def)
            .expect("state type has no registered handler: engine regression, not a flow error");
        handler
            .child_completed(
                ctx,
                out,
                activity,
                &activity_value,
                &variables,
                child.clone(),
            )
            .await;
        Ok(())
    }

    /// A `Wait`'s resume deadline elapsed, or a `Task`'s `TimeoutSeconds` did: the two effects the
    /// activity's own deadline can have, decided by the state the activity fronts. A still-`Running`
    /// activity reaches that state directly — a `Wait` resumes on its processed input, a `Task` fails
    /// `TimedOut` while the call is still in flight — and one already finishing only advances its phase.
    ///
    /// A fire landing after the activity finished is refused, exactly as a settle by a child the
    /// state no longer owns is: the deadline stopped bounding anything when it finished, and a
    /// silent no-op would leave the fire unexplained (see `dispatch_once`).
    pub(crate) async fn on_deadline_fired(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        deadline: Timestamp,
    ) -> Result<(), ProcessingError> {
        let Some(act) = ctx.storage.get_activity(&self.activity).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "activity_element: activity {} is gone from under its deadline",
                    self.activity
                ),
            ));
        };
        match &act.status {
            ActivityStatus::Running => {
                let (_thread, state_def) = self.state_of(ctx, &act).await?;
                match state_def {
                    State::Wait(_) => {
                        out.append_command(Command::CompleteState(CompleteState {
                            activity: self.activity.clone(),
                            output: act.input.clone().unwrap_or(Value::Null),
                        }));
                        Ok(())
                    }
                    // Only while the attempt is still in flight: a deadline can land in the window
                    // between the task's own settle and the `CompleteState` that follows it, and by then
                    // it has stopped bounding anything. A `Task` state's `raw_output` is written by
                    // nothing but its task's settle, so its presence *is* "the attempt has landed".
                    State::Task(_) => {
                        if act.raw_output.is_some() {
                            return Err(ProcessingError::Rejected(
                                RejectionType::InvalidState,
                                format!(
                                    "activity_element: activity {} fired a deadline at {} after its \
                                     task settled",
                                    self.activity,
                                    deadline.as_millis()
                                ),
                            ));
                        }
                        out.append_command(Command::TerminateState(TerminateState {
                            activity: self.activity.clone(),
                            reason: TerminationReason::Failed {
                                error: ExecutionError::Runtime(RuntimeError::TimedOut {
                                    message: format!(
                                        "task ran past its TimeoutSeconds deadline ({})",
                                        deadline.as_millis()
                                    ),
                                }),
                            },
                        }));
                        Ok(())
                    }
                    _ => Err(ProcessingError::Rejected(
                        RejectionType::InvalidState,
                        format!(
                            "activity_element: activity {} fired a deadline, but its state owns none",
                            self.activity
                        ),
                    )),
                }
            }
            ActivityStatus::Completing => {
                out.append_command(Command::ContinueComplete {
                    owner: self.activity.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            ActivityStatus::Terminating(_) => {
                out.append_command(Command::ContinueTerminate {
                    owner: self.activity.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            ActivityStatus::Completed | ActivityStatus::Terminated(_) => {
                Err(ProcessingError::Rejected(
                    RejectionType::InvalidState,
                    format!(
                        "activity_element: activity {} fired a deadline at {} while {}",
                        self.activity,
                        deadline.as_millis(),
                        act.status.phase()
                    ),
                ))
            }
        }
    }

    /// Resolve the `Thread` scope an activity belongs to and the `State` its own `state_path` names in
    /// that scope's machine — what every settle-time decision is made against. A fault stays the leader's
    /// retry; a missing row or definition is refused rather than decided blind.
    async fn state_of(
        &self,
        ctx: &mut HandlerContext<'_>,
        act: &Activity,
    ) -> Result<(Thread, State), ProcessingError> {
        // An activity's owner slot admits only a `Thread`, so that row is read directly, and the run
        // between it and its definition resolves the machine.
        let owner = act.meta.owner.clone();
        let Some(thread) = ctx.storage.get_thread(&owner).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!(
                    "activity_element: owning thread {owner} of activity {} does not exist",
                    self.activity
                ),
            ));
        };
        // `machine_for_thread` mixes a missing definition (domain) with the storage fault under it, so
        // its `Infra` is split out — a fault stays the leader's retry.
        let machine = match ctx.machine_for_thread(&thread).await {
            Ok(machine) => machine,
            Err(ExecutionError::Infra(e)) => return Err(ProcessingError::Unexpected(e.into())),
            Err(e) => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("activity_element: thread {owner} cannot resolve its machine: {e}"),
                ));
            }
        };
        let state_path = act.state_path.clone();
        match machine.state_at(&state_path) {
            Ok(state_def) => Ok((thread, state_def.clone())),
            Err(e) => Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!(
                    "activity_element: state {state_path:?} is not defined by the machine that \
                     thread {owner} binds to: {e}"
                ),
            )),
        }
    }

    /// A settle of a child a state **owns in the settle relation**: a `Task`'s invocation, or a
    /// fan-out's branches/items. How the child settled is the state's own affair, so a `Running`
    /// activity hands every settle to it — a `Parallel` fills every branch up front and converges once
    /// the last drains, a `Map` refills a freed `MaxConcurrency` slot before it converges, and a `Task`
    /// completes on its call's payload.
    ///
    /// A settle landing once the activity is finishing advances that finish instead — the state would be
    /// replenishing under a machine that already decided to stop — and one landing after it finished is
    /// a late cleanup of a child nothing waits for any more. The kind check keeps the pair honest: the
    /// state's own type names the kind it can be settled by, and any other kind is the row and the
    /// lifecycle disagreeing.
    async fn settle_child(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        act: &Activity,
        thread: &Thread,
        state_def: &State,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        let expected = match state_def {
            State::Task(_) => ObjectKind::Task,
            State::Parallel(_) | State::Map(_) => ObjectKind::Thread,
            _ => unreachable!("settle_child is reached only for states that own children"),
        };
        if child.kind != expected {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "activity_element: activity {} settled a {} child {child} while {}; a state is \
                     settled only by the children it owns",
                    self.activity,
                    child.kind,
                    act.status.phase()
                ),
            ));
        }
        match &act.status {
            ActivityStatus::Running => {
                self.hand_to_state(ctx, out, act, thread, state_def, child)
                    .await
            }
            ActivityStatus::Completing => {
                out.append_command(Command::ContinueComplete {
                    owner: self.activity.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            ActivityStatus::Terminating(_) => {
                out.append_command(Command::ContinueTerminate {
                    owner: self.activity.as_raw_object_ref().clone(),
                });
                Ok(())
            }
            ActivityStatus::Completed | ActivityStatus::Terminated(_) => Ok(()),
        }
    }

    /// Advance an activity that a settled child drained and a `ContinueTerminate` named: the failure
    /// side of the deferred hop, mirroring [`Self::continue_complete`].
    ///
    /// The hop lands only on a `Terminating` activity — its reason was fixed when the teardown began, so
    /// nothing here re-derives it. A row that still holds children has each taken down by its own kind's
    /// verb; a drained one runs its state's `after_terminating`, which owns both the terminal and the
    /// relay to the owning thread that the deferred `terminate` step owes.
    pub(crate) async fn continue_terminate(
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        node: ObjectRef<ActivityKind>,
    ) -> Result<(), ProcessingError> {
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
        // `ActivityElementBehavior::after_child_completed`). Any other status means the row
        // moved under the read above; the reason itself stays on the row, so nothing re-derives it.
        if !matches!(act.status, ActivityStatus::Terminating(_)) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "continue_terminate: activity {node} is not terminating (status {:?})",
                    act.status
                ),
            ));
        }
        // The live children are read from the ownership-edge index, not off the row: the set is a cell
        // of its own, so this read never races the status write above it.
        let children = ctx
            .storage
            .get_children(act.meta.object_ref().into_raw_object_ref())
            .await?;
        if !children.is_empty() {
            // Unreachable by construction — a Continue is issued only for a drained node — so this is
            // the row moving under the read. Each child is taken down by its own verb with `Cancelled`:
            // the node is being flushed, not failing, so a bystander must not carry the reason its
            // ancestor died of and, with a `Catch`, route a failure that was never its own (the same
            // rule `terminate_thread` applies).
            for child in &children {
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
                    kind => panic!(
                        "continue_terminate: activity {node} owns a live {kind} child {child}; an \
                         activity's children are only a fan-out Thread or a Task"
                    ),
                }
            }
            return Ok(());
        }
        let activity_value = act.clone();
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
        // Only a state that owns children can defer: a `Task` leaves its invocation to be cancelled,
        // a `Parallel`/`Map` its fan-out. Every other state closes inline in its `after_terminating` —
        // so a drained one arriving here is an engine regression, not a flow.
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
        let variables = ctx.storage.get_variables(&thread.meta.object_ref()).await?;
        // The state's drained arm owns the terminal *and* the relay: it re-reads its own children,
        // finds them gone, and emits `StateTerminated` plus the owner-container relay that carries the
        // settle up, so this hop adds neither.
        handler
            .after_terminating(ctx, out, &activity_value, &variables)
            .await?;
        Ok(())
    }

    /// Advance an activity that a settled child drained and a `ContinueComplete` named: the success
    /// side of the deferred hop, mirroring [`Self::continue_terminate`].
    ///
    /// The hop lands only on a `Completing` activity (`ActivityStatus::Completing` is entered *by* that
    /// very settle). A row that still holds children has each taken down by its own kind's verb with
    /// `Cancelled`; a drained one runs its state's `after_completing`, which owns both the terminal and
    /// the relay up the owner chain.
    pub(crate) async fn continue_complete(
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        node: ObjectRef<ActivityKind>,
    ) -> Result<(), ProcessingError> {
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
        // `ActivityElementBehavior::after_child_completed`). Any other status means the row
        // moved under the read above, leaving this command nothing to apply.
        if !matches!(act.status, ActivityStatus::Completing) {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "continue_complete: activity {node} is not completing (status {:?})",
                    act.status
                ),
            ));
        }
        // The live children are read from the ownership-edge index, not off the row (see
        // `continue_terminate`).
        let children = ctx
            .storage
            .get_children(act.meta.object_ref().into_raw_object_ref())
            .await?;
        if !children.is_empty() {
            // Unreachable by construction — a Continue is issued only for a drained node — so this is
            // the row moving under the read. Each child is taken down by its own verb with `Cancelled`:
            // the node is being flushed, not failing, so a bystander must not carry a failure a state's
            // own `Catch` could route.
            for child in &children {
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
                    kind => panic!(
                        "continue_complete: activity {node} owns a live {kind} child {child}; an \
                         activity's children are only a fan-out Thread or a Task"
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
        let activity_value = act.clone();
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
        let variables = ctx.storage.get_variables(&thread.meta.object_ref()).await?;
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

impl Container for ActivityElementBehavior {
    type Owner = ActivityKind;

    async fn open(
        storage: &dyn ReadonlyStorageTxn,
        owner: ObjectRef<Self::Owner>,
    ) -> Result<Self, ProcessingError> {
        // The activity is the seam's own existence check (see `Container::open`): an activity that is
        // gone is refused here, uniformly for every caller whose settle would have nothing to land on.
        // The call site picks this impl from the owner's kind, and the parameter's own type is that
        // kind — an owner of another kind is unrepresentable rather than a case to check.
        if storage.get_activity(&owner).await?.is_none() {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!("activity_element: activity {owner} is gone"),
            ));
        }
        Ok(Self { activity: owner })
    }

    async fn after_child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        // The activity was resolved *before* the settle's own terminal event was written, and this
        // re-read runs in that same batch — so a row that is gone here is the lifecycle disagreeing with
        // the log rather than a settle to ignore. Refused, never no-op'ed: the terminal is already on
        // this batch, and a silent success would leave it unexplained. A fault is the leader's to retry.
        let Some(act) = ctx.storage.get_activity(&self.activity).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "activity_element: activity {} is gone from under its settled child",
                    self.activity
                ),
            ));
        };

        // What a settle *means* is the state's own affair, so the state's **type** is what picks the arm
        // below — and each arm then decides by the activity's own status, because which statuses a state
        // can even be settled in differ per state. A state's deadline is not a child in this relation:
        // its fire reaches `on_deadline_fired`.
        let (thread, state_def) = self.state_of(ctx, &act).await?;
        match &state_def {
            State::Task(_) | State::Parallel(_) | State::Map(_) => {
                self.settle_child(ctx, out, &act, &thread, &state_def, child)
                    .await
            }
            // Only a state that owns children in the settle relation can have one settle: a `Task` its
            // invocation, a `Parallel`/`Map` its fan-out. A `Wait` owns none (its resume instant is a
            // field, not a child), and every other state type has nothing a settle could come from —
            // so the row and the lifecycle disagree.
            _ => Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "activity_element: activity {} settled a child {child}, but state {:?} owns no \
                     children",
                    self.activity, act.state_path
                ),
            )),
        }
    }

    /// The activity's reaction is the completed hook **verbatim**: how a child settled is not what
    /// decides an activity's next move, only *that* it settled. A terminated child is exactly how a
    /// `Map`/`Parallel` learns one of its items/branches went down (`child_completed` reads the child's
    /// own terminal status to tell a finished item from a failed one), so routing the two outcomes to
    /// different arms would leave a fan-out waiting forever on a settle it was never shown.
    async fn after_child_terminated(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        child: &RawObjectRef,
    ) -> Result<(), ProcessingError> {
        self.after_child_completed(ctx, out, child).await
    }
}
#[cfg(test)]
mod settle_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use serde_json::{Value, json};
    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_storage::InMemoryStorage;
    use spica_testing::MockReadonlyStorageTxn;

    use super::{ActivityElementBehavior, Container};
    use crate::RejectionType;
    use crate::StatePath;
    use crate::StorageError;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, OverlaySink, ProcessingError};
    use crate::handlers::build_state_handlers;
    use crate::handlers::fixtures::{Seeded, at, object_ref, seed_edges};
    use crate::storage::{ReadonlyStorageTxn, Storage};
    use crate::types::activity::ActivityKind;
    use crate::types::command::{Command, CompleteState, TerminateState, TerminationReason};
    use crate::types::error::{ExecutionError, RuntimeError};
    use crate::types::execution::ExecutionKind;
    use crate::types::flow_version::FlowVersionKind;
    use crate::types::id::EntryId;
    use crate::types::meta::{
        HasRawObjectRef, NoOwner, ObjectMeta, ObjectRef, RawObjectRef, ThreadOwner,
    };
    use crate::types::task::TaskKind;
    use crate::types::thread::{Thread, ThreadKind, ThreadStatus};
    use crate::working::WorkingState;
    use crate::{
        Activity, ActivityStatus, EntryPayload, Execution, ExecutionStatus, FlowKind, FlowVersion,
    };

    fn activity_ref() -> ObjectRef<ActivityKind> {
        object_ref("execution-0", 90)
    }

    /// The owning activity's own owner — a `Thread`, the only kind an activity's slot admits.
    fn thread_owner() -> ObjectRef<ThreadKind> {
        object_ref("execution-1", 80)
    }

    /// The child handed to the container — a settled `Task`, the only kind that routes here so far.
    fn task_ref() -> ObjectRef<TaskKind> {
        object_ref("execution-0", 91)
    }

    /// The invocation of a `Task` state, settled.
    fn task_child() -> RawObjectRef {
        task_ref().into_raw_object_ref()
    }

    /// A child of a kind no state settled here owns — a branch/item thread, the fan-out's own child.
    fn thread_child() -> RawObjectRef {
        object_ref::<ThreadKind>("execution-2", 92).into_raw_object_ref()
    }

    /// A seeded activity row with `children` live children still attached. `raw_output` stands in for
    /// what the `TaskCompleted` applier folds onto the row in the settle's own batch.
    fn seeded_activity(
        status: ActivityStatus,
        raw_output: Option<Value>,
        children: usize,
    ) -> Seeded<Activity> {
        let mut path = jsonptr::PointerBuf::new();
        path.push_back("States");
        path.push_back("P");
        let activity = Activity {
            meta: ObjectMeta::builder(activity_ref().uid())
                .name(activity_ref().name().clone())
                .at(at())
                .with_owner(thread_owner()),
            execution: object_ref::<ExecutionKind>("execution", 70),
            state_path: StatePath::from(path),
            status,
            raw_input: json!({ "in": 1 }),
            input: Some(json!({ "in": 1 })),
            raw_output,
            activity_state: None,
            retry_state: None,
            output: None,
        };
        let live = (0..children)
            .map(|n| object_ref::<ThreadKind>("thread", 200 + n as u64).into_raw_object_ref())
            .collect::<Vec<_>>();
        let row = activity;
        Seeded::owning(row, live)
    }

    /// The run the seeded activity and its scope belong to.
    fn execution_ref() -> ObjectRef<ExecutionKind> {
        object_ref("execution", 70)
    }

    /// The version the seeded run binds to — the reference `machine_for_thread` resolves through the
    /// run's row, so the seeded version row has to answer to it.
    fn version_ref() -> ObjectRef<FlowVersionKind> {
        object_ref("execution", 60)
    }

    /// The scope a `Running` activity's settle is handed through: the thread the activity is owned by,
    /// carrying the input its own settle-time variables are read from.
    fn seeded_scope() -> Thread {
        Thread {
            meta: ObjectMeta::builder(thread_owner().uid())
                .name(thread_owner().name().clone())
                .at(at())
                .with_owner(ThreadOwner::Execution(execution_ref())),
            execution: execution_ref(),
            state_path: StatePath::root(),
            start_at: "P".to_string(),
            index: 0,
            status: ThreadStatus::Running,
            input: json!({ "in": 1 }),
            output: None,
        }
    }

    /// The run the scope resolves its machine through — the one row between a thread and its definition.
    fn seeded_run() -> Execution {
        Execution {
            meta: ObjectMeta::builder(execution_ref().uid())
                .name(execution_ref().name().clone())
                .at(at())
                .with_owner(NoOwner::new()),
            flow_version: version_ref(),
            status: ExecutionStatus::Running,
            deadline: None,
            input: json!({ "in": 1 }),
            output: None,
        }
    }

    /// The definition the seeded run binds to. `definition` decides which state type the container's
    /// `Running` arm finds at the activity's own `state_path`.
    fn seeded_version(definition: &str) -> FlowVersion {
        FlowVersion {
            meta: ObjectMeta::builder(version_ref().uid())
                .name(version_ref().name().clone())
                .at(at())
                .with_owner(object_ref::<FlowKind>("execution_flow", 50)),
            version: 1,
            definition: definition.to_string(),
            checksum: 0,
        }
    }

    /// A childless definition: `P` is a `Pass`, so the settled activity's state owns nothing a settle
    /// could come from.
    fn childless_definition() -> String {
        json!({
            "StartAt": "P",
            "States": { "P": { "Type": "Pass", "End": true } }
        })
        .to_string()
    }

    /// A definition whose `P` owns the child the `settle` driver hands over: a `Task` state, whose
    /// reaction to its own invocation settling is the one this container relays.
    fn task_definition() -> String {
        json!({
            "StartAt": "P",
            "States": { "P": { "Type": "Task", "Resource": "service-a", "End": true } }
        })
        .to_string()
    }

    /// A definition whose `P` is a `Wait` — the state whose one child *is* its completion trigger.
    fn wait_definition() -> String {
        json!({
            "StartAt": "P",
            "States": { "P": { "Type": "Wait", "Seconds": 30, "End": true } }
        })
        .to_string()
    }

    /// A definition whose `P` is a fan-out, the state that owns the branches it spawns.
    fn fanout_definition() -> String {
        json!({
            "StartAt": "P",
            "States": { "P": { "Type": "Parallel", "End": true, "Branches": [
                { "StartAt": "B", "States": { "B": { "Type": "Pass", "End": true } } }
            ] } }
        })
        .to_string()
    }

    /// A store holding this activity row alone — all the `Completing`/`Terminating` arms read.
    async fn store_over(activity: Seeded<Activity>) -> InMemoryStorage {
        let mut store = InMemoryStorage::new();
        let owner = activity.row.meta.raw_object_ref();
        seed_edges(&mut store, owner, activity.children).await;
        store
            .put_activity(activity.row)
            .await
            .expect("the in-memory store seeds an activity row");
        store
    }

    /// A store holding the activity, its owning scope and the run `definition` resolves through — the
    /// three rows the `Running` arm reads before it hands the settle to a state.
    async fn store_in_scope(activity: Seeded<Activity>, definition: &str) -> InMemoryStorage {
        let mut store = store_over(activity).await;
        store
            .put_thread(seeded_scope())
            .await
            .expect("the in-memory store seeds a thread row");
        store
            .put_execution(seeded_run())
            .await
            .expect("the in-memory store seeds an execution row");
        store
            .put_flow_version(seeded_version(definition))
            .await
            .expect("the in-memory store seeds a flow version row");
        store
    }

    /// Drive one hook over a working overlay seeded with `store` — the leader's shape, so the assertion
    /// reads the command the container emitted, not merely an intent. The hook's own outcome comes back
    /// too: an arm that refuses is as much the container's answer as one that emits.
    async fn settle(
        store: InMemoryStorage,
        child: &RawObjectRef,
        terminated: bool,
    ) -> (Result<(), ProcessingError>, Vec<EntryPayload>) {
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
        let container = ActivityElementBehavior::open(&work, activity_ref())
            .await
            .expect("the seeded activity resolves its container");
        let mut ctx = HandlerContext {
            env: &mut env,
            storage: &work,
            clock,
            ids,
            definitions: &mut definitions,
            state_handlers: &state_handlers,
        };
        let result = if terminated {
            container
                .after_child_terminated(&mut ctx, &mut out, child)
                .await
        } else {
            container
                .after_child_completed(&mut ctx, &mut out, child)
                .await
        };
        drop(ctx);
        let chain = out
            .into_entries()
            .into_iter()
            .map(|entry| entry.payload)
            .collect();
        (result, chain)
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
        let container = ActivityElementBehavior {
            activity: activity_ref(),
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
            ActivityElementBehavior::open(&work, activity_ref()).await
        else {
            panic!("an open that did not read a missing row");
        };
        assert_eq!(ty, RejectionType::NotFound);
        assert!(
            reason.contains("is gone"),
            "the refusal names the gone owner: {reason}"
        );
    }

    /// A read that **faults** stays a fault, never a missing owner. The two answers are what the callers
    /// refuse on: a missing row is a decision they record, a fault is one the leader retries — so folding
    /// the fault into the missing-owner arm would turn a hiccup into a refusal. Pinned here rather than
    /// per caller, since every handler answers it through this one contract.
    #[tokio::test]
    async fn a_faulted_owner_read_is_not_a_missing_owner() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_activity()
            .times(1)
            .return_once(|_| Err(StorageError::Backend("injected storage fault".to_string())));
        assert!(
            matches!(
                ActivityElementBehavior::open(&store, activity_ref()).await,
                Err(ProcessingError::Unexpected(_))
            ),
            "a fault must surface as Unexpected, not read as a missing row"
        );
    }

    /// An activity gone from under its settled child refuses the settle instead of no-op'ing it: the
    /// terminal is already on this attempt's batch, so a silent success would leave the log saying the
    /// child settled with nothing after it.
    #[tokio::test]
    async fn a_vanished_activity_refuses_the_settle() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_activity()
            .times(1)
            .return_once(|_| Ok(None));
        let result = hook_over(&store, task_ref().as_raw_object_ref()).await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a vanished activity must refuse the settle: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("is gone from under its settled child"),
            "the refusal names what it could not relay to: {reason}"
        );
    }

    /// A settle that reaches an activity whose state owns no children has nothing to mean: a `Pass`
    /// (like a `Choice`/`Succeed`/`Fail`) never gains a child, so the row and the lifecycle disagree and
    /// the batch is refused rather than left `Running` with nothing to advance it.
    #[tokio::test]
    async fn a_settled_child_under_a_childless_state_is_refused() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Running, None, 0),
            &childless_definition(),
        )
        .await;
        let (result, chain) = settle(store, &task_child(), false).await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a settle under a childless state must be refused: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("owns no children"),
            "the refusal names what the state lacks: {reason}"
        );
        assert!(chain.is_empty(), "the refusal emits nothing: {chain:?}");
    }

    /// A settle under a `Running` activity whose state owns children is relayed to that state: the
    /// container decides nothing itself, so what lands is exactly the state's own reaction — here a
    /// `Task` whose invocation settled completes the state with the output the row already carries.
    #[tokio::test]
    async fn a_settled_child_under_a_running_activity_reaches_its_state() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Running, Some(json!({ "out": 1 })), 0),
            &task_definition(),
        )
        .await;
        let (result, chain) = settle(store, &task_child(), false).await;
        result.expect("the hook relays the settle");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::CompleteState(
                CompleteState {
                    activity: activity_ref(),
                    output: json!({ "out": 1 }),
                }
            ))],
            "the state's own reaction is what the container relays"
        );
    }

    /// A `Running` activity whose run cannot be resolved is refused: the state that would decide the
    /// settle cannot be found, so nothing can advance the activity — and the same holds for a settle
    /// under an activity that is *already* finishing, since the state's type is what picks the arm.
    #[tokio::test]
    async fn a_settled_child_under_an_unresolvable_scope_is_refused() {
        for status in [
            ActivityStatus::Running,
            ActivityStatus::Completing,
            ActivityStatus::Terminating(TerminationReason::Cancelled),
        ] {
            let mut store = store_over(seeded_activity(status, None, 0)).await;
            store
                .put_thread(seeded_scope())
                .await
                .expect("the in-memory store seeds a thread row");
            let (result, _) = settle(store, &task_child(), false).await;
            let Err(ProcessingError::Rejected(ty, reason)) = result else {
                panic!("a settle whose definition cannot be resolved must be refused: {result:?}");
            };
            assert_eq!(ty, RejectionType::NotFound);
            assert!(
                reason.contains("cannot resolve its machine"),
                "the refusal names the definition it could not reach: {reason}"
            );
        }
    }

    /// A `Completing` activity with a child still in flight still advances: the Continue names the
    /// activity this settle belongs to, and its own handler takes down whatever is left and stops short
    /// of advancing — so the settle that drains the activity last is the one that completes it.
    #[tokio::test]
    async fn a_settled_child_advances_a_completing_activity_before_the_drain() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Completing, None, 1),
            &task_definition(),
        )
        .await;
        let (result, chain) = settle(store, &task_child(), false).await;
        result.expect("the hook relays the settle");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueComplete {
                owner: activity_ref().into_raw_object_ref(),
            })]
        );
    }

    /// The same for a fan-out: `Completing` is not the state's to decide, whichever state fronts the
    /// activity, so the Continue is emitted without consulting it.
    #[tokio::test]
    async fn a_settled_child_advances_a_completing_fanout() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Completing, None, 1),
            &fanout_definition(),
        )
        .await;
        let (result, chain) = settle(store, &thread_child(), false).await;
        result.expect("the hook relays the settle");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueComplete {
                owner: activity_ref().into_raw_object_ref(),
            })]
        );
    }

    /// A terminated child advances a `Terminating` owner into its ContinueTerminate — child still in
    /// flight or not: the Continue names the activity this settle belongs to, and its own handler takes
    /// down whatever is left and stops short of advancing.
    #[tokio::test]
    async fn a_terminated_child_advances_a_terminating_activity_before_the_drain() {
        let store = store_in_scope(
            seeded_activity(
                ActivityStatus::Terminating(TerminationReason::Cancelled),
                None,
                1,
            ),
            &task_definition(),
        )
        .await;
        let (result, chain) = settle(store, &task_child(), true).await;
        result.expect("the hook relays the settle");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueTerminate {
                owner: activity_ref().into_raw_object_ref(),
            })]
        );
    }

    /// A *completed* child under a `Terminating` activity advances that teardown just the same: the
    /// activity's direction was fixed when the teardown began, and a bystander's settle does not reopen
    /// it.
    #[tokio::test]
    async fn a_completed_child_advances_a_terminating_activity() {
        let store = store_in_scope(
            seeded_activity(
                ActivityStatus::Terminating(TerminationReason::Cancelled),
                None,
                0,
            ),
            &task_definition(),
        )
        .await;
        let (result, chain) = settle(store, &task_child(), false).await;
        result.expect("the hook relays the settle");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::ContinueTerminate {
                owner: activity_ref().into_raw_object_ref(),
            })]
        );
    }

    /// A settled child under an activity that has already finished is left alone: the activity will
    /// never act again, and what reaches it here is a late cleanup — the exit that gave up on an
    /// in-flight call disposes of it by resolving to a `CancelTask`, which lands after the finish.
    /// Refusing that would drop the cancellation of a child that is still live.
    #[tokio::test]
    async fn a_settled_child_under_a_finished_activity_is_left_alone() {
        for status in [
            ActivityStatus::Completed,
            ActivityStatus::Terminated(TerminationReason::Cancelled),
        ] {
            let store = store_in_scope(seeded_activity(status, None, 0), &task_definition()).await;
            let (result, chain) = settle(store, &task_child(), false).await;
            result.expect("a finished activity has nothing to move");
            assert!(
                chain.is_empty(),
                "a finished activity emits nothing: {chain:?}"
            );
        }
    }

    /// Drive one deadline hook over a working overlay seeded with `store` — the leader's shape, so the
    /// assertion reads the command the container emitted, not merely an intent. The hook's own outcome
    /// comes back too: an arm that refuses is as much the container's answer as one that emits.
    async fn deadline(store: InMemoryStorage) -> (Result<(), ProcessingError>, Vec<EntryPayload>) {
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
        let container = ActivityElementBehavior::open(&work, activity_ref())
            .await
            .expect("the seeded activity resolves its container");
        let mut ctx = HandlerContext {
            env: &mut env,
            storage: &work,
            clock,
            ids,
            definitions: &mut definitions,
            state_handlers: &state_handlers,
        };
        let result = container.on_deadline_fired(&mut ctx, &mut out, at()).await;
        drop(ctx);
        let chain = out
            .into_entries()
            .into_iter()
            .map(|entry| entry.payload)
            .collect();
        (result, chain)
    }

    /// A `Wait`'s resume deadline *is* its completion trigger, so a `Running` fire completes the state
    /// on the activity's processed input — the state's own reaction, held by the container because a
    /// `Wait` produces no distinct raw output.
    #[tokio::test]
    async fn a_fired_deadline_completes_a_running_wait_on_its_input() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Running, None, 0),
            &wait_definition(),
        )
        .await;
        let (result, chain) = deadline(store).await;
        result.expect("the hook answers the fire");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::CompleteState(
                CompleteState {
                    activity: activity_ref(),
                    output: json!({ "in": 1 }),
                }
            ))],
            "a `Wait` finishes on its input, which is the whole of its raw result"
        );
    }

    /// A `Task`'s `TimeoutSeconds` firing is the state's own failure: while the attempt is still in
    /// flight the state terminates `TimedOut`, naming the deadline the fire carries rather than a
    /// reason the container had to invent.
    #[tokio::test]
    async fn a_fired_deadline_terminates_a_running_task_before_its_call_lands() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Running, None, 0),
            &task_definition(),
        )
        .await;
        let (result, chain) = deadline(store).await;
        result.expect("the hook answers the fire");
        assert_eq!(
            chain,
            vec![EntryPayload::Command(Command::TerminateState(
                TerminateState {
                    activity: activity_ref(),
                    reason: TerminationReason::Failed {
                        error: ExecutionError::Runtime(RuntimeError::TimedOut {
                            message: format!(
                                "task ran past its TimeoutSeconds deadline ({})",
                                at().as_millis()
                            ),
                        }),
                    },
                }
            ))]
        );
    }

    /// A fire landing after the attempt already settled is refused: the `TaskCompleted` applier wrote
    /// `raw_output` in this same batch, so the deadline stopped bounding the state and the late fire no
    /// longer means anything to it.
    #[tokio::test]
    async fn a_fired_deadline_after_the_task_settled_is_refused() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Running, Some(json!({ "answer": 42 })), 0),
            &task_definition(),
        )
        .await;
        let (result, chain) = deadline(store).await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a late fire must be refused: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("after its task settled"),
            "the refusal names why the fire is late: {reason}"
        );
        assert!(chain.is_empty(), "the refusal emits nothing: {chain:?}");
    }

    /// A fire reaching an activity that has already finished is refused: the activity will never act
    /// again, and a fire that outlived its row must be answered rather than silently dropped.
    #[tokio::test]
    async fn a_deadline_under_a_finished_activity_is_refused() {
        for status in [
            ActivityStatus::Completed,
            ActivityStatus::Terminated(TerminationReason::Cancelled),
        ] {
            let store =
                store_in_scope(seeded_activity(status.clone(), None, 0), &task_definition()).await;
            let (result, chain) = deadline(store).await;
            let Err(ProcessingError::Rejected(ty, _)) = result else {
                panic!("a finished activity must refuse the fire: {result:?}");
            };
            assert_eq!(ty, RejectionType::InvalidState);
            assert!(chain.is_empty(), "the refusal emits nothing: {chain:?}");
        }
    }

    /// A state that owns no deadline cannot have one fire: the `Wait`/`Task` types are the only ones that
    /// arm one, so a `Pass` reaching here is the row and the lifecycle disagreeing.
    #[tokio::test]
    async fn a_fired_deadline_under_a_childless_state_is_refused() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Running, None, 0),
            &childless_definition(),
        )
        .await;
        let (result, chain) = deadline(store).await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a state that owns no deadline must refuse a fire: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("its state owns none"),
            "the refusal names what the state lacks: {reason}"
        );
        assert!(chain.is_empty(), "the refusal emits nothing: {chain:?}");
    }

    /// A `Wait`'s only child is its deadline, which is not in the settle relation — so a settled child
    /// under it is the row and the lifecycle disagreeing rather than a settle to react to.
    #[tokio::test]
    async fn a_settled_child_under_a_wait_is_refused() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Running, None, 0),
            &wait_definition(),
        )
        .await;
        let (result, chain) = settle(store, &task_child(), false).await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a settle under a `Wait` must be refused: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("owns no children"),
            "the refusal names what the state lacks: {reason}"
        );
        assert!(chain.is_empty(), "the refusal emits nothing: {chain:?}");
    }

    /// A `Task` is settled only by its invocation, so a settle of any other kind is the row and the
    /// lifecycle disagreeing — the state's own type names the kind it can be settled by.
    #[tokio::test]
    async fn a_settled_foreign_child_under_a_task_is_refused() {
        let store = store_in_scope(
            seeded_activity(ActivityStatus::Running, None, 0),
            &task_definition(),
        )
        .await;
        let (result, chain) = settle(store, &thread_child(), false).await;
        let Err(ProcessingError::Rejected(ty, reason)) = result else {
            panic!("a foreign child under a `Task` must be refused: {result:?}");
        };
        assert_eq!(ty, RejectionType::InvalidState);
        assert!(
            reason.contains("settled only by the children it owns"),
            "the refusal names what the state owns: {reason}"
        );
        assert!(chain.is_empty(), "the refusal emits nothing: {chain:?}");
    }
}
