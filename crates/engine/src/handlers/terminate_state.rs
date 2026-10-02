use crate::RejectionType;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::{
    Command, TerminateExecution, TerminateState, TerminateThread, TerminationReason,
};
use crate::types::event::Event;
use crate::types::meta::{HasRawObjectRef, ObjectKind, OwnerScope};
use crate::types::task::TaskKind;
use crate::types::thread::ThreadKind;
use crate::types::timer::TimerKind;

/// Handles `Command::TerminateState`: the abnormal finish of the activity bound to it, with
/// `reason`. Emits `StateTerminating`, sweeps the activity's owned children (M1: only timers —
/// e.g. a Wait cancelled mid-flight), and emits `StateTerminated{reason}` immediately when the
/// activity is childless, then runs the inline child-settled reaction so its parent drains.
///
/// It is also the one place a failing state's **scope** is taken down: an activity is always owned by
/// a [`Thread`](crate::Thread) (the slot's own type — a top-level run's derived root thread, or a
/// fan-out branch/item thread), so the site that opened the failure never has to name it, and the
/// root-relays-to-the-run dialect lives here rather than at every failing site. The scope is only
/// told when it is still `Running`: a thread already completing or terminating was reached by an
/// ancestor's sweep, which is tearing this activity down on the way (this command is that sweep's
/// own), and a second termination for it would only be refused.
#[derive(Default)]
pub struct TerminateStateHandler;

impl TerminateStateHandler {
    pub(crate) async fn handle(
        &self,
        p: &TerminateState,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let TerminateState { activity, reason } = p;
        // Nothing ever removes a row, and an activity row is written by the batch that activates it —
        // before any sweep could name it — so a miss is the log and the projection disagreeing (a forged
        // command, or a corrupt store) rather than an activity that outlived its teardown. A fault
        // reading the row is neither, and is returned so the leader can retry it.
        let act = match ctx.storage.get_activity(activity).await? {
            Some(a) => a,
            None => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!(
                        "terminate_state: activity {activity} does not exist; termination refused"
                    ),
                ));
            }
        };
        // Status dispatch before the normal path. Anything other than Running is a duplicate —
        // another handler already claimed the close. The Fail + TerminateExecution cascade
        // produces one of each kind: Fail's own TerminateState lands first (Running → Terminating
        // → Terminated drain); the TerminateExecution-swept TerminateState arrives right behind
        // with the activity *already Terminated in the parent-visible snapshot* because storage
        // was read before the drain batch applied. The projection is idempotent on remove_child
        // (HashSet), so re-emitting the ed as a duplicate is safe AND is what drains the
        // Terminating execution that waited on us.
        use crate::ActivityStatus as S;
        match act.value.status {
            // Running, or Completing, are legitimate pre-failure states: a state can fail either
            // before the complete step opens (Running) or while it is in progress (Completing, since
            // the state's `complete` opens with `StateCompleting` before it projects). Both must be
            // redirected from success to failure — fall through to the normal terminate path.
            S::Running | S::Completing => {}
            S::Terminated(ref reason) => {
                // Re-emit the terminal ed with the recorded reason (ignore the incoming duplicate
                // reason — it arrived later and is the parent's copy). The projection absorbs the
                // duplicate; the owned parent then reacts via the inline child-settled cascade
                // (activity already drained from its snapshot) and advances the parent's finish.
                let mut activity_value = act.value();
                activity_value.meta.updated_at = ctx.now();
                activity_value.status = S::Terminated(reason.clone());
                out.append_event(crate::types::event::Event::StateTerminated {
                    activity: activity_value,
                })
                .await;
                super::child_completed::child_settled(
                    ctx,
                    out,
                    act.value.meta.owner.clone().into_raw_object_ref(),
                    activity.as_raw_object_ref().clone(),
                )
                .await;
                return Ok(());
            }
            S::Completed => {
                // A terminated-after-complete duplicate: the completer's drain is in flight.
                return Ok(());
            }
            // Terminating is mid-sweep: a terminate is already in flight, so a second one here is a
            // duplicate — swallow it (the in-flight sweep owns the drain).
            S::Terminating(_) => return Ok(()),
        }
        let _ = TerminationReason::Cancelled; // referenced above

        // Only a scope that still has to be told is worth reaching for, and the read happens before
        // the first record goes out so a fault is returned while the activity is still untouched. A
        // thread row that is gone cannot be redirected — an anomaly (rows are never removed) that
        // must not hold up the activity's own unwind.
        let owner = act.value.meta.owner.clone();
        let owner_running = match ctx.storage.get_thread(&owner).await? {
            Some(t) => t.value.status.is_running(),
            None => false,
        };

        // The failure reaches the scope the activity runs in, from the same step that opens the
        // activity's close. A fan-out branch/item thread is reachable only by the reference-addressed
        // `TerminateThread`; a root thread's own termination relays up to the run (see
        // `TerminateThreadHandler`), so a state failure anywhere still takes the whole run down.
        // Ahead of `StateTerminating`: an ancestor being torn down sweeps this activity as one of its
        // children, so the scope's command is what carries the reason outward while the activity's own
        // records stay the tail of the close.
        if owner_running {
            super::emit_scope_termination(out, &OwnerScope::Thread(owner), reason.clone());
        }

        // Every record emitted below is this row *after* its own write, so each carries the moment of
        // that write rather than the stored stamp: re-reading the activity would date a termination
        // that happened after a deadline (a `Wait` cancelled a minute into its resume) at the
        // activation it was read from.
        let now = ctx.now();
        let mut terminating_activity = act.value();
        terminating_activity.meta.updated_at = now;
        terminating_activity.status = S::Terminating(reason.clone());
        out.append_event(Event::StateTerminating {
            activity: terminating_activity,
        })
        .await;

        // M1 sweeps the full materialized child set here. TODO(termination+batching): if this ever
        // chunks the sweep Zeebe-style (a partition-scanned `(parent, index)` resume with follow-up
        // batches — to avoid issuing one Command per child at once), correctness depends on a strict
        // monotonic order over child keys: a child created *after* termination began must sort after
        // the current `index`, or a per-index skip would miss it. Spica's node references are ULID-based
        // (monotone), so ordering holds by construction — but an unordered child container (e.g. a
        // `HashSet`, as today) cannot drive an index-resume sweep on its own and would need an
        // explicit ordered key. See Zeebe's ProcessInstanceBatchTerminateStreamProcessor / DbElementInstanceState
        // ELEMENT_INSTANCE_PARENT_CHILD for the reference shape.
        let children = act.active_children.clone();
        let mut pending = 0usize;
        for child in children {
            match child.kind {
                ObjectKind::Timer => {
                    out.append_command(Command::CancelTimer {
                        timer: child.typed::<TimerKind>(),
                    });
                    pending += 1;
                }
                // A `Parallel` state's in-flight branches are child *executions* rooted under this
                // activity; an ancestor cancellation must terminate each one so the branch's own
                // subtree (its timers/activities) unwinds and the child relays its settle back,
                // letting this activity drain. M1 non-container states own no child executions, so
                // the arm is inert there.
                ObjectKind::Execution => {
                    out.append_command(Command::TerminateExecution(TerminateExecution {
                        name: child.name.clone(),
                        uid: Some(child.uid),
                        reason: reason.clone(),
                    }));
                    pending += 1;
                }
                // The split's fan-out children (a `Parallel` branch / `Map` item) are **Threads**
                // rooted under this activity. An ancestor cancellation must tear each one down too;
                // `TerminateThread` is reference-addressed (threads live in thread storage), unlike
                // the name-addressed `TerminateExecution` above.
                ObjectKind::Thread => {
                    out.append_command(Command::TerminateThread(TerminateThread {
                        thread: child.clone().typed::<ThreadKind>(),
                        reason: reason.clone(),
                    }));
                    pending += 1;
                }
                // A non-container activity owns no child activities (only `Parallel`/`Map` do, via
                // child executions); an `Activity` child is unreachable here today.
                ObjectKind::Activity => {}
                // A `Task` is a leaf side-effect node owned by this activity (a `Task` state's
                // in-flight call). Sweep it like a timer: `CancelTask` marks it Cancelled and drains
                // it. The physical call is left running; a later `CompleteTask` is swallowed by the
                // `CompleteTaskHandler`'s non-Running guard.
                ObjectKind::Task => {
                    out.append_command(Command::CancelTask {
                        task: child.typed::<TaskKind>(),
                    });
                    pending += 1;
                }
                // A node container never owns a Flow/FlowVersion child (no such reachable tree edge).
                _ => {}
            }
        }
        if pending == 0 {
            let mut terminated_activity = act.value();
            terminated_activity.meta.updated_at = now;
            terminated_activity.status = S::Terminated(reason.clone());
            out.append_event(Event::StateTerminated {
                activity: terminated_activity,
            })
            .await;
            super::child_completed::child_settled(
                ctx,
                out,
                act.value.meta.owner.clone().into_raw_object_ref(),
                activity.as_raw_object_ref().clone(),
            )
            .await;
        } else {
            tracing::debug!(
                activity = %activity,
                pending,
                "state terminating deferred: waiting on owned children"
            );
        }

        Ok(())
    }
}
