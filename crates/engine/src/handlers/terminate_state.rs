use async_trait::async_trait;

use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::types::command::Command;
use crate::types::event::Event;
use crate::types::meta::ObjectKind;

/// Handles `Command::TerminateState`: the abnormal finish of the activity bound to it, with
/// `reason`. Emits `StateTerminating`, sweeps the activity's owned children (M1: only timers —
/// e.g. a Wait cancelled mid-flight), and emits `StateTerminated{reason}` immediately when the
/// activity is childless, then runs the inline child-settled reaction so its parent drains.
#[derive(Default)]
pub struct TerminateStateHandler;

#[async_trait]
impl CommandHandler for TerminateStateHandler {
    fn command(&self) -> Command {
        Command::TerminateState {
            activity: crate::types::meta::ObjectReference::nil(),
            reason: crate::types::command::TerminationReason::Cancelled,
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector<'_>) {
        let Command::TerminateState { activity, reason } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        let act = match ctx.storage.get_activity(activity).await {
            Ok(Some(a)) => a,
            Ok(None) | Err(_) => return, // gone already; nothing to terminate.
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
        use crate::types::command::TerminationReason;
        match act.value.status {
            // Running, or Completing, are legitimate pre-failure states: a state can fail either
            // before the complete step opens (Running) or while it is in progress (Completing, since
            // `StateCompleting` is emitted eagerly by `CompleteStateHandler` before the state's
            // `complete` runs). Both must be redirected from success to failure — fall through to the
            // normal terminate path.
            S::Running | S::Completing => {}
            S::Terminated(ref reason) => {
                // Re-emit the terminal ed with the recorded reason (ignore the incoming duplicate
                // reason — it arrived later and is the parent's copy). The projection absorbs the
                // duplicate; the owned parent then reacts via the inline child-settled cascade
                // (activity already drained from its snapshot) and advances the parent's finish.
                let mut activity_value = act.value();
                activity_value.status = S::Terminated(reason.clone());
                out.emit_event(crate::types::event::Event::StateTerminated {
                    activity: activity_value,
                })
                .await;
                super::child_completed::child_settled(
                    ctx,
                    out,
                    act.value
                        .meta
                        .owner
                        .clone()
                        .expect("an owned activity has an owner"),
                    activity.clone(),
                )
                .await;
                return;
            }
            S::Completed => {
                // A terminated-after-complete duplicate: the completer's drain is in flight.
                return;
            }
            // Terminating is mid-sweep: a terminate is already in flight, so a second one here is a
            // duplicate — swallow it (the in-flight sweep owns the drain).
            S::Terminating(_) => return,
        }
        let _ = TerminationReason::Cancelled; // referenced above

        let mut terminating_activity = act.value();
        terminating_activity.status = S::Terminating(reason.clone());
        out.emit_event(Event::StateTerminating {
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
                    out.emit_command(Command::CancelTimer { timer: child });
                    pending += 1;
                }
                // A `Parallel` state's in-flight branches are child *executions* rooted under this
                // activity; an ancestor cancellation must terminate each one so the branch's own
                // subtree (its timers/activities) unwinds and the child relays its settle back,
                // letting this activity drain. M1 non-container states own no child executions, so
                // the arm is inert there.
                ObjectKind::Execution => {
                    out.emit_command(Command::TerminateExecution {
                        name: child.name.clone(),
                        uid: Some(child.uid),
                        reason: reason.clone(),
                    });
                    pending += 1;
                }
                // The split's fan-out children (a `Parallel` branch / `Map` item) are **Threads**
                // rooted under this activity. An ancestor cancellation must tear each one down too;
                // `TerminateThread` is reference-addressed (threads live in thread storage), unlike
                // the name-addressed `TerminateExecution` above.
                ObjectKind::Thread => {
                    out.emit_command(Command::TerminateThread {
                        thread: child.clone(),
                        reason: reason.clone(),
                    });
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
                    out.emit_command(Command::CancelTask { task: child });
                    pending += 1;
                }
                // A node container never owns a Flow/FlowVersion child (no such reachable tree edge).
                _ => {}
            }
        }
        if pending == 0 {
            let mut terminated_activity = act.value();
            terminated_activity.status = S::Terminated(reason.clone());
            out.emit_event(Event::StateTerminated {
                activity: terminated_activity,
            })
            .await;
            super::child_completed::child_settled(
                ctx,
                out,
                act.value
                    .meta
                    .owner
                    .clone()
                    .expect("an owned activity has an owner"),
                activity.clone(),
            )
            .await;
        } else {
            tracing::debug!(
                activity = %activity,
                pending,
                "state terminating deferred: waiting on owned children"
            );
        }
    }
}
