use async_trait::async_trait;

use crate::command::Command;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::id::NodeId;

/// Handles `Command::TerminateState`: the abnormal finish of the activity bound to it, with
/// `reason`. Emits `StateTerminating`, sweeps the activity's owned children (M1: only timers —
/// e.g. a Wait cancelled mid-flight), and emits `StateTerminated{reason}` immediately when the
/// activity is childless; otherwise the ed is emitted by [`super::cascade_up`] as the last child
/// terminates. A terminating activity's terminal ed then drains its parent (via cascade_up).
#[derive(Default)]
pub struct TerminateStateHandler;

#[async_trait]
impl CommandHandler for TerminateStateHandler {
    fn command(&self) -> Command {
        Command::TerminateState {
            activity: crate::id::ActivityId::nil(),
            reason: crate::command::TerminationReason::Cancelled,
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::TerminateState { activity, reason } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        let act = match ctx.storage.get_activity(*activity).await {
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
        use crate::command::TerminationReason;
        use crate::storage::Status as S;
        match act.status {
            // Running, or Completing, are legitimate pre-failure states: a state can fail either
            // before the complete step opens (Running) or while it is in progress (Completing, since
            // `StateCompleting` is emitted eagerly by `CompleteStateHandler` before the state's
            // `complete` runs). Both must be redirected from success to failure — fall through to the
            // normal terminate path.
            S::Running | S::Completing => {}
            S::Terminated(reason) => {
                // Re-emit the terminal ed with the recorded reason (ignore the incoming duplicate
                // reason — it arrived later and is the parent's copy). The projection absorbs the
                // duplicate; the owned parent then reacts via `ChildFinalized` (activity already
                // drained from its snapshot) and advances the parent's deferred terminations.
                out.emit_event(crate::event::Event::StateTerminated {
                    activity: *activity,
                    reason,
                });
                out.emit_command(Command::ChildFinalized {
                    parent: act.parent,
                    child: NodeId::Activity(*activity),
                });
                return;
            }
            S::Completed => {
                // A terminated-after-complete duplicate: the completer's drain is in flight.
                return;
            }
            // Terminating is mid-sweep: a terminate is already in flight, so a second one here is a
            // duplicate — swallow it (the in-flight sweep owns the drain).
            S::Terminating => return,
        }
        let _ = TerminationReason::Cancelled; // referenced above

        out.emit_event(Event::StateTerminating {
            activity: *activity,
        });

        let children = act.active_children.clone();
        let mut pending = 0usize;
        for child in children {
            match child {
                NodeId::Timer(t) => {
                    out.emit_command(Command::CancelTimer { timer: t });
                    pending += 1;
                }
                // M1 states own no other activities/executions; Parallel/Map children land in M2.
                NodeId::Activity(_) | NodeId::Execution(_) => {}
            }
        }
        if pending == 0 {
            out.emit_event(Event::StateTerminated {
                activity: *activity,
                reason: reason.clone(),
            });
            out.emit_command(Command::ChildFinalized {
                parent: act.parent,
                child: NodeId::Activity(*activity),
            });
        } else {
            tracing::debug!(
                activity = %activity,
                pending,
                "state terminating deferred: waiting on owned children"
            );
        }
    }
}
