use async_trait::async_trait;

use crate::command::{Command, TerminationReason};
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::id::{NodeId, NodeKind};
use crate::storage::Status;

/// Handles `Command::ChildFinalized`: a child reached a terminal state; the owned `parent` now
/// reacts based on **its own** state.
///
/// The parent's reaction splits into two distinct halves, which are kept in different places:
///
/// 1. **Drain** (a `Completing`/`Terminating` parent whose children are all gone): emit the
///    parent's terminal ed and relay a fresh `ChildFinalized` to the parent's own parent so the
///    drain continues up the tree. This is *tree-lifecycle* logic — it is identical regardless of
///    what state type the parent is (Pass, Choice, Map, Parallel all drain the same way), so it
///    belongs here in the handler, **not** on the state's `StateHandler`. Cache-alike per-state
///    copies would be pure duplication, and `StateHandler`'s contract is "the state decides its own
///    outcome" (activate/complete), whereas a drain is *the child deciding the parent may finish* —
///    a parent–child relation, not the state's own semantics.
/// 2. **Replenish** (a `Running` parent with an open slot, M2/M3 `Map`/`Parallel`): start the next
///    branch. This *is* state-specific — only the parent knows whether it is a `Map` (pull the
///    next `Items`/`ItemSelector` and compute its input) or a `Parallel` (fan-out pre-filled, no
///    replenish), just as `activate`/`complete` are. When M2/M3 arrive, `react`'s `Running` arm
///    should route to the matching [`StateHandler`](super::state_handler::StateHandler) via the
///    existing dispatch table (the same table `CompleteStateHandler` uses for `complete`), rather
///    than growing state logic inline here.
///
/// It replaces the old `cascade_up` walk, which made each child's handler guess whether it was the
/// parent's last child (the `len()==1 && contains` / re-entry snapshot special-cases).
///
/// **Why this is clean:** the child's terminal event is applied *before* this command is
/// dispatched (the Processor applies an `Event` then dispatches the following `Command` in batch
/// order), so `parent`'s `active_children` is the real post-drain projection — no snapshot
/// arithmetic needed.
#[derive(Default)]
pub struct ChildFinalizedHandler;

#[async_trait]
impl CommandHandler for ChildFinalizedHandler {
    fn command(&self) -> Command {
        Command::ChildFinalized {
            parent: NodeId::Execution(crate::id::ExecutionId::nil()),
            child: NodeId::Execution(crate::id::ExecutionId::nil()),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::ChildFinalized { parent, child } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        tracing::debug!(parent = ?parent, child = ?child, "child finalized");
        self.react(ctx.storage, out, *parent).await;
    }
}

impl ChildFinalizedHandler {
    /// The **drain** half of a child-settled reaction (see the type-level docs for the full
    /// drain-vs-replenish split). Applied to the owned `parent` node:
    ///
    /// - `Completing` / `Terminating` + no remaining children → emit the parent's terminal ed and
    ///   relay a `ChildFinalized` to the parent's own parent, so the drain continues up the tree
    ///   one level per dispatch round (mirrors the old `cascade_up` ascent, distributed as
    ///   commands instead of an inline loop).
    /// - `Running` → the parent is still accepting work. In M1 a node never owns a child while
    ///   `Running` (activities parent only timers, and only while the activity is finishing), so
    ///   this is a no-op. M2/M3: a `Map`/`Parallel` replenishes a work slot here — routed to the
    ///   matching `StateHandler` rather than inlined (see the `Running` arms' TODOs).
    /// - Anything terminal → a duplicate/replayed notice is already-settled; idempotent no-op.
    async fn react(
        &self,
        storage: &dyn crate::storage::Storage,
        out: &mut Collector,
        parent: NodeId,
    ) {
        match parent.kind() {
            NodeKind::Execution(id) => {
                let Some(exec) = storage.get_execution(id).await.ok().flatten() else {
                    return; // gone already; nothing to drain.
                };
                if !exec.active_children.is_empty() {
                    return; // not drained yet — some other child owns the parent's finish.
                }
                match exec.status {
                    Status::Completing => {
                        let output = exec.pending_output.clone().unwrap_or(Default::default());
                        out.emit_event(Event::ExecutionCompleted {
                            id,
                            output: output.clone(),
                        });
                        if let Some(gp) = exec.parent {
                            out.emit_command(Command::ChildFinalized {
                                parent: gp,
                                child: NodeId::Execution(id),
                            });
                        }
                    }
                    Status::Terminating => {
                        let reason = exec
                            .pending_reason
                            .clone()
                            .unwrap_or(TerminationReason::Cancelled);
                        out.emit_event(Event::ExecutionTerminated {
                            id,
                            reason: reason.clone(),
                        });
                        if let Some(gp) = exec.parent {
                            out.emit_command(Command::ChildFinalized {
                                parent: gp,
                                child: NodeId::Execution(id),
                            });
                        }
                    }
                    // Running + children: the **replenish** half (state-specific, M2/M3).
                    // Route to the matching StateHandler (parallel to `complete`) so a Map pulls
                    // its next Items/ItemSelector and ActivateState's it, and Parallel just waits
                    // for all branches; do not grow state logic inline here.
                    // TODO(Map/Parallel): dispatch to StateHandler::replenish via the state table.
                    _ => {}
                }
            }
            NodeKind::Activity(id) => {
                let Some(act) = storage.get_activity(id).await.ok().flatten() else {
                    return;
                };
                if !act.active_children.is_empty() {
                    return;
                }
                match act.status {
                    Status::Completing => {
                        let output = act.pending_output.clone().unwrap_or(Default::default());
                        out.emit_event(Event::StateCompleted {
                            activity: id,
                            output: output.clone(),
                        });
                        out.emit_command(Command::ChildFinalized {
                            parent: act.parent,
                            child: NodeId::Activity(id),
                        });
                    }
                    Status::Terminating => {
                        let reason = act
                            .pending_reason
                            .clone()
                            .unwrap_or(TerminationReason::Cancelled);
                        out.emit_event(Event::StateTerminated {
                            activity: id,
                            reason: reason.clone(),
                        });
                        out.emit_command(Command::ChildFinalized {
                            parent: act.parent,
                            child: NodeId::Activity(id),
                        });
                    }
                    // Running + children: the **replenish** half (state-specific, M2/M3).
                    // Route to the matching StateHandler (parallel to `complete`) so a Map pulls
                    // its next Items/ItemSelector and ActivateState's it, and Parallel just waits
                    // for all branches; do not grow state logic inline here.
                    // TODO(Map/Parallel): dispatch to StateHandler::replenish via the state table.
                    _ => {}
                }
            }
            // A timer never owns children; `ChildFinalized` is never issued to one.
            NodeKind::Timer(_) => {}
        }
    }
}
