use async_trait::async_trait;

use crate::command::Command;
use crate::event::Event;
use crate::handler::{ActivityCtx, Collector, CommandHandler, CtxKind, HandlerContext};
use crate::id::{NodeId, NodeKind};
use crate::storage::{ActivityStatus, ExecutionStatus};

/// Handles `Command::ProcessChildCompleted`: a child reached a terminal state; the owned `parent` now
/// reacts based on **its own** state.
///
/// The parent's reaction splits into two distinct halves, which are kept in different places:
///
/// 1. **Drain** (a `Completing`/`Terminating` parent whose children are all gone): emit the
///    parent's terminal ed and relay a fresh `ProcessChildCompleted` to the parent's own parent so the
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
pub struct ProcessChildCompletedHandler {
    state_handlers: std::collections::HashMap<
        std::mem::Discriminant<spica_asl::State>,
        Box<dyn super::state_handler::StateHandler>,
    >,
}

impl ProcessChildCompletedHandler {
    pub fn new() -> Self {
        Self {
            state_handlers: super::dispatch::build_state_handlers(),
        }
    }
}

impl Default for ProcessChildCompletedHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for ProcessChildCompletedHandler {
    fn command(&self) -> Command {
        Command::ProcessChildCompleted {
            parent: NodeId::Execution(crate::id::ExecutionId::nil()),
            child: NodeId::Execution(crate::id::ExecutionId::nil()),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::ProcessChildCompleted { parent, child } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        tracing::debug!(parent = ?parent, child = ?child, "child finalized");
        self.react(ctx, out, *parent, *child).await;
    }
}

impl ProcessChildCompletedHandler {
    /// The **drain** half of a child-settled reaction (see the type-level docs for the full
    /// drain-vs-replenish split). Applied to the owned `parent` node:
    ///
    /// - `Completing` / `Terminating` + no remaining children → emit the parent's terminal ed and
    ///   relay a `ProcessChildCompleted` to the parent's own parent, so the drain continues up the tree
    ///   one level per dispatch round (mirrors the old `cascade_up` ascent, distributed as
    ///   commands instead of an inline loop).
    /// - `Running` → the parent is still accepting work. For a container state (M3 `Parallel`/`Map`)
    ///   this is the **replenish** moment: the state's `child_completed` hook is dispatched on *every*
    ///   settled child — a `Map` pulls its next `Items`/`ItemSelector` item into a freed `MaxConcurrency`
    ///   slot (and converges when the last item settles), while a `Parallel` just waits for all its
    ///   pre-fanned-out branches (its own hook guards on the children having fully drained). A
    ///   non-container `Running` activity owns no settled children, so its dispatch is a safe no-op.
    /// - Anything terminal → a duplicate/replayed notice is already-settled; idempotent no-op.
    async fn react(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector,
        parent: NodeId,
        child: NodeId,
    ) {
        match parent.kind() {
            NodeKind::Execution(id) => {
                let Some(exec) = ctx.storage.get_execution(id).await.ok().flatten() else {
                    return; // gone already; nothing to drain.
                };
                if !exec.active_children.is_empty() {
                    return; // not drained yet — some other child owns the parent's finish.
                }
                match exec.status {
                    ExecutionStatus::Completing => {
                        let output = exec.output.clone().unwrap_or(Default::default());
                        out.emit_event(Event::ExecutionCompleted {
                            id,
                            output: output.clone(),
                        });
                        if let Some(gp) = exec.parent {
                            out.emit_command(Command::ProcessChildCompleted {
                                parent: gp,
                                child: NodeId::Execution(id),
                            });
                        }
                    }
                    ExecutionStatus::Terminating(reason) => {
                        out.emit_event(Event::ExecutionTerminated {
                            id,
                            reason: reason.clone(),
                        });
                        if let Some(gp) = exec.parent {
                            out.emit_command(Command::ProcessChildCompleted {
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
                let Some(act) = ctx.storage.get_activity(id).await.ok().flatten() else {
                    return;
                };
                match act.status {
                    ActivityStatus::Completing => {
                        // The success drain only fires once every owned child has terminated — the
                        // `ing` (`StateCompleting`) awaits them. A still-in-flight child owns this
                        // finish, so we don't emit the `ed` yet.
                        if !act.active_children.is_empty() {
                            return;
                        }
                        // Success-drain uses the activity's raw result when present; otherwise the
                        // state's processed input remains the default output. This path is currently
                        // a defensive fallback (container states emit their own `StateCompleted`
                        // after projecting convergence), but keeping it aligned with the shared
                        // complete semantics avoids silently changing the result if a future state
                        // defers its success ed to drain.
                        let output = act.raw_output.clone().unwrap_or_else(|| act.input.clone());
                        out.emit_event(Event::StateCompleted {
                            activity: id,
                            output: output.clone(),
                        });
                        out.emit_command(Command::ProcessChildCompleted {
                            parent: act.parent,
                            child: NodeId::Activity(id),
                        });
                    }
                    ActivityStatus::Terminating(reason) => {
                        // The failure drain likewise only fires once every child has terminated.
                        if !act.active_children.is_empty() {
                            return;
                        }
                        out.emit_event(Event::StateTerminated {
                            activity: id,
                            reason: reason.clone(),
                        });
                        out.emit_command(Command::ProcessChildCompleted {
                            parent: act.parent,
                            child: NodeId::Activity(id),
                        });
                    }
                    // Running + children: the **replenish** half (state-specific, M2/M3). Dispatched
                    // on *every* settled child (not just when `active_children` is empty), so a `Map`
                    // can refill a freed `MaxConcurrency` slot one item at a time even while other
                    // items are still in flight; the matching `StateHandler::child_completed` decides
                    // the state-specific next move (replenish / converge / fail). For a `Parallel`,
                    // whose branches were all fanned out up front, the hook itself guards on the
                    // children having fully drained before it converges.
                    ActivityStatus::Running => {
                        self.dispatch_child_completed(ctx, out, id, &act, child)
                            .await;
                    }
                    _ => {}
                }
            }
            // A timer never owns children; `ProcessChildCompleted` is never issued to one.
            NodeKind::Timer(_) => {}
            // A task (M2 external-resource call) is likewise a leaf side-effect node — it owns no
            // children, so `ProcessChildCompleted` is never issued to it either. M1 does not drive tasks.
            NodeKind::Task(_) => {}
        }
    }

    /// The **replenish** half of a `Running` activity whose child settled: build an
    /// `ActivityCtx` for the activity and dispatch to its `StateHandler::child_completed` so the
    /// state itself decides the next move (for M3 `Map`: refill a `MaxConcurrency` slot, converge
    /// when the last item settles, or fail on a failure; for `Parallel`: aggregate branch outputs
    /// and complete, or fail on a branch failure). Mirrors how `CompleteStateHandler` constructs a
    /// ctx for `complete` — a container state's replenish is one of its lifecycle moments.
    ///
    /// `child` is the node that just settled, threaded through so a `Map` can identify which item it
    /// was.
    ///
    /// `actx.execution` is the owning execution, whose `root_execution`/`state_path` thread
    /// through so a nested `Map`/`Parallel` can fan out deeper children.
    async fn dispatch_child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector,
        activity: crate::id::ActivityId,
        act: &crate::storage::Activity,
        child: NodeId,
    ) {
        // The owning execution: an activity is owned by an Execution (top-level or a Parallel-branch
        // child execution). Build the ctx from that execution's row.
        let execution = match act.parent {
            NodeId::Execution(e) => e,
            _ => return, // internal fault: an activity's owner is always an Execution.
        };
        let Some(exec) = ctx.storage.get_execution(execution).await.ok().flatten() else {
            return; // owning execution gone — nothing to replenish into.
        };
        let state_def = match super::resolve_state_for(
            ctx.storage,
            ctx.sm,
            execution,
            &crate::handlers::state_name_from_path(act.state_path.as_ptr()),
        )
        .await
        {
            Ok(s) => s,
            Err(_) => return, // definition gone — nothing to decide.
        };
        let actx = ActivityCtx {
            execution,
            root_execution: exec.root_execution,
            execution_state_path: exec.state_path.clone(),
            exec_input: exec.input.clone(),
            raw_input: act.raw_input.clone(),
            input: act.input.clone(),
            raw_output: act.raw_output.clone(),
            scope: exec.scope.clone(),
            state_path: act.state_path.clone(),
            retry_count: act.retry_state.retry_count,
            kind: CtxKind::Complete,
        };
        match self.state_handlers.get(&std::mem::discriminant(state_def)) {
            Some(handler) => {
                handler
                    .child_completed(ctx, out, activity, Some(&actx), state_def, child)
                    .await;
            }
            None => {
                // A non-container state owning children while Running is an internal fault; nothing
                // to do (the activity stays Running, but no container logic runs).
                tracing::warn!(activity = %activity, "no container child_completed for state");
            }
        }
    }
}
