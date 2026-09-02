use async_trait::async_trait;

use crate::handler::{ActivityCtx, Collector, CommandHandler, CtxKind, HandlerContext};
use crate::log::Timestamp;
use crate::types::command::Command;
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectReference};
use crate::{ActivityStatus, ExecutionStatus};

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
/// dispatched (the StreamProcessor applies an `Event` then dispatches the following `Command` in batch
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
            owner: ObjectReference::nil(),
            child: ObjectReference::nil(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::ProcessChildCompleted { owner, child } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        tracing::debug!(owner = ?owner, child = ?child, "child finalized");
        self.react(ctx, out, owner.clone(), child.clone()).await;
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
        parent: ObjectReference,
        child: ObjectReference,
    ) {
        // Dispatch by the parent's structural kind — `parent` itself is the reference, so the arm
        // uses it directly rather than re-binding a wrapped id.
        match parent.kind {
            ObjectKind::Execution => {
                let Some(exec) = ctx.storage.get_execution(&parent).await.ok().flatten() else {
                    return; // gone already; nothing to drain.
                };
                if !exec.active_children.is_empty() {
                    return; // not drained yet — some other child owns the parent's finish.
                }
                match &exec.status {
                    ExecutionStatus::Completing => {
                        let output = exec.output.clone().unwrap_or(Default::default());
                        let mut completed_execution = exec.value();
                        completed_execution.status = ExecutionStatus::Completed;
                        completed_execution.output = Some(output.clone());
                        // Parent reaches its own terminal transition — advance `updated_at` at event
                        // construction; the event carries the fresh domain timestamps.
                        completed_execution.meta.touch(Timestamp::now());
                        // The parent execution's finish is observable durably; `start`'s
                        // `wait_for_execution` poll surfaces it from Storage. No deferred ack — the
                        // execution (a Parallel branch or its owner) was never awaited via an ack.
                        let completed_event = Event::ExecutionCompleted {
                            execution: completed_execution,
                        };
                        out.emit_event(completed_event);
                        if let Some(owner) = exec.value.meta.owner.clone() {
                            out.emit_command(Command::ProcessChildCompleted {
                                owner,
                                child: parent.clone(),
                            });
                        }
                    }
                    ExecutionStatus::Terminating(reason) => {
                        let mut terminated_execution = exec.value();
                        terminated_execution.status = ExecutionStatus::Terminated(reason.clone());
                        terminated_execution.meta.touch(Timestamp::now());
                        // Same as the Completing branch: reachable durably via `wait_for_execution`
                        // poll; no deferred ack needed.
                        let terminated_event = Event::ExecutionTerminated {
                            execution: terminated_execution,
                        };
                        out.emit_event(terminated_event);
                        if let Some(owner) = exec.value.meta.owner.clone() {
                            out.emit_command(Command::ProcessChildCompleted {
                                owner,
                                child: parent.clone(),
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
            // A fan-out `Thread` (a Parallel branch / Map item) drains like a top-level execution:
            // once its own children are gone and it is Completing/Terminating, emit its terminal ed
            // and relay to its owner (the container Activity) so the parallel/map converges.
            ObjectKind::Thread => {
                let Some(thread) = ctx.storage.get_thread(&parent).await.ok().flatten() else {
                    return; // gone already; nothing to drain.
                };
                if !thread.active_children.is_empty() {
                    return; // not drained yet — some other child owns the thread's finish.
                }
                use crate::types::thread::ThreadStatus;
                match &thread.status {
                    ThreadStatus::Completing => {
                        let output = thread.output.clone().unwrap_or(Default::default());
                        let mut completed_thread = thread.value();
                        completed_thread.status = ThreadStatus::Completed;
                        completed_thread.output = Some(output);
                        completed_thread.meta.touch(Timestamp::now());
                        let completed_event = Event::ThreadCompleted {
                            thread: completed_thread,
                        };
                        out.emit_event(completed_event);
                        if let Some(owner) = thread.value.meta.owner.clone() {
                            out.emit_command(Command::ProcessChildCompleted {
                                owner,
                                child: parent.clone(),
                            });
                        }
                    }
                    ThreadStatus::Terminating(reason) => {
                        let mut terminated_thread = thread.value();
                        terminated_thread.status = ThreadStatus::Terminated(reason.clone());
                        terminated_thread.meta.touch(Timestamp::now());
                        let terminated_event = Event::ThreadTerminated {
                            thread: terminated_thread,
                        };
                        out.emit_event(terminated_event);
                        if let Some(owner) = thread.value.meta.owner.clone() {
                            out.emit_command(Command::ProcessChildCompleted {
                                owner,
                                child: parent.clone(),
                            });
                        }
                    }
                    _ => {}
                }
            }
            ObjectKind::Activity => {
                let Some(act) = ctx.storage.get_activity(&parent).await.ok().flatten() else {
                    return;
                };
                match act.value.status {
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
                        let output = act
                            .value
                            .raw_output
                            .clone()
                            .unwrap_or_else(|| act.value.input.clone());
                        let mut activity_value = act.value();
                        activity_value.status = ActivityStatus::Completed;
                        activity_value.output = Some(output.clone());
                        out.emit_event(Event::StateCompleted {
                            activity: activity_value,
                        });
                        out.emit_command(Command::ProcessChildCompleted {
                            owner: act
                                .value
                                .meta
                                .owner
                                .clone()
                                .expect("an owned activity has an owner"),
                            child: parent.clone(),
                        });
                    }
                    ActivityStatus::Terminating(ref reason) => {
                        // The failure drain likewise only fires once every child has terminated.
                        if !act.active_children.is_empty() {
                            return;
                        }
                        let mut activity_value = act.value();
                        activity_value.status = ActivityStatus::Terminated(reason.clone());
                        out.emit_event(Event::StateTerminated {
                            activity: activity_value,
                        });
                        out.emit_command(Command::ProcessChildCompleted {
                            owner: act
                                .value
                                .meta
                                .owner
                                .clone()
                                .expect("an owned activity has an owner"),
                            child: parent.clone(),
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
                        self.dispatch_child_completed(
                            ctx,
                            out,
                            parent.clone(),
                            &act,
                            child.clone(),
                        )
                        .await;
                    }
                    _ => {}
                }
            }
            // A Flow / FlowVersion / Timer / Task parent owns no children in this path:
            // `ProcessChildCompleted` is never issued to one, so anything else is a silent no-op.
            _ => {}
        }
        // Guard the CCES watermark rule: every dispatched command must leave a causally-tied
        // follow-up, even when the parent's reaction produced nothing to project (a `Parallel`
        // whose sibling branches are still in flight, a parent already terminal, a duplicate).
        // The confirmation event carries no state — the no-op changed none — so it is a pure
        // durable receipt the stream (and the watermark) records without inventing projection.
        if out.is_empty() {
            out.emit_event(Event::ProcessChildCompletedHandled {
                owner: parent,
                child,
            });
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
    /// `actx.activity.execution` is the owning execution, whose `root_execution`/`state_path` thread
    /// through so a nested `Map`/`Parallel` can fan out deeper children.
    async fn dispatch_child_completed(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector,
        activity: crate::types::meta::ObjectReference,
        act: &crate::storage::ActivityRecord,
        child: ObjectReference,
    ) {
        // The owning scope: an activity is owned by an Execution (top-level) or a Thread
        // (a Parallel-branch / Map-item child). Build the ctx from that scope's row. A non-scope
        // owner resolves to `None` here (silent), and we return — nothing to replenish into.
        let scope_ref = act
            .value
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        let scope = match crate::storage::load_scope_ref(ctx.storage, &scope_ref).await {
            Ok(Some(s)) => s,
            _ => return, // owning scope gone — nothing to replenish into.
        };
        // Resolve the machine revision this scope is bound to. First use of a revision in a
        // fresh StreamProcessor loads it from storage into the cache.
        let sm = match ctx.machine_for_scope(&scope).await {
            Ok(s) => s,
            Err(_) => return, // definition gone — nothing to decide.
        };
        let state_def = match super::resolve_state_for(
            &sm,
            &scope,
            &crate::handlers::state_name_from_path(act.value.state_path.as_ptr()),
        )
        .await
        {
            Ok(s) => s,
            Err(_) => return, // definition gone — nothing to decide.
        };
        let actx = ActivityCtx {
            // `child_completed` runs after the child's terminal event landed, so the activity row is
            // the latest projection snapshot; convert it to the canonical event-shaped value before
            // handing it to the container state logic.
            activity: act.value(),
            execution_state_path: scope.state_path().cloned(),
            exec_input: scope.input().clone(),
            variables: scope.variables().clone(),
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
