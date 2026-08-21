use std::collections::HashMap;

use async_trait::async_trait;
use spica_asl::State;

use super::dispatch::build_state_handlers;
use super::resolve_state_for;
use super::state_completing_value;
use super::state_handler::StateHandler;
use crate::ActivityStatus;
use crate::command::Command;
use crate::error::ExecutionError;
use crate::handler::{ActivityCtx, Collector, CommandHandler, CtxKind, HandlerContext};
use crate::id::NodeId;

/// Handles `Command::CompleteState`: the success finish of the running activity bound to it.
/// Dispatches to the matching [`StateHandler::complete`], which emits the projection
/// (`StateCompleting` + `StateCompleted`) and the transition. If the activity owns children (M2
/// states only), the ed is deferred until they finish — only M1 synchronous/timed states reach here
/// childless (Wait fires its own timer before `CompleteState`).
pub struct CompleteStateHandler {
    state_handlers: HashMap<std::mem::Discriminant<State>, Box<dyn StateHandler>>,
}

impl CompleteStateHandler {
    pub fn new() -> Self {
        Self {
            state_handlers: build_state_handlers(),
        }
    }

    async fn cascade_parent_after_terminal(
        &self,
        ctx: &HandlerContext<'_>,
        out: &mut Collector,
        activity: crate::id::ActivityId,
    ) {
        // A synchronous state that owns no children drains its parent Execution as soon as its own
        // terminal lands; notify the parent so its own handler walks the drain.
        let parent = ctx
            .storage
            .get_activity(activity)
            .await
            .ok()
            .flatten()
            .map(|a| a.value.parent);
        if let Some(parent) = parent {
            out.emit_command(crate::command::Command::ProcessChildCompleted {
                parent,
                child: NodeId::Activity(activity),
            });
        }
    }
}

impl Default for CompleteStateHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for CompleteStateHandler {
    fn command(&self) -> Command {
        Command::CompleteState {
            activity: crate::id::ActivityId::nil(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::CompleteState { activity } = cmd else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        let act = match ctx.storage.get_activity(*activity).await {
            Ok(Some(a)) => a,
            Ok(None) => {
                out.terminate(
                    Some(*activity),
                    crate::id::ExecutionId::nil(),
                    ExecutionError::StateNotFound(format!("activity {activity}")),
                );
                return;
            }
            Err(e) => {
                out.terminate(Some(*activity), crate::id::ExecutionId::nil(), e);
                return;
            }
        };

        if act.value.status != ActivityStatus::Running {
            match act.value.status {
                // Race fix: a cancel already won on this activity. The drain that would have been
                // emitted by the cancel side may have been missed because the ordering interleaved
                // (e.g. timer-fired + cancel together). Emit the deferred termination ed so the
                // parent finishes, reusing the reason embedded in the terminating status itself.
                ActivityStatus::Terminating(ref reason) => {
                    // Re-emit the terminal lifecycle event using the canonical activity payload shape,
                    // preserving every previously-folded domain field while only flipping the status
                    // from `Terminating(reason)` to `Terminated(reason)`.
                    let mut activity_value = act.value();
                    activity_value.status = ActivityStatus::Terminated(reason.clone());
                    out.emit_event(crate::event::Event::StateTerminated {
                        activity: activity_value,
                    });
                }
                _ => return,
            }
            self.cascade_parent_after_terminal(ctx, out, *activity)
                .await;
            return;
        }
        if !act.active_children.is_empty() {
            // Defensive (unreachable in M1): an activity with live children cannot enter success
            // yet; its ed is deferred until drain.
            return;
        }

        let execution = match &act.value.parent {
            NodeId::Execution(e) => *e,
            NodeId::Activity(_) => {
                out.terminate(
                    Some(*activity),
                    crate::id::ExecutionId::nil(),
                    ExecutionError::InvalidDefinition(
                        "activity parent must be an Execution in M1".into(),
                    ),
                );
                return;
            }
            NodeId::Timer(_) => unreachable!(),
            // M1 activities are only ever owned by an Execution. A task (M2) is a leaf with no
            // activity parent, so this too is unreachable in M1.
            NodeId::Task(_) => unreachable!("activity parent cannot be a Task in M1"),
        };
        let exec = match super::load_execution(ctx.storage, execution).await {
            Ok(Some(e)) => e,
            Ok(None) => return, // owning execution already gone — nothing to complete into.
            Err(_) => return,
        };
        if exec.status.is_terminal() || exec.status.is_terminating() {
            return; // owner is past acceptance; a late CompleteState is a no-op.
        }

        let actx = ActivityCtx {
            // Rehydrate the same entity-shaped activity value lifecycle events carry, so the
            // complete step observes the canonical domain payload rather than the projection-only row.
            activity: act.value(),
            execution_state_path: exec.state_path.clone(),
            exec_input: exec.input.clone(),
            variables: exec.variables.clone(),
            kind: CtxKind::Complete,
        };
        // Resolve the machine revision this execution is bound to. First use of a revision in a
        // fresh StreamProcessor loads it from storage into the cache.
        let sm = fail_or!(
            out,
            Some(*activity),
            execution,
            ctx.machine(exec.flow_version_id).await
        );
        let state_def = fail_or!(
            out,
            Some(*activity),
            execution,
            resolve_state_for(ctx.storage, &sm, execution, &actx.state_name()).await
        );

        // `StateCompleting` (the ing) is emitted by the framework on entering the success-finish
        // step — before the state's `complete` runs, mirroring how `StateActivating` opens the
        // activate step. The state's `complete` then emits the ed (`StateCompleted`) after it has
        // projected `Assign`/`Output`, and routes via `emit_transition` (`StateTransitioned` +
        // command). This keeps the ing uniform across states regardless of their output handling.
        out.emit_event(crate::event::Event::StateCompleting {
            activity: state_completing_value(&actx),
        });

        match self.state_handlers.get(&std::mem::discriminant(state_def)) {
            Some(handler) => handler.complete(ctx.env, out, *activity, &actx, state_def),
            None => out.terminate(
                Some(*activity),
                execution,
                ExecutionError::InvalidDefinition("state type not supported in M1".into()),
            ),
        }
    }
}
