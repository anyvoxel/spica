use std::collections::HashMap;

use async_trait::async_trait;
use spica_asl::State;

use super::dispatch::build_state_handlers;
use super::resolve_state_for;
use super::state_completing_value;
use super::state_handler::StateHandler;
use crate::ActivityStatus;
use crate::handler::{ActivityCtx, Collector, CommandHandler, CtxKind, HandlerContext};
use crate::types::command::Command;
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::meta::ObjectKind;

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
        activity: crate::types::meta::ObjectReference,
    ) {
        // A synchronous state that owns no children drains its owner Execution as soon as its own
        // terminal lands; notify the owner so its own handler walks the drain.
        let owner = ctx
            .storage
            .get_activity(&activity)
            .await
            .ok()
            .flatten()
            .map(|a| {
                a.value
                    .meta
                    .owner
                    .clone()
                    .expect("an owned activity has an owner")
            });
        if let Some(owner) = owner {
            out.emit_command(crate::types::command::Command::ProcessChildCompleted {
                owner,
                child: activity,
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
            activity: crate::types::meta::ObjectReference::nil(),
            output: serde_json::Value::Null,
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::CompleteState {
            activity,
            output: raw_result,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        let act = match ctx.storage.get_activity(activity).await {
            Ok(Some(a)) => a,
            Ok(None) => {
                out.terminate(
                    Some(activity.clone()),
                    crate::types::meta::ObjectReference::nil(),
                    ExecutionError::Runtime(RuntimeError::StateNotFound(format!(
                        "activity {activity}"
                    ))),
                );
                return;
            }
            Err(e) => {
                out.terminate(
                    Some(activity.clone()),
                    crate::types::meta::ObjectReference::nil(),
                    e,
                );
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
                    out.emit_event(crate::types::event::Event::StateTerminated {
                        activity: activity_value,
                    });
                }
                _ => return,
            }
            self.cascade_parent_after_terminal(ctx, out, activity.clone())
                .await;
            return;
        }
        if !act.active_children.is_empty() {
            // Defensive (unreachable in M1): an activity with live children cannot enter success
            // yet; its ed is deferred until drain.
            return;
        }

        let parent = act
            .value
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        // The activity's owner is its *scope* — the top-level `Execution` or a fan-out `Thread`.
        // Resolve which, then drive the complete step against that scope's machine/state/context.
        let scope_ref = match parent.kind {
            ObjectKind::Execution | ObjectKind::Thread => parent.clone(),
            ObjectKind::Activity => {
                out.terminate(
                    Some(activity.clone()),
                    crate::types::meta::ObjectReference::nil(),
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "activity parent must be a scope (Execution or Thread)".into(),
                    )),
                );
                return;
            }
            ObjectKind::Timer => unreachable!(),
            // M1 activities are only ever owned by a scope. A task (M2) is a leaf with no
            // activity parent, so this too is unreachable in M1 (as is any non-node kind).
            _ => unreachable!("activity parent cannot be a Task or non-scope in M1"),
        };
        let scope = match crate::storage::load_scope_ref(ctx.storage, &scope_ref).await {
            Ok(Some(s)) => s,
            Ok(None) => return, // owning scope already gone — nothing to complete into.
            Err(_) => return,
        };
        if !scope.is_running() {
            return; // owner is past accepting a new transition; a late CompleteState is a no-op.
        }

        let actx = ActivityCtx {
            // Rehydrate the same entity-shaped activity value lifecycle events carry, so the
            // complete step observes the canonical domain payload rather than the projection-only row.
            //
            // The command's `output` is the state's raw result; fold it onto the rehydrated activity
            // as `raw_output` so the complete-step events (`state_completing_value`/
            // `state_completed_value` via `state_raw_result`) and `complete_activity`'s
            // `$states.result` all record the command-carried result — the complete step is
            // self-contained and no longer depends on the `TaskCompleted` projection fold to have
            // landed `raw_output` first.
            activity: {
                let mut a = act.value();
                a.raw_output = Some(raw_result.clone());
                a
            },
            execution_state_path: scope.state_path().cloned(),
            exec_input: scope.input().clone(),
            variables: scope.variables().clone(),
            kind: CtxKind::Complete,
        };
        // Resolve the machine revision this scope is bound to. First use of a revision in a
        // fresh StreamProcessor loads it from storage into the cache.
        let sm = fail_or!(
            out,
            Some(activity.clone()),
            scope_ref.clone(),
            ctx.machine_for_scope(&scope).await
        );
        let state_def = fail_or!(
            out,
            Some(activity.clone()),
            scope_ref.clone(),
            resolve_state_for(&sm, &scope, &actx.state_name()).await
        );

        // `StateCompleting` (the ing) is emitted by the framework on entering the success-finish
        // step — before the state's `complete` runs, mirroring how `StateActivating` opens the
        // activate step. The state's `complete` then emits the ed (`StateCompleted`) after it has
        // projected `Assign`/`Output`, and routes via `emit_transition` (`StateTransitioned` +
        // command). This keeps the ing uniform across states regardless of their output handling.
        out.emit_event(crate::types::event::Event::StateCompleting {
            activity: state_completing_value(&actx),
        });

        match self.state_handlers.get(&std::mem::discriminant(state_def)) {
            Some(handler) => handler.complete(ctx.env, out, activity.clone(), &actx, state_def),
            None => out.terminate(
                Some(activity.clone()),
                scope_ref.clone(),
                ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                    "state type not supported in M1".into(),
                )),
            ),
        }
    }
}
