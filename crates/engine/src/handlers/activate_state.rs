use std::collections::HashMap;

use async_trait::async_trait;
use spica_asl::State;

use super::dispatch::build_state_handlers;
use super::state_handler::StateHandler;
use super::{resolve_state_for, state_activating};
use crate::command::Command;
use crate::error::ExecutionError;
use crate::handler::{ActivityCtx, Collector, CommandHandler, CtxKind, HandlerContext};

/// Handles `Command::ActivateState`: enters one state. Emits `StateActivating`, then dispatches to
/// the matching [`StateHandler::activate`]. The state's own handler decides whether it finishes at
/// activate (Pass/Fail/Choice/Succeed) or leaves after arming a side effect (Wait) — and, as part
/// of its activation work, emits the `StateActivated` ed once it has processed the input.
pub struct ActivateStateHandler {
    state_handlers: HashMap<std::mem::Discriminant<State>, Box<dyn StateHandler>>,
}

impl ActivateStateHandler {
    pub fn new() -> Self {
        Self {
            state_handlers: build_state_handlers(),
        }
    }
}

impl Default for ActivateStateHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for ActivateStateHandler {
    fn command(&self) -> Command {
        Command::ActivateState {
            execution: crate::id::ExecutionId::nil(),
            activity: crate::id::ActivityId::nil(),
            state: String::new(),
            input: Default::default(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::ActivateState {
            activity,
            execution,
            state,
            input,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        let exec = match super::load_execution(ctx.storage, *execution).await {
            Ok(Some(e)) => e,
            Ok(None) => {
                out.terminate(
                    Some(*activity),
                    *execution,
                    ExecutionError::StateNotFound(format!("execution {execution}")),
                );
                return;
            }
            Err(e) => {
                out.terminate(Some(*activity), *execution, e);
                return;
            }
        };
        if !exec.status.is_running() {
            return; // execution not running — a rescheduled activate is a no-op.
        }
        if exec.current_activity.is_some() {
            // Defensive: a single in-flight activity per execution is the M1 invariant; a second
            // ActivateState for the same execution would reuse the dispatch table idempotently.
        }

        // The owning execution's `state_path`, where this state lives. A `Parallel` handler extends
        // it to build each child's path.
        let execution_state_path = exec.state_path.clone();
        // The full `state_path` of the state being entered = the owning execution's `state_path`
        // (its enclosing `states` table) extended by this state's name; `None` for a top-level
        // execution means the path starts at the machine's top-level `states` table.
        let mut state_path = match &execution_state_path {
            Some(pointer) => pointer.clone(),
            None => jsonptr::PointerBuf::new(),
        };
        if execution_state_path.is_none() {
            state_path.push_back("states");
        }
        state_path.push_back(state);
        let actx = ActivityCtx {
            execution: *execution,
            // The top-level run id is this activity's flat anchor (for a top-level activity it is
            // the owning execution itself; inside a branch it is the branch's root_execution) —
            // carried so a `Parallel` fan-out can thread the true root to its children.
            root_execution: exec.root_execution,
            execution_state_path,
            exec_input: exec.input.clone(),
            // Fresh entry: no preprocessing has run yet, so raw == processed. `StateActivated` (and
            // the state's own emit) carry/produce the processed view; the raw input stays verbatim.
            raw_input: input.clone(),
            input: input.clone(),
            raw_output: None,
            scope: exec.scope.clone(),
            state_path,
            // A fresh `ActivateState` is always a first (or post-`End`-loop) entry — the activity
            // row created by `StateActivating` starts at 0 unless a retry has already re-invoked it.
            retry_count: 0,
            kind: CtxKind::Activate,
        };
        let state_def = fail_or!(
            out,
            Some(*activity),
            *execution,
            resolve_state_for(ctx.storage, ctx.sm, *execution, &actx.state_name()).await
        );
        // `StateActivating` (the ing) is emitted unconditionally on entry. The matching ed —
        // `StateActivated` — is not emitted here: it belongs to each `StateHandler::activate`, which
        // publishes it only once the state has finished processing its input (e.g. after Choice has
        // routed its rules), just before the state's own follow-up command.
        out.emit_event(state_activating(&actx, *activity));

        match self.state_handlers.get(&std::mem::discriminant(state_def)) {
            Some(handler) => handler.activate(ctx.env, out, *activity, &actx, state_def),
            None => out.terminate(
                Some(*activity),
                *execution,
                ExecutionError::InvalidDefinition("state type not supported in M1".into()),
            ),
        }
    }
}
