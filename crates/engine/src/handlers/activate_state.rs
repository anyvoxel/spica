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
            // Defensive: `current_activity` is now projection-only, but it still records the
            // single in-flight state cursor for this execution. A second `ActivateState` for the
            // same execution would violate the M1 one-active-state invariant even though the cursor
            // itself is derived rather than domain-carried.
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
            // Build the same entity-shaped activity value the forthcoming `StateActivating` event
            // carries, so activation logic reads the canonical domain object even before the storage
            // projection row exists.
            activity: crate::ActivityValue {
                id: *activity,
                execution: *execution,
                root_execution: exec.root_execution,
                parent: crate::id::NodeId::Execution(*execution),
                state_path: state_path.clone(),
                status: crate::ActivityStatus::Running,
                raw_input: input.clone(),
                input: input.clone(),
                raw_output: None,
                activity_state: crate::ActivityState::Leaf,
                retry_state: crate::RetryState::default(),
                output: None,
            },
            execution_state_path,
            exec_input: exec.input.clone(),
            // Fresh entry: no preprocessing has run yet, so raw == processed. `StateActivated` (and
            // the state's own emit) carry/produce the processed view; the raw input stays verbatim.
            variables: exec.variables.clone(),
            kind: CtxKind::Activate,
        };
        // Resolve the machine revision this execution is bound to. First use of a revision in a
        // fresh StreamProcessor loads it from storage into the cache.
        let sm = fail_or!(
            out,
            Some(*activity),
            *execution,
            ctx.machine(exec.flow_version_id).await
        );
        let state_def = fail_or!(
            out,
            Some(*activity),
            *execution,
            resolve_state_for(ctx.storage, &sm, *execution, &actx.state_name()).await
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
