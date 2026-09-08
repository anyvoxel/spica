use std::collections::HashMap;

use async_trait::async_trait;
use spica_asl::State;

use super::dispatch::build_state_handlers;
use super::state_handler::StateHandler;
use super::{resolve_state_from_path, state_activating};
use crate::handler::{ActivityCtx, Collector, CommandHandler, CtxKind, HandlerContext};
use crate::types::command::Command;
use crate::types::error::{ExecutionError, RuntimeError};

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
            execution: crate::types::meta::ObjectReference::nil(),
            owner: crate::types::meta::ObjectReference::nil(),
            state_path: jsonptr::PointerBuf::new(),
            input: Default::default(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector<'_>) {
        let Command::ActivateState {
            execution,
            owner,
            state_path,
            input,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        // This command creates a *new* activity, so its identity is allocated here (not carried by
        // the command, and not by the preceding `StateTransitioned` marker, which names only the
        // target state's path — see its doc). Minting via the collector keeps it deterministic within
        // the same atomic batch.
        let activity_uid: ulid::Ulid = ulid::Ulid::new();
        // Name the activity as a child of its owning execution (finding #3): the generated name's
        // plain base is the execution's name (carried verbatim through every nesting level, so a
        // branch activity still names its root run). The suffix is an independently minted random
        // tail via `PlainName::to_generated` — the canonical generated-child convention (same as the
        // ExecutionTimeout timer), which deliberately decouples a child's name suffix from its own
        // `uid`. Minted once and reused for both the reference and the serialized `meta.name` below,
        // because storage lookups are keyed by the reference's name and must match the stored row.
        let activity_name = execution
            .name
            .base()
            .generated_from_key(out.next_generated_seq().await);
        let activity = crate::types::meta::ObjectReference::new(
            crate::types::meta::ObjectKind::Activity,
            activity_name.clone(),
            activity_uid,
        );
        // Resolve the *scope* this state lives in — an `Execution` (top-level) or a `Thread`
        // (a `Parallel` branch / `Map` item). Both are valid owners of an `ActivateState`; the scope
        // abstraction (see `crate::storage::ScopeRecord`) is the single place that classifies them.
        // The scope is addressed directly by the command's `owner` — the immediate container the new
        // Activity enters — not by `execution`, which is only the flat top-level anchor.
        let scope = match crate::storage::load_scope_ref(ctx.storage, owner).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                out.terminate(
                    Some(activity.clone()),
                    execution.clone(),
                    ExecutionError::Runtime(RuntimeError::StateNotFound(format!(
                        "execution {execution}"
                    ))),
                );
                return;
            }
            Err(e) => {
                out.terminate(Some(activity.clone()), execution.clone(), e);
                return;
            }
        };
        if !scope.is_running() {
            return; // scope not running — a rescheduled activate is a no-op.
        }
        if scope.current_activity().is_some() {
            // Defensive: `current_activity` is now projection-only, but it still records the
            // single in-flight state cursor for this scope. A second `ActivateState` for the same
            // scope would violate the M1 one-active-state invariant even though the cursor
            // itself is derived rather than domain-carried.
        }

        // The scope's `state_path`, where this state lives. A `Parallel` handler extends
        // it to build each child's path.
        let execution_state_path = scope.state_path().cloned();
        // The exact path to the state being entered is carried by the command (self-locating), so the
        // activity's `state_path` is taken verbatim — no reconstruction from the scope above.
        let actx = ActivityCtx {
            // Build the same entity-shaped activity value the forthcoming `StateActivating` event
            // carries, so activation logic reads the canonical domain object even before the storage
            // projection row exists.
            activity: crate::Activity {
                // `execution` is the tree anchor, carried by the command so every Activity addresses
                // its owning run directly (no re-derivation). `meta.owner` below is the immediate
                // scope (the `owner` the command named) — not this field.
                execution: execution.clone(),
                state_path: state_path.clone(),
                status: crate::ActivityStatus::Running,
                raw_input: input.clone(),
                input: None,
                raw_output: None,
                activity_state: None,
                retry_state: None,
                output: None,
                // Birth: `meta.created_at == meta.updated_at == now` (entry moment). The owner is
                // the owning scope named by the command — an activity's parent is always the scope
                // (execution or thread) that contains it. The generated name reuses the same
                // reference-address name minted above (execution-name base + activity uid).
                meta: crate::types::meta::ObjectMeta::builder(
                    crate::types::meta::ObjectKind::Activity,
                    activity_uid,
                )
                .name(activity_name)
                .at(crate::log::Timestamp::now())
                .build()
                .with_owner(owner.clone()),
            },
            execution_state_path,
            exec_input: scope.input().clone(),
            // Fresh entry: no preprocessing has run yet, so raw == processed. `StateActivated` (and
            // the state's own emit) carry/produce the processed view; the raw input stays verbatim.
            variables: scope.variables().clone(),
            kind: CtxKind::Activate,
        };
        // Resolve the machine revision this scope is bound to. First use of a revision in a
        // fresh StreamProcessor loads it from storage into the cache.
        let sm = fail_or!(
            out,
            Some(activity.clone()),
            execution.clone(),
            ctx.machine_for_scope(&scope).await
        );
        let state_def = fail_or!(
            out,
            Some(activity.clone()),
            execution.clone(),
            resolve_state_from_path(&sm, state_path.as_ptr())
        );
        // `StateActivating` (the ing) is emitted unconditionally on entry. The matching ed —
        // `StateActivated` — is not emitted here: it belongs to each `StateHandler::activate`, which
        // publishes it only once the state has finished processing its input (e.g. after Choice has
        // routed its rules), just before the state's own follow-up command.
        out.emit_event(state_activating(&actx, activity.clone()))
            .await;

        match self.state_handlers.get(&std::mem::discriminant(state_def)) {
            Some(handler) => {
                handler
                    .activate(ctx.env, out, activity.clone(), &actx, state_def)
                    .await
            }
            None => out.terminate(
                Some(activity.clone()),
                execution.clone(),
                ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                    "state type not supported in M1".into(),
                )),
            ),
        }
    }
}
