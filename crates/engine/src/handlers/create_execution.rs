use async_trait::async_trait;

use crate::RejectionType;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::log::Timestamp;
use crate::types::command::{Command, TimerPurpose};
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::Event;
use crate::types::meta::{ObjectKind, ObjectMeta, ObjectName, ObjectReference};

/// Handles `CreateExecution`: records the execution (via `ExecutionCreated`) and starts it. Also
/// arms the state-machine `TimeoutSeconds` timer if configured. Immediately enters the start state
/// via `ActivateState`.
#[derive(Default)]
pub struct CreateExecutionHandler;

#[async_trait]
impl CommandHandler for CreateExecutionHandler {
    fn command(&self) -> Command {
        Command::CreateExecution {
            request_id: crate::types::id::RequestId::nil(),
            name: ObjectName::plain("default").expect("static placeholder name is valid"),
            flow_version: crate::types::meta::ObjectReference::nil(),
            input: Default::default(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector<'_>) {
        // `request_id` is the awaiting caller's correlation key — echoed onto `ExecutionCreated` so
        // the StreamProcessor's request-ack correlator wakes the awaiting `start` operation with the fact
        // that this execution was durably created (see `Event::ExecutionCreated`). The handler
        // otherwise ignores it.
        let Command::CreateExecution {
            request_id,
            name,
            flow_version,
            input,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        // `CreateExecution` creates only new names: the name is the execution's storage primary key
        // (per-scope unique), so a second run under an already-live name must be **rejected** — the
        // authoritative serialized point the StreamProcessor reaches in strict log order — never
        // aliased onto a fresh execution that would silently clobber the first row. Mirror
        // `create_flow.rs`: the `Engine` boundary pre-check may have been racy (two concurrent
        // same-name starts both passed its read), so this in-order check is the enforcement, and the
        // loser surfaces `ALREADY_EXISTS` to its awaiter (and the log) via a `Reject`, not a silent
        // drop. A name-only probe (nil uid) suffices — storage keys executions by name, so the uid is
        // irrelevant to the read.
        let probe = ObjectReference::new(ObjectKind::Execution, name.clone(), ulid::Ulid::nil());
        if let Ok(Some(_)) = ctx.storage.get_execution(&probe).await {
            tracing::warn!(
                name = %name.as_str(),
                "create_execution: rejecting — execution already exists"
            );
            out.reject(
                *request_id,
                RejectionType::AlreadyExists,
                format!("create_execution: execution {name} already exists"),
            );
            return;
        }

        // Mint the execution's durable identity here: a fresh `uid` plus the caller-supplied `name`.
        // The uid is NOT carried in the command — replay stays deterministic because the produced
        // `ExecutionCreated` lands in the same atomic batch as this command (committed ⇒ conclusive,
        // never re-dispatched), so a re-dispatch mints a fresh consistent uid.
        let uid: ulid::Ulid = out.next_execution().into();
        let id = ObjectReference::new(ObjectKind::Execution, name.clone(), uid);
        // Resolve the machine this execution binds to. This is the first use of the version in a
        // fresh StreamProcessor — it loads the definition (keyed by the version's object reference)
        // from Storage into the cache. If the version is missing (definition GC'd), the execution
        // cannot run and fails before any state is entered.
        let sm = fail_or!(out, None, id.clone(), ctx.machine(flow_version).await);
        // Build the birth event once and emit it; an injected `Hook` observer wakes the awaiting `start`
        // caller from this same event (the `request_id` it echoes), so no separate ack echo is needed.
        let created_event = Event::ExecutionCreated {
            request_id: *request_id,
            execution: crate::Execution {
                // The version this run executes against — every nested Thread inherits it.
                flow_version: flow_version.clone(),
                // A top-level run is its own root: it carries no `root_execution` and no branch
                // `state_path` (it resolves states against the machine's top-level `states`). Fan-out
                // children are `Thread`s instead, created via `SpawnThread`.
                status: crate::ExecutionStatus::Running,
                input: input.clone(),
                output: None,
                meta: ObjectMeta::builder(ObjectKind::Execution, uid)
                    .name(name.clone())
                    .at(Timestamp::now())
                    .build(),
            },
        };
        // The birth `ExecutionCreated` echoes the awaiting `start` caller's request id; the `AckHook`
        // observer wakes it once the engine reports this event applied (see `Engine::start_for_revision`,
        // which returns the execution id at that point, leaving the terminal settle to
        // `wait_for_execution`).
        out.emit_event(created_event).await;

        if let Some(secs) = sm.timeout_seconds
            && secs > 0
        {
            // Normalize the relative TimeoutSeconds into an absolute deadline at activation, so the
            // persisted TimerActivated fact carries the wall-clock moment the execution must finish by.
            let deadline =
                Timestamp::now().checked_add(std::time::Duration::from_secs(secs as u64));
            let Some(deadline) = deadline else {
                out.fail_execution(
                    id.clone(),
                    ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                        "TimeoutSeconds overflows the absolute deadline".into(),
                    )),
                );
                return;
            };
            // The ExecutionTimeout timer is generated **here** (inline): mint the
            // timer's durable uid, and derive the timer's name as `{execution.name}-{8-char-suffix}`
            // (k8s generateName style `PlainName::to_generated`, which mints its own suffix uid
            // internally) — deterministic and replay-safe, since this `TimerActivated` lands in the
            // same atomic batch as the `ExecutionCreated`. The name is decoupled from the timer's own
            // `uid`, and must be carried forward by later timer events, so TimerActivated children
            // stay resolvable (see `TimerTriggered`/`TimerCancelled`, which preserve the row's meta
            // rather than re-deriving the name).
            let uid: ulid::Ulid = out.next_timer().into();
            // A generated child's base is the execution's own name, which is user-supplied (`Plain`)
            // by construction; unwrap it to derive the timer's `{name}-{8-char}` handle. The
            // `Generated` arm is unreachable for a CreateExecution name but kept explicit so a future
            // misuse fails loudly instead of silently mis-naming the timer.
            let base = match id.name.as_plain() {
                Some(p) => p,
                None => {
                    out.fail_execution(
                        id.clone(),
                        ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                            "cannot derive a child name: execution name is not a plain user name"
                                .into(),
                        )),
                    );
                    return;
                }
            };
            // The suffix is this partition's local generated-name counter (see `Storage::next_generated_seq`),
            // read via the working overlay so a sibling minted earlier in the same batch is visible; the
            // `TimerActivated` applier bumps the counter past it in the same fold.
            let name = base.generated_from_key(out.next_generated_seq().await);
            let timer = crate::Timer {
                meta: crate::types::meta::ObjectMeta::builder(ObjectKind::Timer, uid)
                    .name(name)
                    .at(Timestamp::now())
                    .build()
                    .with_owner(id.clone()),
                execution: id.clone(),
                purpose: TimerPurpose::ExecutionTimeout,
                status: crate::TimerStatus::Active,
                deadline,
            };
            out.emit_event(Event::TimerActivated { timer }).await;
        }

        let start = sm.start_at.clone();
        // The start state's path under the machine's top-level `states` table.
        let mut start_path = jsonptr::PointerBuf::new();
        start_path.push_back("states");
        start_path.push_back(start.as_str());
        out.emit_command(Command::ActivateState {
            // Top-level: the execution is its own anchor AND its own owner (no surrounding thread).
            execution: id.clone(),
            owner: id.clone(),
            state_path: start_path,
            input: input.clone(),
        });
    }
}
