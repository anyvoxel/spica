use crate::RejectionType;
use crate::StatePath;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::{ActivateState, Command, CreateExecution};
use crate::types::error::{ExecutionError, RuntimeError};
use crate::types::event::{Event, ExecutionCreated};
use crate::types::execution::ExecutionKind;
use crate::types::meta::{NoOwner, ObjectMeta, ObjectRef};
use crate::types::thread::ThreadKind;

/// Handles `CreateExecution`: records the execution (via `ExecutionCreated`) and starts it. Also
/// arms the state-machine `TimeoutSeconds` timer if configured. Immediately enters the start state
/// via `ActivateState`.
#[derive(Default)]
pub struct CreateExecutionHandler;

impl CreateExecutionHandler {
    pub(crate) async fn handle(
        &self,
        p: &CreateExecution,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        // `request_id` is the awaiting caller's correlation key — echoed onto `ExecutionCreated` so
        // the StreamProcessor's request-ack correlator wakes the awaiting `start` operation with the fact
        // that this execution was durably created (see `Event::ExecutionCreated`). The handler
        // otherwise ignores it.
        let CreateExecution {
            request_id,
            name,
            flow_version,
            input,
        } = p;
        // `CreateExecution` creates only new names: the name is the execution's storage primary key
        // (per-scope unique), so a second run under an already-live name must be **rejected** — the
        // authoritative serialized point the StreamProcessor reaches in strict log order — never
        // aliased onto a fresh execution that would silently clobber the first row. Mirror
        // `create_flow.rs`: the `Engine` boundary pre-check may have been racy (two concurrent
        // same-name starts both passed its read), so this in-order check is the enforcement, and the
        // loser surfaces `ALREADY_EXISTS` to its awaiter (and the log) via a `Reject`, not a silent
        // drop. A name-only probe (nil uid) suffices — storage keys executions by name, so the uid is
        // irrelevant to the read.
        let probe = ObjectRef::<ExecutionKind>::new(name.clone(), ulid::Ulid::nil());
        if let Ok(Some(_)) = ctx.storage.get_execution(&probe).await {
            return Err(ProcessingError::Rejected(
                RejectionType::AlreadyExists,
                format!("create_execution: execution {name} already exists"),
            ));
        }

        // Mint the execution's durable identity here: a fresh `uid` plus the caller-supplied `name`.
        // The uid is NOT carried in the command — replay stays deterministic because the produced
        // `ExecutionCreated` lands in the same atomic batch as this command (committed ⇒ conclusive,
        // never re-dispatched), so a re-dispatch mints a fresh consistent uid.
        let uid: ulid::Ulid = ctx.mint();
        let id = ObjectRef::<ExecutionKind>::new(name.clone(), uid);

        // Resolve the machine this execution binds to. This is the first use of the version in a fresh
        // StreamProcessor — it loads the definition (keyed by the version's object reference) from
        // Storage into the cache. A version that is missing (definition GC'd) or whose stored definition
        // no longer parses cannot produce a run, and re-dispatching would not change that, so the
        // command is refused rather than fanned out into a termination: there is no execution row yet to
        // terminate, and a termination carries no `request_id`, so it could never reach the `start`
        // caller awaiting this command.
        let sm = ctx.machine(flow_version).await.map_err(|e| {
            ProcessingError::Rejected(
                RejectionType::InvalidArgument,
                format!(
                    "create_execution: flow version {flow_version} cannot be resolved for execution \
                     {name}: {e}"
                ),
            )
        })?;
        // Normalize the machine's relative `TimeoutSeconds` into an absolute deadline here, before the
        // birth event, so the run's own `deadline` and the timer that enforces it are
        // written from one computation and cannot disagree. A `TimeoutSeconds` the clock cannot add is a
        // malformed definition, so it is refused *before* the run exists: accepting it, birthing the
        // execution, and then failing it immediately would leave a dead row standing for a command the
        // engine had already decided not to apply — and would answer the caller with a creation that
        // succeeded rather than a refusal.
        let timeout = sm.timeout_seconds.filter(|secs| *secs > 0);
        let deadline = timeout.and_then(|secs| {
            ctx.now()
                .checked_add(std::time::Duration::from_secs(secs as u64))
        });
        if timeout.is_some() && deadline.is_none() {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidArgument,
                format!(
                    "create_execution: execution {name} cannot start: TimeoutSeconds overflows the \
                     absolute deadline"
                ),
            ));
        }

        // Build the birth event once and emit it; an injected `Hook` observer wakes the awaiting `start`
        // caller from this same event (the `request_id` it echoes), so no separate ack echo is needed.
        let created_event = Event::ExecutionCreated(ExecutionCreated {
            request_id: *request_id,
            execution: crate::Execution {
                // The version this run executes against — every nested Thread inherits it.
                flow_version: flow_version.clone(),
                // A top-level run is its own root: it carries no `root_execution` and no branch
                // `state_path` (it resolves states against the machine's top-level `States`). Fan-out
                // children are `Thread`s instead, created via `SpawnThread`.
                status: crate::ExecutionStatus::Running,
                deadline,
                input: input.clone(),
                output: None,
                meta: ObjectMeta::builder(uid)
                    .name(name.clone())
                    .at(ctx.now())
                    // A top-level run is its own root: its owner slot is `NoOwner` by type.
                    .with_owner(NoOwner::new()),
            },
        });
        // The birth `ExecutionCreated` echoes the awaiting `start` caller's request id; the `AckHook`
        // observer wakes it once the engine reports this event applied (see `Engine::start_for_revision`,
        // which returns the execution id at that point, leaving the terminal settle to
        // `wait_for_execution`).
        out.append_event(created_event).await;

        if let Some(deadline) = deadline {
            // The run's own deadline timer is generated **here** (inline): mint the
            // timer's durable uid, and derive the timer's name as `{execution.name}-{8-char-suffix}`
            // (k8s generateName style `PlainName::to_generated`, which mints its own suffix uid
            // internally) — deterministic and replay-safe, since this `TimerActivated` lands in the
            // same atomic batch as the `ExecutionCreated`. The name is decoupled from the timer's own
            // `uid`, and must be carried forward by later timer events, so TimerActivated children
            // stay resolvable (see `TimerTriggered`/`TimerCancelled`, which preserve the row's meta
            // rather than re-deriving the name).
            let uid: ulid::Ulid = ctx.mint();
            // A generated child's base is the execution's own name, which is user-supplied (`Plain`)
            // by construction; unwrap it to derive the timer's `{name}-{8-char}` handle. The
            // `Generated` arm is unreachable for a CreateExecution name but kept explicit so a future
            // misuse fails loudly instead of silently mis-naming the timer.
            let base = match id.name().as_plain() {
                Some(p) => p,
                None => {
                    // TODO：这里貌似应该是不可能发生的事情
                    out.fail_execution(
                        &id,
                        ExecutionError::Runtime(RuntimeError::InvalidDefinition(
                            "cannot derive a child name: execution name is not a plain user name"
                                .into(),
                        )),
                    );
                    return Ok(());
                }
            };
            // The suffix is this partition's local generated-name counter (see `Storage::next_generated_seq`),
            // read via the working overlay so a sibling minted earlier in the same batch is visible; the
            // `TimerActivated` applier bumps the counter past it in the same fold.
            let name = base.generated_from_key(out.next_generated_seq().await);
            let timer = crate::Timer {
                meta: crate::types::meta::ObjectMeta::builder(uid)
                    .name(name)
                    .at(ctx.now())
                    // An execution timeout is armed by the run itself — the `Execution` variant of the
                    // timer slot, never an activity's.
                    .with_owner(crate::types::meta::TimerOwner::Execution(id.clone())),
                execution: id.clone(),
                status: crate::TimerStatus::Active,
                deadline,
            };
            out.append_event(Event::TimerActivated { timer }).await;
        }

        // Derive the execution's single root Thread — the top-level owner of every state, standing in
        // for a whole top-level run the way a Process has one main Thread. It is owned by the
        // Execution (so the `ThreadCreated` applier never aggregates its placeholder index 0 into a
        // container fan-out map) and names the machine's own top-level `States` table, so it resolves
        // through the same walk a fan-out thread does. Its completion/termination bridges back to
        // the Execution (see `complete_thread`/`terminate_thread`), so the externally-addressed root
        // still settles through `wait_for_execution`.
        let root_uid: ulid::Ulid = ctx.mint();
        let root_name = id
            .name()
            .base()
            .generated_from_key(out.next_generated_seq().await);
        let root_thread = ObjectRef::<ThreadKind>::new(root_name.clone(), root_uid);
        // The root thread runs the machine's top-level `States` table and enters the machine's own
        // `StartAt` — the pointer and the entry point are the same pair every fan-out thread carries.
        let root_states = StatePath::root();
        let start_at = sm.start_at.clone();
        out.append_event(Event::ThreadCreated {
            // TODO：应该提供一个 Thread::new 函数？
            thread: crate::Thread {
                meta: ObjectMeta::builder(root_uid)
                    .name(root_name)
                    .at(ctx.now())
                    // A root thread's owner is the run it stands in for — the `Execution` variant, the
                    // one a fan-out thread (owned by its container activity) never carries.
                    .with_owner(crate::types::meta::ThreadOwner::Execution(id.clone())),
                execution: id.clone(),
                state_path: root_states.clone(),
                start_at: start_at.clone(),
                index: 0,
                status: crate::ThreadStatus::Running,
                input: input.clone(),
                output: None,
            },
        })
        .await;

        // Enter the start state: the root thread's `state_path` (the top-level `States` table)
        // extended by its own `start_at`, exactly how `SpawnThread` derives a branch's entry path.
        out.append_command(Command::ActivateState(ActivateState {
            // The top-level state is owned by the derived root Thread, not the Execution.
            execution: id.clone(),
            owner: root_thread,
            state_path: root_states.state(&start_at),
            input: input.clone(),
        }));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use serde_json::json;
    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_testing::MockReadonlyStorageTxn;

    use super::CreateExecutionHandler;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, ProcessingError};
    use crate::handlers::dispatch::build_state_handlers;
    use crate::handlers::fixtures::{at, obj_name, object_ref};
    use crate::types::command::CreateExecution;
    use crate::types::id::{EntryId, RequestId};
    use crate::types::meta::{ObjectMeta, ObjectRef};
    use crate::{
        EntryPayload, FlowKind, FlowVersion, FlowVersionKind, RejectionType, StorageError,
    };

    /// The version the run binds to. Its name is load-bearing: `HandlerContext::machine` loads the
    /// definition by exactly this reference, so the seeded row has to answer to it.
    fn flow_version_ref() -> ObjectRef<FlowVersionKind> {
        object_ref("lifecycle_flow-1", 2)
    }

    /// A definition whose `TimeoutSeconds` is the largest `i64` the ASL model admits: normalizing it
    /// into an absolute deadline needs a millisecond count past `u64::MAX`, so the machine parses
    /// cleanly while the deadline cannot be computed. That gap is the only input the guard exists for.
    fn overflowing_definition() -> String {
        json!({
            "StartAt": "P",
            "TimeoutSeconds": i64::MAX,
            "States": { "P": { "Type": "Pass", "End": true } }
        })
        .to_string()
    }

    /// The handler exercised against a **mock** read-only store: the two reads `CreateExecution` makes
    /// — the execution-name probe and the version's definition — are the only ones scripted, so any
    /// further read panics. Both the outcome and the entries are returned, because for this guard the
    /// *absence* of a birth event is as much the point as the refusal itself.
    async fn create_over(
        store: &MockReadonlyStorageTxn,
    ) -> (Result<(), ProcessingError>, Vec<EntryPayload>) {
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(at()));
        let ids: Arc<dyn IdGenerator> = Arc::new(CountingIdGenerator::new());
        let mut out = Collector::new(EntryId::new(1), None, clock.clone(), ids.clone());
        let mut env = EvalEnv::new();
        let mut definitions = HashMap::new();
        let state_handlers = build_state_handlers();
        let mut ctx = HandlerContext {
            env: &mut env,
            storage: store,
            clock,
            ids,
            definitions: &mut definitions,
            state_handlers: &state_handlers,
        };
        let result = CreateExecutionHandler
            .handle(
                &CreateExecution {
                    request_id: RequestId::nil(),
                    name: obj_name("lifecycle_execution"),
                    flow_version: flow_version_ref(),
                    input: json!({ "n": 1 }),
                },
                &mut ctx,
                &mut out,
            )
            .await;
        (
            result,
            out.into_entries().into_iter().map(|e| e.payload).collect(),
        )
    }

    /// A store whose name probe finds nothing and whose version publishes `definition`.
    fn store_over(definition: &str) -> MockReadonlyStorageTxn {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_execution()
            .times(1)
            .return_const(Ok::<_, StorageError>(None));
        let version = FlowVersion {
            meta: ObjectMeta::builder(flow_version_ref().uid())
                .name(flow_version_ref().name().clone())
                .at(at())
                .with_owner(object_ref::<FlowKind>("lifecycle_flow", 1)),
            version: 1,
            definition: definition.to_string(),
            checksum: 0,
        };
        store
            .expect_get_flow_version()
            .times(1)
            .return_once(move |_| Ok(Some(version)));
        store
    }

    /// A store whose name probe finds nothing and whose version row does not exist — the "definition
    /// GC'd out from under a command that names it" case.
    fn store_without_version() -> MockReadonlyStorageTxn {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_get_execution()
            .times(1)
            .return_const(Ok::<_, StorageError>(None));
        store
            .expect_get_flow_version()
            .times(1)
            .return_const(Ok::<_, StorageError>(None));
        store
    }

    /// A definition that cannot be resolved is refused, and refused as the *command's* failure: the run
    /// is never born, and no termination is fanned out — a termination for a row that does not exist
    /// could not answer this command's `request_id` either, so the awaiting `start` caller would be left
    /// with nothing to wake it.
    #[tokio::test]
    async fn an_unresolvable_definition_is_refused_before_the_run_is_born() {
        let store = store_without_version();
        let (result, entries) = create_over(&store).await;
        let Err(err) = result else {
            panic!("a missing flow version must be refused, not accepted");
        };
        let ProcessingError::Rejected(ty, reason) = err else {
            panic!(
                "an unresolvable definition is the command's failure, not the engine's: {err:?}"
            );
        };
        assert_eq!(ty, RejectionType::InvalidArgument);
        assert!(
            reason.contains(&flow_version_ref().to_string()),
            "the refusal names the version it could not resolve: {reason}"
        );
        assert!(
            entries.is_empty(),
            "the refusal fans out no termination for a row that does not exist: {entries:?}"
        );
    }

    /// A `TimeoutSeconds` the clock cannot add is refused as a malformed definition, and refused
    /// **before the run exists**: no `ExecutionCreated` reaches the log, so a caller is never handed a
    /// creation that "succeeded" and then died. The window between the two reads is closed — the
    /// version is read once and the refusal follows from its definition alone.
    #[tokio::test]
    async fn an_unrepresentable_timeout_is_refused_before_the_run_is_born() {
        let store = store_over(&overflowing_definition());
        let (result, entries) = create_over(&store).await;
        let Err(err) = result else {
            panic!("an unaddable TimeoutSeconds must be refused, not accepted");
        };
        let ProcessingError::Rejected(ty, reason) = err else {
            panic!("a malformed definition is the command's failure, not the engine's: {err:?}");
        };
        assert_eq!(ty, RejectionType::InvalidArgument);
        assert!(
            reason.contains("lifecycle_execution"),
            "the refusal names the execution that cannot start: {reason}"
        );
        assert!(
            entries.is_empty(),
            "the refusal leaves no birth event behind it: {entries:?}"
        );
    }
}
