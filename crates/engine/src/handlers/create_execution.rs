use async_trait::async_trait;

use crate::command::{Command, TimerPurpose};
use crate::error::ExecutionError;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::id::NodeId;
use crate::log::Timestamp;

/// Handles `CreateExecution`: records the execution (via `ExecutionCreated`) and starts it. Also
/// arms the state-machine `TimeoutSeconds` timer if configured. Immediately enters the start state
/// via `ActivateState`.
#[derive(Default)]
pub struct CreateExecutionHandler;

#[async_trait]
impl CommandHandler for CreateExecutionHandler {
    fn command(&self) -> Command {
        Command::CreateExecution {
            request_id: crate::id::RequestId::nil(),
            id: crate::id::ExecutionId::nil(),
            flow_version_id: crate::id::FlowVersionId::nil(),
            input: Default::default(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        // `request_id` is the awaiting caller's correlation key — echoed onto `ExecutionCreated` so
        // the StreamProcessor's request-ack correlator wakes the awaiting `start` operation with the fact
        // that this execution was durably created (see `Event::ExecutionCreated`). The handler
        // otherwise ignores it.
        let Command::CreateExecution {
            request_id,
            id,
            flow_version_id,
            input,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };
        // Resolve the machine this execution binds to. This is the first use of the version in a
        // fresh StreamProcessor — it loads the definition (keyed by its never-reused id) from Storage into
        // the cache. If the version is missing (definition GC'd), the execution cannot run and
        // fails before any state is entered.
        let sm = fail_or!(out, None, *id, ctx.machine(*flow_version_id).await);
        out.emit_event(Event::ExecutionCreated {
            request_id: *request_id,
            execution: crate::ExecutionValue {
                id: *id,
                // The version this run executes against — every nested child execution inherits it.
                flow_version_id: *flow_version_id,
                // The top-level run is its own root: `root_execution = self`, and it has no parent and
                // no branch `state_path` (it resolves states against the machine's top-level
                // `states`). A child Parallel branch would instead set these via `SpawnBranch`.
                root_execution: *id,
                parent: None,
                state_path: None,
                status: crate::ExecutionStatus::Running,
                input: input.clone(),
                output: None,
            },
        });
        // Declare the *birth* acknowledgement for the awaiting `start` caller: the StreamProcessor wakes
        // it (delivering the applied event) once this `ExecutionCreated` lands on Storage — see
        // `Engine::start_for_revision`, which returns the execution id at that point, leaving the
        // terminal settle to `wait_for_execution`. Correlation is by event variant + echoed
        // `request_id` (see `AckRouter`) — never full-value equality — so the expected event here
        // need only carry that id, not the full execution value above.
        out.ack_request(
            *request_id,
            Event::ExecutionCreated {
                request_id: *request_id,
                execution: crate::ExecutionValue {
                    id: crate::id::ExecutionId::nil(),
                    flow_version_id: crate::id::FlowVersionId::nil(),
                    root_execution: crate::id::ExecutionId::nil(),
                    parent: None,
                    state_path: None,
                    status: crate::ExecutionStatus::Running,
                    input: Default::default(),
                    output: None,
                },
            },
        );

        if let Some(secs) = sm.timeout_seconds
            && secs > 0
        {
            let timer = out.next_timer();
            // Normalize the relative TimeoutSeconds into an absolute deadline at activation, so the
            // persisted TimerActivated fact carries the wall-clock moment the execution must finish by.
            let deadline =
                Timestamp::now().checked_add(std::time::Duration::from_secs(secs as u64));
            let Some(deadline) = deadline else {
                out.fail_execution(
                    *id,
                    ExecutionError::InvalidDefinition(
                        "TimeoutSeconds overflows the absolute deadline".into(),
                    ),
                );
                return;
            };
            out.emit_command(Command::ActivateTimer {
                parent: NodeId::Execution(*id),
                timer,
                purpose: TimerPurpose::ExecutionTimeout,
                deadline,
            });
        }

        let start = sm.start_at.clone();
        let activity = out.next_activity();
        out.emit_command(Command::ActivateState {
            execution: *id,
            activity,
            state: start,
            input: input.clone(),
        });
    }
}
