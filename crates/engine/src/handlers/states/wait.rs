use spica_asl::{IntOrExpr, State, WaitState, WaitTimestamp};

use super::super::complete_activity;
use super::super::state_handler::StateHandler;
use crate::command::{Command, TimerPurpose};
use crate::error::ExecutionError;
use crate::eval_env::EvalEnv;
use crate::handler::{ActivityCtx, Collector};
use crate::id::ActivityId;
use crate::log::Timestamp;

pub struct WaitStateHandler;

impl StateHandler for WaitStateHandler {
    fn state(&self) -> State {
        State::Wait(WaitState::default())
    }

    fn activate(
        &self,
        _env: &mut EvalEnv,
        out: &mut Collector,
        activity: ActivityId,
        actx: &ActivityCtx,
        state: &State,
    ) {
        let State::Wait(s) = state else {
            unreachable!(
                "activate dispatch guarantees the state handler receives its own variant; got {state:?}"
            );
        };
        activate_wait(out, activity, actx, s);
    }

    /// Resumed by `CompleteState` after the Wait's timer (`WaitResume`) has fired. Projects
    /// `Assign`/`Output` against the stored input and routes to `Next`/`End`.
    fn complete(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector,
        activity: ActivityId,
        actx: &ActivityCtx,
        state: &State,
    ) {
        let State::Wait(s) = state else {
            unreachable!(
                "complete dispatch guarantees the state handler receives its own variant; got {state:?}"
            );
        };
        complete_activity(
            env,
            out,
            activity,
            actx,
            s.assign.as_ref(),
            s.output.as_ref(),
            s.next.as_deref(),
            s.end,
        );
    }
}

fn activate_wait(out: &mut Collector, activity: ActivityId, actx: &ActivityCtx, state: &WaitState) {
    // Compute the absolute deadline the Wait holds until. `Seconds` is relative — normalized to an
    // absolute moment at activation; `Timestamp` is already absolute (parsed from RFC3339). Exactly
    // one must be present; this is validated against the ASL definition.
    let deadline = match (&state.seconds, &state.timestamp) {
        (Some(IntOrExpr::Int(n)), None) => {
            if *n < 0 {
                out.terminate(
                    Some(activity),
                    actx.execution,
                    ExecutionError::InvalidDefinition("Wait Seconds must be non-negative".into()),
                );
                return;
            }
            let deadline = Timestamp::now().checked_add(std::time::Duration::from_secs(*n as u64));
            let Some(deadline) = deadline else {
                out.terminate(
                    Some(activity),
                    actx.execution,
                    ExecutionError::InvalidDefinition(
                        "Wait Seconds overflows the absolute deadline".into(),
                    ),
                );
                return;
            };
            deadline
        }
        (Some(IntOrExpr::Expr(_)), None) | (Some(IntOrExpr::Expr(_)), Some(_)) => {
            // TODO(M2): `Seconds` as a JSONata expression is outside M1 (only literal integers are
            // supported) — a later milestone adds expression evaluation here, mirroring `Timestamp`
            // below. Explicitly rejected so a definition relying on it fails loudly at activation
            // rather than arming a wrong timer.
            out.terminate(
                Some(activity),
                actx.execution,
                ExecutionError::InvalidDefinition(
                    "JSONata Wait Seconds are not supported in M1".into(),
                ),
            );
            return;
        }
        (Some(IntOrExpr::Int(_)), Some(_)) => {
            out.terminate(
                Some(activity),
                actx.execution,
                ExecutionError::InvalidDefinition(
                    "Wait state cannot specify both Seconds and Timestamp".into(),
                ),
            );
            return;
        }
        (None, Some(WaitTimestamp::Literal(s))) => match Timestamp::from_rfc3339(s) {
            Some(t) => t,
            None => {
                out.terminate(
                    Some(activity),
                    actx.execution,
                    ExecutionError::InvalidDefinition(format!(
                        "Wait Timestamp is not a valid RFC3339 timestamp: {s}"
                    )),
                );
                return;
            }
        },
        (None, Some(WaitTimestamp::Expr(_))) => {
            // TODO(M2): `Timestamp` as a JSONata expression is outside M1 (only literal RFC3339
            // strings are supported) — a later milestone adds expression evaluation, mirroring the
            // `Seconds` case above. Rejected loudly at activation rather than arming a wrong timer.
            out.terminate(
                Some(activity),
                actx.execution,
                ExecutionError::InvalidDefinition(
                    "JSONata Wait Timestamp is not supported in M1".into(),
                ),
            );
            return;
        }
        (None, None) => {
            out.terminate(
                Some(activity),
                actx.execution,
                ExecutionError::InvalidDefinition(
                    "Wait state must specify Seconds or Timestamp".into(),
                ),
            );
            return;
        }
    };
    let timer = out.next_timer();
    // The activation work (computing the absolute deadline from Seconds/Timestamp) is done: emit the
    // activation-complete ed, then arm the resume timer as the transition's side effect.
    out.emit_event(crate::event::Event::StateActivated { activity });
    out.emit_command(Command::ActivateTimer {
        parent: crate::id::NodeId::Activity(activity),
        timer,
        purpose: TimerPurpose::WaitResume,
        deadline,
    });
}
