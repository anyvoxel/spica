use async_trait::async_trait;
use spica_asl::State;

use crate::command::{Command, TerminationReason, TimerPurpose};
use crate::error::ExecutionError;
use crate::event::Event;
use crate::handler::{ActivityCtx, Collector, CommandHandler, CtxKind, HandlerContext};
use crate::id::NodeId;
use crate::{ActivityStatus, RetryState, TaskStatus};

/// Defaults for a `Retry` when the `Retrier` omits the optional fields, per the ASL spec.
const DEFAULT_RETRY_INTERVAL_SECONDS: i64 = 1;
const DEFAULT_RETRY_MAX_ATTEMPTS: i64 = 3;
const DEFAULT_RETRY_BACKOFF_RATE: f64 = 2.0;

/// Handles `FailTask`: a claimed task was reported **failed** (Zeebe `FailJob`), or the engine's own
/// deadline backstop (`TaskTimeout`) marked it failed.
///
/// Idempotent and lease-guarded like [`CompleteTaskHandler`](self::super::CompleteTaskHandler): a
/// no-op unless the task is not yet terminal. A *worker* report must hold the lease right now
/// (`Running` to the reporting `worker_id`) — a foreign or duplicate report drops; an
/// *engine-authoritative* failure (empty `worker_id`, e.g. the `TimeoutSeconds` backstop) may settle
/// any non-terminal task. This is the source of Zeebe's at-least-once contract: only the current
/// lease holder (or the engine deadline) advances the state once.
///
/// On success it emits `TaskFailed` and routes the failure through the owning state's
/// `Retry`/`Catch`/terminate policy — the same decision a settled failure always took, now owned
/// exclusively by this handler (the success-only `CompleteTaskHandler` no longer carries it).
#[derive(Default)]
pub struct FailTaskHandler;

#[async_trait]
impl CommandHandler for FailTaskHandler {
    fn command(&self) -> Command {
        Command::FailTask {
            task: crate::id::TaskId::nil(),
            worker_id: String::new(),
            error: ExecutionError::InvalidDefinition(String::new()),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::FailTask {
            task,
            worker_id,
            error,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };

        let act = match ctx.storage.get_task(*task).await {
            Ok(Some(t)) => t,
            Ok(None) | Err(_) => return, // task never activated; nothing to do.
        };
        if act.status.is_terminal() {
            return; // already settled — a duplicate fail is a no-op.
        }
        let reported_by_worker = !worker_id.is_empty();
        if reported_by_worker {
            // A worker reporting a failure must own the lease right now (Zeebe job-owner check).
            // Anything else — a task not yet claimed, or one re-leased after expiry — is a foreign
            // report and drops.
            if !act.status.is_running() || act.worker_id.as_deref() != Some(worker_id.as_str()) {
                tracing::warn!(
                    task = %act.value.id,
                    reported = %worker_id,
                    leased = ?act.worker_id,
                    "worker tried to fail a task it does not lease; report rejected"
                );
                return;
            }
        }

        let activity_id = match act.parent {
            NodeId::Activity(a) => a,
            _ => return, // a task without an activity owner is an internal fault.
        };

        // Emit the failed task entity (lease cleared, status terminal). The paired `TaskLease` timer
        // is a child of the activity and is swept with it on settlement — same mechanism the
        // `TimeoutSeconds` timer relies on, so no explicit cancel here.
        let mut task_value = act.value();
        task_value.status = TaskStatus::Failed;
        task_value.worker_id = None;
        task_value.lease_until = None;
        out.emit_event(Event::TaskFailed {
            task: task_value,
            error: error.clone(),
        });
        // Sweep the activity's task timers (the `TaskLease` armed on assign, and any `TaskTimeout`)
        // so a settled task leaves no live child behind: a non-terminal Retry re-arms a fresh
        // invocation (ReleaseTaskLease would, at best, re-queue a now-terminal task), and a terminal
        // fail is then free to terminate/drain the activity (which would sweep them anyway — this
        // just makes the settle self-contained and avoids a stale child blocking a later complete).
        super::cancel_activity_timers(ctx, out, activity_id).await;
        // Route the failure through the state's error-handling policy: `Retry` (re-arm on a
        // backoff), then `Catch` (bind errorOutput + route to the catcher's Next), then terminate.
        // This keeps the policy local to the state definition and defers the decision to the next
        // dispatch round where a retry timer / catcher runs.
        self.route_failure(ctx, out, activity_id, error).await;
    }
}

impl FailTaskHandler {
    /// Route a `Task` failure through the owning state's `Retry`/`Catch` policy, terminating only
    /// when neither matches. Order per ASL: the first `Retry` whose `ErrorEquals` matches and whose
    /// attempt budget is not exhausted wins (re-arm on the computed backoff); otherwise the first
    /// `Catch` whose `ErrorEquals` matches wins (bind errorOutput + route to its `Next`); otherwise
    /// the failure terminates the state and its execution.
    async fn route_failure(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector,
        activity_id: crate::id::ActivityId,
        error: &ExecutionError,
    ) {
        let activity = match ctx.storage.get_activity(activity_id).await {
            Ok(Some(a)) => a,
            _ => return,
        };
        // Determine the owning execution. For a top-level activity this is `NodeId::Execution`.
        // For a Parallel-branch child execution the owning *activity* is itself the branch's child
        // execution's activity, but its `parent` is still the child `ExecutionId` that owns it —
        // so resolving the state definition by the owning execution honors the branch's
        // `state_path` (retry/catch then consult the right per-branch definition).
        let execution = match activity.value.parent {
            NodeId::Execution(e) => e,
            _ => return, // internal fault: a completing activity must be owned by an execution.
        };
        // The owning execution binds to a machine revision; resolve it (cached by the Processor)
        // before consulting the state definition.
        let exec = match ctx.storage.get_execution(execution).await {
            Ok(Some(e)) => e,
            _ => return, // owning execution gone — nothing to consult.
        };
        let sm = match ctx.machine(exec.flow_version_id).await {
            Ok(s) => s,
            Err(_) => {
                // Definition no longer resolvable — nothing left to consult; terminate.
                self.terminate_failure(ctx, out, activity_id, error).await;
                return;
            }
        };
        let state_def = match super::resolve_state_for(
            ctx.storage,
            &sm,
            execution,
            &crate::handlers::state_name_from_path(activity.value.state_path.as_ptr()),
        )
        .await
        {
            Ok(s) => s,
            Err(_) => {
                // Definition no longer resolvable — nothing left to consult; terminate.
                self.terminate_failure(ctx, out, activity_id, error).await;
                return;
            }
        };
        let State::Task(task_state) = state_def else {
            // A non-Task activity settling a failure can't consult a Task retry/catch; terminate.
            self.terminate_failure(ctx, out, activity_id, error).await;
            return;
        };

        // ── Retry ──────────────────────────────────────────────────────────────────────────────
        // Scan the state's `Retry` array for the first entry matching the error name. Each retrier's
        // own attempt budget is independent: `retrier_attempts[index]` counts how many times that
        // specific retrier already fired, while `retry_count` stays the activity-wide total exposed on
        // `$states.context.State.RetryCount`.
        if let Some((retrier_index, retry)) = task_state.retry.as_deref().and_then(|rs| {
            rs.iter()
                .enumerate()
                .find(|(_, r)| error_matches(r.error_equals.as_slice(), error))
        }) {
            let retrier_attempts =
                retrier_attempt_count(&activity.value.retry_state, retrier_index);
            let max_attempts = retry.max_attempts.unwrap_or(DEFAULT_RETRY_MAX_ATTEMPTS);
            if (retrier_attempts as i64) < max_attempts {
                // Backoff: interval × backoff_rate^attempt, capped at MaxDelaySeconds. The exponent
                // uses the matched retrier's own already-made attempt count (not the activity-wide
                // total), so two retriers do not pollute each other's backoff ladders.
                let delay_secs = compute_backoff(retry, retrier_attempts);
                let next_retrier_attempt = retrier_attempts + 1;
                let next_retry_count = activity.value.retry_state.retry_count + 1;
                // Stamp both the retry-bookkeeping event and the paired retry-delay timer from the
                // same wall-clock instant so the persisted "last retry at" metadata and the derived
                // deadline stay causally aligned.
                let scheduled_at = crate::log::Timestamp::now();
                out.emit_event(Event::RetryScheduled {
                    activity: activity_id,
                    retrier_index,
                    retrier_attempt: next_retrier_attempt,
                    retry_count: next_retry_count,
                    scheduled_at,
                });
                let timer = out.next_timer();
                let deadline = scheduled_at
                    .checked_add(std::time::Duration::from_secs(delay_secs))
                    .unwrap_or(scheduled_at);
                out.emit_command(Command::ActivateTimer {
                    parent: NodeId::Activity(activity_id),
                    timer,
                    purpose: TimerPurpose::TaskRetryDelay,
                    deadline,
                });
                return;
            }
            // Attempt budget exhausted — fall through to `Catch` (a retry that hit `MaxAttempts`
            // no longer applies).
        }

        // ── Catch ──────────────────────────────────────────────────────────────────────────────
        if let Some(catcher) = task_state.catch.as_deref().and_then(|cs| {
            cs.iter()
                .find(|c| error_matches(c.error_equals.as_slice(), error))
        }) {
            let exec = match activity.value.parent {
                NodeId::Execution(e) => e,
                _ => return,
            };
            let Some(ex) = ctx.storage.get_execution(exec).await.ok().flatten() else {
                return; // owning execution gone — nothing to catch into.
            };
            let actx = ActivityCtx {
                // Catch handling reuses the same entity-shaped activity value lifecycle events carry,
                // so the success-style completion path sees the canonical domain payload.
                activity: activity.value(),
                execution_state_path: ex.state_path.clone(),
                exec_input: ex.input.clone(),
                variables: ex.variables.clone(),
                kind: CtxKind::Complete,
            };
            // Bind `$states.errorOutput` (the error-output object) for the catcher's `Assign`/
            // `Output`, then complete the activity as a successful finish routed to the catcher's
            // `Next` — the catcher's `Assign`/`Output` project against the error output.
            let error_output = error.error_output().unwrap_or(serde_json::Value::Null);
            super::complete_activity(
                ctx.env,
                out,
                activity_id,
                &actx,
                catcher.assign.as_ref(),
                catcher.output.as_ref(),
                Some(&catcher.next),
                None,
                activity.value.retry_state.retry_count,
                Some(&error_output),
            );
            return;
        }

        // ── Neither matched → terminate ────────────────────────────────────────────────────────
        self.terminate_failure(ctx, out, activity_id, error).await;
    }

    /// Terminate the failing activity and its execution, mirroring `states/fail.rs::complete_fail`:
    /// emit the activity's terminal events and throw `TerminateExecution` from the same causal
    /// chain. The activity is `Terminating`→`Terminated` before the sweep, so `TerminateExecution`'s
    /// sweep sees it already drained and emits `ExecutionTerminated` immediately.
    async fn terminate_failure(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector,
        activity_id: crate::id::ActivityId,
        error: &ExecutionError,
    ) {
        let reason = TerminationReason::Failed {
            error: error.clone(),
        };
        let activity = match ctx.storage.get_activity(activity_id).await {
            Ok(Some(a)) => a,
            Ok(None) | Err(_) => return,
        };
        let mut terminating_activity = activity.value();
        terminating_activity.status = ActivityStatus::Terminating(reason.clone());
        out.emit_event(Event::StateTerminating {
            activity: terminating_activity,
        });
        let mut terminated_activity = activity.value();
        terminated_activity.status = ActivityStatus::Terminated(reason.clone());
        out.emit_event(Event::StateTerminated {
            activity: terminated_activity,
        });
        let execution_id = match activity.value.parent {
            NodeId::Execution(e) => e,
            _ => return, // internal fault: activity not owned by an execution.
        };
        out.emit_command(Command::TerminateExecution {
            id: execution_id,
            reason,
        });
    }
}

/// Whether `error` matches a `Retry`/`Catch` `ErrorEquals` array.
///
/// `States.ALL` is a wildcard matching any error name and must stand alone. `States.TaskFailed`
/// matches any error name except `States.Timeout`. Otherwise the error's name must be a member of
/// the array. Errors whose name the engine doesn't produce (e.g. a lambda-specific name) are
/// matched only if the array lists them explicitly — matching is by the ASL reserved
/// [`ExecutionError::error_name`].
fn error_matches(error_equals: &[String], error: &ExecutionError) -> bool {
    if error_equals.iter().any(|s| s == "States.ALL") {
        return true;
    }
    let name = error.error_name();
    if error_equals.iter().any(|s| s == "States.TaskFailed") {
        return name != "States.Timeout";
    }
    error_equals.iter().any(|s| s == name)
}

/// The already-consumed attempt count for `retrier_index`, defaulting to 0 when the retrier has not
/// fired yet.
fn retrier_attempt_count(retry_state: &RetryState, retrier_index: usize) -> u32 {
    retry_state
        .retrier_attempts
        .get(retrier_index)
        .map(|attempt| attempt.attempt_count)
        .unwrap_or(0)
}

/// Compute the backoff delay in seconds for the next retry of a single retrier, given that retrier's
/// own already-made attempt count (0 for its first retry), per `Retrier`:
/// `IntervalSeconds * BackoffRate^attempt`, capped at `MaxDelaySeconds`.
fn compute_backoff(retry: &spica_asl::Retrier, retrier_attempts: u32) -> u64 {
    let interval = retry
        .interval_seconds
        .unwrap_or(DEFAULT_RETRY_INTERVAL_SECONDS)
        .max(0) as f64;
    let backoff = retry
        .backoff_rate
        .as_ref()
        .and_then(|r| r.as_f64())
        .unwrap_or(DEFAULT_RETRY_BACKOFF_RATE);
    let attempts = retrier_attempts as f64;
    let raw = interval * backoff.powf(attempts);
    let capped = match retry.max_delay_seconds {
        Some(max) if max > 0 => raw.min(max as f64),
        _ => raw,
    };
    capped.ceil().max(1.0) as u64
}

#[cfg(test)]
mod tests {
    use super::{compute_backoff, error_matches, retrier_attempt_count};
    use crate::error::ExecutionError;
    use crate::{RetrierAttemptState, RetryState};

    #[test]
    fn backoff_first_attempt() {
        let r = spica_asl::Retrier {
            error_equals: vec!["States.ALL".into()],
            interval_seconds: Some(1),
            max_attempts: Some(3),
            backoff_rate: None,
            max_delay_seconds: None,
            jitter_strategy: None,
        };
        // Per-retrier attempt 0 → 1s (backoff_rate default 2.0 ^ 0 = 1)
        assert_eq!(compute_backoff(&r, 0), 1);
        // Per-retrier attempt 1 → 2s
        assert_eq!(compute_backoff(&r, 1), 2);
        // Per-retrier attempt 2 → 4s
        assert_eq!(compute_backoff(&r, 2), 4);
    }

    #[test]
    fn backoff_capped() {
        let r = spica_asl::Retrier {
            error_equals: vec!["States.ALL".into()],
            interval_seconds: Some(2),
            max_attempts: Some(3),
            backoff_rate: None,
            max_delay_seconds: Some(3),
            jitter_strategy: None,
        };
        // Per-retrier attempt 2 → 2*4=8, capped at 3.
        assert_eq!(compute_backoff(&r, 2), 3);
    }

    #[test]
    fn retrier_attempts_default_to_zero() {
        let retry_state = RetryState {
            retry_count: 4,
            retrier_attempts: vec![RetrierAttemptState {
                attempt_count: 2,
                last_retry_at: None,
            }],
        };
        assert_eq!(retrier_attempt_count(&retry_state, 0), 2);
        assert_eq!(retrier_attempt_count(&retry_state, 1), 0);
    }

    #[test]
    fn error_matches_wildcards() {
        let err = ExecutionError::TimedOut {
            message: "t".into(),
        };
        assert!(error_matches(&["States.ALL".into()], &err));
        assert!(error_matches(
            &["States.TaskFailed".into()],
            &ExecutionError::Cancelled {
                message: "c".into()
            }
        ));
        // TaskFailed does NOT match Timeout.
        assert!(!error_matches(&["States.TaskFailed".into()], &err));
        // explicit name
        assert!(error_matches(&["States.Timeout".into()], &err));
        assert!(!error_matches(&["Some.Other".into()], &err));
    }
}
