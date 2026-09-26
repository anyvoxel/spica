use spica_asl::State;

use crate::handler::{Collector, HandlerContext};
use crate::types::command::{FailTask, TerminationReason};
use crate::types::error::ExecutionError;
use crate::types::event::{Event, TaskFailed};
use crate::types::meta::ObjectKind;
use crate::{ActivityStatus, RetrierAttemptState, TaskStatus};

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
/// The task **decides its own retry** from its frozen `retry_plan` ([[task-retry-model]] stage 2):
/// on a matching retrier with budget remaining, the *same* task entity re-queues to `Pending` gated
/// by `next_available_at` (the backoff) — no timer, no fresh invocation — and the handler returns.
/// Only when no retrier matches or the budget is exhausted does the task fail terminally and hand
/// off to the owning state's `Catch`/terminate policy, which stays on the activity.
#[derive(Default)]
pub struct FailTaskHandler;

impl FailTaskHandler {
    pub(crate) async fn handle(
        &self,
        p: &FailTask,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) {
        let FailTask {
            task,
            worker_id,
            error,
        } = p;

        let act = match ctx.storage.get_task(task).await {
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
                    task = %act.value.reference(),
                    reported = %worker_id,
                    leased = ?act.worker_id,
                    "worker tried to fail a task it does not lease; report rejected"
                );
                return;
            }
        }

        let activity_id = act
            .meta
            .owner
            .clone()
            .expect("a task always has an activity owner");
        if activity_id.kind != ObjectKind::Activity {
            return; // a task without an activity owner is an internal fault.
        }

        // Build the failing task entity with the lease cleared; the retry decision below mutates it.
        let mut task_value = act.value();
        task_value.worker_id = None;
        task_value.lease_expires_at = None;
        // Stamp the (re-queue / fail) decision moment; `created_at` is already carried on
        // `task_value`. Both the retry and the terminal paths emit below from this same value.
        task_value.meta.with_update_at(ctx.now());

        // ── Retry self-decision (on the task, from its frozen `retry_plan`) ─────────────────────
        // Scan the frozen plan for the first entry matching the error name. Each retrier's attempt
        // budget is independent (`retrier_attempts[index]`), and the backoff uses only that
        // retrier's own history. On a match with budget remaining, the SAME task entity re-queues to
        // `Pending` gated by `next_available_at` — claimable again no earlier than that instant — so
        // the backoff needs no timer and the worker re-invocation is a normal re-claim.
        if let Some((retrier_index, policy)) = task_value
            .retry_plan
            .iter()
            .enumerate()
            .find(|(_, p)| error_matches(&p.error_equals, error))
        {
            let retrier_attempts = task_value
                .retry_state
                .retrier_attempts
                .get(retrier_index)
                .map(|a| a.attempt_count)
                .unwrap_or(0);
            if retrier_attempts < policy.max_attempts as u32 {
                // Backoff: policy interval × backoff_rate^attempt, capped at MaxDelaySeconds — the
                // exponent uses this retrier's own made attempts, so two retriers never pollute each
                // other's ladders.
                let next_attempt = retrier_attempts + 1;
                let delay = policy.backoff_for_attempt(retrier_attempts);
                let now = ctx.now();
                let next_available_at = now
                    .checked_add(std::time::Duration::from_secs(delay))
                    .unwrap_or(now);
                // Stamp per-retrier counter + total onto the task entity, and the claimability gate.
                if task_value.retry_state.retrier_attempts.len() <= retrier_index {
                    task_value
                        .retry_state
                        .retrier_attempts
                        .resize(retrier_index + 1, RetrierAttemptState::default());
                }
                task_value.retry_state.retrier_attempts[retrier_index] = RetrierAttemptState {
                    attempt_count: next_attempt,
                    last_retry_at: Some(now),
                };
                task_value.retry_state.attempts += 1;
                task_value.status = TaskStatus::Pending;
                task_value.retry_state.next_available_at = Some(next_available_at);
                out.append_event(Event::TaskFailed(TaskFailed {
                    task: task_value,
                    error: error.clone(),
                }))
                .await;
                // Sweep the failed attempt's `TaskTimeout` child (a settled task leaves no live child
                // behind). No retry timer is armed — `next_available_at` is the gate, and the
                // re-claimed attempt re-arms what it needs (TODO(M2): `TaskTimeout`).
                super::cancel_activity_timers(ctx, out, activity_id.clone()).await;
                return;
            }
            // Attempt budget exhausted — fall through to `Catch` (a retry that hit `MaxAttempts`
            // no longer applies).
        }

        // ── Terminal failure → route to Catch / Terminate ──────────────────────────────────────
        task_value.status = TaskStatus::Failed;
        task_value.retry_state.next_available_at = None;
        out.append_event(Event::TaskFailed(TaskFailed {
            task: task_value,
            error: error.clone(),
        }))
        .await;
        // Sweep the activity's task timers (any `TaskTimeout`) so a settled task leaves no live child
        // behind; a terminal fail is then free to
        // terminate/drain the activity (which would sweep them anyway — this just makes the settle
        // self-contained and avoids a stale child blocking a later complete).
        super::cancel_activity_timers(ctx, out, activity_id.clone()).await;
        // Route the terminal failure through the state's error-handling policy: `Catch` (bind
        // errorOutput + route to the catcher's Next), then terminate. Retry was already decided
        // above (on the task); this activity-level remainder applies only to an exhausted failure.
        self.route_failure(ctx, out, activity_id, error).await;
    }
}

impl FailTaskHandler {
    /// Route a **terminal** `Task` failure (retry budget exhausted) through the owning state's
    /// `Catch` policy, terminating when none matches. Order per ASL: the first `Catch` whose
    /// `ErrorEquals` matches wins (bind errorOutput + route to its `Next`); otherwise the failure
    /// terminates the state and its execution. Retry is **not** consulted here — the reused task
    /// already decided it on the failure path above.
    async fn route_failure(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity_id: crate::types::meta::ObjectReference,
        error: &ExecutionError,
    ) {
        let activity = match ctx.storage.get_activity(&activity_id).await {
            Ok(Some(a)) => a,
            _ => return,
        };
        // An activity's owner is always a `Thread` — the derived root Thread for a top-level run, or a
        // fan-out branch — so read it directly rather than through the kind-dispatching scope reader.
        // Resolving it is what lets the state definition be consulted against the right `state_path`:
        // for a branch, retry/catch then see the per-branch definition at the pointer location.
        let owner = activity
            .value
            .meta
            .owner
            .clone()
            .expect("a completing activity is owned by a scope");
        let Some(thread) = ctx.storage.get_thread(&owner).await.ok().flatten() else {
            return; // owning scope gone — nothing to consult.
        };
        // TODO(step 2): drop this adapter — and the `ScopeRecord` wrapper it needs — once
        // `machine_for_scope` / `resolve_state_for` take a `Thread` instead of a scope.
        let scope = crate::storage::ScopeRecord::Thread(thread);
        // The owning scope binds to a machine revision; resolve it (cached by the Processor)
        // before consulting the state definition.
        let sm = match ctx.machine_for_scope(&scope).await {
            Ok(s) => s,
            Err(_) => {
                // Definition no longer resolvable — nothing left to consult; terminate.
                self.terminate_failure(ctx, out, activity_id, error).await;
                return;
            }
        };
        let state_def =
            match super::resolve_state_for(&sm, &scope, &activity.value.state_path.state_name())
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
            // A non-Task activity settling a failure can't consult a Task catch; terminate.
            self.terminate_failure(ctx, out, activity_id, error).await;
            return;
        };

        // ── Catch ──────────────────────────────────────────────────────────────────────────────
        if let Some(catcher) = task_state.catch.as_deref().and_then(|cs| {
            cs.iter()
                .find(|c| error_matches(c.error_equals.as_slice(), error))
        }) {
            // Catch handling reuses the same entity-shaped activity value lifecycle events carry,
            // so the success-style completion path sees the canonical domain payload.
            let activity_value = activity.value();
            // The owning scope is the catch context. Its record was resolved above and nothing has
            // written to it since — the only intervening steps are definition reads.
            let variables = scope.variables().clone();
            // Bind `$states.errorOutput` (the error-output object) for the catcher's `Assign`/
            // `Output`, then complete the activity as a successful finish routed to the catcher's
            // `Next` — the catcher's `Assign`/`Output` project against the error output.
            let error_output = error.error_output().unwrap_or(serde_json::Value::Null);
            super::complete_activity(
                ctx.env,
                out,
                activity_id,
                &activity_value,
                &variables,
                catcher.assign.as_ref(),
                catcher.output.as_ref(),
                Some(&catcher.next),
                None,
                activity.value.retry_count(),
                Some(&error_output),
            )
            .await;
            return;
        }

        // ── Nothing caught → terminate ─────────────────────────────────────────────────────────
        self.terminate_failure(ctx, out, activity_id, error).await;
    }

    /// Terminate the failing activity and its execution, mirroring `states/fail.rs::complete_fail`:
    /// emit the activity's terminal events and throw `TerminateExecution` from the same causal
    /// chain. The activity is `Terminating`→`Terminated` before the sweep, so `TerminateExecution`'s
    /// sweep sees it already drained and emits `ExecutionTerminated` immediately.
    async fn terminate_failure(
        &self,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
        activity_id: crate::types::meta::ObjectReference,
        error: &ExecutionError,
    ) {
        let reason = TerminationReason::Failed {
            error: error.clone(),
        };
        let activity = match ctx.storage.get_activity(&activity_id).await {
            Ok(Some(a)) => a,
            Ok(None) | Err(_) => return,
        };
        // Both records are this row *after* their write, so they carry the moment of the write rather
        // than the stored stamp: a task that times out a minute into its deadline terminated *now*,
        // and re-reading the activity would date its termination at its activation (its `updated_at`
        // would then stand still across the whole timeout it just spent).
        let now = ctx.now();
        let mut terminating_activity = activity.value();
        terminating_activity.meta.updated_at = now;
        terminating_activity.status = ActivityStatus::Terminating(reason.clone());
        out.append_event(Event::StateTerminating {
            activity: terminating_activity,
        })
        .await;
        let mut terminated_activity = activity.value();
        terminated_activity.meta.updated_at = now;
        terminated_activity.status = ActivityStatus::Terminated(reason.clone());
        out.append_event(Event::StateTerminated {
            activity: terminated_activity,
        })
        .await;
        // Route the terminal failure at the owning scope: an activity's owner is always a `Thread`, the
        // root Thread for a top-level run (which relays onward to `TerminateExecution`) or a fan-out
        // branch (reachable only via `TerminateThread`).
        let owner = activity
            .value
            .meta
            .owner
            .clone()
            .expect("a completing activity is owned by a scope");
        super::emit_scope_termination(out, &owner, reason);
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

#[cfg(test)]
mod tests {
    use super::error_matches;
    use crate::types::error::{ExecutionError, RuntimeError};

    #[test]
    fn error_matches_wildcards() {
        let err = ExecutionError::Runtime(RuntimeError::TimedOut {
            message: "t".into(),
        });
        assert!(error_matches(&["States.ALL".into()], &err));
        assert!(error_matches(
            &["States.TaskFailed".into()],
            &ExecutionError::Runtime(RuntimeError::Cancelled {
                message: "c".into()
            })
        ));
        // TaskFailed does NOT match Timeout.
        assert!(!error_matches(&["States.TaskFailed".into()], &err));
        // explicit name
        assert!(error_matches(&["States.Timeout".into()], &err));
        assert!(!error_matches(&["Some.Other".into()], &err));
    }
}
