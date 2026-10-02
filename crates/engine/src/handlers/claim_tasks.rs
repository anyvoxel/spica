use std::time::Duration;

use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::ClaimTasks;
use crate::types::event::{Event, TasksClaimed};
use crate::types::reject::RejectionType;

/// Handles `ClaimTasks` — the durable claim behind `TaskApi::poll_tasks` (Zeebe `ActivateJobs`).
///
/// Runs in the StreamProcessor's serialized, lock-holding command arm, so discovery and leasing are
/// decided against the same projection snapshot the fold writes. It discovers up to `max_tasks`
/// claimable tasks of `resource` ([`Task::is_claimable_at`] — pending with backoff lapsed, or a
/// lease that has expired) and leases each to `worker_id`, reporting one batched `TasksClaimed`
/// **durable** response. The awaiting `poll_tasks` resolves its grant from that durable event — the
/// discovery-time `Task` snapshot embedded in it — so the exact set is re-derived from the log,
/// never re-read.
///
/// The returned set is the handler's *discovery-time* grant (direct return, not re-read): a narrow
/// race — a concurrent pull that discovers the same task before this one's `TasksClaimed` is applied —
/// can hand a task to two workers. The `TasksClaimed` applier re-decides claimability per entry with
/// the *entry's* timestamp, so the lease stake is exactly-once regardless, and the *state* never
/// advances twice even if the *work* is at-least-once (handlers must be idempotent, Zeebe's contract).
///
/// A `lease_seconds` the clock cannot represent is refused with `InvalidArgument` rather than answered
/// with an empty set: the payload is malformed, and that is Zeebe's own classification for a bad job
/// timeout.
#[derive(Default)]
pub struct ClaimTasksHandler;

impl ClaimTasksHandler {
    pub(crate) async fn handle(
        &self,
        p: &ClaimTasks,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let ClaimTasks {
            request_id,
            worker_id,
            resource,
            max_tasks,
            lease_seconds,
        } = p;
        // Read *now* once, at the moment the lease window is decided: the persisted `lease_expires_at` is
        // what a restarted engine and every later poll reconstruct claimability from. An overflow is the
        // caller's own `lease_seconds` being unrepresentable, so it is refused rather than answered with
        // an empty set — an empty `TasksClaimed` is indistinguishable from "nothing claimable now",
        // hiding the malformed lease behind a legitimate-looking answer.
        let now = ctx.now();
        let Some(lease_expires_at) = now.checked_add(Duration::from_secs(*lease_seconds)) else {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidArgument,
                format!(
                    "worker {worker_id} asked for a {lease_seconds}s lease on {resource}, which \
                     overflows the clock; claim refused"
                ),
            ));
        };
        // Discover claimable tasks of `resource` under the command arm's storage lock — the same
        // snapshot the fold writes against — so allocation is decided against authoritative state. A
        // read that *faults* is the engine's own failure, not a domain answer: folding it into an empty
        // grant would dress a store hiccup up as "nothing claimable", so it propagates as `Unexpected`
        // — the leader retries it, and the dispatch that exhausts its attempts records the command's
        // single `Reject` (`ProcessingError`). The poll is never answered with a lie.
        let tasks = ctx
            .storage
            .activatable_tasks(resource, now, *max_tasks)
            .await?;
        let mut claimed = Vec::new();
        for t in tasks {
            let mut value = t.value;
            // The claim moment is the very reading the lease window was computed from, so the row's
            // transition stamp and its `lease_expires_at` share one base.
            value.claim(worker_id, lease_expires_at, now);
            claimed.push(value);
        }
        // One batched claim fact for the whole poll — every appended `ClaimTasks` answers its awaiting
        // caller with a durable `TasksClaimed` (all entries share this single causal batch).
        out.append_event(Event::TasksClaimed(TasksClaimed {
            request_id: *request_id,
            tasks: claimed,
        }))
        .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use spica_machinery::{Clock, CountingIdGenerator, IdGenerator, ManualClock};
    use spica_testing::MockReadonlyStorageTxn;

    use super::ClaimTasksHandler;
    use crate::StorageError;
    use crate::eval_env::EvalEnv;
    use crate::handler::{Collector, HandlerContext, ProcessingError};
    use crate::handlers::dispatch::build_state_handlers;
    use crate::handlers::fixtures::at;
    use crate::types::command::ClaimTasks;
    use crate::types::id::{EntryId, RequestId};
    use crate::types::reject::RejectionType;

    /// The handler exercised against a **mock** read-only store carrying **no** expectations: every
    /// read it makes panics, so the case below is itself an assertion about how far the handler got —
    /// the lease window is decided from the command's own payload, before any storage look.
    async fn claim_over(
        store: &MockReadonlyStorageTxn,
        lease_seconds: u64,
    ) -> Result<(), ProcessingError> {
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
        ClaimTasksHandler
            .handle(
                &ClaimTasks {
                    request_id: RequestId::nil(),
                    worker_id: "w1".to_string(),
                    resource: "service-a".to_string(),
                    max_tasks: 10,
                    lease_seconds,
                },
                &mut ctx,
                &mut out,
            )
            .await
    }

    /// A lease the clock cannot represent is the *command's* refusal, recorded rather than answered
    /// with an empty grant: an empty `TasksClaimed` is exactly what a poll that found nothing
    /// claimable looks like, so granting one would hide a malformed `lease_seconds` behind a
    /// legitimate answer. The classification is `InvalidArgument` because the payload itself is at
    /// fault, not the state of the tasks (Zeebe rejects a bad job timeout the same way).
    #[tokio::test]
    async fn an_unrepresentable_lease_is_the_commands_own_refusal() {
        let store = MockReadonlyStorageTxn::new();
        let err = claim_over(&store, u64::MAX)
            .await
            .expect_err("a lease the clock cannot add must be refused");
        let ProcessingError::Rejected(ty, reason) = err else {
            panic!("a malformed lease is the command's failure, not the engine's: {err:?}");
        };
        assert_eq!(ty, RejectionType::InvalidArgument);
        assert!(
            reason.contains(&u64::MAX.to_string()),
            "the refusal names the lease it was given: {reason}"
        );
        assert!(
            reason.contains("service-a"),
            "the refusal names the resource the poll was for: {reason}"
        );
    }

    /// A **faulted discovery read** is the *engine's* failure, exactly as a faulted row read is in the
    /// sibling handlers: it must not be folded into an empty grant, which would be indistinguishable
    /// from a poll that legitimately found nothing claimable. Surfacing it as `Unexpected` is what
    /// gets the command retried and, exhausted, recorded as a `Reject` (`ProcessingError`) — the
    /// command still answers its caller, and answers truthfully.
    #[tokio::test]
    async fn a_faulted_discovery_read_surfaces_as_unexpected() {
        let mut store = MockReadonlyStorageTxn::new();
        store
            .expect_activatable_tasks()
            .times(1)
            .return_once(|_, _, _| {
                Err(StorageError::Backend("injected storage fault".to_string()))
            });
        let err = claim_over(&store, 30)
            .await
            .expect_err("a store that cannot read is not the command's failure");
        assert!(
            matches!(err, ProcessingError::Unexpected(_)),
            "a discovery read fault is the engine's, never an empty grant: {err:?}"
        );
    }
}
