use crate::RejectionType;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::{ActivateState, Command, SpawnThread};
use crate::types::event::Event;

/// Handles `SpawnThread`: fans out one branch of a `Parallel` state — or one item of a `Map` state —
/// into a new child **Thread** (a scoped sub-run, distinct from a top-level `Execution`).
///
/// The child is projected by a `ThreadCreated{parent, execution, state_path}` (which wires it
/// into its owner's `active_children` via the applier), then entered at its `StartAt` state via
/// `ActivateState`. From there it runs as a self-contained sub-state-machine — its internal hops
/// happen entirely within the thread, and its terminal hop runs the inline child-settled reaction
/// back to `parent`, so the owning `Parallel`/`Map` activity only learns the child settled
/// when the whole branch/item finishes. This is the flat (non-recursive) fan-out: the thread is
/// stored as one row with a `state_path` into the shared machine, never copying branch state.
///
/// For a `Map`, `index` carries the **item index** and `start_at`/`input` are the item
/// processor's `StartAt` and the item's input value — the same command shape, reused verbatim for
/// map items via the identical `index`→`children` mapping.
///
/// TODO(command-design): the rename from `SpawnBranch` resolved the naming concern — `SpawnThread`
/// now matches the `Thread` entity it creates. The remaining concern is the payload: it still does
/// not preserve the higher-level source context (container kind, whether the index is a branch or
/// item index, and any future fan-out metadata), only how to start the child. Extend the payload
/// before more fan-out modes or source-specific behavior are added.
#[derive(Default)]
pub struct SpawnThreadHandler;

impl SpawnThreadHandler {
    pub(crate) async fn handle(
        &self,
        p: &SpawnThread,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let SpawnThread {
            owner,
            execution,
            state_path,
            index,
            start_at,
            input,
        } = p;

        // A fan-out is always emitted while its container is `Running` — both `after_activated` sites
        // activate it in the same batch, and the `Map` replenish runs only on the `Running` arm — and the
        // leader dispatches in append order, so a command that could stop the container is always
        // appended *after* this one. An owner that is missing, or present but no longer `Running`, is
        // therefore the log and the projection disagreeing rather than a command that arrived late:
        // refused, not dropped silently. A fault reading the row is neither, and is returned so the
        // leader can retry it.
        let owner_activity = match ctx.storage.get_activity(owner).await? {
            Some(a) => a,
            None => {
                return Err(ProcessingError::Rejected(
                    RejectionType::NotFound,
                    format!("spawn_thread: owner activity {owner} not found"),
                ));
            }
        };
        if !owner_activity.status.is_running() {
            tracing::warn!(
                owner = %owner,
                status = ?owner_activity.status,
                "fan-out against a container that is no longer Running; refused"
            );
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidState,
                format!(
                    "spawn_thread: owner activity {owner} is {}, not Running; fan-out refused",
                    owner_activity.status.phase()
                ),
            ));
        }

        // The child Thread runs against the *same* machine version as its owner: the owning
        // activity's parent is the owning **scope** (a top-level `Execution` or a fan-out `Thread`),
        // and the whole tree binds to one definition (that of the top-level `Execution`). The child
        // does **not** re-record `flow_version` (derived via its `execution` later, see `Thread`) —
        // it only inherits the tree's top-level anchor verbatim, so its states resolve against the
        // same shared machine doc.
        let scope_ref = owner_activity.value.meta.owner.clone();
        // Resolve the owning thread to inherit the tree's top-level anchor (`execution`): the child
        // Thread shares the owner's tree, so the anchor is taken verbatim while the Thread itself is
        // the child's `owner`. The slot admits only a `Thread`, so the row is read directly.
        //
        // Nothing ever removes a row, and an activity's owner slot is written in the batch that births
        // it — with a thread created in that same batch. An activity that is here, naming a thread that
        // is not, is therefore the log and the projection disagreeing, not a fan-out that arrived
        // late: refused, like the two arms above.
        let Some(owner_thread) = ctx.storage.get_thread(&scope_ref).await? else {
            return Err(ProcessingError::Rejected(
                RejectionType::NotFound,
                format!(
                    "spawn_thread: owner activity {owner} names thread {scope_ref}, which does not exist; fan-out refused"
                ),
            ));
        };
        let root_execution = owner_thread.value.execution.clone();

        // Mint the child's uid and its stable RawObjectRef up front: the child `Thread` row is
        // keyed by that reference, and the sibling `ActivateState` entry must name the same run
        // before the `ThreadCreated` applier builds the entity.
        let uid: ulid::Ulid = ctx.mint();
        // Name the thread as a child of its owning execution (the #3/#11/#13 convention, applied to
        // threads): the generated name's plain base is the owning execution's name, inherited
        // verbatim through every nesting level — so a branch thread still names its root run — and
        // its suffix is a random tail via `PlainName::to_generated`, decoupled from the thread's own
        // `uid`. Not the opaque `child-<uid>` placeholder. Minted once and reused for the reference
        // and the serialized `meta.name`, so the storage row key (`thread.meta.raw_object_ref()`) matches
        // the sibling `ActivateState` owner.
        let thread_name = execution
            .name()
            .base()
            .generated_from_key(out.next_generated_seq().await);
        let reference =
            crate::types::meta::ObjectRef::<crate::ThreadKind>::new(thread_name.clone(), uid);
        tracing::debug!(child = ?reference, owner = ?owner, "spawning child thread from fan-out command");

        // Root the child in the owning tree: `parent` links it to the Parallel activity (whose
        // `active_children` the applier populates, so the Parallel drains only once every branch
        // settles); `execution` inherits the top-level run's id verbatim (the flat query
        // anchor shared by the whole tree); `state_path` lets the child resolve its own branch
        // states without querying its parent or the root — it already names the branch's `States`
        // table within the single shared machine document.
        //
        // A thread's `state_path` is its defining property — it always descends into the shared
        // machine — so the command must carry it. `SpawnThread` is only issued by the container
        // states (Parallel/Map), which always compute the branch/item pointer.
        let branch_states = state_path
            .clone()
            .expect("a spawned thread always receives its state_path from the container");
        out.append_event(Event::ThreadCreated {
            thread: crate::Thread {
                execution: execution.clone(),
                state_path: branch_states.clone(),
                // The entry point inside that table travels with the thread too, so the entity names
                // its whole sub-run (which table it runs, and where in it it starts) rather than
                // leaving the entry recoverable only from the `ActivateState` emitted just below.
                start_at: start_at.clone(),
                // The thread records its own ordinal (branch/item index in declaration order); the
                // container's ordered fan-out map is projected from this by the `ThreadCreated`
                // applier, so the value lives here on the entity as its identity.
                index: *index,
                status: crate::ThreadStatus::Running,
                input: input.clone(),
                output: None,
                // Birth: `created_at == now` (fan-out moment). The owner is the SpawnThread command's
                // `parent` Parallel/Map activity — its checked conversion above is what makes the
                // `Activity` variant the only one a fan-out thread can carry.
                meta: crate::types::meta::ObjectMeta::builder(uid)
                    .name(thread_name)
                    .at(ctx.now())
                    .with_owner(crate::types::meta::ThreadOwner::Activity(owner.clone())),
            },
        })
        .await;
        // Enter the branch at its `StartAt` state. The child's path = the thread's branch/item
        // `state_path` (its enclosing `States` table) extended by the start state's name, so the
        // carried path locates the state self-containedly. `owner` is the new Thread (the branch's
        // immediate scope); `execution` is the inherited top-level anchor.
        out.append_command(Command::ActivateState(ActivateState {
            execution: root_execution,
            owner: reference,
            state_path: branch_states.state(start_at),
            input: input.clone(),
        }));

        Ok(())
    }
}
