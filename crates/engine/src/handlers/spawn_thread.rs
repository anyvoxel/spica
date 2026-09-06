use async_trait::async_trait;

use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::log::Timestamp;
use crate::types::command::Command;
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

#[async_trait]
impl CommandHandler for SpawnThreadHandler {
    fn command(&self) -> Command {
        Command::SpawnThread {
            owner: crate::types::meta::ObjectReference::nil(),
            execution: crate::types::meta::ObjectReference::nil(),
            state_path: None,
            index: 0,
            start_at: String::new(),
            input: Default::default(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector<'_>) {
        let Command::SpawnThread {
            owner,
            execution,
            state_path,
            index,
            start_at,
            input,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };

        // The `owner` Parallel activity must still be running (it may have since been terminated —
        // e.g. a sibling branch failed and drained the Parallel). If it is gone or no longer
        // accepting children, the fan-out is a no-op: the child simply never spawns.
        if owner.kind != crate::types::meta::ObjectKind::Activity {
            return; // internal fault: a branch owner must be an Activity.
        }
        let owner_activity = match ctx.storage.get_activity(owner).await {
            Ok(Some(a)) => a,
            _ => return, // owner gone — the fan-out is dropped.
        };
        if !owner_activity.status.is_running() {
            return; // owner not running — the fan-out is dropped.
        }

        // The child Thread runs against the *same* machine version as its owner: the owning
        // activity's parent is the owning **scope** (a top-level `Execution` or a fan-out `Thread`),
        // and the whole tree binds to one definition (that of the top-level `Execution`). The child
        // does **not** re-record `flow_version` (derived via its `execution` later, see `Thread`) —
        // it only inherits the tree's top-level anchor verbatim, so its states resolve against the
        // same shared machine doc.
        let scope_ref = owner_activity
            .value
            .meta
            .owner
            .clone()
            .expect("an owned activity has an owner");
        // Resolve the owning scope to inherit the tree's top-level anchor (`execution`): the child
        // Thread shares the owner's tree, so the anchor is taken verbatim while the Thread itself is
        // the child's `owner`.
        let root_execution = match scope_ref.kind {
            crate::types::meta::ObjectKind::Execution | crate::types::meta::ObjectKind::Thread => {
                match crate::storage::load_scope_ref(ctx.storage, &scope_ref).await {
                    Ok(Some(scope)) => scope.root_execution(),
                    _ => return, // owning scope gone — nothing to bind the child to.
                }
            }
            _ => return, // internal fault: a branch owner's parent must be a scope.
        };

        // Mint the child's uid and its stable ObjectReference up front: the child `Thread` row is
        // keyed by that reference, and the sibling `ActivateState` entry must name the same run
        // before the `ThreadCreated` applier builds the entity.
        let id = out.next_execution();
        let uid: ulid::Ulid = id.into();
        // Name the thread as a child of its owning execution (the #3/#11/#13 convention, applied to
        // threads): the generated name's plain base is the owning execution's name, inherited
        // verbatim through every nesting level — so a branch thread still names its root run — and
        // its suffix is a random tail via `PlainName::to_generated`, decoupled from the thread's own
        // `uid`. Not the opaque `child-<uid>` placeholder. Minted once and reused for the reference
        // and the serialized `meta.name`, so the storage row key (`thread.reference()`) matches the
        // sibling `ActivateState` owner.
        let thread_name = execution
            .name
            .base()
            .generated_from_key(out.next_generated_seq().await);
        let reference = crate::types::meta::ObjectReference::new(
            crate::types::meta::ObjectKind::Thread,
            thread_name.clone(),
            uid,
        );
        tracing::debug!(child = ?reference, owner = ?owner, "spawning child thread from fan-out command");

        // Root the child in the owning tree: `parent` links it to the Parallel activity (whose
        // `active_children` the applier populates, so the Parallel drains only once every branch
        // settles); `execution` inherits the top-level run's id verbatim (the flat query
        // anchor shared by the whole tree); `state_path` lets the child resolve its own branch
        // states without querying its parent or the root — it already names the branch's `states`
        // table within the single shared machine document.
        out.emit_event(Event::ThreadCreated {
            thread: crate::Thread {
                execution: execution.clone(),
                // A thread's `state_path` is its defining property — it always descends into the
                // shared machine — so the command must carry it. `SpawnThread` is only issued by the
                // container states (Parallel/Map), which always compute the branch/item pointer.
                state_path: state_path
                    .clone()
                    .expect("a spawned thread always receives its state_path from the container"),
                // The thread records its own ordinal (branch/item index in declaration order); the
                // container's ordered fan-out map is projected from this by the `ThreadCreated`
                // applier, so the value lives here on the entity as its identity.
                index: *index,
                status: crate::ThreadStatus::Running,
                input: input.clone(),
                output: None,
                // Birth: `created_at == now` (fan-out moment). The owner is the SpawnThread command's
                // `parent` Parallel/Map activity, converted to the reference form stored in meta.
                meta: crate::types::meta::ObjectMeta::builder(
                    crate::types::meta::ObjectKind::Thread,
                    uid,
                )
                .name(thread_name)
                .at(Timestamp::now())
                .build()
                .with_owner(owner.clone()),
            },
        })
        .await;
        // Enter the branch at its `StartAt` state. The child's path = the thread's branch/item
        // `state_path` (its enclosing `states` table) extended by the start state's name, so the
        // carried path locates the state self-containedly. `owner` is the new Thread (the branch's
        // immediate scope); `execution` is the inherited top-level anchor.
        let mut enter_path = state_path
            .clone()
            .expect("a spawned thread always receives its state_path from the container");
        enter_path.push_back(start_at.as_str());
        out.emit_command(Command::ActivateState {
            execution: root_execution,
            owner: reference,
            state_path: enter_path,
            input: input.clone(),
        });
    }
}
