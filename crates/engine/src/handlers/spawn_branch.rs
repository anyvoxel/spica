use async_trait::async_trait;

use crate::command::Command;
use crate::event::Event;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::id::NodeId;

/// Handles `SpawnBranch`: fans out one branch of a `Parallel` state — or one item of a `Map` state —
/// into a new **child execution**.
///
/// The child is projected by an `ExecutionCreated{parent, root_execution, state_path}` (which
/// wires it into its owner's `active_children` via the applier), then entered at its `StartAt`
/// state via `ActivateState`. From there it runs as a self-contained sub-state-machine — its
/// internal hops happen entirely within the child execution, and its terminal hop cascades back to
/// `parent` through `ProcessChildCompleted`, so the owning `Parallel`/`Map` activity only learns the child
/// settled when the whole branch/item finishes. This is the flat (non-recursive) fan-out: the child is
/// stored as one row with a `state_path` into the shared machine, never copying branch state.
///
/// For a `Map`, `branch_index` carries the **item index** and `state`/`input` are the item
/// processor's `StartAt` and the item's input value — the same command shape, reused verbatim for
/// map items via the identical `ParallelBranchSpawned`/`parallel_children` mapping.
///
/// TODO(command-design): this handler already proves `SpawnBranch` is misnamed and underspecified.
/// It no longer spawns only a `Parallel` branch, and the command payload does not preserve enough
/// source context to describe *why* this child execution exists — only how to start it. Rename it
/// to a container-neutral child-execution spawn command and extend its payload before more fan-out
/// modes or source-specific behavior are added.
#[derive(Default)]
pub struct SpawnBranchHandler;

#[async_trait]
impl CommandHandler for SpawnBranchHandler {
    fn command(&self) -> Command {
        Command::SpawnBranch {
            parent: NodeId::Execution(crate::id::ExecutionId::nil()),
            root_execution: crate::id::ExecutionId::nil(),
            state_path: None,
            branch_index: 0,
            state: String::new(),
            input: Default::default(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::SpawnBranch {
            parent,
            root_execution,
            state_path,
            branch_index,
            state,
            input,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };

        // The `parent` Parallel activity must still be running (it may have since been terminated —
        // e.g. a sibling branch failed and drained the Parallel). If it is gone or no longer
        // accepting children, the fan-out is a no-op: the child simply never spawns.
        let owner = match parent {
            NodeId::Activity(a) => *a,
            _ => return, // internal fault: a branch owner must be an Activity.
        };
        let owner_activity = match ctx.storage.get_activity(owner).await {
            Ok(Some(a)) => a,
            _ => return, // owner gone — the fan-out is dropped.
        };
        if !owner_activity.status.is_running() {
            return; // owner not running — the fan-out is dropped.
        }

        // The child execution runs against the *same* machine version as its owner: the owning
        // activity's parent is the owning execution, whose `flow_version_id` names the definition the
        // whole tree binds to. Resolve it from the owner's execution so the child's later
        // `ActivateState`/`CompleteState` resolve states against the same shared machine doc.
        let flow_version_id = match owner_activity.value.parent {
            NodeId::Execution(e) => match ctx.storage.get_execution(e).await {
                Ok(Some(ex)) => ex.flow_version_id,
                _ => return, // owning execution gone — nothing to bind the child to.
            },
            _ => return, // internal fault: a branch owner's parent must be an Execution.
        };

        let id = out.next_execution();
        tracing::debug!(child = ?id, parent = ?parent, "spawning child execution from fan-out command");

        // Root the child in the owning tree: `parent` links it to the Parallel activity (whose
        // `active_children` the applier populates, so the Parallel drains only once every branch
        // settles); `root_execution` inherits the top-level run's id verbatim (the flat query
        // anchor shared by the whole tree); `state_path` lets the child resolve its own branch
        // states without querying its parent or the root — it already names the branch's `states`
        // table within the single shared machine document.
        out.emit_event(Event::ExecutionCreated {
            // A child spawned by fan-out has no client request awaiting its creation — the `nil`
            // placeholder means this record is never interpreted as a request acknowledgement.
            request_id: crate::id::RequestId::nil(),
            execution: crate::ExecutionValue {
                id,
                flow_version_id,
                root_execution: *root_execution,
                parent: Some(*parent),
                state_path: state_path.clone(),
                status: crate::ExecutionStatus::Running,
                input: input.clone(),
                output: None,
            },
        });
        // Record the child under its branch index so the owning `Parallel` can aggregate branch
        // outputs in the declared `Branches` order when it converges.
        out.emit_event(Event::ParallelBranchSpawned {
            activity: owner,
            index: *branch_index,
            execution: id,
        });
        // Enter the branch at its `StartAt` state. The child execution's own ActivateState resolves
        // states via its `state_path`, so this `state`/`input` re-enter naturally.
        let activity = out.next_activity();
        out.emit_command(Command::ActivateState {
            execution: id,
            activity,
            state: state.clone(),
            input: input.clone(),
        });
    }
}
