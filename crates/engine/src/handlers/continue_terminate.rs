//! Handles `Command::ContinueTerminate`: the **failure** side of the deferred drain-continuation.
//!
//! A settled child no longer drains its owner inline (the old recursive cascade in `child_completed`);
//! the one-hop reactor instead issues one `ContinueTerminate` per drained-and-finishing **failure** owner,
//! dispatched on a **later round** (Zeebe's `COMPLETE_ELEMENT` decoupling). This handler settles the
//! owner by its own kind and hands the settled owner up to *its* owner — exactly one hop, never
//! recursion, so stack depth is independent of owner-chain depth.
//!
//! It mirrors the success side ([`continue_complete`](super::continue_complete)): the node is dispatched
//! by kind to its own element's `continue_terminate`, and every one shares a shape — a node that still
//! holds children cannot advance, so each child is taken down by its own kind's verb; a drained node
//! emits its `Terminated` and hands the settle up. An **activity**'s advance
//! ([`ActivityElementBehavior::continue_terminate`]) runs its state's `after_terminating`, so the
//! terminal and the relay the deferred `terminate` step owes happen there. The failure reason is never
//! re-derived: it was fixed when the teardown began
//! (`Terminating(reason)` on the row), so each kind's terminal only advances that stored value.

use crate::elements::{ActivityElementBehavior, ExecutionElementBehavior, ThreadElementBehavior};
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::activity::ActivityKind;
use crate::types::execution::ExecutionKind;
use crate::types::meta::{ObjectKind, RawObjectRef};
use crate::types::thread::ThreadKind;

/// Handles `Command::ContinueTerminate`: dispatches the drained-and-finishing `owner` to its own kind's
/// element, which emits the owner's failure terminal and hands the settle up its owner chain on this
/// round — one hop.
#[derive(Default)]
pub struct ContinueTerminateHandler;

impl ContinueTerminateHandler {
    pub(crate) async fn handle(
        &self,
        owner: &RawObjectRef,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        match owner.kind {
            ObjectKind::Activity => {
                ActivityElementBehavior::continue_terminate(
                    ctx,
                    out,
                    owner.clone().typed::<ActivityKind>(),
                )
                .await
            }
            ObjectKind::Thread => {
                ThreadElementBehavior::continue_terminate(
                    ctx,
                    out,
                    owner.clone().typed::<ThreadKind>(),
                )
                .await
            }
            ObjectKind::Execution => {
                ExecutionElementBehavior::continue_terminate(
                    ctx,
                    out,
                    owner.clone().typed::<ExecutionKind>(),
                )
                .await
            }
            // A Flow / FlowVersion / Task is never a `ContinueTerminate` owner: the only
            // issuers are the three containers' settle hooks, which address an Activity, a Thread or
            // an Execution, so another kind here is a dispatch fault, not a command.
            kind => panic!(
                "continue_terminate: {kind} is never a Continue owner; only an Activity, a Thread or an Execution defers"
            ),
        }
    }
}
