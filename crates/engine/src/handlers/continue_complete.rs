//! Handles `Command::ContinueComplete`: the **success** side of the deferred drain-continuation.
//!
//! A settled child no longer drains its owner inline (the old recursive cascade in `child_completed`);
//! the one-hop reactor instead issues one `ContinueComplete` per drained-and-finishing **success** owner,
//! dispatched on a **later round** (Zeebe's `COMPLETE_ELEMENT` decoupling). This handler settles the
//! owner by its own kind and, on success, hands the settled owner up to *its* owner via that owner's
//! [`Container`] — exactly one hop, never recursion, so stack depth is independent of owner-chain depth.
//!
//! The node is dispatched by kind to its own element's `continue_complete`, and every one shares a
//! shape: a node that still holds children cannot advance, so each child is taken down by its own kind's
//! verb; a drained node advances to `Completed` and hands the settle up to its owner.
//! An **activity**'s advance ([`ActivityElementBehavior::continue_complete`]) runs its state's
//! `after_completing` rather than a kind-local close, so the projection, the `Next`/`End` routing and
//! the relay the deferred `complete` step owes happen there — the state emits the terminal *and* the
//! `CompleteThread`/`ActivateState` that carries it up.
//!
//! The failure side (`Terminate`) lives in [`continue_terminate`](super::continue_terminate); the two are
//! split by direction so each file carries only its own per-kind terminal logic, mirroring
//! `complete_state` / `terminate_state`.

use crate::elements::{ActivityElementBehavior, ExecutionElementBehavior, ThreadElementBehavior};
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::activity::ActivityKind;
use crate::types::execution::ExecutionKind;
use crate::types::meta::{ObjectKind, RawObjectRef};
use crate::types::thread::ThreadKind;

/// Handles `Command::ContinueComplete`: dispatches the drained-and-finishing `owner` to its own kind's
/// element, which emits the owner's success terminal and hands the settle up its owner chain on this
/// round — one hop.
#[derive(Default)]
pub struct ContinueCompleteHandler;

impl ContinueCompleteHandler {
    pub(crate) async fn handle(
        &self,
        owner: &RawObjectRef,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        match owner.kind {
            ObjectKind::Activity => {
                ActivityElementBehavior::continue_complete(
                    ctx,
                    out,
                    owner.clone().typed::<ActivityKind>(),
                )
                .await
            }
            ObjectKind::Thread => {
                ThreadElementBehavior::continue_complete(
                    ctx,
                    out,
                    owner.clone().typed::<ThreadKind>(),
                )
                .await
            }
            ObjectKind::Execution => {
                ExecutionElementBehavior::continue_complete(
                    ctx,
                    out,
                    owner.clone().typed::<ExecutionKind>(),
                )
                .await
            }
            // A Flow / FlowVersion / Task is never a `ContinueComplete` owner: the only
            // issuers are the three containers' settle hooks, which address an Activity, a Thread or
            // an Execution, so another kind here is a dispatch fault, not a command.
            kind => panic!(
                "continue_complete: {kind} is never a Continue owner; only an Activity, a Thread or an Execution defers"
            ),
        }
    }
}
