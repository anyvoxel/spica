//! Handles `Command::HandleChildFinished`: the one place a finished child is handed to its owner.
//!
//! A child never reacts to its owner inline — the terminal writes a command and the owner answers on a
//! **later round** (Zeebe's `COMPLETE_ELEMENT` decoupling). So the reaction is exactly one hop deep no
//! matter how long the owner chain is, and it reads committed storage: the child's own terminal row
//! says how it finished, and the owner's status says what that means.
//!
//! The owner's **kind** selects its element's `on_child_finished`, which owns the whole decision —
//! resume a state, close a thread or a run, converge a fan-out, or refuse a stale hand-off.

use crate::elements::{ActivityElementBehavior, ExecutionElementBehavior, ThreadElementBehavior};
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::activity::ActivityKind;
use crate::types::execution::ExecutionKind;
use crate::types::meta::{ObjectKind, RawObjectRef};
use crate::types::thread::ThreadKind;

/// Handles `Command::HandleChildFinished`: dispatches the finished `child` to its `owner`'s own element,
/// which decides what the finish means for an owner of that kind.
#[derive(Default)]
pub struct HandleChildFinishedHandler;

impl HandleChildFinishedHandler {
    pub(crate) async fn handle(
        &self,
        owner: &RawObjectRef,
        child: &RawObjectRef,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        match owner.kind {
            ObjectKind::Activity => {
                ActivityElementBehavior::on_child_finished(
                    ctx,
                    out,
                    owner.clone().typed::<ActivityKind>(),
                    child,
                )
                .await
            }
            ObjectKind::Thread => {
                ThreadElementBehavior::on_child_finished(
                    ctx,
                    out,
                    owner.clone().typed::<ThreadKind>(),
                    child,
                )
                .await
            }
            ObjectKind::Execution => {
                ExecutionElementBehavior::on_child_finished(
                    ctx,
                    out,
                    owner.clone().typed::<ExecutionKind>(),
                    child,
                )
                .await
            }
            // A Flow / FlowVersion / Task owns no child in the settle relation: the only issuers are
            // the children themselves, which address an Activity, a Thread or an Execution.
            kind => panic!(
                "handle_child_finished: {kind} never owns a finished child; only an Activity, a \
                 Thread or an Execution does"
            ),
        }
    }
}
