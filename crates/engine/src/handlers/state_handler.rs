use spica_asl::State;

use crate::eval_env::EvalEnv;
use crate::handler::{ActivityCtx, Collector};
use crate::id::ActivityId;

/// Handles one [`State`] variant across the two moments of its lifecycle. Concrete variants
/// (`PassStateHandler`, `WaitStateHandler`, …) are registered in
/// [`dispatch::build_state_handlers`](super::dispatch::build_state_handlers), the dispatch table
/// (keyed by [`Discriminant`](std::mem::Discriminant), mirroring `CommandHandler`) shared by the
/// `ActivateState` and `CompleteState` command handlers.
///
/// - [`activate`](Self::activate) runs on `Command::ActivateState`. The framework emits
///   `StateActivating` (for every entry) and then calls `activate`. Each state's handler is
///   responsible for emitting the matching `StateActivated` ed — **after** it has finished processing
///   the input (e.g. Choice after routing its rules) — and then issuing its follow-up command:
///   synchronous states self-emit a `CompleteState` / `TerminateState` sequence; asynchronous states
///   (e.g. `Wait`) arm their side effects and return. The full per-entry causal chain is therefore
///   `StateActivating → StateActivated → (StateCompleting/StateCompleted | side-effect command)`.
/// - [`complete`](Self::complete) runs on `Command::CompleteState`. `StateCompleting` is emitted by
///   the framework when the complete step opens — before `complete` runs, mirroring `StateActivating`
///   opening activate — so the ed (`StateCompleted`) and routing (`StateTransitioned` + command) are
///   the handler's only emissions. The framework's `CompleteStateHandler` route is uniform: every M1
///   state (Pass, Succeed, Fail, Choice, Wait) opens its complete step through `CompleteState`, so
///   `StateCompleting` is always emitted by the framework, never by the state itself.
pub trait StateHandler: Send + Sync {
    /// The [`State`] variant this handler serves, identified by a `Default` instance of the wrapped
    /// definition standing in only to read its discriminant — the real activity instance the table
    /// later dispatches on is built by the framework. The dispatch table reads this off the handler
    /// to derive the table key, so the handler itself is the single source of truth for which
    /// variant it handles. Takes `&self` (rather than being a `Self: Sized` associated function) so
    /// the trait stays object-safe for the `Box<dyn StateHandler>` dispatch table.
    fn state(&self) -> State;

    /// Enter the state. Emits the `StateActivated` ed once the input is processed, then either
    /// self-terminates (fully-synchronous variants) or arms a side effect and returns (asynchronous
    /// ones).
    fn activate(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector,
        activity: ActivityId,
        actx: &ActivityCtx,
        state: &State,
    );

    /// Finish the state successfully. The shared [`super::complete_activity`] projection covers the
    /// common case; a state with a custom finish (`Pass`, `Succeed`, `Fail`) overrides it.
    fn complete(
        &self,
        env: &mut EvalEnv,
        out: &mut Collector,
        activity: ActivityId,
        actx: &ActivityCtx,
        state: &State,
    );
}
