use std::collections::HashMap;
use std::mem::discriminant;

use spica_asl::State;

use super::state_handler::StateHandler;
use super::states::{
    ChoiceStateHandler, FailStateHandler, PassStateHandler, SucceedStateHandler, WaitStateHandler,
};

/// Expresses one or more `StateHandler`s as dispatch-table entries.
///
/// Each handler already knows which variant it serves via its [`StateHandler::state`], which
/// returns that variant as a `State` (a `Default` instance of the wrapped definition standing in
/// only to identify the variant — the real activity instance the table later dispatches on is built
/// by the framework). The handler type is therefore the single source of truth for its own table
/// key; there is no hand-written placeholder to keep in sync. `$handler` is captured as a `path`
/// because the handlers are unit structs: the fragment serves both as a type (`<… as StateHandler>`)
/// and as a unit-struct value (`&$handler`, `Box::new($handler)`).
///
/// Recursive: `entry!` handles the first handler and recurses into the `entry!`-rest form; the tail
/// emits nothing. This lets a single invocation register any number of handlers.
macro_rules! state_handler_entry {
    ($map:expr, $handler:path $(, $rest:path)*) => {{
        let sample = <$handler as StateHandler>::state(&$handler);
        $map.insert(discriminant(&sample), Box::new($handler));
        state_handler_entry!($map $(, $rest)*);
    }};
    ($map:expr) => {};
}

/// The shared `State` -> `StateHandler` dispatch table, built once per `Processor` and reused by
/// both the `ActivateState` and `CompleteState` command handlers. Routing by
/// [`Discriminant`](std::mem::Discriminant) mirrors the `Command` table and keeps each state's
/// activate/complete behavior in one impl.
pub(super) fn build_state_handlers() -> HashMap<std::mem::Discriminant<State>, Box<dyn StateHandler>>
{
    let mut h: HashMap<std::mem::Discriminant<State>, Box<dyn StateHandler>> = HashMap::new();
    state_handler_entry!(
        h,
        PassStateHandler,
        ChoiceStateHandler,
        SucceedStateHandler,
        FailStateHandler,
        WaitStateHandler
    );
    h
}
