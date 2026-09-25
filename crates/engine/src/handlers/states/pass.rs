use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{AssignObject, PassState, State};

use super::super::state_handler::{StateHandler, StateHandlerFactory};

pub struct PassStateHandlerFactory;

#[async_trait]
impl StateHandlerFactory for PassStateHandlerFactory {
    fn state(&self) -> State {
        State::Pass(PassState::default())
    }

    fn create<'a>(&self, state: &'a State) -> Box<dyn StateHandler + 'a> {
        let State::Pass(s) = state else {
            unreachable!(
                "create dispatch guarantees the factory receives its own variant; got {state:?}"
            );
        };
        Box::new(PassStateHandler { state: s })
    }
}

struct PassStateHandler<'a> {
    state: &'a PassState,
}

#[async_trait]
impl StateHandler for PassStateHandler<'_> {
    // Pass's projection — `Assign` (a delta on the execution scope, emitted as `VariablesAssigned`)
    // then `Output` (defaults to the input) — is the canonical ASL success projection and is exactly
    // the base's default `finish`: Pass supplies only its own `Assign`/`Output` sources and routing.
    fn assign(&self) -> Option<&AssignObject> {
        self.state.assign.as_ref()
    }

    fn output(&self) -> Option<&Value> {
        self.state.output.as_ref()
    }

    fn next(&self) -> Option<&str> {
        self.state.next.as_deref()
    }

    fn end(&self) -> Option<bool> {
        self.state.end
    }
}
