use async_trait::async_trait;
use serde_json::Value;
use spica_asl::{State, SucceedState};

use super::super::state_handler::{StateHandler, StateHandlerFactory};

pub struct SucceedStateHandlerFactory;

#[async_trait]
impl StateHandlerFactory for SucceedStateHandlerFactory {
    fn state(&self) -> State {
        State::Succeed(SucceedState::default())
    }

    fn create<'a>(&self, state: &'a State) -> Box<dyn StateHandler + 'a> {
        let State::Succeed(s) = state else {
            unreachable!(
                "create dispatch guarantees the factory receives its own variant; got {state:?}"
            );
        };
        Box::new(SucceedStateHandler { state: s })
    }
}

struct SucceedStateHandler<'a> {
    state: &'a SucceedState,
}

#[async_trait]
impl StateHandler for SucceedStateHandler<'_> {
    // Succeed is terminal: it finishes itself with its evaluated output, then completes the whole
    // execution with the same output. Per the ASL spec it carries only an optional `Output` (no
    // `Assign`), so it runs the base's default `finish` and supplies just its output and the terminal
    // routing.
    fn output(&self) -> Option<&Value> {
        self.state.output.as_ref()
    }

    // TODO：终态的路由不应该走 `emit_transition` 的通用分支，而应像 Fail 一样直接完成执行。
    fn end(&self) -> Option<bool> {
        Some(true)
    }
}
