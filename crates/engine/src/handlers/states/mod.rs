mod choice;
mod fail;
mod map;
mod parallel;
mod pass;
mod succeed;
mod task;
mod wait;

pub use choice::ChoiceStateHandlerFactory;
pub use fail::FailStateHandlerFactory;
pub use map::MapStateHandlerFactory;
pub use parallel::ParallelStateHandlerFactory;
pub use pass::PassStateHandlerFactory;
pub use succeed::SucceedStateHandlerFactory;
pub use task::TaskStateHandlerFactory;
pub use wait::WaitStateHandlerFactory;

// TODO(Map): M3 `Map` is implemented (activate → first batch fan-out → per-settle `child_completed`
// replenish → converge/fail). Deferred: `ItemSelector` (per-item `$states.context.Map.Item`
// projection → item input), `ToleratedFailureCount`/`ToleratedFailurePercentage` (the current
// behavior equals the default 0 tolerance — any item failure fails the whole Map), and threading
// `$states.context.Map.Item` down into the item-processor child states.
