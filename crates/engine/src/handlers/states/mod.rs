mod choice;
mod fail;
mod map;
mod parallel;
mod pass;
mod succeed;
mod task;
mod wait;

pub use choice::ChoiceStateHandler;
pub use fail::FailStateHandler;
pub use map::MapStateHandler;
pub use parallel::ParallelStateHandler;
pub use pass::PassStateHandler;
pub use succeed::SucceedStateHandler;
pub use task::TaskStateHandler;
pub use wait::WaitStateHandler;

// TODO(Map): M3 `Map` is implemented (activate → first batch fan-out → per-settle `child_completed`
// replenish → converge/fail). Deferred: `ItemSelector` (per-item `$states.context.Map.Item`
// projection → item input), `ToleratedFailureCount`/`ToleratedFailurePercentage` (the current
// behavior equals the default 0 tolerance — any item failure fails the whole Map), and threading
// `$states.context.Map.Item` down into the item-processor child states.
