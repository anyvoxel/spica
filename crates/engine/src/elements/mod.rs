//! Per-object **element** behavior — every piece of one kind of workflow object's lifecycle logic in
//! one place.
//!
//! The object graph is made of *elements*: the definitions a run is built from (`Flow`, with its
//! immutable `FlowVersion`s), the run itself (`Execution`), its `Thread`s, their `Activity` states,
//! and the leaf work each activity hands out (`Task`) — plus, one layer down, the
//! state-type behaviors. This module gathers them: one `*ElementBehavior` per kind, named for the
//! element it governs. An element's **birth** belongs here too — the same aggregation, one step
//! earlier in its life.
//!
//! A finished child is handed to its owner as a deferred
//! [`Command::HandleChildFinished`](crate::types::command::Command::HandleChildFinished), never as
//! an inline callback: the owner's reaction is one command hop, so it cannot recurse up the owner
//! chain inside the batch that wrote the child's terminal. Each element's `on_child_finished`
//! decides what that hop means for an owner of its kind.

mod activity;
mod execution;
mod flow;
pub(crate) mod state_handler;
pub(crate) mod states;
mod task;
mod thread;

pub(crate) use activity::ActivityElementBehavior;
pub(crate) use execution::ExecutionElementBehavior;
pub(crate) use flow::FlowElementBehavior;
pub(crate) use task::TaskElementBehavior;
pub(crate) use thread::ThreadElementBehavior;
