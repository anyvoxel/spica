//! Pure domain data types shared across the engine — commands, events, rejections, and the
//! projected entity values (activity / execution / flow / task / timer). No execution logic lives
//! here: the engine proper (`engine`, `stream_processor`, `handler`, `applier`, `handlers`)
//! consumes and produces these values.

pub mod activity;
pub mod command;
pub mod context;
pub mod entry;
pub mod error;
pub mod event;
pub mod execution;
pub mod flow;
pub mod flow_version;
pub mod id;
// P0 foundation (docs/identity-and-partitioning-design.md): the shared metric types are not yet
// consumed by any entity until the P1 embedding pass wires ObjectMeta into Execution/Activity/
// Timer/Task/Flow, so dead_code is expected here until then.
#[allow(dead_code)]
pub mod meta;
pub mod reject;
pub mod task;
pub mod thread;
pub mod timer;
pub mod variables;
