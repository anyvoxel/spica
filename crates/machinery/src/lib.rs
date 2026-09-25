//! Shared foundational primitives for the spica workspace.
//!
//! `spica-machinery` is the leaf **kernel** crate that hosts the low-level types shared across every
//! other spica crate (the engine, its log, storage, scheduler, …). A type lands here when it carries
//! no business logic, is needed by more than one crate, and belongs in no single consumer — hosting
//! it in its own leaf keeps the crate dependency graph layered: machinery depends on nothing
//! internal, and everything else depends on it, never the other way around.
//!
//! Machinery is deliberately the *bottom* of the graph: it must not pull in any spica crate, so the
//! shared primitives it grows (e.g. the wall-clock [`Timestamp`], the injectable [`Clock`] that reads
//! it, and the injectable [`IdGenerator`] that names objects) can be imported by any layer without
//! dragging down a larger dependency.

pub mod clock;
pub mod id;
pub mod name;
pub mod timestamp;
pub use self::clock::{Clock, ManualClock, SystemClock};
pub use self::id::{CountingIdGenerator, IdGenerator, SystemIdGenerator};
pub use self::name::{NameError, ObjectName, PlainName, ScopeName};
pub use self::timestamp::Timestamp;
