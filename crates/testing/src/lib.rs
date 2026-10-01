//! Test doubles for the engine's seams.
//!
//! The mocks live in a crate of their own because a `mock!` expansion belongs to the crate that
//! writes it: the generated type is a local type, and `#[cfg(test)]` items are compiled only for the
//! crate's own test harness. A handler test can therefore mock a trait of its own crate inline, but
//! an integration test under `tests/` — a separate crate — cannot see it at all. Putting the whole
//! set here makes one mock serve every consumer: the engine's unit tests, its integration tests, and
//! any other crate's.
//!
//! The alternative, `#[automock]`-annotating the trait where it is defined, would cost the
//! production crate a mock-only feature and an optional `mockall` dependency. Nothing here is
//! production code, so nothing production carries it.
//!
//! A `mock!` block restates the trait's signatures rather than deriving them. That duplication is
//! checked: the expansion implements the trait, so a method added to or changed on the real trait
//! fails this crate's build instead of letting a stale double drift.

use std::collections::HashSet;

use async_trait::async_trait;
use mockall::mock;
use spica_engine_types::storage::ReadonlyStorageTxn;
use spica_engine_types::{
    ActivityKind, ActivityRecord, ExecutionKind, ExecutionRecord, Flow, FlowName, FlowVersion,
    FlowVersionKind, ObjectRef, RawObjectRef, StorageError, TaskKind, TaskRecord, ThreadKind,
    ThreadRecord, TimerKind, TimerRecord, Timestamp,
};

mock! {
    /// A read-only store a test scripts per call. Every method it does *not* expect panics when
    /// called, so a double doubles as a pin on the surface its consumer actually touches.
    pub ReadonlyStorageTxn {}

    #[async_trait]
    impl ReadonlyStorageTxn for ReadonlyStorageTxn {
        async fn get_execution(
            &self,
            reference: &ObjectRef<ExecutionKind>,
        ) -> Result<Option<ExecutionRecord>, StorageError>;
        async fn get_thread(
            &self,
            reference: &ObjectRef<ThreadKind>,
        ) -> Result<Option<ThreadRecord>, StorageError>;
        async fn get_activity(
            &self,
            reference: &ObjectRef<ActivityKind>,
        ) -> Result<Option<ActivityRecord>, StorageError>;
        async fn get_timer(
            &self,
            reference: &ObjectRef<TimerKind>,
        ) -> Result<Option<TimerRecord>, StorageError>;
        async fn get_task(
            &self,
            reference: &ObjectRef<TaskKind>,
        ) -> Result<Option<TaskRecord>, StorageError>;
        async fn get_children(
            &self,
            id: RawObjectRef,
        ) -> Result<HashSet<RawObjectRef>, StorageError>;
        async fn activatable_tasks(
            &self,
            resource: &str,
            now: Timestamp,
            limit: usize,
        ) -> Result<Vec<TaskRecord>, StorageError>;
        async fn get_flow_by_name(
            &self,
            name: FlowName,
        ) -> Result<Option<Flow>, StorageError>;
        async fn get_flow_version(
            &self,
            version: &ObjectRef<FlowVersionKind>,
        ) -> Result<Option<FlowVersion>, StorageError>;
        async fn flow_version_of(
            &self,
            name: FlowName,
            version: u32,
        ) -> Result<Option<FlowVersion>, StorageError>;
        async fn next_generated_seq(&self) -> Result<i64, StorageError>;
    }
}
