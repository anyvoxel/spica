//! The target of a deadline: the element whose own row records when it is due.
//!
//! A deadline is not an entity. It is a field of the element that acts on it — a run's
//! `TimeoutSeconds` on the `Execution`, a `Task` state's on the `Task`, a `Wait`'s on the waiting
//! `Activity` — so a fire names the element rather than a side-effect node standing beside it. The
//! `deadline` travelling with the fire is the armed instant, letting the element recognise (and
//! refuse) a fire against a moment it no longer holds.

use serde::{Deserialize, Serialize};

use crate::types::activity::ActivityKind;
use crate::types::execution::ExecutionKind;
use crate::types::meta::{HasRawObjectRef, ObjectRef, RawObjectRef};

/// The element a deadline belongs to — the scope that armed it and the only thing a fire acts on.
///
/// The two variants are exactly the scopes that own a deadline: a run (its own `TimeoutSeconds`) and
/// an activity (a `Wait`'s `Seconds`, or the `TimeoutSeconds` bounding a `Task` state).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DeadlineTarget {
    Execution(ObjectRef<ExecutionKind>),
    Activity(ObjectRef<ActivityKind>),
}

impl DeadlineTarget {
    /// The scope-relative reference, for matching a fire against storage without carrying the kind.
    pub fn raw_object_ref(&self) -> &RawObjectRef {
        match self {
            DeadlineTarget::Execution(execution) => execution.as_raw_object_ref(),
            DeadlineTarget::Activity(activity) => activity.as_raw_object_ref(),
        }
    }
}

impl std::fmt::Display for DeadlineTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.raw_object_ref().fmt(f)
    }
}

impl From<ObjectRef<ExecutionKind>> for DeadlineTarget {
    fn from(execution: ObjectRef<ExecutionKind>) -> Self {
        DeadlineTarget::Execution(execution)
    }
}

impl From<ObjectRef<ActivityKind>> for DeadlineTarget {
    fn from(activity: ObjectRef<ActivityKind>) -> Self {
        DeadlineTarget::Activity(activity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::meta::ObjectName;

    fn exec_ref() -> ObjectRef<ExecutionKind> {
        ObjectRef::new(
            ObjectName::from_parsed("execution-0").expect("a valid object name"),
            ulid::Ulid::from(7u128),
        )
    }

    #[test]
    fn a_target_round_trips_with_its_kind() {
        let target = DeadlineTarget::Execution(exec_ref());
        let json = serde_json::to_value(&target).expect("a target serializes");
        assert_eq!(json["Execution"]["kind"], serde_json::json!("Execution"));
        assert_eq!(
            serde_json::from_value::<DeadlineTarget>(json).expect("the target round-trips"),
            target
        );
    }

    #[test]
    fn a_target_refuses_a_kind_it_does_not_name() {
        let json = serde_json::json!({
            "Execution": { "kind": "Thread", "name": "thread-0", "uid": ulid::Ulid::nil() }
        });
        let err =
            serde_json::from_value::<DeadlineTarget>(json).expect_err("a thread owns no deadline");
        assert!(err.to_string().contains("mismatch"), "{err}");
    }

    #[test]
    fn two_targets_compare_by_kind_and_identity() {
        let a = DeadlineTarget::Execution(exec_ref());
        let b = DeadlineTarget::Execution(exec_ref());
        assert_eq!(a, b);
        assert_eq!(a.to_string(), b.to_string());
    }
}
