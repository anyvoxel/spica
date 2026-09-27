//! The `States`-hierarchy walk and the pointer type that carries it: locating a `States` table (or
//! one state's definition) inside a machine document. Both halves are the document's own knowledge —
//! the keys it is serialized with, which of its states are containers, and how a location inside it
//! is spelled — so they live here rather than in any consumer that happens to hold a pointer into it.

use std::collections::HashMap;
use std::fmt;
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::{State, StateMachine};

/// A JSON Pointer locating a sub-`States` table (or a state's own definition within one) inside the
/// shared machine document — e.g. `/States` for the machine's own top-level table (a root thread's
/// scope), `/States/<name>` for a top-level state, or
/// `/States/P1/Branches/0/States/P2/ItemProcessor/States` for a nested Parallel-branch / Map-item
/// descent. Distinct from an arbitrary JSON Pointer: its tokens form a well-formed walk over the
/// machine's `States` / `Branches` / `ItemProcessor` hierarchy (see [`StateMachine::state_at`]), so
/// wrap the pointer and host the derivations the container handlers otherwise repeat inline.
///
/// A `Parallel`/`Map` extends its own `state_path` to build each child's path, and the leaf is the
/// state's name — both are `state_path`-specific and had been re-derived in every container.
///
/// Its tokens are the definition document's own keys, spelled as this crate serializes them
/// (PascalCase), so the constants below are the grammar's whole vocabulary and the walk compares
/// them case-sensitively: a pointer recorded against a differently-spelled key is malformed, not a
/// silently different table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatePath(jsonptr::PointerBuf);

impl StatePath {
    /// The table of named states — the machine's own, or a nested scope's (a branch's, or a Map's
    /// item processor's). It is the opener of every descent, and the walk compares against it, so
    /// the vocabulary is defined once here and both halves read it from the same place.
    pub const STATES: &'static str = "States";

    /// A `Parallel` state's array of branch sub-state-machines.
    pub const BRANCHES: &'static str = "Branches";

    /// A `Map` state's single per-item sub-state-machine.
    pub const ITEM_PROCESSOR: &'static str = "ItemProcessor";

    /// The path of a thread that runs the machine's own top-level `States` table: `/States`. This is
    /// a **root** thread's path, and the shape every fan-out thread's path repeats one level down
    /// (a branch's `/…/Branches/<i>/States`), which is why one walk resolves them all.
    pub fn root() -> StatePath {
        let mut p = jsonptr::PointerBuf::new();
        p.push_back(Self::STATES);
        StatePath(p)
    }

    /// The leaf name of the state this path names — its final (decoded) token: the key of the state
    /// in its enclosing `States` table. Empty for a document-root path (a shape no state handler
    /// should see, kept total — `jsonptr`'s [`decoded`](jsonptr::Token::decoded) API only yields a
    /// `Cow` tied to a transient token borrow, hence the owned `String` here).
    pub fn state_name(&self) -> String {
        self.0
            .last()
            .map(|t| t.decoded().into_owned())
            .unwrap_or_default()
    }

    /// The path of the state named `name` inside this path's `States` table: self extended by one
    /// token. How a thread's entry point is derived from the table it runs plus its `StartAt`.
    pub fn state(&self, name: &str) -> StatePath {
        let mut p = self.0.clone();
        p.push_back(name);
        StatePath(p)
    }

    /// The path of the `index`-th branch's `States` table under this `Parallel` state: self extended
    /// by `/Branches/<index>/States`. A `Parallel` extends it once per child it fans out, and the
    /// result is that branch thread's own `state_path` — a pointer *to* the table, the same shape a
    /// root thread carries (`/States`), so every thread is resolved by one walk.
    pub fn branch(&self, index: usize) -> StatePath {
        let mut p = self.0.clone();
        p.push_back(Self::BRANCHES);
        p.push_back(index);
        p.push_back(Self::STATES);
        StatePath(p)
    }

    /// The path of this `Map`'s single `ItemProcessor`'s `States` table: self extended by
    /// `/ItemProcessor/States`. Every item runs the *same* processor, so all of a Map's item
    /// children share this one path.
    pub fn item_processor(&self) -> StatePath {
        let mut p = self.0.clone();
        p.push_back(Self::ITEM_PROCESSOR);
        p.push_back(Self::STATES);
        StatePath(p)
    }

    /// The path of a sibling state in the same enclosing `States` table: this path minus its own
    /// leaf, then extended by `next` — how a transition routes to the successor state.
    pub fn sibling(&self, next: &str) -> StatePath {
        let mut p = self.0.clone();
        p.pop_back();
        p.push_back(next);
        StatePath(p)
    }
}

impl Deref for StatePath {
    type Target = jsonptr::PointerBuf;

    fn deref(&self) -> &jsonptr::PointerBuf {
        &self.0
    }
}

impl DerefMut for StatePath {
    fn deref_mut(&mut self) -> &mut jsonptr::PointerBuf {
        &mut self.0
    }
}

impl From<jsonptr::PointerBuf> for StatePath {
    fn from(pointer: jsonptr::PointerBuf) -> Self {
        StatePath(pointer)
    }
}

/// Why a walk did not land on a `States` table.
///
/// The two variants mirror the two failure classes a caller must keep apart: a walk that cannot
/// spell a descent at all (a definition-level defect) versus a walk that spells one the document
/// simply does not have (a missing name, which a corrected definition or the right document fixes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatePathError {
    /// The walk is not a well-formed descent: a token where the grammar requires one of
    /// [`StatePath::STATES`] / [`StatePath::BRANCHES`] / [`StatePath::ITEM_PROCESSOR`], a step naming
    /// a non-container state, a `Map` without an `ItemProcessor`, or a non-integer branch index. The
    /// message names the offending step.
    Malformed(String),
    /// A well-formed step named a state (or a branch index) this document does not have, or the walk
    /// stopped before it named one.
    NotFound(String),
}

impl fmt::Display for StatePathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatePathError::Malformed(msg) | StatePathError::NotFound(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for StatePathError {}

impl StateMachine {
    /// Resolve a state's definition by its full path: `States/<leaf>` for a top-level state, or a
    /// deeper `<…>/States/<leaf>` for a branch / item. Both shapes walk the same way — the top-level
    /// form is simply the walk's shortest instance, since a path's parent names the table the leaf
    /// lives in.
    ///
    /// A path whose parent is not a table (empty, or a truncated descent) falls out as `NotFound` on
    /// the leaf, exactly as a name the document does not have does.
    pub fn state_at(&self, path: &StatePath) -> Result<&State, StatePathError> {
        let leaf = path.state_name();
        let mut parent = path.as_ptr().to_owned();
        parent.pop_back();
        let states = self.walk_states(parent.as_ptr())?;
        states.get(&leaf).ok_or(StatePathError::NotFound(leaf))
    }

    /// The walk itself, over the raw pointer: each descent step opens with [`StatePath::STATES`] (the
    /// table the walk currently stands on is where a path *ends*, so an opener is what a step
    /// consumes) and names the container plus the child to descend into — a branch index for a
    /// `Parallel`, the item processor for a `Map`. It lands on the deepest table in one pass, at any
    /// nesting depth, and mixes the two kinds freely because every step yields a table of the same
    /// type; the document stays one shared instance, so the walk only *locates* a table inside it and
    /// never copies child states. Lives apart from [`StateMachine::state_at`] because it walks a
    /// path's *parent* — a pointer the caller never spelled.
    fn walk_states(
        &self,
        pointer: &jsonptr::Pointer,
    ) -> Result<&HashMap<String, State>, StatePathError> {
        use State as S;
        let tokens: Vec<jsonptr::Token<'_>> = pointer.tokens().collect();
        let mut states: &HashMap<String, State> = &self.states;
        let mut i = 0usize;
        loop {
            // A walk with nothing left names the table it currently stands on. Two shapes land here:
            // an empty pointer (a path recorded before it spelled out its `States` opener — still
            // read, so an old log replays) and the tail of every descent below.
            if i >= tokens.len() {
                return Ok(states);
            }
            if tokens[i].decoded().as_ref() != StatePath::STATES {
                return Err(StatePathError::Malformed(format!(
                    "state_path malformed: expected '{}', got '{}'",
                    StatePath::STATES,
                    tokens[i].decoded()
                )));
            }
            i += 1; // consumed the opener
            // An opener with nothing behind it *is* the target: `/States` is the top-level table.
            if i >= tokens.len() {
                return Ok(states);
            }
            let name = tokens[i].decoded();
            let state = states
                .get(name.as_ref())
                .ok_or_else(|| StatePathError::NotFound(name.as_ref().to_string()))?;
            i += 1; // consumed the state name
            match state {
                // A `Parallel` descent names a branch — `Branches/<idx>` — whose `States` opener the
                // next iteration consumes.
                S::Parallel(p) => {
                    if !tokens
                        .get(i)
                        .is_some_and(|t| t.decoded() == StatePath::BRANCHES)
                    {
                        return Err(StatePathError::Malformed(format!(
                            "state_path malformed: expected '{}'",
                            StatePath::BRANCHES
                        )));
                    }
                    let idx: usize = tokens
                        .get(i + 1)
                        .ok_or_else(|| {
                            StatePathError::NotFound("state_path truncated at branch index".into())
                        })?
                        .decoded()
                        .parse()
                        .map_err(|_| {
                            StatePathError::Malformed(
                                "state_path branch index is not an integer".into(),
                            )
                        })?;
                    let branch = p.branches.get(idx).ok_or_else(|| {
                        StatePathError::NotFound(format!(
                            "branch index {idx} of state {}",
                            name.as_ref()
                        ))
                    })?;
                    i += 2; // consumed `Branches` + the index
                    states = &branch.states;
                }
                // A `Map` descent names its single `ItemProcessor` (no index: every item runs the
                // same processor), whose `States` opener the next iteration consumes.
                S::Map(m) => {
                    if !tokens
                        .get(i)
                        .is_some_and(|t| t.decoded() == StatePath::ITEM_PROCESSOR)
                    {
                        return Err(StatePathError::Malformed(format!(
                            "state_path malformed: expected '{}'",
                            StatePath::ITEM_PROCESSOR
                        )));
                    }
                    let processor = m.item_processor.as_ref().ok_or_else(|| {
                        StatePathError::Malformed(format!(
                            "Map state '{}' has no ItemProcessor",
                            name.as_ref()
                        ))
                    })?;
                    i += 1; // consumed the item processor key
                    states = &processor.states;
                }
                // Any other state cannot be descended into: a walk step must name the child of a
                // container.
                _ => {
                    return Err(StatePathError::Malformed(format!(
                        "state_path step '{}' is not a Parallel or Map state",
                        name.as_ref()
                    )));
                }
            }
            // Loop: the next token is either another opener (a nested container) or the walk ended.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One document exercising both container kinds and every nesting shape a path can spell:
    /// `P` (Parallel, two branches), `M` (Map with an item processor), `S` (a non-container), and an
    /// item processor whose single state is itself a `Parallel` (a mixed descent).
    fn document() -> StateMachine {
        serde_json::from_str(
            r#"{
              "StartAt": "S",
              "States": {
                "S": { "Type": "Pass", "End": true },
                "P": {
                  "Type": "Parallel",
                  "Branches": [
                    { "StartAt": "Inner", "States": { "Inner": { "Type": "Pass", "End": true } } },
                    { "StartAt": "Deep",  "States": { "Deep":  { "Type": "Pass", "End": true } } }
                  ],
                  "End": true
                },
                "M": {
                  "Type": "Map",
                  "ItemProcessor": {
                    "StartAt": "Nested",
                    "States": {
                      "Nested": {
                        "Type": "Parallel",
                        "Branches": [
                          { "StartAt": "Leaf", "States": { "Leaf": { "Type": "Pass", "End": true } } }
                        ],
                        "End": true
                      }
                    }
                  },
                  "End": true
                }
              }
            }"#,
        )
        .expect("the fixture is a well-formed document")
    }

    /// The path spelled by a token list — exactly what `StatePath`'s own builders compose.
    fn path(tokens: &[&str]) -> StatePath {
        let mut p = jsonptr::PointerBuf::new();
        for t in tokens {
            p.push_back(*t);
        }
        StatePath::from(p)
    }

    /// The builder's own derivations land on the tables and states the walk resolves: a branch's
    /// path is a *table* path, and its sibling derivation is how a transition routes.
    #[test]
    fn the_builders_compose_paths_the_walk_resolves() {
        let sm = document();
        let branch = StatePath::root().state("P").branch(1);
        assert!(sm.state_at(&branch.state("Deep")).is_ok());

        let item = StatePath::root().state("M").item_processor();
        assert!(sm.state_at(&item.state("Nested")).is_ok());

        // A sibling derivation stays inside the table it was derived from: branch 1's own states
        // route among themselves, so `Inner`→`Deep` there names the branch's `Deep`, not a top-level
        // one (there is none).
        let sibling = branch.state("Inner").sibling("Deep");
        assert_eq!(sibling.state_name(), "Deep");
        assert!(sm.state_at(&sibling).is_ok());
    }

    /// Every top-level state resolves through the opener path `/States/<name>` — the walk's shortest
    /// instance, and the shape a root thread's entry activity carries.
    #[test]
    fn a_top_level_state_resolves_through_the_opener_path() {
        let sm = document();
        for name in ["S", "P", "M"] {
            assert!(
                sm.state_at(&StatePath::root().state(name)).is_ok(),
                "`/States/{name}` names a top-level state"
            );
        }
    }

    /// A path recorded before it spelled out its `States` opener still resolves (an old log replays):
    /// the parent handed to the walk is then empty, which names the top-level table.
    #[test]
    fn a_path_that_omits_its_opener_still_resolves() {
        let sm = document();
        assert!(sm.state_at(&path(&["S"])).is_ok());
    }

    #[test]
    fn a_branch_state_resolves_by_its_branch_index() {
        let sm = document();
        let state = sm
            .state_at(&path(&["States", "P", "Branches", "1", "States", "Deep"]))
            .expect("branch 1 holds `Deep`");
        assert!(matches!(state, State::Pass(_)));
    }

    #[test]
    fn an_item_processor_state_resolves() {
        let sm = document();
        assert!(
            sm.state_at(&path(&["States", "M", "ItemProcessor", "States", "Nested"]))
                .is_ok()
        );
    }

    #[test]
    fn parallel_and_map_steps_compose_in_one_walk() {
        let sm = document();
        assert!(
            sm.state_at(&path(&[
                "States",
                "M",
                "ItemProcessor",
                "States",
                "Nested",
                "Branches",
                "0",
                "States",
                "Leaf"
            ]))
            .is_ok()
        );
    }

    /// Each branch's table is its own: the sibling branch's state is unreachable from this one, so a
    /// path's branch index — not the state name alone — is what selects the table.
    #[test]
    fn each_branch_resolves_only_its_own_states() {
        let sm = document();
        let branch = StatePath::root().state("P").branch(0);
        assert!(sm.state_at(&branch.state("Inner")).is_ok());
        assert_eq!(
            sm.state_at(&branch.state("Deep")),
            Err(StatePathError::NotFound("Deep".into()))
        );
    }

    #[test]
    fn a_step_that_is_not_an_opener_is_malformed() {
        let sm = document();
        assert_eq!(
            sm.state_at(&path(&["Branches", "P"])),
            Err(StatePathError::Malformed(
                "state_path malformed: expected 'States', got 'Branches'".into()
            ))
        );
    }

    #[test]
    fn a_descent_into_a_parallel_without_its_branches_key_is_malformed() {
        let sm = document();
        assert_eq!(
            sm.state_at(&path(&["States", "P", "States", "Inner"])),
            Err(StatePathError::Malformed(
                "state_path malformed: expected 'Branches'".into()
            ))
        );
    }

    #[test]
    fn a_descent_into_a_map_without_its_item_processor_key_is_malformed() {
        let sm = document();
        assert_eq!(
            sm.state_at(&path(&["States", "M", "States", "Nested"])),
            Err(StatePathError::Malformed(
                "state_path malformed: expected 'ItemProcessor'".into()
            ))
        );
    }

    #[test]
    fn a_step_into_a_non_container_state_is_malformed() {
        let sm = document();
        assert_eq!(
            sm.state_at(&path(&["States", "S", "States", "S"])),
            Err(StatePathError::Malformed(
                "state_path step 'S' is not a Parallel or Map state".into()
            ))
        );
    }

    #[test]
    fn a_missing_state_is_not_found() {
        let sm = document();
        assert_eq!(
            sm.state_at(&path(&["States", "Nope"])),
            Err(StatePathError::NotFound("Nope".into()))
        );
    }

    #[test]
    fn a_branch_index_off_the_end_is_not_found() {
        let sm = document();
        assert_eq!(
            sm.state_at(&path(&["States", "P", "Branches", "9", "States", "Inner"])),
            Err(StatePathError::NotFound("branch index 9 of state P".into()))
        );
    }

    #[test]
    fn a_non_integer_branch_index_is_malformed() {
        let sm = document();
        assert_eq!(
            sm.state_at(&path(&["States", "P", "Branches", "first", "Inner"])),
            Err(StatePathError::Malformed(
                "state_path branch index is not an integer".into()
            ))
        );
    }

    #[test]
    fn a_step_through_the_wrong_container_key_is_malformed() {
        let sm = document();
        // `ItemProcessor` names a `Parallel`, so its descent must be `Branches`, not a repeat.
        assert_eq!(
            sm.state_at(&path(&[
                "States",
                "M",
                "ItemProcessor",
                "States",
                "Nested",
                "ItemProcessor",
                "Leaf"
            ])),
            Err(StatePathError::Malformed(
                "state_path malformed: expected 'Branches'".into()
            ))
        );
    }

    /// The constants are pointer *tokens*, so a rename here silently invalidates every recorded
    /// path. This pins them against the document the model actually serializes.
    #[test]
    fn constants_spell_the_keys_the_model_serializes() {
        let definition = r#"{
            "StartAt": "P",
            "States": {
                "P": {
                    "Type": "Parallel",
                    "Branches": [
                        { "StartAt": "A", "States": { "A": { "Type": "Succeed" } } }
                    ],
                    "End": true
                },
                "M": {
                    "Type": "Map",
                    "ItemProcessor": {
                        "StartAt": "A",
                        "States": { "A": { "Type": "Succeed" } }
                    },
                    "End": true
                }
            }
        }"#;
        let sm: StateMachine =
            serde_json::from_str(definition).expect("the fixture parses as a state machine");
        let doc = serde_json::to_value(&sm).expect("a state machine serializes");

        let states = &doc[StatePath::STATES];
        assert!(
            states.get("P").is_some(),
            "no '{}' table at the root",
            StatePath::STATES
        );
        assert!(
            states["P"][StatePath::BRANCHES][0]
                .get(StatePath::STATES)
                .is_some(),
            "a '{}' entry has no '{}'",
            StatePath::BRANCHES,
            StatePath::STATES
        );
        assert!(
            states["M"][StatePath::ITEM_PROCESSOR]
                .get(StatePath::STATES)
                .is_some(),
            "'{}' has no '{}'",
            StatePath::ITEM_PROCESSOR,
            StatePath::STATES
        );

        for legacy in ["states", "branches", "item_processor"] {
            assert!(
                doc.pointer(&format!("/{legacy}")).is_none(),
                "the document key is not '{legacy}' — the pointer grammar is case-sensitive"
            );
        }
    }
}
