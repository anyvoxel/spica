use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

/// A JSON Pointer locating a sub-`States` table (or a state's own definition within one) inside the
/// shared machine document — e.g. `/States` for the machine's own top-level table (a root thread's
/// scope), `/States/<name>` for a top-level state, or
/// `/States/P1/Branches/0/States/P2/ItemProcessor/States` for a nested Parallel-branch / Map-item
/// descent. Distinct from an arbitrary JSON Pointer: its tokens form a well-formed walk over the
/// machine's `States` / `Branches` / `ItemProcessor` hierarchy (see
/// `resolve_states_map`), so wrap the pointer and host the
/// derivations the container handlers otherwise repeat inline.
///
/// The ownership model kicked this off the plain [`jsonptr::PointerBuf`]: a `Parallel`/`Map`
/// extends its own `state_path` to build each child's path, and the leaf is the state's name — both
/// are `state_path`-specific and had been re-derived in every container.
///
/// Its tokens are the definition document's own keys, spelled as `crates/asl` serializes them
/// (PascalCase), so the constants below are the grammar's whole vocabulary and the walk compares
/// them case-sensitively: a pointer recorded against a differently-spelled key is malformed, not a
/// silently different table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatePath(jsonptr::PointerBuf);

impl StatePath {
    /// The table of named states — the machine's own, or a nested scope's (a branch's, or a Map's
    /// item processor's).
    pub const STATES: &str = "States";

    /// A `Parallel` state's array of branch sub-state-machines.
    pub const BRANCHES: &str = "Branches";

    /// A `Map` state's single per-item sub-state-machine.
    pub const ITEM_PROCESSOR: &str = "ItemProcessor";

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

#[cfg(test)]
mod tests {
    use super::StatePath;

    /// The constants are pointer *tokens*, so a rename in `crates/asl` silently invalidates every
    /// recorded path. This pins them against the document the model actually serializes.
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
        let sm: spica_asl::StateMachine =
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
