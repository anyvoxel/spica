use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

/// A JSON Pointer locating a sub-`states` table (or a state's own definition within one) inside the
/// shared machine document — e.g. `/states/<name>` for a top-level state, or
/// `/states/P1/branches/0/states/P2/item_processor/states/…` for a nested Parallel-branch / Map-item
/// descent. Distinct from an arbitrary JSON Pointer: its tokens form a well-formed walk over the
/// machine's `states` / `branches` / `item_processor` hierarchy (see
/// [`resolve_states_map`](crate::handlers::resolve_states_map)), so wrap the pointer and host the
/// derivations the container handlers otherwise repeat inline.
///
/// The ownership model kicked this off the plain [`jsonptr::PointerBuf`]: a `Parallel`/`Map`
/// extends its own `state_path` to build each child's path, and the leaf is the state's name — both
/// are `state_path`-specific and had been re-derived in every container.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatePath(jsonptr::PointerBuf);

impl StatePath {
    /// The leaf name of the state this path names — its final (decoded) token: the key of the state
    /// in its enclosing `states` table. Empty for a document-root path (a shape no state handler
    /// should see, kept total — `jsonptr`'s [`decoded`](jsonptr::Token::decoded) API only yields a
    /// `Cow` tied to a transient token borrow, hence the owned `String` here).
    pub fn state_name(&self) -> String {
        self.0
            .last()
            .map(|t| t.decoded().into_owned())
            .unwrap_or_default()
    }

    /// The path of the `index`-th branch's `states` table under this `Parallel` state: self extended
    /// by `/branches/<index>`. A `Parallel` extends it once per child it fans out.
    pub fn branch(&self, index: usize) -> StatePath {
        let mut p = self.0.clone();
        p.push_back("branches");
        p.push_back(index);
        StatePath(p)
    }

    /// The path of this `Map`'s single `item_processor`'s `states` table: self extended by
    /// `/item_processor`. Every item runs the *same* processor, so all of a Map's item children
    /// share this one path.
    pub fn item_processor(&self) -> StatePath {
        let mut p = self.0.clone();
        p.push_back("item_processor");
        StatePath(p)
    }

    /// The path of a sibling state in the same enclosing `states` table: this path minus its own
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
