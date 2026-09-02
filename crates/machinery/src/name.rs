//! Object naming — the validated, user-supplied name segments shared by every spica object.
//!
//! Every object is addressed by `(tenant, namespace, kind, name)`; the name layer here is the
//! foundation for that model:
//!
//! - **User-supplied segments** (`tenant`, `namespace`, user-supplied `name`, `FlowName`) are all
//!   one type — [`PlainName`] (aliased as [`ScopeName`] / `FlowName`) — sharing one 4..=64 charset,
//!   and **never contain `-`**.
//! - **`-` is reserved** for the system's `generateName` suffix separator ([`ObjectName::generated_with_suffix`]).
//!   Because a plain name bans `-`, the two flavors are disjoint **by type** ([`ObjectName::Plain`] /
//!   [`ObjectName::Generated`]): the user-name space and generated-name space never overlap, and
//!   "is this generated?" is a total match rather than a string scan.
//!
//! This crate is the leaf kernel, so a validation failure surfaces as the minimal
//! [`NameError`] here rather than a domain error from a consumer; consumers adapt it into their own
//! error model (e.g. `spica-engine` folds it into `RuntimeError::InvalidDefinition`).
//! See `docs/identity-and-partitioning-design.md` for the full design.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// The low-level error for an invalid name/segment — the leaf kernel's only error, deliberately
/// thin (no domain categorization). A consumer maps it onto its own duplicate of this concern
/// (e.g. `spica-engine`'s `RuntimeError::InvalidDefinition`) at the crate boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NameError {
    #[error("{0}")]
    Invalid(String),
}

/// A scoping dimension (`tenant` or `namespace`): the k8s-slot identity prefix under which objects
/// live in storage. A scope segment obeys exactly the same 4..=64 user-name rules as [`PlainName`]
/// (charset is `PlainName`'s, `-` reserved), so instead of a duplicate newtype this is a
/// **type alias of [`PlainName`]** — the readable, semantic `ScopeName` spelling stays at call sites
/// (storage key prefixes, `Scope` addresses, `ObjectAddress` parsing) while sharing one validation /
/// serde / `as_str` surface.
pub type ScopeName = PlainName;

/// A validated, user-supplied name segment: 4..=64 bytes, first char an ASCII letter/digit, remaining
/// chars ASCII alnum or `_` (see [`PlainName::new`]). **`-` is structurally impossible here** — that
/// is exactly what lets a [`GeneratedName`]'s base never itself be a generated name (no nested
/// `{...}-{...}`), decided by type rather than by a string scan.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PlainName(String);

impl PlainName {
    /// Validate `raw` as a plain name segment and wrap it. Rejects `-` (reserved for the
    /// `generateName` separator), any non-alnum/`_` byte, and any name shorter than 4 chars — the
    /// 4..=64 length floor lives here (not in a caller) so a short, trivially-collidable/referable
    /// name is rejected at the single choke point every plain name flows through, `FlowName`
    /// included (it is an alias of `PlainName`). `-` is reserved for generated names only.
    ///
    /// The charset is checked inline — it has exactly one caller, so a shared helper would only add
    /// indirection. The `raw.len() >= 4` guard short-circuits before `bytes[0]` is indexed, so an
    /// empty input is rejected without an out-of-bounds panic.
    pub fn new(raw: &str) -> Result<Self, NameError> {
        let bytes = raw.as_bytes();
        let valid = raw.len() >= 4
            && raw.len() <= 64
            && bytes[0].is_ascii_alphanumeric()
            && bytes
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'_');
        if !valid {
            return Err(NameError::Invalid(format!(
                "invalid plain object name {raw:?}: must be 4..=64 chars of A-Za-z0-9_; '-' is reserved for generated names"
            )));
        }
        Ok(Self(raw.to_string()))
    }

    /// The name as a `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Convert this plain name into a system-generated child name: `{self}-{suffix}` where `suffix`
    /// is an **8-char** token derived from a freshly-minted uid. Centralized here so every
    /// child-naming site shares the same rule; the hyphen + suffix fall outside the user-name length
    /// budget (see [`ObjectName::generated_with_suffix`]).
    ///
    /// The suffix is the 8-char tail of the uid's ULID string. A ULID's low 80 bits are random (its
    /// high 48 are a timestamp), so this tail carries 40 fresh random bits per child — a
    /// uniformly-distributed token. The uid is minted **inside** this method (the caller hands in no
    /// uid): only its 8-char tail is materialized into the name, which is what gets persisted, so the
    /// child's own `uid` and its name suffix are deliberately decoupled.
    ///
    /// TODO(name-collision, multi-child): today uniqueness rests on the random suffix alone — fine
    /// while there is at most one generated child per base (the ExecutionTimeout timer), but a
    /// silent overwrite the moment two children share an 8-char tail. When a base can own several
    /// generated children, probe storage by name before committing (k8s-style Get → 409/retry with a
    /// fresh uid): on a hit re-mint a fresh suffix and re-derive, in-handler before the event enters
    /// the batch, keeping the CCES same-batch determinism.
    pub fn to_generated(&self) -> ObjectName {
        let uid = ulid::Ulid::new();
        let s = uid.to_string();
        let suffix = s[s.len() - 8..].to_string();
        ObjectName::Generated(GeneratedName {
            base: self.clone(),
            suffix,
        })
    }
}

impl std::fmt::Display for PlainName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A system-generated name: a plain `base` joined to a random alnum `suffix` by `-` (k8s
/// `generateName`). `base` is **structurally a [`PlainName`]** — it can never itself be a generated
/// name, so nested generated names are unrepresentable (the decided rule: a generated child's base
/// is always a user-supplied plain name).
///
/// Only the two structured fields are kept — no cached `full` string. The canonical `{base}-{suffix}`
/// form is rebuilt on demand by [`ObjectName::as_str`], trading a per-name duplicated `String` for a
/// transient allocation when the full form is actually read (a name is keyed/compared far less often
/// than it is stored).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GeneratedName {
    base: PlainName,
    suffix: String,
}

/// The *name* part of an object's identity.
///
/// Two flavors, distinguished by construction:
/// - **plain** names ([`ObjectName::Plain`], built by [`ObjectName::plain`]) — the same charset as
///   [`ScopeName`] (no `-`); for persistent, client-created objects this **is** the idempotency key.
/// - **generated** names ([`ObjectName::Generated`], k8s `generateName`) — a plain `<base>` joined
///   to a random `<suffix>` by `-`, e.g. `checkout-a1b2c`, for system-created children.
///
/// The flavors are disjoint **by type**: a plain name can never contain `-`, so it can never equal a
/// generated name — making [`ObjectName::is_generated`] a total match rather than a string scan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectName {
    /// A plain, user-supplied name.
    Plain(PlainName),
    /// A system-generated name (a plain base plus a random suffix).
    Generated(GeneratedName),
}

impl ObjectName {
    /// A user-supplied plain name; rejects any input containing `-`. The 4..=64 length floor is
    /// enforced by [`PlainName::new`] (this flavor *is* a `PlainName`), so callers need no extra
    /// check here. Mirrors the plain flavor [`ObjectName::Plain`].
    pub fn plain(raw: &str) -> Result<Self, NameError> {
        Ok(ObjectName::Plain(PlainName::new(raw)?))
    }

    /// A system-generated name `<base>-<suffix>` (k8s `generateName`). `base` must be a valid plain
    /// (user) name — structurally a [`PlainName`], so it can **never** be a generated name (no nested
    /// base) and is subject to the same 4..=64 length floor. `suffix` must be non-empty ASCII
    /// alphanumeric (a random token). The hyphen + suffix are **system-added** and fall outside the
    /// user-name length budget, so the combined length is **not** capped at the 64-char user limit —
    /// only `base` must itself be a valid (4..=64 char) name.
    pub fn generated_with_suffix(base: &str, suffix: &str) -> Result<Self, NameError> {
        let base = PlainName::new(base)?;
        if suffix.is_empty() || !suffix.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err(NameError::Invalid(format!(
                "invalid generateName suffix {suffix:?}: must be non-empty ASCII alphanumeric"
            )));
        }
        Ok(ObjectName::Generated(GeneratedName {
            base: base.clone(),
            suffix: suffix.to_string(),
        }))
    }

    /// The base name — `self` for a plain name, the (plain) base for a generated name. A generated
    /// name's base is always plain by construction.
    pub fn base(&self) -> &PlainName {
        match self {
            ObjectName::Plain(p) => p,
            ObjectName::Generated(g) => &g.base,
        }
    }

    /// The random suffix when this is a generated name; `None` for a plain name.
    pub fn suffix(&self) -> Option<&str> {
        match self {
            ObjectName::Plain(_) => None,
            ObjectName::Generated(g) => Some(&g.suffix),
        }
    }

    /// `Some` (the underlying plain name) when this is the `Plain` flavor — the callers that derive a
    /// child's name need to reach a [`PlainName`] base.
    pub fn as_plain(&self) -> Option<&PlainName> {
        match self {
            ObjectName::Plain(p) => Some(p),
            ObjectName::Generated(_) => None,
        }
    }

    /// True iff this name is system-generated. Total and unambiguous: the plain and generated flavors
    /// are disjoint by type (a plain name never contains `-`).
    pub fn is_generated(&self) -> bool {
        matches!(self, ObjectName::Generated(_))
    }

    /// The canonical single-string form: the plain name, or `{base}-{suffix}` for a generated name.
    /// Returns an owned `String`: a generated name has no cached full form, so it is rebuilt here on
    /// demand (see [`GeneratedName`]).
    pub fn as_str(&self) -> String {
        match self {
            ObjectName::Plain(p) => p.as_str().to_string(),
            ObjectName::Generated(g) => format!("{}-{}", g.base.as_str(), g.suffix),
        }
    }

    /// Interpret this name as a user-style flow name: `Some` when it is a plain user name, `None`
    /// for a generated name. The plumbing/return type is generic over the plain-name flavor so the
    /// same view serves whichever name alias a consumer uses (`FlowName`/`ScopeName` are aliases of
    /// `PlainName`).
    pub fn as_flow_name(&self) -> Option<PlainName> {
        // A plain name is by construction a valid flow/scope name — identical charset — so this is
        // an infallible clone; a generated name is never a user name.
        match self {
            ObjectName::Plain(p) => Some(p.clone()),
            ObjectName::Generated(_) => None,
        }
    }

    /// Reconstruct a name from an address string's Display form, accepting either the plain or the
    /// generated flavor. Generated names may exceed the 64-char user base cap (the suffix is
    /// system-added and unbounded), so the overall length is **not** capped here — only `base` must be
    /// a valid ≤ 64-char plain name. A `-` marks the generated flavor; since `base` is plain and the
    /// suffix alphanumeric, a generated name carries **exactly one** `-`.
    pub fn from_parsed(raw: &str) -> Result<Self, NameError> {
        let bytes = raw.as_bytes();
        let ok = !raw.is_empty()
            && bytes[0].is_ascii_alphanumeric()
            && bytes
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-');
        if !ok {
            return Err(NameError::Invalid(format!(
                "invalid object name {raw:?} in address"
            )));
        }
        match raw.split_once('-') {
            // Delegated: the generated flavor is constructed by `generated_with_suffix`, which
            // enforces the same base `PlainName` + alnum-suffix rules (and can never admit a nested
            // `-` in `suffix`, keeping the canonical `{base}-{suffix}` form reconstructable).
            Some((base, suffix)) => Self::generated_with_suffix(base, suffix),
            None => Ok(ObjectName::Plain(PlainName::new(raw)?)),
        }
    }
}

impl std::fmt::Display for ObjectName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for ObjectName {
    type Err = NameError;

    /// Parse the canonical string form back into a name (see [`ObjectName::from_parsed`]).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_parsed(s)
    }
}

impl PartialOrd for ObjectName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ObjectName {
    /// Order by the canonical string, not by variant discriminant — preserving the pre-refactor
    /// lexicographic ordering that range scans over storage keys depend on.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(&other.as_str())
    }
}

impl Serialize for ObjectName {
    /// Serialize as the canonical string (never an enum tag), so the wire / on-disk form stays
    /// byte-identical to the pre-refactor `String`-backed newtype.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.as_str())
    }
}

impl<'de> Deserialize<'de> for ObjectName {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::from_parsed(&raw).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_name_validates_user_charset() {
        // Valid.
        assert!(ScopeName::new("myco").is_ok());
        assert!(ScopeName::new("payments_2").is_ok());
        assert!(ScopeName::new(&"x".repeat(64)).is_ok());
        // Invalid.
        assert!(ScopeName::new("").is_err()); // empty
        assert!(ScopeName::new(&"x".repeat(65)).is_err()); // too long
        assert!(ScopeName::new("_lead").is_err()); // leading underscore
        assert!(ScopeName::new("has-dash").is_err()); // '-' is reserved for generated names
        assert!(ScopeName::new("a/b").is_err()); // '/' is the address separator
        assert!(ScopeName::new("a b").is_err()); // whitespace
        assert!(ScopeName::new("中文").is_err()); // non-ASCII
    }

    #[test]
    fn object_name_user_forbids_dash_generated_allows_it() {
        // User names are the segment charset; '-' is rejected.
        assert!(ObjectName::plain("checkout_flow").is_ok());
        assert!(ObjectName::plain("checkout-flow").is_err());
        // User names must be 4..=64 chars (the floor avoids unreferable short names).
        assert!(ObjectName::plain("abc").is_err()); // 3 chars — too short
        assert!(ObjectName::plain("abcd").is_ok()); // 4 chars — minimum
        assert!(ObjectName::plain(&"x".repeat(64)).is_ok()); // 64 — maximum
        assert!(ObjectName::plain(&"x".repeat(65)).is_err()); // 65 — too long

        // Generated names join base + random suffix with '-'.
        let generated_name = ObjectName::generated_with_suffix("checkout", "a1b2c3d4").unwrap();
        assert_eq!(generated_name.as_str(), "checkout-a1b2c3d4");
        assert!(generated_name.is_generated());

        // A user name is never generated.
        assert!(!ObjectName::plain("checkout").unwrap().is_generated());

        // Generated base must be a plain (user) name: a dashed base is rejected — nested generated
        // names (`{generated}-{suffix}`) are deliberately disallowed, as is an empty base.
        assert!(ObjectName::generated_with_suffix("checkout-flow", "abc").is_err());
        assert!(ObjectName::generated_with_suffix("", "abc").is_err());
        // Suffix must be non-empty ASCII alphanumeric.
        assert!(ObjectName::generated_with_suffix("checkout", "").is_err());
        assert!(ObjectName::generated_with_suffix("checkout", "ab-c").is_err());
        // The hyphen + suffix are system-added and fall outside the user-name budget: a base of 60
        // (itself a valid name) may carry an 8-char suffix past the 64-char user limit — only the
        // base is bounded, so the combined 69 chars are accepted, while a > 64-char base is not.
        assert!(ObjectName::generated_with_suffix(&"x".repeat(60), &"y".repeat(8)).is_ok());
        assert!(ObjectName::generated_with_suffix(&"x".repeat(65), "y").is_err());

        // `to_generated` mints its own suffix uid internally and derives an 8-char suffix over a
        // plain base: name = `{base}-{8 chars}`. The base is a `PlainName` (reached via `as_plain`);
        // a generated name has no plain base to hang a child off.
        let parent = ObjectName::plain("checkout").unwrap();
        let base = parent.as_plain().expect("user name is the Plain flavor");
        let child = base.to_generated();
        assert!(child.is_generated());
        assert!(child.as_plain().is_none()); // generated names carry no plain base
        assert_eq!(child.base().as_str(), "checkout");
        let full = child.as_str();
        let (b, suffix) = full.rsplit_once('-').unwrap();
        assert_eq!(b, "checkout");
        assert_eq!(suffix.len(), 8);
        assert!(suffix.bytes().all(|b| b.is_ascii_alphanumeric()));
        assert_eq!(child.suffix(), Some(suffix));
        // Each call mints its own uid, so two calls yield distinct names (uniqueness per call).
        assert_ne!(child, base.to_generated());
    }

    #[test]
    fn object_name_serde_roundtrips_as_plain_string() {
        // The enum serializes to its canonical string (not an enum tag), so storage keys / event-log
        // strings stay byte-identical to the pre-refactor `String`-backed newtype.
        for n in [
            ObjectName::plain("checkout").unwrap(),
            ObjectName::generated_with_suffix("checkout", "a1b2c3d4").unwrap(),
            // A generated name may exceed 64 chars (suffix is system-added and unbounded); it must
            // still round-trip despite the base being only 60 chars.
            ObjectName::generated_with_suffix(&"x".repeat(60), &"y".repeat(8)).unwrap(),
        ] {
            let wire = serde_json::to_string(&n).unwrap();
            assert_eq!(wire, format!("\"{}\"", n.as_str()));
            assert_eq!(serde_json::from_str::<ObjectName>(&wire).unwrap(), n);
        }
    }
}
