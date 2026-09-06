//! Object naming — the validated, user-supplied name segments shared by every spica object.
//!
//! Every object is addressed by `(tenant, namespace, kind, name)`; the name layer here is the
//! foundation for that model:
//!
//! - **User-supplied segments** (`tenant`, `namespace`, user-supplied `name`, `FlowName`) are all
//!   one type — [`PlainName`] (aliased as [`ScopeName`] / `FlowName`) — sharing one 4..=64 charset,
//!   and **never contain `-`**.
//! - **`-` is reserved** for the system's `generateName` suffix separator ([`PlainName::generated_from_key`]).
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
/// (storage key prefixes, `Scope` addresses) while sharing one validation / serde / `as_str`
/// surface.
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
    /// is the **decimal** of a caller-supplied `key` — the partition-local generated-name counter (see
    /// `crate::storage` `next_generated_seq`) — making the child name unique within the partition
    /// **by construction** (a monotonic counter never repeats a suffix). Centralized here so every
    /// child-naming site shares the same rule; the hyphen + suffix fall outside the user-name length
    /// budget (see [`PlainName::generated_from_key`]).
    ///
    /// The caller passes the key, which it read from the partition's persisted counter before
    /// minting; the matching create applier bumps that counter past this key in the same batch, so
    /// the next mint in the same batch sees a fresh value. The child's own `uid` (a ULID) is
    /// deliberately **not** the suffix source: a u64 key is partition-scoped and needs no global
    /// coordination, whereas `uid` is globally unique and decoupled from the (namespaced) name.
    pub fn generated_from_key(&self, key: u64) -> ObjectName {
        ObjectName::Generated(GeneratedName {
            base: self.clone(),
            suffix: key,
        })
    }
}

impl std::fmt::Display for PlainName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A system-generated name: a plain `base` joined to a decimal `u64` `suffix` by `-` (k8s
/// `generateName`). `base` is **structurally a [`PlainName`]** — it can never itself be a generated
/// name, so nested generated names are unrepresentable (the decided rule: a generated child's base
/// is always a user-supplied plain name). The suffix is the partition-local generated-name counter
/// (see [`PlainName::generated_from_key`]), so the generated space is monotonic, never random.
///
/// Only the two structured fields are kept — no cached `full` string. The canonical `{base}-{suffix}`
/// form is rebuilt on demand by [`ObjectName::as_str`], trading a per-name duplicated `String` for a
/// transient allocation when the full form is actually read (a name is keyed/compared far less often
/// than it is stored).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GeneratedName {
    base: PlainName,
    suffix: u64,
}

/// The *name* part of an object's identity.
///
/// Two flavors, distinguished by construction:
/// - **plain** names ([`ObjectName::Plain`], built by [`ObjectName::plain`]) — the same charset as
///   [`ScopeName`] (no `-`); for persistent, client-created objects this **is** the idempotency key.
/// - **generated** names ([`ObjectName::Generated`], k8s `generateName`) — a plain `<base>` joined
///   to a decimal `<suffix>` by `-`, e.g. `checkout-123` (the positional suffix of a generated
///   object), for system-created children.
///
/// The flavors are disjoint **by type**: a plain name can never contain `-`, so it can never equal a
/// generated name — making [`ObjectName::is_generated`] a total match rather than a string scan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectName {
    /// A plain, user-supplied name.
    Plain(PlainName),
    /// A system-generated name (a plain base plus a decimal counter suffix).
    Generated(GeneratedName),
}

impl ObjectName {
    /// A user-supplied plain name; rejects any input containing `-`. The 4..=64 length floor is
    /// enforced by [`PlainName::new`] (this flavor *is* a `PlainName`), so callers need no extra
    /// check here. Mirrors the plain flavor [`ObjectName::Plain`].
    pub fn plain(raw: &str) -> Result<Self, NameError> {
        Ok(ObjectName::Plain(PlainName::new(raw)?))
    }

    /// The base name — `self` for a plain name, the (plain) base for a generated name. A generated
    /// name's base is always plain by construction.
    pub fn base(&self) -> &PlainName {
        match self {
            ObjectName::Plain(p) => p,
            ObjectName::Generated(g) => &g.base,
        }
    }

    /// The generated suffix (the partition counter) when this is a generated name; `None` for a plain
    /// name.
    pub fn suffix(&self) -> Option<u64> {
        match self {
            ObjectName::Plain(_) => None,
            ObjectName::Generated(g) => Some(g.suffix),
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

    /// Reconstruct a name from an address string's Display form, accepting either the plain or the
    /// generated flavor. Generated names may exceed the 64-char user base cap (the suffix is
    /// system-added), so the overall length is **not** capped here — only `base` must be
    /// a valid ≤ 64-char plain name. A `-` marks the generated flavor; since `base` is plain and the
    /// suffix decimal, a generated name carries **exactly one** `-`.
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
            // The generated flavor is built over a plain base: split the single `-` (a plain base
            // never contains one, so the `{base}-{suffix}` form is always reconstructable) and mint
            // from the decimal suffix exactly as a counter-minted name would.
            Some((base, suffix)) => {
                let suffix = suffix.parse::<u64>().map_err(|_| {
                    NameError::Invalid(format!(
                        "invalid generateName suffix {suffix:?}: must be a decimal u64"
                    ))
                })?;
                Ok(PlainName::new(base)?.generated_from_key(suffix))
            }
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

        // Generated names join base + decimal counter suffix with '-'.
        let generated_name = PlainName::new("checkout")
            .unwrap()
            .generated_from_key(12_345_678);
        assert_eq!(generated_name.as_str(), "checkout-12345678");
        assert!(generated_name.is_generated());

        // A user name is never generated.
        assert!(!ObjectName::plain("checkout").unwrap().is_generated());

        // A generated base must be a plain (user) name, so it reaches `generated_from_key` only over a
        // validated base: nested generated names (`{generated}-{suffix}`) and empty bases are rejected
        // by `PlainName::new` itself.
        assert!(PlainName::new("checkout-flow").is_err()); // '-', reserved for generated names
        assert!(PlainName::new("").is_err());
        // The hyphen + suffix are system-added and fall outside the user-name budget: a base of 60
        // (itself a valid name) may carry an 8-digit suffix past the 64-char user limit — only the
        // base is bounded, so the combined 69 chars are accepted, while a > 64-char base is not.
        let wide = PlainName::new(&"x".repeat(60))
            .unwrap()
            .generated_from_key(12_345_678);
        assert_eq!(wide.as_str().len(), 69);
        assert!(PlainName::new(&"x".repeat(65)).is_err());

        // `generated_from_key` builds `{base}-{key}` over a plain base. The base is a `PlainName`
        // (reached via `as_plain`); a generated name has no plain base to hang a child off.
        let parent = ObjectName::plain("checkout").unwrap();
        let base = parent.as_plain().expect("user name is the Plain flavor");
        let child = base.generated_from_key(7);
        assert!(child.is_generated());
        assert!(child.as_plain().is_none()); // generated names carry no plain base
        assert_eq!(child.base().as_str(), "checkout");
        let full = child.as_str();
        let (b, suffix) = full.rsplit_once('-').unwrap();
        assert_eq!(b, "checkout");
        assert_eq!(suffix, "7");
        assert_eq!(suffix.parse::<u64>().unwrap(), 7);
        assert_eq!(child.suffix(), Some(7));
        // A distinct key yields a distinct name: uniqueness is by-construction (never a random 40-bit
        // tail), so the same base + different keys never collide.
        assert_ne!(child, base.generated_from_key(8));
    }

    #[test]
    fn object_name_serde_roundtrips_as_plain_string() {
        // The enum serializes to its canonical string (not an enum tag), so storage keys / event-log
        // strings stay byte-identical to the pre-refactor `String`-backed newtype.
        for n in [
            ObjectName::plain("checkout").unwrap(),
            PlainName::new("checkout")
                .unwrap()
                .generated_from_key(12_345_678),
            // A generated name may exceed 64 chars (suffix is system-added); it must
            // still round-trip despite the base being only 60 chars.
            PlainName::new(&"x".repeat(60))
                .unwrap()
                .generated_from_key(12_345_678),
        ] {
            let wire = serde_json::to_string(&n).unwrap();
            assert_eq!(wire, format!("\"{}\"", n.as_str()));
            assert_eq!(serde_json::from_str::<ObjectName>(&wire).unwrap(), n);
        }
    }
}
