use crate::PositionSet;

/// A compact ternary (three–valued) logical view over a fixed span of
/// positional indices.
///
/// The domain is `[0, span)` where `span` is inherited from the underlying
/// [`PositionSet`] instances. Each position can be in exactly one of three
/// semantic states:
///
/// * `true`    – the position is definitely present.
/// * `false`   – the position is definitely absent.
/// * `unknown` – the position may or may not be present (indeterminate).
///
/// Internally the type selects one of three storage strategies for space /
/// performance efficiency:
///
/// * [`TernaryPositionSet::Definite`] — all positions are either definitely
///   true or definitely false (no unknowns). Stored as a single bitmap of the
///   definite `true` positions.
/// * [`TernaryPositionSet::Unknown`] — there are unknowns, but no definite
///   trues (only false / unknown). Stored as a single bitmap of the unknown
///   positions (all remaining positions are definite false). This corresponds
///   logically to `true = ∅`, `unknown = U`.
/// * [`TernaryPositionSet::Mixed`] — general case with both definite trues and
///   (possibly) unknowns. Stored as two bitmaps `(true_set, unknown_superset)`.
///
/// Representation invariant (intended): in the `Mixed` form the `true` and
/// `unknown` sets are disjoint: `true ∩ unknown = ∅`. This is enforced by a
/// debug assertion in [`check_full_invariants`]; production (release) builds do
/// not panic if the invariant is violated, but query methods cope by masking
/// appropriately (e.g. [`is_unknown`] removes any accidentally overlapping
/// `true` bits with an intersection against the inverse of `true`).
///
/// Bit‑encoding intuition:
///
/// Conceptually we can think of two logical bitmaps (not always materialised
/// as separate allocations):
///
/// * First bit – definite TRUE flag.
/// * Second bit – UNKNOWN flag.
///
/// Valid combinations are:
/// * `11` => true
/// * `01` => unknown
/// * `00` => false
/// * `10` is unused / invalid (not produced by constructors).
///
/// Construction picks the most compact variant automatically via
/// [`TernaryPositionSet::new`]. All set‑wise operations (`union`, `intersect`,
/// `negate`) preserve span and will *re‑canonicalize* the representation by
/// calling `new` internally, so downstream code can rely on variant choice
/// staying minimal.
///
/// Performance: all operations delegate to the underlying [`PositionSet`]
/// primitives (`union`, `intersect`, `invert`, etc.). Complexity therefore
/// matches the cost characteristics of those operations (typically linear in
/// the number of internal word blocks / segments). No extra allocations are
/// performed unless a new bitmap needs to be materialized for a result.
#[derive(Clone)]
pub enum TernaryPositionSet {
    /// Definite bitmap: encodes a pure boolean set (no unknowns).
    ///
    /// Semantics:
    /// * `true = bitmap`
    /// * `unknown = ∅`
    /// * `false = ¬bitmap`
    Definite(PositionSet),
    /// Unknown-only bitmap: there are no definite trues; all `1` bits are
    /// potential (unknown) positives.
    ///
    /// Semantics:
    /// * `true = ∅`
    /// * `unknown = bitmap`
    /// * `false = ¬bitmap`
    Unknown(PositionSet),
    /// Full ternary representation: first bitmap for definite trues, second
    /// bitmap a (super)set of unknowns. Invariant expectation (debug checked):
    /// `true ∩ unknown = ∅`.
    ///
    /// Semantics (conceptual logical sets):
    /// * `true = t`
    /// * `unknown = u \ t` (code applies this masking on query)
    /// * `false = ¬(t ∪ u)`
    Mixed(PositionSet, PositionSet),
}

impl TernaryPositionSet {
    /// Canonical constructor from (candidate) `true` and `unknown` bitmaps.
    ///
    /// Behavior:
    /// * Panics if spans differ.
    /// * Collapses to `Definite` if `t == u` (no unknowns) OR both are empty.
    /// * Collapses to `Unknown` if `t` is empty but `u` is not.
    /// * Otherwise returns `Mixed(t, u)` (without removing any overlap).
    ///
    /// Overlapping bits (where `t` and `u` both contain a position) are
    /// tolerated; query accessors ensure they aren't double‑counted as
    /// unknown by masking. Use [`check_full_invariants`] in debug contexts to
    /// assert disjointness during development.
    pub fn new(t: PositionSet, u: PositionSet) -> TernaryPositionSet {
        assert_eq!(t.span(), u.span());
        if t.is_equal_to(&u) {
            TernaryPositionSet::Definite(t)
        } else if t.count_positions() == 0 {
            if u.count_positions() == 0 {
                TernaryPositionSet::Definite(t)
            } else {
                TernaryPositionSet::Unknown(u)
            }
        } else {
            TernaryPositionSet::Mixed(t, u)
        }
    }

    pub fn make_true(span: u64) -> Self {
        TernaryPositionSet::Definite(PositionSet::full(span))
    }

    /// Create a ternary set where every position is definitely false.
    ///
    /// Equivalent to a `Definite` with an empty internal bitmap.
    pub fn make_false(span: u64) -> Self {
        TernaryPositionSet::Definite(PositionSet::empty(span))
    }

    /// Create a ternary set where every position is unknown (no definite
    /// trues or falses beyond that minimal knowledge).
    pub fn make_unknown(span: u64) -> Self {
        TernaryPositionSet::Unknown(PositionSet::full(span))
    }

    /// Total number of addressable positions (the span) – not the count of
    /// true / unknown bits.
    pub fn len(&self) -> usize {
        match self {
            TernaryPositionSet::Definite(t) => t.span() as usize,
            TernaryPositionSet::Unknown(u) => u.span() as usize,
            TernaryPositionSet::Mixed(t, _) => t.span() as usize,
        }
    }

    /// Return a bitmap of positions that are definitely true.
    pub fn is_true(&self) -> PositionSet {
        match self {
            TernaryPositionSet::Definite(t) => t.clone(),
            TernaryPositionSet::Unknown(u) => PositionSet::empty(u.span()),
            TernaryPositionSet::Mixed(t, _) => t.clone(),
        }
    }

    /// Return a bitmap of positions that are unknown (maybe true, maybe false).
    ///
    /// For the `Mixed` variant we compute `unknown \ true` to avoid leaking
    /// overlapping bits should the internal representation contain them.
    pub fn is_unknown(&self) -> PositionSet {
        match self {
            TernaryPositionSet::Definite(t) => PositionSet::empty(t.span()),
            TernaryPositionSet::Unknown(u) => u.clone(),
            TernaryPositionSet::Mixed(t, u) => u.intersect(&t.invert()),
        }
    }

    /// Return a bitmap of positions that are definitely false.
    ///
    /// For `Mixed`, this is the complement of `true ∪ unknown`.
    pub fn is_false(&self) -> PositionSet {
        match self {
            TernaryPositionSet::Definite(t) => t.invert(),
            TernaryPositionSet::Unknown(u) => u.invert(),
            TernaryPositionSet::Mixed(t, u) => t.union(&u).invert(),
        }
    }

    /// Return a bitmap of positions that are either definitely true OR unknown
    /// (i.e. not definitely false). For `Mixed` this is `true ∪ unknown`.
    pub fn is_true_or_unknown(&self) -> PositionSet {
        match self {
            TernaryPositionSet::Definite(t) => t.clone(),
            TernaryPositionSet::Unknown(u) => u.clone(),
            TernaryPositionSet::Mixed(t, u) => t.union(u),
        }
    }

    /// Return a bitmap of positions that are not definitely true (i.e. either
    /// false or unknown). For `Mixed` we return `¬true` – relying on (debug)
    /// invariants that unknowns do not overlap trues; if they did, those
    /// overlapping unknown bits would be omitted here.
    pub fn is_false_or_unknown(&self) -> PositionSet {
        match self {
            TernaryPositionSet::Definite(t) => t.invert(),
            TernaryPositionSet::Unknown(u) => PositionSet::full(u.span()),
            TernaryPositionSet::Mixed(t, _) => t.invert(),
        }
    }

    /// Logical union of two ternary sets (position‑wise OR on truth knowledge).
    ///
    /// Rules (per position):
    /// * If either side is true => true.
    /// * Else if any side is unknown => unknown.
    /// * Else => false.
    ///
    /// Returns a canonicalized representation (may downcast to `Definite` or
    /// `Unknown`). Panics if spans differ.
    pub fn union(&self, other: &TernaryPositionSet) -> TernaryPositionSet {
        assert_eq!(self.len(), other.len());
        match *self {
            TernaryPositionSet::Definite(ref t0) => match *other {
                TernaryPositionSet::Definite(ref t1) => TernaryPositionSet::Definite(t0.union(t1)),
                TernaryPositionSet::Unknown(ref u1) => {
                    TernaryPositionSet::new(t0.clone(), t0.union(u1))
                }
                TernaryPositionSet::Mixed(ref t1, ref u1) => {
                    TernaryPositionSet::new(t0.union(t1), t0.union(u1))
                }
            },
            TernaryPositionSet::Unknown(ref u0) => match *other {
                TernaryPositionSet::Unknown(ref u1) => TernaryPositionSet::Unknown(u0.union(u1)),
                _ => other.union(self),
            },
            TernaryPositionSet::Mixed(ref t0, ref u0) => match *other {
                TernaryPositionSet::Unknown(ref u1) => {
                    TernaryPositionSet::new(t0.clone(), u0.union(u1))
                }
                TernaryPositionSet::Mixed(ref t1, ref u1) => {
                    TernaryPositionSet::new(t0.union(t1), u0.union(u1))
                }
                _ => other.union(self),
            },
        }
    }

    /// Logical intersection of two ternary sets (position‑wise AND).
    ///
    /// Rules (per position):
    /// * If both sides are true => true.
    /// * Else if (at least one side unknown) AND neither side true => unknown.
    /// * Else => false.
    ///
    /// Returns a canonicalized representation. Panics if spans differ.
    pub fn intersect(&self, other: &TernaryPositionSet) -> TernaryPositionSet {
        assert_eq!(self.len(), other.len());
        match *self {
            TernaryPositionSet::Definite(ref t0) => match *other {
                TernaryPositionSet::Definite(ref t1) => {
                    TernaryPositionSet::Definite(t0.intersect(t1))
                }
                TernaryPositionSet::Unknown(ref u1) => {
                    TernaryPositionSet::Unknown(t0.intersect(u1))
                }
                TernaryPositionSet::Mixed(ref t1, ref u1) => {
                    TernaryPositionSet::new(t0.intersect(t1), t0.intersect(u1))
                }
            },
            TernaryPositionSet::Unknown(ref u0) => match *other {
                TernaryPositionSet::Unknown(ref u1) => {
                    TernaryPositionSet::Unknown(u0.intersect(u1))
                }
                _ => other.intersect(self),
            },
            TernaryPositionSet::Mixed(ref t0, ref u0) => match *other {
                TernaryPositionSet::Unknown(ref u1) => {
                    TernaryPositionSet::Unknown(u0.intersect(u1))
                }
                TernaryPositionSet::Mixed(ref t1, ref u1) => {
                    TernaryPositionSet::new(t0.intersect(t1), u0.intersect(u1))
                }
                _ => other.intersect(self),
            },
        }
    }

    /// Logical negation (NOT) of the ternary set.
    ///
    /// Mapping of states:
    /// * true    -> false
    /// * false   -> true
    /// * unknown -> unknown
    ///
    /// Implementation details:
    /// * `Definite` simply inverts the bitmap.
    /// * `Unknown(U)` becomes a `Mixed` with `true = ¬U`, `unknown = full`.
    ///   (Constructed via `new` so it may fold if span conditions allow.)
    /// * `Mixed(t,u)` computes according to bit‑pair inversion: resulting
    ///   `true = ¬t ∧ ¬u`; `unknown = ¬t ∨ ¬u`.
    pub fn negate(&self) -> TernaryPositionSet {
        match *self {
            TernaryPositionSet::Definite(ref t) => TernaryPositionSet::Definite(t.invert()),
            TernaryPositionSet::Unknown(ref u) => {
                TernaryPositionSet::new(u.invert(), PositionSet::full(u.span()))
            }
            TernaryPositionSet::Mixed(ref t, ref u) => {
                let t_not = t.invert();
                let u_not = u.invert();
                let t1 = t_not.intersect(&u_not);
                let u1 = t_not.union(&u_not);
                TernaryPositionSet::new(t1, u1)
            }
        }
    }

    /// Run lightweight structural invariant checks.
    ///
    /// Ensures for each stored internal `PositionSet`:
    /// - Segment coverage/layout is valid (`PositionSet::check_basic_invariants`).
    /// - For `Mixed`, spans match.
    pub fn check_basic_invariants(&self) {
        use TernaryPositionSet::*;
        match self {
            Definite(t) => t.check_basic_invariants(),
            Unknown(u) => u.check_basic_invariants(),
            Mixed(t, u) => {
                t.check_basic_invariants();
                u.check_basic_invariants();
                assert_eq!(
                    t.span(),
                    u.span(),
                    "span mismatch between true and unknown sets"
                );
            }
        }
    }

    /// Run comprehensive invariant checks (debug/testing).
    ///
    /// Extends `check_basic_invariants` by also:
    /// - Calling `check_full_invariants` on underlying sets.
    /// - Verifying disjointness: `true ∩ unknown = ∅` (for `Mixed`).
    pub fn check_full_invariants(&self) {
        use TernaryPositionSet::*;
        match self {
            Definite(t) => t.check_full_invariants(),
            Unknown(u) => u.check_full_invariants(),
            Mixed(t, u) => {
                t.check_full_invariants();
                u.check_full_invariants();
                assert_eq!(
                    t.span(),
                    u.span(),
                    "span mismatch between true and unknown sets"
                );
                debug_assert_eq!(
                    0,
                    t.intersect(u).count_positions(),
                    "true and unknown sets overlap"
                );
            }
        }
    }
}
