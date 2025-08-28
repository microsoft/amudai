//! Ternary position set over a fixed conceptual space.
//!
//! `TernaryPositionSet` models a 3-valued classification of positions within a
//! fixed domain `[0, span)`: Definitely Yes, Definitely No, or Unknown. It builds
//! upon [`PositionSet`] and reuses its adaptive segmented representation.
//!
//! Representation
//! - The domain `[0, span)` is shared by two internal [`PositionSet`]s:
//!   - `yes`: positions known to be present (definite matches).
//!   - `no`: positions known to be absent (definite non-matches).
//! - Unknown positions are implicit and equal to the complement of
//!   `(yes ∪ no)` within `[0, span)`.
//!
//! Identity and invariants
//! - `span` is fixed per instance and must be identical for `yes` and `no`.
//! - The triple classification is defined by the pair `(yes, no)`; `unknown` is
//!   derived and not stored explicitly.
//! - It is intended (but not strictly enforced in release builds) that `yes` and
//!   `no` are disjoint. In debug/tests, use
//!   [`check_full_invariants`](TernaryPositionSet::check_full_invariants) or
//!   [`check_basic_invariants`](TernaryPositionSet::check_basic_invariants) to
//!   validate structure and disjointness.
//!
//! Typical usage
//! - Construct an all-unknown domain via [`all_unknown`](TernaryPositionSet::all_unknown)
//!   and progressively refine to `yes` / `no`.
//! - Wrap an exact set using [`exact`](TernaryPositionSet::exact) to create a
//!   ternary view with no unknowns.
//! - Combine precomputed definite matches and definite non-matches using
//!   [`from_yes_no`](TernaryPositionSet::from_yes_no).
//!
//! Performance notes
//! - Operations on `yes`/`no` share the complexity characteristics of
//!   [`PositionSet`]. Computing `unknown()` performs a union followed by an
//!   inversion over `[0, span)`, allocating a new `PositionSet`.
//! - The internal segmented layout enables efficient handling of sparse, dense,
//!   and mixed distributions, matching `PositionSet` behavior.
//!
//! Panics
//! - [`from_yes_no`](TernaryPositionSet::from_yes_no) panics if the spans of `yes`
//!   and `no` differ.
//! - Other constructors and queries do not panic for in-range usage, but the
//!   underlying `PositionSet` methods retain their own constraints.
use crate::PositionSet;

/// A ternary classification of positions over a fixed span, backed by two
/// [`PositionSet`]s (`yes` and `no`). Positions not covered by `yes ∪ no` are
/// considered Unknown.
#[derive(Clone)]
pub struct TernaryPositionSet {
    yes: PositionSet,
    no: PositionSet,
}

impl TernaryPositionSet {
    /// Create a ternary set where all positions are Unknown.
    ///
    /// Semantics:
    /// - `yes` is empty; `no` is empty; thus every position is Unknown.
    pub fn all_unknown(span: u64) -> Self {
        Self {
            yes: PositionSet::empty(span),
            no: PositionSet::empty(span),
        }
    }

    /// Logical complement over `[0, span)`.
    ///
    /// Behavior:
    /// - Swaps the roles of `yes` and `no`; Unknown remains Unknown.
    pub fn invert(&self) -> Self {
        Self {
            yes: self.no.clone(),
            no: self.yes.clone(),
        }
    }

    /// In-place logical complement over `[0, span)`.
    pub fn invert_in_place(&mut self) {
        std::mem::swap(&mut self.yes, &mut self.no);
    }

    /// Ternary union (OR) with another set of the same span.
    ///
    /// 3-valued logic per position p:
    /// - Yes if `self.Yes(p) || other.Yes(p)`.
    /// - No if `self.No(p) && other.No(p)` and not Yes.
    /// - Unknown otherwise.
    ///
    /// Panics: if spans differ.
    pub fn union(&self, other: &TernaryPositionSet) -> Self {
        assert_eq!(self.span(), other.span());
        let yes = self.yes.union(&other.yes);
        // A position is No if it's No in both, AND it did not become Yes.
        let no = self.no.intersect(&other.no).intersect(&yes.invert());
        Self { yes, no }
    }

    /// In-place ternary union (self <- self ∪ other).
    pub fn union_with(&mut self, other: &TernaryPositionSet) {
        assert_eq!(self.span(), other.span());
        // A position is No if it's No in both.
        self.no.intersect_with(&other.no);
        // A position is Yes if it's Yes in either.
        self.yes.union_with(&other.yes);
        // Disjointness: if it became Yes, it cannot be No.
        self.no.intersect_with(&self.yes.invert());
    }

    /// Ternary intersection (AND) with another set of the same span.
    ///
    /// 3-valued logic per position p:
    /// - Yes if `self.Yes(p) && other.Yes(p)` and not No.
    /// - No if `self.No(p) || other.No(p)`.
    /// - Unknown otherwise.
    ///
    /// Panics: if spans differ.
    pub fn intersect(&self, other: &TernaryPositionSet) -> Self {
        assert_eq!(self.span(), other.span());
        let no = self.no.union(&other.no);
        // A position is Yes if it's Yes in both, AND it did not become No.
        let yes = self.yes.intersect(&other.yes).intersect(&no.invert());
        Self { yes, no }
    }

    /// In-place ternary intersection (self <- self ∩ other).
    pub fn intersect_with(&mut self, other: &TernaryPositionSet) {
        assert_eq!(self.span(), other.span());
        // A position is No if it's No in either.
        self.no.union_with(&other.no);
        // A position is Yes if it's Yes in both.
        self.yes.intersect_with(&other.yes);
        // Disjointness: if it became No, it cannot be Yes.
        self.yes.intersect_with(&self.no.invert());
    }
    /// Create a ternary set that exactly represents `yes` (no Unknowns).
    ///
    /// Behavior:
    /// - `no` is computed as the logical complement of `yes` over `[0, span)`.
    /// - The result satisfies `yes ∪ no = [0, span)` and `yes ∩ no = ∅`.
    pub fn exact(yes: PositionSet) -> Self {
        let span = yes.span();
        let no = yes.invert();
        let t = Self { yes, no };
        debug_assert_eq!(t.span(), span);
        t
    }

    /// Construct from `yes`/`no` sets over the same span.
    ///
    /// Requirements:
    /// - `yes.span() == no.span()`; otherwise this panics.
    /// - Callers should ensure `yes` and `no` are disjoint. This function does not
    ///   enforce disjointness in release builds.
    pub fn from_yes_no(yes: PositionSet, no: PositionSet) -> Self {
        assert_eq!(yes.span(), no.span(), "yes/no span mismatch");
        Self { yes, no }
    }

    /// Create a ternary set where all positions are Definitely Yes.
    ///
    /// Equivalent to `yes = full(span)`, `no = empty(span)`.
    pub fn all_yes(span: u64) -> Self {
        Self {
            yes: PositionSet::full(span),
            no: PositionSet::empty(span),
        }
    }

    /// Create a ternary set where all positions are Definitely No.
    ///
    /// Equivalent to `yes = empty(span)`, `no = full(span)`.
    pub fn all_no(span: u64) -> Self {
        Self {
            yes: PositionSet::empty(span),
            no: PositionSet::full(span),
        }
    }

    /// The fixed domain size `[0, span)` for this ternary set.
    #[inline]
    pub fn span(&self) -> u64 {
        self.yes.span()
    }

    /// The set of positions that are Definitely Yes.
    #[inline]
    pub fn yes(&self) -> &PositionSet {
        &self.yes
    }

    /// The set of positions that are Definitely No.
    #[inline]
    pub fn no(&self) -> &PositionSet {
        &self.no
    }

    /// Compute Unknown positions as a new [`PositionSet`].
    ///
    /// Definition:
    /// - `unknown = [0, span) \ (yes ∪ no)`.
    ///
    /// Complexity:
    /// - Performs a union followed by an inversion; see [`PositionSet::union`]
    ///   and [`PositionSet::invert`] for details.
    pub fn unknown(&self) -> PositionSet {
        let union = self.yes.union(&self.no);
        union.invert()
    }

    /// Returns `true` if there are no Unknown positions in the set.
    ///
    /// Check:
    /// - Compares `yes.count() + no.count()` to `span`.
    pub fn is_exact(&self) -> bool {
        self.yes.count_positions() + self.no.count_positions() == self.span()
    }

    /// If no Unknowns exist, returns the exact `yes` set; otherwise returns `None`.
    ///
    /// Check:
    /// - Compares `yes.count() + no.count()` to `span` and returns `Some(yes)`
    ///   if equal.
    pub fn into_exact_if_no_unknown(self) -> Option<PositionSet> {
        if self.is_exact() {
            Some(self.yes)
        } else {
            None
        }
    }

    /// Basic invariant checks for debug/testing.
    ///
    /// Ensures:
    /// - Both internal sets have valid layout.
    /// - `yes.span() == no.span()`.
    pub fn check_basic_invariants(&self) {
        self.yes.check_basic_invariants();
        self.no.check_basic_invariants();
        assert_eq!(self.yes.span(), self.no.span());
    }

    /// Full invariant checks including disjointness (debug/testing).
    ///
    /// Ensures:
    /// - Per-segment invariants for both sets.
    /// - `span` equality.
    /// - `count(yes) + count(no) == count(yes ∪ no)` (disjointness proxy).
    pub fn check_full_invariants(&self) {
        self.yes.check_full_invariants();
        self.no.check_full_invariants();
        assert_eq!(self.yes.span(), self.no.span());
        debug_assert_eq!(
            self.yes.count_positions() + self.no.count_positions(),
            self.yes.union(&self.no).count_positions()
        );
    }
}
