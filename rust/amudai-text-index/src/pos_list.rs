//! Position list representations for term occurrences within indexed data.
//!
//! This module defines the various ways position information can be represented
//! and stored within the text index. Different representations provide different
//! tradeoffs between storage space, query precision, and memory usage.
//!
//! # Position List Types
//!
//! ## Exact Positions
//! Individual position values providing precise location information. This is the
//! most accurate representation but can consume significant memory for terms that
//! appear frequently.
//!
//! ## Exact Ranges
//! Contiguous position ranges that maintain full precision while reducing memory
//! usage when terms appear in clusters. All positions within the ranges are
//! guaranteed to contain the term.
//!
//! ## Approximate Ranges
//! Position ranges with some imprecision to achieve maximum compression. Positions
//! within the ranges *might* contain the term, requiring additional verification
//! during query processing.
//!
//! ## Unknown
//! A special representation indicating that a term appears throughout the data
//! without specific position information. Used for optimization when a term is
//! so common that detailed position tracking would be inefficient.
//!
//! # Usage in Index Architecture
//!
//! Position lists are stored separately from the B-Tree navigation structure,
//! enabling efficient compression and lazy loading during queries. The representation
//! type is encoded with each position list to ensure correct interpretation.

use std::ops::Range;

/// Represents the occurrence positions of a term within indexed data.
///
/// A position list is the final result of an index query, detailing the record locations
/// for a specific term across each stripe and field within that stripe where the term appears.
/// It represents an ordered sequence of `uint64` record positions within the stripe.
///
/// The position list provides different representations optimized for various scenarios:
/// - Dense term occurrences can use exact ranges to reduce memory usage
/// - Sparse term occurrences use individual positions for precision
/// - Very common terms may use approximate ranges or unknown representation for efficiency
#[derive(Debug, Clone)]
pub enum PositionList {
    /// Exact Position List: A list of precise positions, such as `[10, 100, 101, ...]`.
    /// Each record at these positions is confirmed to contain the specified term.
    Positions(Vec<u64>),

    /// Exact Position Ranges: Precise position ranges, like `[10..20, 50..55, ...]`.
    /// Every record within these ranges is confirmed to contain the specified term.
    ExactRanges(Vec<Range<u64>>),

    /// Approximate Position Ranges: Approximate position ranges, such as `[10..20, 50..55, ...]`.
    /// Records within these ranges *might* contain the specified term.
    ApproximateRanges(Vec<Range<u64>>),

    /// Unknown Position List: Any record within the stripe might contain the specified term.
    /// No specific position information is available.
    Unknown,
}

impl PositionList {
    /// Creates a new exact position list from a vector of positions.
    /// The positions will be sorted automatically.
    pub fn new_exact_positions(mut positions: Vec<u64>) -> Self {
        positions.sort_unstable();
        PositionList::Positions(positions)
    }

    /// Creates a new exact ranges position list from a vector of ranges.
    /// The ranges will be sorted by their start positions.
    pub fn new_exact_ranges(mut ranges: Vec<Range<u64>>) -> Self {
        ranges.sort_unstable_by_key(|range| range.start);
        PositionList::ExactRanges(ranges)
    }

    /// Creates a new approximate ranges position list from a vector of ranges.
    /// The ranges will be sorted by their start positions.
    pub fn new_approximate_ranges(mut ranges: Vec<Range<u64>>) -> Self {
        ranges.sort_unstable_by_key(|range| range.start);
        PositionList::ApproximateRanges(ranges)
    }

    /// Creates a new unknown position list.
    pub fn new_unknown() -> Self {
        PositionList::Unknown
    }

    /// Returns true if this position list represents exact positions or ranges.
    pub fn is_exact(&self) -> bool {
        matches!(
            self,
            PositionList::Positions(_) | PositionList::ExactRanges(_)
        )
    }

    /// Returns true if this position list represents approximate information.
    pub fn is_approximate(&self) -> bool {
        matches!(self, PositionList::ApproximateRanges(_))
    }

    /// Returns true if this position list has no specific position information.
    pub fn is_unknown(&self) -> bool {
        matches!(self, PositionList::Unknown)
    }

    /// Returns the total number of positions or ranges in this list.
    /// Returns 0 for Unknown position lists.
    pub fn len(&self) -> usize {
        match self {
            PositionList::Positions(positions) => positions.len(),
            PositionList::ExactRanges(ranges) => ranges.len(),
            PositionList::ApproximateRanges(ranges) => ranges.len(),
            PositionList::Unknown => 0,
        }
    }

    /// Returns true if this position list is empty or unknown.
    pub fn is_empty(&self) -> bool {
        match self {
            PositionList::Positions(positions) => positions.is_empty(),
            PositionList::ExactRanges(ranges) => ranges.is_empty(),
            PositionList::ApproximateRanges(ranges) => ranges.is_empty(),
            PositionList::Unknown => true,
        }
    }

    /// Checks if a given position might be contained in this position list.
    /// For exact positions/ranges, this provides a definitive answer.
    /// For approximate ranges, this indicates possibility.
    /// For unknown lists, this always returns true.
    pub fn might_contain(&self, position: u64) -> bool {
        match self {
            PositionList::Positions(positions) => positions.binary_search(&position).is_ok(),
            PositionList::ExactRanges(ranges) | PositionList::ApproximateRanges(ranges) => {
                ranges.iter().any(|range| range.contains(&position))
            }
            PositionList::Unknown => true,
        }
    }

    /// For exact position lists, returns true if the position definitely contains the term.
    /// For approximate or unknown lists, returns false.
    pub fn definitely_contains(&self, position: u64) -> bool {
        match self {
            PositionList::Positions(positions) => positions.binary_search(&position).is_ok(),
            PositionList::ExactRanges(ranges) => {
                ranges.iter().any(|range| range.contains(&position))
            }
            PositionList::ApproximateRanges(_) | PositionList::Unknown => false,
        }
    }

    /// Returns an iterator over all individual positions in this list.
    /// For ranges, this expands all positions within the ranges.
    /// For unknown lists, this returns an empty iterator.
    pub fn iter_positions(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        match self {
            PositionList::Positions(positions) => Box::new(positions.iter().copied()),
            PositionList::ExactRanges(ranges) | PositionList::ApproximateRanges(ranges) => {
                Box::new(ranges.iter().flat_map(|range| range.clone()))
            }
            PositionList::Unknown => Box::new(std::iter::empty()),
        }
    }

    /// Merges two position lists, preferring more specific information.
    /// The result will be the most specific representation possible.
    pub fn merge(self, other: PositionList) -> PositionList {
        match (self, other) {
            // If either is unknown, use the other
            (PositionList::Unknown, other) | (other, PositionList::Unknown) => other,

            // If both are exact positions, merge them
            (PositionList::Positions(mut pos1), PositionList::Positions(pos2)) => {
                pos1.extend(pos2);
                PositionList::new_exact_positions(pos1)
            }

            // If both are exact ranges, merge them
            (PositionList::ExactRanges(mut ranges1), PositionList::ExactRanges(ranges2)) => {
                ranges1.extend(ranges2);
                PositionList::new_exact_ranges(ranges1)
            }

            // If both are approximate ranges, merge them
            (
                PositionList::ApproximateRanges(mut ranges1),
                PositionList::ApproximateRanges(ranges2),
            ) => {
                ranges1.extend(ranges2);
                PositionList::new_approximate_ranges(ranges1)
            }

            // For mixed types, convert to the less specific type
            (PositionList::Positions(positions), PositionList::ExactRanges(mut ranges))
            | (PositionList::ExactRanges(mut ranges), PositionList::Positions(positions)) => {
                // Convert positions to single-element ranges
                ranges.extend(positions.into_iter().map(|pos| pos..pos + 1));
                PositionList::new_exact_ranges(ranges)
            }

            // If one is approximate, the result is approximate
            (
                PositionList::Positions(exact),
                PositionList::ApproximateRanges(mut approx_ranges),
            )
            | (
                PositionList::ApproximateRanges(mut approx_ranges),
                PositionList::Positions(exact),
            ) => {
                // Convert exact to ranges and merge with approximate
                approx_ranges.extend(exact.into_iter().map(|pos| pos..pos + 1));
                PositionList::new_approximate_ranges(approx_ranges)
            }

            // If one is approximate, the result is approximate
            (
                PositionList::ExactRanges(exact_ranges),
                PositionList::ApproximateRanges(mut approx_ranges),
            )
            | (
                PositionList::ApproximateRanges(mut approx_ranges),
                PositionList::ExactRanges(exact_ranges),
            ) => {
                approx_ranges.extend(exact_ranges);
                PositionList::new_approximate_ranges(approx_ranges)
            }
        }
    }
}

impl Default for PositionList {
    fn default() -> Self {
        PositionList::Unknown
    }
}

/// Represents how to interpret the positions information stored in the index.
/// Please note that `PositionList::Unknown` is a query-time concept and does not
/// Enumeration of position data representation types stored in the positions shard.
///
/// This enum represents how position data is encoded in storage, enabling the
/// positions decoder to correctly interpret the raw integer sequences. The representation
/// type is stored alongside position references in the B-Tree structure.
///
/// # Storage Format
///
/// All position data is stored as sequences of `u64` values in the positions shard.
/// The representation type determines how these values should be interpreted when
/// reconstructing the original position information.
///
/// # Wire Format Compatibility
///
/// The `#[repr(u8)]` attribute ensures stable numeric representation for storage
/// and network serialization. The numeric values must remain consistent across
/// different versions of the index format.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum PositionsReprType {
    /// Individual position values: each `u64` represents a precise record position.
    ///
    /// Each `u64` value in the positions shard represents the logical record position
    /// where the term appears within the relevant stripe. This provides the highest
    /// precision but may consume more storage for frequently occurring terms.
    Positions = 0,

    /// Position ranges with exact boundaries: pairs of `u64` values form precise ranges.
    ///
    /// The sequence includes an even number of `u64` values. Each pair forms an
    /// `inclusive..exclusive` range, signifying the logical record positions where
    /// the term appears FOR SURE within the relevant stripe. This representation
    /// provides memory efficiency for terms that appear in contiguous sequences.
    ExactRanges = 1,

    /// Position ranges with approximate boundaries: pairs of `u64` values form imprecise ranges.
    ///
    /// The sequence includes an even number of `u64` values. Each pair forms an
    /// `inclusive..exclusive` range, signifying the logical record positions where
    /// the term MAY appear within the relevant stripe. This representation provides
    /// maximum compression at the cost of some precision.
    ApproximateRanges = 2,
}

impl TryFrom<u8> for PositionsReprType {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(PositionsReprType::Positions),
            1 => Ok(PositionsReprType::ExactRanges),
            2 => Ok(PositionsReprType::ApproximateRanges),
            _ => Err("PositionsReprType: invalid value"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_positions() {
        let pos_list = PositionList::new_exact_positions(vec![10, 100, 101, 50]);

        assert!(pos_list.is_exact());
        assert!(!pos_list.is_approximate());
        assert!(!pos_list.is_unknown());
        assert_eq!(pos_list.len(), 4);

        assert!(pos_list.definitely_contains(10));
        assert!(pos_list.definitely_contains(100));
        assert!(!pos_list.definitely_contains(99));

        assert!(pos_list.might_contain(10));
        assert!(!pos_list.might_contain(99));
    }

    #[test]
    fn test_exact_ranges() {
        let ranges = vec![10..20, 50..55];
        let pos_list = PositionList::new_exact_ranges(ranges);

        assert!(pos_list.is_exact());
        assert!(!pos_list.is_approximate());
        assert!(pos_list.definitely_contains(15));
        assert!(pos_list.definitely_contains(52));
        assert!(!pos_list.definitely_contains(25));
    }

    #[test]
    fn test_approximate_ranges() {
        let ranges = vec![10..20, 50..55];
        let pos_list = PositionList::new_approximate_ranges(ranges);

        assert!(!pos_list.is_exact());
        assert!(pos_list.is_approximate());
        assert!(pos_list.might_contain(15));
        assert!(!pos_list.definitely_contains(15));
    }

    #[test]
    fn test_unknown() {
        let pos_list = PositionList::new_unknown();

        assert!(!pos_list.is_exact());
        assert!(!pos_list.is_approximate());
        assert!(pos_list.is_unknown());
        assert!(pos_list.is_empty());
        assert!(pos_list.might_contain(12345));
        assert!(!pos_list.definitely_contains(12345));
    }

    #[test]
    fn test_merge_exact_positions() {
        let pos1 = PositionList::new_exact_positions(vec![10, 20]);
        let pos2 = PositionList::new_exact_positions(vec![30, 40]);
        let merged = pos1.merge(pos2);

        if let PositionList::Positions(positions) = merged {
            assert_eq!(positions, vec![10, 20, 30, 40]);
        } else {
            panic!("Expected ExactPositions");
        }
    }

    #[test]
    fn test_merge_with_unknown() {
        let pos = PositionList::new_exact_positions(vec![10, 20]);
        let unknown = PositionList::new_unknown();

        let merged1 = pos.clone().merge(unknown.clone());
        let merged2 = unknown.merge(pos);

        assert!(matches!(merged1, PositionList::Positions(_)));
        assert!(matches!(merged2, PositionList::Positions(_)));
    }

    #[test]
    fn test_iter_positions() {
        let pos_list = PositionList::new_exact_ranges(vec![10..13, 20..22]);
        let positions: Vec<u64> = pos_list.iter_positions().collect();
        assert_eq!(positions, vec![10, 11, 12, 20, 21]);
    }
}
