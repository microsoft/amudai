//! Position List Builder for Text Index
//!
//! This module provides `PositionListBuilder`, a smart builder for creating position lists
//! that automatically optimizes the representation based on data characteristics.
//!
//! ## Overview
//!
//! The builder starts by collecting exact positions and intelligently transitions to more
//! compact representations when the data becomes large or sparse:
//!
//! 1. **Exact Positions**: Individual position values (e.g., `[10, 100, 101, ...]`)
//! 2. **Exact Ranges**: Precise position ranges (e.g., `[10..20, 50..55, ...]`)  
//! 3. **Approximate Ranges**: Approximate position ranges for very large/sparse data
//!
//! ## Optimization Strategy
//!
//! The builder employs several optimizations to balance memory usage and precision:
//!
//! - **Consecutive Position Merging**: Only consecutive positions are merged into exact ranges
//! - **Gap-based Merging**: In approximate mode, small gaps between ranges are bridged
//! - **Lazy Conversion**: State transitions only occur when beneficial (e.g., when merging is possible)
//! - **Threshold-based Switching**: Configurable thresholds control when to transition between representations
//!
//! ## Usage Constraints
//!
//! - Positions must be added in strictly ascending order
//! - The builder must not be empty when `build()` is called
//! - Configuration thresholds should be chosen based on expected data characteristics
//!
//! **Note**: This builder never produces `PositionList::Unknown`. The `Unknown` variant
//! represents a query-time optimization where no specific position information is available
//! (e.g., when a term appears in every record). The builder always produces concrete
//! position information for the terms it processes.
//!

use std::ops::Range;

use crate::pos_list::PositionList;

/// Configuration for controlling when the builder transitions between different representations
#[derive(Debug, Clone)]
pub struct PositionListBuilderConfig {
    /// Maximum number of individual positions before converting to ranges
    pub max_positions: usize,
    /// Maximum number of exact ranges before converting to approximate ranges
    pub max_exact_ranges: usize,
    /// Maximum gap size to consider when merging ranges into approximate ranges
    /// (exact ranges never merge across gaps to maintain precision)
    /// Keep this small (e.g., 3) to maintain reasonable precision in approximate mode
    pub max_approximate_gap: u64,
}

impl Default for PositionListBuilderConfig {
    fn default() -> Self {
        Self {
            max_positions: 64,
            max_exact_ranges: 64,
            max_approximate_gap: 3,
        }
    }
}

/// A builder for creating position lists that automatically optimizes the representation
/// based on the data characteristics and configurable thresholds.
#[derive(Debug, Clone)]
pub struct PositionListBuilder {
    config: PositionListBuilderConfig,
    state: BuilderState,
}

/// Internal state of the position list builder
///
/// The builder progresses through these states as more positions are added
/// and the representation is optimized for size and access patterns.
#[derive(Debug, Clone)]
enum BuilderState {
    /// Building an exact position list
    ///
    /// Stores individual position values. This is the most precise representation
    /// but becomes memory-intensive for large numbers of positions.
    ExactPositions(Vec<u64>),
    /// Building exact ranges  
    ///
    /// Stores precise position ranges where only consecutive positions are merged.
    /// This maintains full precision while providing better memory efficiency
    /// for sequences of consecutive positions.
    ExactRanges(Vec<Range<u64>>),
    /// Building approximate ranges
    ///
    /// Stores approximate position ranges where positions within a small gap
    /// tolerance are merged. This provides the most compact representation
    /// but sacrifices some precision for memory efficiency.
    ApproximateRanges(Vec<Range<u64>>),
}

impl Default for BuilderState {
    fn default() -> Self {
        BuilderState::ExactPositions(Vec::new())
    }
}

impl PositionListBuilder {
    /// Creates a new builder with default configuration
    pub fn new() -> Self {
        Self::with_config(PositionListBuilderConfig::default())
    }

    /// Creates a new builder with custom configuration
    pub fn with_config(config: PositionListBuilderConfig) -> Self {
        Self {
            config,
            state: BuilderState::ExactPositions(Vec::new()),
        }
    }

    /// Adds a position to the builder, potentially triggering optimization
    ///
    /// Positions must be added in strictly ascending order. The builder automatically
    /// transitions between different representations based on the configured thresholds:
    /// - Starts with exact positions
    /// - Converts to exact ranges when position count exceeds `max_positions` and consecutive positions exist
    /// - Converts to approximate ranges when range count exceeds `max_exact_ranges` and ranges can be merged
    ///
    /// # Panics
    /// Panics if positions are not added in strictly ascending order.
    pub fn insert(&mut self, position: u64) {
        match std::mem::take(&mut self.state) {
            BuilderState::ExactPositions(mut positions) => {
                // Assert that positions are added in sorted order
                if let Some(&last_pos) = positions.last() {
                    assert!(
                        position > last_pos,
                        "Positions must be added in sorted order: {} <= {}",
                        position,
                        last_pos
                    );
                }
                positions.push(position);

                // Check if we should convert to ranges
                if positions.len() > self.config.max_positions {
                    if self.can_merge_any_positions(&positions) {
                        let ranges = self.merge_positions_to_ranges(&positions);
                        self.state = BuilderState::ExactRanges(ranges);
                    } else {
                        // No consecutive positions to merge and not too many positions,
                        // stay as positions to avoid memory overhead
                        self.state = BuilderState::ExactPositions(positions);
                    }
                } else {
                    self.state = BuilderState::ExactPositions(positions);
                }
            }
            BuilderState::ExactRanges(mut ranges) => {
                Self::add_position_to_ranges(&mut ranges, position);

                // Check if we should convert to approximate ranges
                if ranges.len() > self.config.max_exact_ranges {
                    // Optimization: Quick check to see if any ranges can be merged with the given gap tolerance.
                    // If no merging is possible, stay in exact ranges to avoid needless memory allocation
                    // and preserve the more precise exact representation.
                    if self.can_merge_any_ranges(&ranges, self.config.max_approximate_gap) {
                        let merged_ranges =
                            self.merge_ranges_with_gaps(&ranges, self.config.max_approximate_gap);
                        self.state = BuilderState::ApproximateRanges(merged_ranges);
                    } else {
                        // No merging possible, stay in exact ranges to avoid needless allocation
                        self.state = BuilderState::ExactRanges(ranges);
                    }
                } else {
                    self.state = BuilderState::ExactRanges(ranges);
                }
            }
            BuilderState::ApproximateRanges(mut ranges) => {
                // For approximate ranges, we can use gap-based merging
                Self::add_position_to_ranges_with_gap(
                    &mut ranges,
                    position,
                    self.config.max_approximate_gap,
                );
                self.state = BuilderState::ApproximateRanges(ranges);
            }
        }
    }

    /// Adds multiple positions to the builder
    ///
    /// This is a convenience method for adding multiple positions in sequence.
    /// All positions must be in strictly ascending order.
    ///
    /// # Panics
    /// Panics if positions are not in strictly ascending order.
    #[allow(unused)]
    pub fn insert_many<I>(&mut self, positions: I)
    where
        I: IntoIterator<Item = u64>,
    {
        for position in positions {
            self.insert(position);
        }
    }

    /// Builds the final position list, consuming the builder
    ///
    /// This method never returns `PositionList::Unknown`. The `Unknown` variant is a
    /// query-time optimization representing cases where no specific position information
    /// is available (e.g., a term that appears in every record). The builder always
    /// produces concrete position information based on the data it has been fed.
    ///
    /// # Panics
    /// Panics if the builder is empty (no positions have been added).
    pub fn build(self) -> PositionList {
        assert!(!self.is_empty(), "Cannot build empty position list");
        match self.state {
            BuilderState::ExactPositions(positions) => PositionList::new_exact_positions(positions),
            BuilderState::ExactRanges(ranges) => PositionList::new_exact_ranges(ranges),
            BuilderState::ApproximateRanges(ranges) => PositionList::new_approximate_ranges(ranges),
        }
    }

    /// Returns the current number of elements (positions or ranges) in the builder
    pub fn len(&self) -> usize {
        match &self.state {
            BuilderState::ExactPositions(positions) => positions.len(),
            BuilderState::ExactRanges(ranges) | BuilderState::ApproximateRanges(ranges) => {
                ranges.len()
            }
        }
    }

    /// Returns true if the builder is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Adds a position to existing ranges, only merging if it extends an existing range
    ///
    /// This method maintains exact precision by only merging positions that are
    /// adjacent to or overlap with existing ranges. No gaps are allowed in exact ranges.
    ///
    /// # Panics
    /// Panics if positions are not added in sorted order.
    fn add_position_to_ranges(ranges: &mut Vec<Range<u64>>, position: u64) {
        // Assert that positions are added in sorted order
        if let Some(last_range) = ranges.last() {
            assert!(
                position >= last_range.start,
                "Positions must be added in sorted order: {} < {}",
                position,
                last_range.start
            );
        }

        // Find if position can extend an existing range (adjacent or overlapping)
        for range in ranges.iter_mut() {
            if position + 1 >= range.start && position <= range.end {
                // Position extends or is within this range
                range.start = range.start.min(position);
                range.end = range.end.max(position + 1);
                return;
            }
        }

        // Position doesn't extend any existing range, add a new single-position range
        ranges.push(position..position + 1);
        // No need to sort since positions are added in sorted order

        // Merge only truly overlapping ranges (no gaps)
        Self::merge_overlapping_ranges(ranges, 0); // gap=0 for exact ranges
    }

    /// Adds a position to approximate ranges, allowing gap-based merging
    ///
    /// Unlike exact ranges, this method allows merging positions with existing ranges
    /// even when there are small gaps, up to the specified `max_gap` tolerance.
    /// This provides more compact representation at the cost of some precision.
    ///
    /// # Panics
    /// Panics if positions are not added in sorted order.
    fn add_position_to_ranges_with_gap(ranges: &mut Vec<Range<u64>>, position: u64, max_gap: u64) {
        // Assert that positions are added in sorted order
        if let Some(last_range) = ranges.last() {
            assert!(
                position >= last_range.start,
                "Positions must be added in sorted order: {} < {}",
                position,
                last_range.start
            );
        }

        // Find if position can be merged with existing range within gap tolerance
        for range in ranges.iter_mut() {
            if position + 1 + max_gap >= range.start && position <= range.end + max_gap {
                // Extend the range to include the position
                range.start = range.start.min(position);
                range.end = range.end.max(position + 1);
                return;
            }
        }

        // Position doesn't fit in any existing range, add a new single-position range
        ranges.push(position..position + 1);
        // No need to sort since positions are added in sorted order

        // Merge overlapping ranges with gap tolerance
        Self::merge_overlapping_ranges(ranges, max_gap);
    }

    /// Helper function to merge overlapping ranges in-place
    ///
    /// Merges ranges that are within `max_gap` distance of each other.
    /// When `max_gap` is 0, only truly overlapping ranges are merged (exact mode).
    /// When `max_gap` > 0, ranges separated by small gaps are merged (approximate mode).
    ///
    /// The input ranges must be sorted by start position.
    fn merge_overlapping_ranges(ranges: &mut Vec<Range<u64>>, max_gap: u64) {
        if ranges.len() <= 1 {
            return;
        }

        let mut write_index = 0;

        for read_index in 1..ranges.len() {
            let should_merge = {
                let current = &ranges[read_index];
                let last_merged = &ranges[write_index];
                current.start <= last_merged.end + max_gap
            };

            if should_merge {
                // Merge ranges
                let current_end = ranges[read_index].end;
                ranges[write_index].end = ranges[write_index].end.max(current_end);
            } else {
                // Keep separate
                write_index += 1;
                if write_index != read_index {
                    ranges[write_index] = ranges[read_index].clone();
                }
            }
        }

        ranges.truncate(write_index + 1);
    }

    /// Merges positions into ranges, combining only consecutive positions
    ///
    /// Converts a sorted list of positions into ranges by merging consecutive positions.
    /// Only truly consecutive positions (differing by exactly 1) are merged.
    /// This maintains exact precision with no approximation.
    ///
    /// # Panics
    /// Panics if the input position list is empty.
    fn merge_positions_to_ranges(&self, sorted_positions: &[u64]) -> Vec<Range<u64>> {
        assert!(
            !sorted_positions.is_empty(),
            "Cannot merge empty position list"
        );

        let mut ranges = Vec::new();
        let mut current_start = sorted_positions[0];
        let mut current_end = sorted_positions[0] + 1;

        for &pos in &sorted_positions[1..] {
            if pos == current_end {
                // Consecutive position - extend current range
                current_end = pos + 1;
            } else {
                // Gap found - start new range
                ranges.push(current_start..current_end);
                current_start = pos;
                current_end = pos + 1;
            }
        }

        ranges.push(current_start..current_end);
        ranges
    }

    /// Quick check to see if any positions can be merged into ranges
    ///
    /// Returns true if at least one pair of consecutive positions exists in the sorted list.
    /// This is used as an optimization to avoid unnecessary conversions when no merging
    /// would actually reduce the representation size.
    ///
    /// Positions are considered consecutive if they differ by exactly 1.
    fn can_merge_any_positions(&self, sorted_positions: &[u64]) -> bool {
        if sorted_positions.len() <= 1 {
            return false;
        }

        // Check each pair of adjacent positions
        for i in 1..sorted_positions.len() {
            let prev_pos = sorted_positions[i - 1];
            let current_pos = sorted_positions[i];

            // Are these positions consecutive?
            if current_pos == prev_pos + 1 {
                return true;
            }
        }

        false
    }

    /// Quick check to see if any ranges can be merged with the given gap tolerance
    ///
    /// Returns true if at least one pair of adjacent ranges can be merged within the
    /// specified `max_gap` distance. This is used as an optimization to avoid
    /// unnecessary conversions when no merging would reduce the representation size.
    ///
    /// Ranges can be merged if the gap between them is less than or equal to `max_gap`.
    fn can_merge_any_ranges(&self, ranges: &[Range<u64>], max_gap: u64) -> bool {
        if ranges.len() <= 1 {
            return false;
        }

        // Check each pair of adjacent ranges
        for i in 1..ranges.len() {
            let prev_range = &ranges[i - 1];
            let current_range = &ranges[i];

            // Can these ranges be merged?
            if current_range.start <= prev_range.end + max_gap {
                return true;
            }
        }

        false
    }

    /// Merges ranges by combining those within max_gap distance
    ///
    /// Creates a new vector of ranges where adjacent ranges that are within
    /// `max_gap` distance of each other are merged into single larger ranges.
    /// This is used in approximate mode to create more compact representations
    /// at the cost of some precision.
    ///
    /// The input ranges must be sorted by start position.
    fn merge_ranges_with_gaps(&self, ranges: &[Range<u64>], max_gap: u64) -> Vec<Range<u64>> {
        if ranges.is_empty() {
            return Vec::new();
        }

        // Ranges are already sorted since positions are added in sorted order
        let mut merged = Vec::new();
        let mut current = ranges[0].clone();

        for range in &ranges[1..] {
            if range.start <= current.end + max_gap {
                // Merge ranges
                current.end = current.end.max(range.end);
            } else {
                // Start new range
                merged.push(current);
                current = range.clone();
            }
        }

        merged.push(current);
        merged
    }
}

impl Default for PositionListBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_exact_positions() {
        let mut builder = PositionListBuilder::new();
        builder.insert(10);
        builder.insert(20);
        builder.insert(30);

        let pos_list = builder.build();
        assert!(pos_list.is_exact());
        assert!(pos_list.definitely_contains(10));
        assert!(pos_list.definitely_contains(20));
        assert!(pos_list.definitely_contains(30));
    }

    #[test]
    fn test_conversion_to_ranges() {
        let mut config = PositionListBuilderConfig::default();
        config.max_positions = 5; // Force conversion early

        let mut builder = PositionListBuilder::with_config(config);

        // Add consecutive positions that should be merged into ranges
        builder.insert_many(vec![1, 2, 3, 5, 6, 10, 11, 12]);

        let pos_list = builder.build();
        assert!(pos_list.is_exact());

        // Should create ranges like [1..4, 5..7, 10..13] (only consecutive positions merged)
        assert!(pos_list.definitely_contains(1));
        assert!(pos_list.definitely_contains(3));
        assert!(pos_list.definitely_contains(5));
        assert!(pos_list.definitely_contains(11));
        assert!(!pos_list.definitely_contains(4)); // Gap between 3 and 5
        assert!(!pos_list.definitely_contains(7)); // Gap between 6 and 10
    }

    #[test]
    fn test_conversion_to_approximate() {
        let mut config = PositionListBuilderConfig::default();
        config.max_positions = 3;
        config.max_exact_ranges = 2;
        config.max_approximate_gap = 10;

        let mut builder = PositionListBuilder::with_config(config);

        // Add positions with some consecutive ones to enable conversion to ranges,
        // then to approximate ranges
        builder.insert_many(vec![1, 2, 5, 10, 15, 20, 25, 30]); // 1,2 are consecutive

        let pos_list = builder.build();
        assert!(pos_list.is_approximate());

        // Should still contain the positions, but as approximate ranges
        assert!(pos_list.might_contain(1));
        assert!(pos_list.might_contain(20));
        assert!(!pos_list.definitely_contains(1)); // Approximate, not definite
    }

    #[test]
    fn test_merge_consecutive_positions() {
        let mut config = PositionListBuilderConfig::default();
        config.max_positions = 3;

        let mut builder = PositionListBuilder::with_config(config);
        builder.insert_many(vec![10, 11, 12, 20]); // 10-12 are consecutive, 20 is separate

        let pos_list = builder.build();
        assert!(pos_list.is_exact());
        assert!(pos_list.definitely_contains(10));
        assert!(pos_list.definitely_contains(11));
        assert!(pos_list.definitely_contains(12));
        assert!(!pos_list.definitely_contains(13)); // Gap before 20
        assert!(pos_list.definitely_contains(20));
    }

    #[test]
    fn test_builder_state_tracking() {
        let mut builder = PositionListBuilder::new();
        assert_eq!(builder.len(), 0);
        assert!(builder.is_empty());

        builder.insert(10);
        assert_eq!(builder.len(), 1);
        assert!(!builder.is_empty());

        builder.insert(20);
        assert_eq!(builder.len(), 2);
    }

    #[test]
    fn test_exact_vs_approximate_behavior() {
        // Test that exact ranges only merge consecutive positions
        let mut config_exact = PositionListBuilderConfig::default();
        config_exact.max_positions = 3; // Force conversion to ranges
        config_exact.max_exact_ranges = 10; // Stay in exact mode

        let mut builder_exact = PositionListBuilder::with_config(config_exact);
        builder_exact.insert_many(vec![10, 12, 20]); // gaps present

        let pos_list_exact = builder_exact.build();
        assert!(pos_list_exact.is_exact());
        // Exact ranges: positions stay separate due to gaps
        assert!(pos_list_exact.definitely_contains(10));
        assert!(!pos_list_exact.definitely_contains(11)); // Gap
        assert!(pos_list_exact.definitely_contains(12));
        assert!(!pos_list_exact.definitely_contains(15)); // Gap 
        assert!(pos_list_exact.definitely_contains(20));

        // Test that approximate ranges can merge with gaps
        let mut config_approx = PositionListBuilderConfig::default();
        config_approx.max_positions = 1; // Force conversion to ranges 
        config_approx.max_exact_ranges = 1; // Force conversion to approximate quickly
        config_approx.max_approximate_gap = 10; // Allow gap merging

        let mut builder_approx = PositionListBuilder::with_config(config_approx);
        builder_approx.insert(10);
        builder_approx.insert(11); // Make consecutive with 10 to enable conversion
        builder_approx.insert(20); // This will convert to ranges due to consecutive positions
        builder_approx.insert(25); // This should trigger conversion to approximate

        let pos_list_approx = builder_approx.build();
        assert!(pos_list_approx.is_approximate());
        // Approximate ranges: might contain positions in gaps
        assert!(pos_list_approx.might_contain(10));
        assert!(pos_list_approx.might_contain(15)); // Might be in merged range
        assert!(pos_list_approx.might_contain(20));
        assert!(!pos_list_approx.definitely_contains(15)); // But not definitely
    }

    #[test]
    fn test_optimization_stays_exact_when_no_merging_possible() {
        // Test the optimization: if gaps are too large, stay in exact ranges
        let mut config = PositionListBuilderConfig::default();
        config.max_positions = 2; // Force conversion to ranges quickly
        config.max_exact_ranges = 2; // Force attempt to convert to approximate
        config.max_approximate_gap = 5; // Small gap - won't allow merging of large gaps

        let mut builder = PositionListBuilder::with_config(config);
        builder.insert(10);
        builder.insert(20); // Gap of 10 > max_approximate_gap of 5
        builder.insert(30); // This should trigger the check

        let pos_list = builder.build();
        // Should stay exact because no ranges can be merged with gap=5
        assert!(pos_list.is_exact());
        assert!(pos_list.definitely_contains(10));
        assert!(pos_list.definitely_contains(20));
        assert!(pos_list.definitely_contains(30));
        assert!(!pos_list.definitely_contains(15)); // Gap should remain
        assert!(!pos_list.definitely_contains(25)); // Gap should remain
    }

    #[test]
    fn test_can_merge_any_positions_helper() {
        let builder = PositionListBuilder::new();

        // Test case 1: positions that can be merged (consecutive)
        let positions1 = vec![10, 11, 15]; // 10-11 are consecutive
        assert!(builder.can_merge_any_positions(&positions1));

        // Test case 2: positions that cannot be merged (no consecutive pairs)
        let positions2 = vec![10, 15, 20, 30]; // all have gaps
        assert!(!builder.can_merge_any_positions(&positions2));

        // Test case 3: single position
        let positions3 = vec![10];
        assert!(!builder.can_merge_any_positions(&positions3)); // no merging possible

        // Test case 4: empty positions
        let positions4 = vec![];
        assert!(!builder.can_merge_any_positions(&positions4)); // no merging possible

        // Test case 5: all consecutive positions
        let positions5 = vec![10, 11, 12, 13];
        assert!(builder.can_merge_any_positions(&positions5));
    }

    #[test]
    fn test_optimization_stays_positions_when_no_merging_possible() {
        // Test the optimization: if no consecutive positions exist, stay as positions
        // (unless there are too many positions)
        let mut config = PositionListBuilderConfig::default();
        config.max_positions = 3; // Force attempt to convert to ranges

        let mut builder = PositionListBuilder::with_config(config);
        builder.insert(10);
        builder.insert(20); // Gap of 10, not consecutive
        builder.insert(30); // Gap of 10, not consecutive  
        builder.insert(40); // This should trigger the check, but no consecutive positions

        let pos_list = builder.build();
        // Should stay as exact positions because no consecutive positions can be merged
        // and we don't have too many positions (4 <= 3*2)
        assert!(pos_list.is_exact());
        assert!(pos_list.definitely_contains(10));
        assert!(pos_list.definitely_contains(20));
        assert!(pos_list.definitely_contains(30));
        assert!(pos_list.definitely_contains(40));
        assert!(!pos_list.definitely_contains(15)); // Gap should remain
        assert!(!pos_list.definitely_contains(25)); // Gap should remain
        assert!(!pos_list.definitely_contains(35)); // Gap should remain
    }

    #[test]
    fn test_forces_conversion_with_too_many_positions() {
        // Test that with too many positions, we force conversion even without consecutive positions
        let mut config = PositionListBuilderConfig::default();
        config.max_positions = 3; // threshold * 2 = 6

        let mut builder = PositionListBuilder::with_config(config);
        // Add 7 scattered positions (more than threshold * 2)
        builder.insert_many(vec![10, 20, 30, 40, 50, 60, 70]);

        let pos_list = builder.build();
        // Should convert to ranges despite no consecutive positions because we have too many
        assert!(pos_list.is_exact());
        assert!(pos_list.definitely_contains(10));
        assert!(pos_list.definitely_contains(40));
        assert!(pos_list.definitely_contains(70));
        assert!(!pos_list.definitely_contains(15)); // Gaps should remain
    }

    #[test]
    fn test_converts_to_ranges_when_merging_possible() {
        // Test that when consecutive positions exist, conversion to ranges still happens
        let mut config = PositionListBuilderConfig::default();
        config.max_positions = 3; // Force attempt to convert to ranges

        let mut builder = PositionListBuilder::with_config(config);
        builder.insert(10);
        builder.insert(11); // Consecutive with 10
        builder.insert(20); // Gap, not consecutive
        builder.insert(30); // This should trigger the check, and consecutive positions exist

        let pos_list = builder.build();
        // Should convert to ranges because consecutive positions (10,11) can be merged
        assert!(pos_list.is_exact());
        assert!(pos_list.definitely_contains(10));
        assert!(pos_list.definitely_contains(11));
        assert!(pos_list.definitely_contains(20));
        assert!(pos_list.definitely_contains(30));
        assert!(!pos_list.definitely_contains(12)); // Gap after consecutive range
        assert!(!pos_list.definitely_contains(25)); // Gap should remain
    }

    #[test]
    fn test_can_merge_any_ranges_helper() {
        let builder = PositionListBuilder::new();

        // Test case 1: ranges that can be merged
        let ranges1 = vec![10..11, 15..16]; // gap = 4
        assert!(builder.can_merge_any_ranges(&ranges1, 5)); // max_gap=5 > gap=4
        assert!(!builder.can_merge_any_ranges(&ranges1, 3)); // max_gap=3 < gap=4

        // Test case 2: ranges that cannot be merged
        let ranges2 = vec![10..11, 100..101]; // gap = 89
        assert!(!builder.can_merge_any_ranges(&ranges2, 50)); // max_gap=50 < gap=89
        assert!(builder.can_merge_any_ranges(&ranges2, 100)); // max_gap=100 > gap=89

        // Test case 3: single range
        let ranges3 = vec![10..11];
        assert!(!builder.can_merge_any_ranges(&ranges3, 100)); // no merging possible

        // Test case 4: empty ranges
        let ranges4 = vec![];
        assert!(!builder.can_merge_any_ranges(&ranges4, 100)); // no merging possible
    }
}
