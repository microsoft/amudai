use std::ops::Range;

use crate::segment::{Segment, SegmentKind, builder::SegmentBuilder};

fn span_small() -> Range<u64> {
    0..200
}

fn span_medium() -> Range<u64> {
    0..10_000
}

fn span_full() -> Range<u64> {
    0..Segment::SPAN
}

fn span_offset() -> Range<u64> {
    Segment::SPAN * 5..(Segment::SPAN * 5 + 1000)
}

#[cfg(test)]
mod push_position_tests {
    use super::*;

    #[test]
    fn push_position_basic_and_boundary() {
        let span = span_small();
        let mut b = SegmentBuilder::new(span.clone());

        assert!(b.push_position(105));
        assert!(b.push_position(110));
        assert!(b.push_position(119));

        // Out of span: should return false and not advance min_next_pos
        let before = b.min_next_pos();
        assert!(!b.push_position(span.end));
        assert_eq!(b.min_next_pos(), before);

        let seg = b.build(None);
        assert_eq!(seg.span(), span);
        assert_eq!(seg.kind(), SegmentKind::List);
        let positions: Vec<u64> = seg.positions().collect();
        assert_eq!(positions, vec![105, 110, 119]);
    }

    #[test]
    fn many_sparse_positions_prefer_bits() {
        let span = span_full();
        let mut b = SegmentBuilder::new(span.clone());

        let n = (Segment::MAX_LIST_LEN as usize) + 1; // force beyond List threshold
        // Use non-consecutive positions to keep runs high, favoring Bits.
        for i in 0..n {
            assert!(b.push_position((2 * i) as u64));
        }

        let seg = b.build(None);
        assert_eq!(seg.span(), span);
        assert_eq!(seg.kind(), SegmentKind::Bits);
        assert_eq!(seg.count_positions(), n);

        // Spot-check membership
        assert!(seg.contains(0));
        assert!(seg.contains((2 * (n - 1)) as u64));
        assert!(!seg.contains(1));
    }

    #[test]
    #[should_panic]
    fn push_position_unsorted_panics() {
        let mut b = SegmentBuilder::new(span_small());
        assert!(b.push_position(150));
        // Next position must be >= previous + 1 (sorted and unique)
        // This violates the contract and must panic.
        let _ = b.push_position(149);
    }

    #[test]
    fn push_position_empty_builder() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        assert_eq!(builder.min_next_pos(), span.start);

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::Empty);
        assert_eq!(segment.count_positions(), 0);
    }

    #[test]
    fn push_position_single_position() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        assert!(builder.push_position(50));
        assert_eq!(builder.min_next_pos(), 51);

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::List);
        assert_eq!(segment.count_positions(), 1);
        assert!(segment.contains(50));
        assert!(!segment.contains(49));
        assert!(!segment.contains(51));
    }

    #[test]
    fn many_runs_cause_ranges_then_bits() {
        let span = span_full();
        let mut b = SegmentBuilder::new(span.clone());

        // First, force Ranges by inserting a long run (longer than MAX_LIST_LEN).
        let long_len = (Segment::MAX_LIST_LEN + 2) as u64;
        let base = 0u64;
        let _ = b.push_range(base..(base + long_len));
        // min_next_pos is now base + long_len

        // Now add enough small, disjoint runs to exceed MAX_RANGES_LEN and trigger switch to Bits.
        let max_ranges = Segment::MAX_RANGES_LEN as usize;
        let mut start = base + long_len;
        for _ in 0..(max_ranges + 1) {
            // each range is length 1, with a gap of 1 to avoid merging
            let _ = b.push_range(start..(start + 1));
            start += 2;
        }

        let seg = b.build(None);
        assert_eq!(seg.span(), span);
        assert_eq!(seg.kind(), SegmentKind::Bits);

        // Verify we still cover the first long range and some of the singletons
        assert!(seg.contains(base));
        assert!(seg.contains(base + long_len - 1));
        assert!(seg.contains(base + long_len)); // first singleton start
        assert!(!seg.contains(base + long_len + 1)); // gap after first singleton
    }

    #[test]
    fn push_position_consecutive_positions() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        for i in 10..20 {
            assert!(builder.push_position(i));
        }

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::Ranges); // Consecutive -> ranges
        assert_eq!(segment.count_positions(), 10);

        for i in 10..20 {
            assert!(segment.contains(i));
        }
        assert!(!segment.contains(9));
        assert!(!segment.contains(20));
    }

    #[test]
    fn push_position_sparse_positions() {
        let span = span_medium();
        let mut builder = SegmentBuilder::new(span.clone());

        let positions = [10, 50, 100, 500, 1000, 5000];
        for &pos in &positions {
            assert!(builder.push_position(pos));
        }

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::List);
        assert_eq!(segment.count_positions(), positions.len());

        for &pos in &positions {
            assert!(segment.contains(pos));
        }
    }

    #[test]
    fn push_position_at_span_boundaries() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        // At start of span
        assert!(builder.push_position(span.start));
        assert_eq!(builder.min_next_pos(), span.start + 1);

        // Near end of span
        assert!(builder.push_position(span.end - 1));
        assert_eq!(builder.min_next_pos(), span.end);

        // At span end should fail
        assert!(!builder.push_position(span.end));
        assert_eq!(builder.min_next_pos(), span.end); // Unchanged

        let segment = builder.build(None);
        assert_eq!(segment.count_positions(), 2);
        assert!(segment.contains(span.start));
        assert!(segment.contains(span.end - 1));
    }

    #[test]
    fn push_position_beyond_span_returns_false() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        let before_min = builder.min_next_pos();

        assert!(!builder.push_position(span.end));
        assert!(!builder.push_position(span.end + 100));

        // min_next_pos should not change
        assert_eq!(builder.min_next_pos(), before_min);

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::Empty);
    }

    #[test]
    fn push_position_force_list_to_bits_transition() {
        let span = span_full();
        let mut builder = SegmentBuilder::new(span.clone());

        // Add just over MAX_LIST_LEN positions sparsely to force bits
        let n = (Segment::MAX_LIST_LEN as usize) + 10;
        for i in 0..n {
            let pos = (i * 10) as u64; // Sparse to avoid ranges optimization
            assert!(builder.push_position(pos));
        }

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::Bits);
        assert_eq!(segment.count_positions(), n);

        // Verify positions are preserved
        for i in 0..n {
            let pos = (i * 10) as u64;
            assert!(segment.contains(pos));
        }
    }

    #[test]
    fn push_position_offset_span() {
        let span = span_offset();
        let mut builder = SegmentBuilder::new(span.clone());

        let pos = span.start + 100;
        assert!(builder.push_position(pos));
        assert_eq!(builder.min_next_pos(), pos + 1);

        let segment = builder.build(None);
        assert!(segment.contains(pos));
        assert_eq!(segment.span(), span);
    }

    #[test]
    #[should_panic(expected = "Positions must be sorted and unique")]
    fn push_position_decreasing_panics() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span);

        assert!(builder.push_position(100));
        builder.push_position(99); // Should panic
    }

    #[test]
    #[should_panic(expected = "Positions must be sorted and unique")]
    fn push_position_duplicate_panics() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span);

        assert!(builder.push_position(100));
        builder.push_position(100); // Should panic
    }

    #[test]
    fn push_position_exactly_at_min_next_pos() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        assert!(builder.push_position(50));
        assert_eq!(builder.min_next_pos(), 51);

        assert!(builder.push_position(51)); // Exactly at min_next_pos
        assert_eq!(builder.min_next_pos(), 52);

        let segment = builder.build(None);
        assert_eq!(segment.count_positions(), 2);
        assert!(segment.contains(50));
        assert!(segment.contains(51));
    }
}

#[cfg(test)]
mod push_range_tests {
    use super::*;

    #[test]
    fn push_range_basic_and_tail() {
        let span = 0..110;
        let mut b = SegmentBuilder::new(span.clone());

        // Partially overlaps, tail should be returned.
        let tail = b.push_range(105..115);
        assert_eq!(tail, 110..115);
        // min_next_pos must advance to the clamped end
        assert_eq!(b.min_next_pos(), 110);

        let seg = b.build(None);
        assert_eq!(seg.span(), span);
        assert_eq!(seg.kind(), SegmentKind::Ranges);
        let ranges: Vec<Range<u64>> = seg.ranges().collect();
        assert_eq!(ranges, vec![105..110]);
    }

    #[test]
    fn push_range_zero_len_is_noop() {
        let span = span_small();
        let mut b = SegmentBuilder::new(span.clone());

        let before_min = b.min_next_pos();
        let tail = b.push_range(150..150);
        // No change, returned tail is empty and unchanged
        assert_eq!(tail, 150..150);
        assert_eq!(b.min_next_pos(), before_min);

        let seg = b.build(None);
        assert_eq!(seg.kind(), SegmentKind::Empty);
        assert_eq!(seg.count_positions(), 0);
    }

    #[test]
    #[should_panic]
    fn push_range_overlaps_previous_panics() {
        let span = span_small();
        let mut b = SegmentBuilder::new(span);
        // First range advances min_next_pos to 120
        let _ = b.push_range(110..120);
        // Overlapping start < min_next_pos should panic
        let _ = b.push_range(115..130);
    }

    #[test]
    fn push_range_start_at_or_after_span_end_returns_unchanged() {
        let span = span_small();
        let mut b = SegmentBuilder::new(span.clone());

        let tail = b.push_range(span.end..(span.end + 10));
        assert_eq!(tail, span.end..(span.end + 10));

        let seg = b.build(None);
        assert_eq!(seg.kind(), SegmentKind::Empty);
    }

    #[test]
    fn push_range_entire_span_yields_full() {
        let span = span_full();
        let mut b = SegmentBuilder::new(span.clone());

        let tail = b.push_range(span.start..span.end);
        assert_eq!(tail, span.end..span.end);
        assert_eq!(b.min_next_pos(), span.end);

        let seg = b.build(None);
        assert_eq!(seg.span(), span);
        assert_eq!(seg.kind(), SegmentKind::Full);
        assert_eq!(seg.count_positions(), (span.end - span.start) as usize);
        assert!(seg.contains(span.start));
        assert!(seg.contains(span.end - 1));
    }

    #[test]
    fn large_range_prefers_ranges() {
        let span = span_full();
        let mut b = SegmentBuilder::new(span.clone());

        // Length strictly greater than MAX_LIST_LEN to force List->Ranges in builder
        let len = (Segment::MAX_LIST_LEN + 10) as u64;
        let start = 1234;
        let end = start + len;
        let tail = b.push_range(start..end);
        assert_eq!(tail, end..end);

        // After build, Ranges is much smaller than Bits or List here
        let seg = b.build(None);
        assert_eq!(seg.span(), span);
        assert_eq!(seg.kind(), SegmentKind::Ranges);
        assert_eq!(seg.count_positions(), len as usize);

        // Verify coverage
        assert!(seg.contains(start));
        assert!(seg.contains(end - 1));
        assert!(!seg.contains(start - 1));
        assert!(!seg.contains(end));
    }

    #[test]
    fn push_range_empty_range() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        let before_min = builder.min_next_pos();
        let tail = builder.push_range(50..50); // Empty range

        assert_eq!(tail, 50..50);
        assert_eq!(builder.min_next_pos(), before_min); // Unchanged

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::Empty);
    }

    #[test]
    fn push_range_single_element_range() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        let tail = builder.push_range(50..51);
        assert_eq!(tail, 51..51); // Empty tail
        assert_eq!(builder.min_next_pos(), 51);

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::List);
        assert_eq!(segment.count_positions(), 1);
        assert!(segment.contains(50));
        assert!(!segment.contains(51));
    }

    #[test]
    fn push_range_small_range() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        let tail = builder.push_range(10..20);
        assert_eq!(tail, 20..20); // Empty tail
        assert_eq!(builder.min_next_pos(), 20);

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::Ranges);
        assert_eq!(segment.count_positions(), 10);

        for i in 10..20 {
            assert!(segment.contains(i));
        }
        assert!(!segment.contains(9));
        assert!(!segment.contains(20));
    }

    #[test]
    fn push_range_exceeds_span_returns_tail() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        let tail = builder.push_range(150..(span.end + 50));
        assert_eq!(tail, span.end..(span.end + 50));
        assert_eq!(builder.min_next_pos(), span.end);

        let segment = builder.build(None);
        assert_eq!(segment.count_positions(), (span.end - 150) as usize);

        for i in 150..span.end {
            assert!(segment.contains(i));
        }
    }

    #[test]
    fn push_range_completely_outside_span() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        let range = (span.end + 10)..(span.end + 20);
        let tail = builder.push_range(range.clone());
        assert_eq!(tail, range); // Unchanged

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::Empty);
    }

    #[test]
    fn push_range_at_span_start() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        let tail = builder.push_range(span.start..(span.start + 10));
        assert_eq!(tail, (span.start + 10)..(span.start + 10)); // Empty tail

        let segment = builder.build(None);
        assert_eq!(segment.count_positions(), 10);
        assert!(segment.contains(span.start));
        assert!(segment.contains(span.start + 9));
        assert!(!segment.contains(span.start + 10));
    }

    #[test]
    fn push_range_entire_span() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        let tail = builder.push_range(span.clone());
        assert_eq!(tail, span.end..span.end); // Empty tail
        assert_eq!(builder.min_next_pos(), span.end);

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::Full);
        assert_eq!(segment.count_positions(), (span.end - span.start) as usize);
    }

    #[test]
    fn push_range_force_list_to_ranges_transition() {
        let span = span_full();
        let mut builder = SegmentBuilder::new(span.clone());

        // Add a large range that exceeds MAX_LIST_LEN
        let range_len = Segment::MAX_LIST_LEN + 100;
        let tail = builder.push_range(1000..(1000 + range_len));
        assert_eq!(tail, (1000 + range_len)..(1000 + range_len));

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::Ranges);
        assert_eq!(segment.count_positions(), range_len as usize);
    }

    #[test]
    fn push_range_force_ranges_to_bits_transition() {
        let span = span_full();
        let mut builder = SegmentBuilder::new(span.clone());

        // Force initial Ranges mode with large range
        let initial_len = Segment::MAX_LIST_LEN + 100;
        let _ = builder.push_range(0..initial_len);

        // Add many small ranges to exceed MAX_RANGES_LEN
        let max_ranges = Segment::MAX_RANGES_LEN as usize;
        let mut start = initial_len + 10;
        for _ in 0..(max_ranges + 5) {
            let _ = builder.push_range(start..(start + 1));
            start += 2; // Leave gap to prevent merging
        }

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::Bits);
    }

    #[test]
    fn push_range_adjacent_ranges_merge() {
        let span = span_full();
        let mut builder = SegmentBuilder::new(span.clone());

        // Force ranges mode first
        let initial_len = Segment::MAX_LIST_LEN + 10;
        let _ = builder.push_range(100..(100 + initial_len));

        // Add adjacent range - should merge
        let _ = builder.push_range((100 + initial_len)..(100 + initial_len + 50));

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::Ranges);

        let ranges: Vec<Range<u64>> = segment.ranges().collect();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], 100..(100 + initial_len + 50));
    }

    #[test]
    #[should_panic(expected = "range.start: ")]
    fn push_range_overlapping_panics() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span);

        let _ = builder.push_range(50..60);
        builder.push_range(55..65); // Overlaps, should panic
    }

    #[test]
    #[should_panic(expected = "range.start: ")]
    fn push_range_before_min_next_pos_panics() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span);

        let _ = builder.push_range(50..60);
        builder.push_range(40..45); // Before min_next_pos, should panic
    }

    #[test]
    fn push_range_exactly_at_min_next_pos() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        let _ = builder.push_range(50..60);
        assert_eq!(builder.min_next_pos(), 60);

        let tail = builder.push_range(60..70); // Exactly at min_next_pos
        assert_eq!(tail, 70..70);
        assert_eq!(builder.min_next_pos(), 70);

        let segment = builder.build(None);
        assert_eq!(segment.count_positions(), 20);
    }

    #[test]
    fn push_range_offset_span() {
        let span = span_offset();
        let mut builder = SegmentBuilder::new(span.clone());

        let range_start = span.start + 100;
        let range_end = range_start + 50;
        let tail = builder.push_range(range_start..range_end);

        assert_eq!(tail, range_end..range_end);

        let segment = builder.build(None);
        assert_eq!(segment.span(), span);
        assert_eq!(segment.count_positions(), 50);

        for i in range_start..range_end {
            assert!(segment.contains(i));
        }
    }

    #[test]
    fn mixed_positions_and_ranges_merge_in_ranges_mode() {
        let span = span_full();
        let mut b = SegmentBuilder::new(span.clone());

        // Force Ranges mode with a large initial range.
        let long_len = (Segment::MAX_LIST_LEN + 2) as u64;
        let start = 10_000u64;
        let end = start + long_len;
        let _ = b.push_range(start..end);

        // Now push an adjacent single position that should merge with the last range
        assert_eq!(b.min_next_pos(), end);
        assert!(b.push_position(end)); // will merge into [start, end+1)
        assert_eq!(b.min_next_pos(), end + 1);

        let seg = b.build(None);
        assert_eq!(seg.span(), span);
        assert_eq!(seg.kind(), SegmentKind::Ranges);

        // The contiguous coverage should include the extended position.
        let ranges: Vec<Range<u64>> = seg.ranges().collect();
        // Single long range starting at `start`, at least up to `end + 1`
        assert!(!ranges.is_empty());
        assert_eq!(ranges[0].start, start);
        assert!(ranges[0].end >= end + 1);
    }

    #[test]
    fn mixed_small_then_range_on_list() {
        let span = 0..100;
        let mut b = SegmentBuilder::new(span.clone());

        assert!(b.push_position(10));
        let tail = b.push_range(11..15); // contiguous with previous point
        assert_eq!(tail, 15..15);

        let seg = b.build(None);
        // Small, single run -> Ranges is optimal
        assert_eq!(seg.kind(), SegmentKind::Ranges);
        let positions: Vec<u64> = seg.positions().collect();
        assert_eq!(positions, vec![10, 11, 12, 13, 14]);
    }
}

#[cfg(test)]
mod mixed_operations_tests {
    use super::*;

    #[test]
    fn mixed_position_then_range_list_mode() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        // Start with positions in List mode
        assert!(builder.push_position(10));
        assert!(builder.push_position(20));
        assert!(builder.push_position(30));

        // Add range that doesn't force transition
        let tail = builder.push_range(40..45);
        assert_eq!(tail, 45..45);

        let segment = builder.build(None);
        assert_eq!(segment.count_positions(), 8); // 3 positions + 5 range

        assert!(segment.contains(10));
        assert!(segment.contains(20));
        assert!(segment.contains(30));
        for i in 40..45 {
            assert!(segment.contains(i));
        }
    }

    #[test]
    fn mixed_range_then_position_ranges_mode() {
        let span = span_full();
        let mut builder = SegmentBuilder::new(span.clone());

        // Force ranges mode with large range
        let large_len = Segment::MAX_LIST_LEN + 10;
        let _ = builder.push_range(100..(100 + large_len));

        // Add adjacent position - should merge
        assert!(builder.push_position(100 + large_len));

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::Ranges);

        let ranges: Vec<Range<u64>> = segment.ranges().collect();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], 100..(100 + large_len + 1));
    }

    #[test]
    fn mixed_alternating_positions_and_ranges() {
        let span = span_medium();
        let mut builder = SegmentBuilder::new(span.clone());

        // Alternating pattern
        assert!(builder.push_position(10));
        let _ = builder.push_range(20..25);
        assert!(builder.push_position(30));
        let _ = builder.push_range(35..40);
        assert!(builder.push_position(45));

        let segment = builder.build(None);
        assert_eq!(segment.count_positions(), 13);

        // Verify all positions
        assert!(segment.contains(10));
        assert!(segment.contains(30));
        assert!(segment.contains(45));
        for i in 20..25 {
            assert!(segment.contains(i));
        }
        for i in 35..40 {
            assert!(segment.contains(i));
        }
    }

    #[test]
    fn mixed_force_multiple_transitions() {
        let span = span_full();
        let mut builder = SegmentBuilder::new(span.clone());

        // Start in List mode with positions
        for i in 0..10 {
            assert!(builder.push_position(i * 100));
        }

        // Force to Ranges with large range
        let large_len = Segment::MAX_LIST_LEN + 10;
        let _ = builder.push_range(10000..(10000 + large_len));

        // Add many small ranges to force to Bits
        let max_ranges = Segment::MAX_RANGES_LEN as usize;
        let mut start = 10000 + large_len + 10;
        for _ in 0..(max_ranges + 5) {
            let _ = builder.push_range(start..(start + 1));
            start += 2;
        }

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::Bits);

        // Verify original positions are preserved
        for i in 0..10 {
            assert!(segment.contains(i * 100));
        }
    }

    #[test]
    fn mixed_build_and_reset() {
        let mut builder = SegmentBuilder::new(0..Segment::SPAN);

        // Build first segment
        assert!(builder.push_position(50));
        let _ = builder.push_range(60..70);

        let segment1 = builder.build(None);
        assert_eq!(segment1.span(), 0..Segment::SPAN);
        assert_eq!(segment1.count_positions(), 11);

        // Reset and build second segment
        builder.reset(Segment::SPAN..Segment::SPAN + 10000);
        assert_eq!(builder.min_next_pos(), Segment::SPAN);

        let _ = builder.push_range(Segment::SPAN + 1000..Segment::SPAN + 2000);
        assert!(builder.push_position(Segment::SPAN + 2500));

        let segment2 = builder.build(None);
        assert_eq!(segment2.span(), Segment::SPAN..Segment::SPAN + 10000);
        assert_eq!(segment2.count_positions(), 1001);

        assert!(segment1.contains(50));
        assert!(segment1.contains(60));
        assert!(segment1.contains(61));
        assert!(segment2.contains(Segment::SPAN + 1000));
    }

    #[test]
    fn mixed_large_contiguous_via_operations() {
        let span = span_full();
        let mut builder = SegmentBuilder::new(span.clone());

        // Build a large contiguous range using mixed operations
        let start = 1000u64;
        let mid = start + 5000;
        let end = start + 10000;

        // Add first half as range
        let _ = builder.push_range(start..mid);

        // Add second half as individual positions (would be slow in practice)
        for pos in mid..end {
            assert!(builder.push_position(pos));
        }

        let segment = builder.build(None);
        assert_eq!(segment.count_positions(), (end - start) as usize);

        // Should optimize to a single range
        let ranges: Vec<Range<u64>> = segment.ranges().collect();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], start..end);
    }
}

#[cfg(test)]
mod edge_cases_and_error_conditions {
    use super::*;

    #[test]
    fn empty_span_behavior() {
        // Note: This might panic in constructor due to span validation
        // Testing boundary behavior with minimal span
        let span = 0..0;

        // This should likely panic during construction
        std::panic::catch_unwind(|| {
            let _builder = SegmentBuilder::new(span);
        })
        .expect_err("Empty span should not be allowed");
    }

    #[test]
    fn single_element_span() {
        let span = 0..1;
        let mut builder = SegmentBuilder::new(span.clone());

        // Only position 0 should be valid
        assert!(builder.push_position(0));
        assert!(!builder.push_position(1));

        let segment = builder.build(None);
        assert_eq!(segment.count_positions(), 1);
        assert!(segment.contains(0));
    }

    #[test]
    fn exact_transition_boundaries() {
        let span = span_full();
        let mut builder = SegmentBuilder::new(span.clone());

        // Add exactly MAX_LIST_LEN positions to test boundary
        for i in 0..(Segment::MAX_LIST_LEN as usize) {
            assert!(builder.push_position((i * 2) as u64)); // Sparse to avoid ranges
        }

        let segment = builder.build(None);
        // Should still be List at exactly the boundary
        assert_eq!(segment.kind(), SegmentKind::List);

        // Now test one more to trigger transition
        let mut builder2 = SegmentBuilder::new(span.clone());
        for i in 0..((Segment::MAX_LIST_LEN as usize) + 1) {
            assert!(builder2.push_position((i * 2) as u64));
        }

        let segment2 = builder2.build(None);
        assert_eq!(segment2.kind(), SegmentKind::Bits);
    }

    #[test]
    fn ranges_boundary_transition() {
        let span = span_full();
        let mut builder = SegmentBuilder::new(span.clone());

        // Force ranges mode then add exactly MAX_RANGES_LEN ranges
        let initial_len = Segment::MAX_LIST_LEN + 10;
        let _ = builder.push_range(0..initial_len);

        let mut start = initial_len + 10;
        for _ in 1..(Segment::MAX_RANGES_LEN as usize) {
            // -1 because we already have 1 range
            let _ = builder.push_range(start..(start + 1));
            start += 2;
        }

        let segment = builder.build(None);
        assert_eq!(segment.kind(), SegmentKind::Ranges);

        // Add one more range to trigger transition
        let mut builder2 = SegmentBuilder::new(span.clone());
        let _ = builder2.push_range(0..initial_len);

        let mut start2 = initial_len + 10;
        for _ in 1..((Segment::MAX_RANGES_LEN as usize) + 1) {
            let _ = builder2.push_range(start2..(start2 + 1));
            start2 += 2;
        }

        let segment2 = builder2.build(None);
        assert_eq!(segment2.kind(), SegmentKind::Bits);
    }

    #[test]
    fn min_next_pos_consistency() {
        let span = span_small();
        let mut builder = SegmentBuilder::new(span.clone());

        assert_eq!(builder.min_next_pos(), span.start);

        assert!(builder.push_position(span.start + 10));
        assert_eq!(builder.min_next_pos(), span.start + 11);

        let tail = builder.push_range((span.start + 15)..(span.start + 20));
        assert_eq!(tail, (span.start + 20)..(span.start + 20));
        assert_eq!(builder.min_next_pos(), span.start + 20);

        // Failed operations shouldn't change min_next_pos
        assert!(!builder.push_position(span.end));
        assert_eq!(builder.min_next_pos(), span.start + 20);

        let tail2 = builder.push_range(span.end..(span.end + 10));
        assert_eq!(tail2, span.end..(span.end + 10));
        assert_eq!(builder.min_next_pos(), span.start + 20);
    }
}

#[cfg(test)]
mod extend_from_range_slice_tests {
    use super::*;
    use amudai_ranges::RangeListSlice;

    fn rls<'a>(ranges: &'a [Range<u64>]) -> RangeListSlice<'a, u64> {
        RangeListSlice::from_slice(ranges)
    }

    #[test]
    fn extend_ranges_empty_slice_noop() {
        let span = 0..200;
        let mut b = SegmentBuilder::new(span.clone());

        let empty = RangeListSlice::<u64>::empty();
        let before = b.min_next_pos();
        let tail = b.extend_from_range_slice(empty);

        assert!(tail.is_empty());
        assert_eq!(b.min_next_pos(), before);

        let seg = b.build(None);
        assert_eq!(seg.kind(), SegmentKind::Empty);
        assert_eq!(seg.count_positions(), 0);
        assert_eq!(seg.span(), span);
    }

    #[test]
    fn extend_ranges_all_fit_no_tail() {
        let span = 0..200;
        let mut b = SegmentBuilder::new(span.clone());

        let ranges = vec![5..10, 20..25, 30..31];
        let slice = rls(&ranges);
        let tail = b.extend_from_range_slice(slice);

        assert!(tail.is_empty());
        assert_eq!(b.min_next_pos(), 31);

        let seg = b.build(None);
        assert_eq!(seg.span(), span);
        // Small disjoint coverage -> likely Ranges
        assert_eq!(seg.kind(), SegmentKind::Ranges);
        let got: Vec<Range<u64>> = seg.ranges().collect();
        assert_eq!(got, ranges);
    }

    #[test]
    fn extend_ranges_entirely_beyond_span_returns_unchanged() {
        let span = 0..50;
        let mut b = SegmentBuilder::new(span.clone());
        let before_min = b.min_next_pos();

        let ranges = vec![60..70, 80..90];
        let slice = rls(&ranges);
        let tail = b.extend_from_range_slice(slice.clone());

        // Returned slice is unchanged (not consumed)
        assert_eq!(tail.to_vec(), slice.to_vec());
        assert_eq!(b.min_next_pos(), before_min);

        let seg = b.build(None);
        assert_eq!(seg.kind(), SegmentKind::Empty);
        assert_eq!(seg.count_positions(), 0);
    }

    #[test]
    fn extend_ranges_partial_overlap_produces_tail_and_merges_adjacent() {
        let span = 0..50;
        let mut b = SegmentBuilder::new(span.clone());

        // Second range straddles span.end; should split at 50
        let ranges = vec![10..30, 30..60];
        let slice = rls(&ranges);

        let tail = b.extend_from_range_slice(slice);
        assert!(!tail.is_empty());
        assert_eq!(tail.bounds(), 50..60);
        assert_eq!(tail.to_vec(), vec![50..60]);

        // Left part [10..30, 30..50] should be merged to [10..50]
        assert_eq!(b.min_next_pos(), 50);

        let seg = b.build(None);
        assert_eq!(seg.span(), span);
        assert_eq!(seg.kind(), SegmentKind::Ranges);
        let got: Vec<Range<u64>> = seg.ranges().collect();
        assert_eq!(got, vec![10..50]);
    }

    #[test]
    #[should_panic(expected = "slice_start")]
    fn extend_ranges_panics_if_slice_starts_before_min_next_pos() {
        let span = 0..200;
        let mut b = SegmentBuilder::new(span);
        // Advance min_next_pos to 30 via a previous operation
        let _ = b.push_range(10..30);
        assert_eq!(b.min_next_pos(), 30);

        // This slice starts at 25, which is < min_next_pos (30) -> should panic
        let ranges = vec![25..40];
        let slice = rls(&ranges);
        let _ = b.extend_from_range_slice(slice);
    }

    #[test]
    fn extend_ranges_list_to_ranges_transition() {
        // A single long run via range slice should force List -> Ranges
        let span = 0..Segment::SPAN;
        let mut b = SegmentBuilder::new(span.clone());

        b.push_position(100);
        b.push_position(110);

        let long_len = (Segment::MAX_LIST_LEN + 10) as u64;
        let start = 1010u64;
        let end = start + long_len;

        let s = [start..end];
        let slice = rls(&s);
        let tail = b.extend_from_range_slice(slice);
        assert!(tail.is_empty());
        assert_eq!(b.min_next_pos(), end);

        let seg = b.build(None);
        assert_eq!(seg.span(), span);
        assert_eq!(seg.kind(), SegmentKind::Ranges);
        assert_eq!(seg.count_positions(), long_len as usize + 2);
        assert!(seg.contains(start));
        assert!(seg.contains(end - 1));
        assert!(!seg.contains(start - 1));
    }

    #[test]
    fn extend_ranges_ranges_to_bits_transition_with_many_runs() {
        let span = 0..Segment::SPAN;
        let mut b = SegmentBuilder::new(span.clone());

        // First, force Ranges mode with a long run
        let long_len = (Segment::MAX_LIST_LEN + 20) as u64;
        let base = 10_000u64;
        let s = [base..(base + long_len)];
        let first = rls(&s);
        let _ = b.extend_from_range_slice(first);

        // Now add more than MAX_RANGES_LEN disjoint singletons to trigger Ranges -> Bits
        let max_ranges = Segment::MAX_RANGES_LEN as usize;
        let mut smalls: Vec<Range<u64>> = Vec::with_capacity(max_ranges + 5);
        let mut s = base + long_len + 10;
        for _ in 0..(max_ranges + 5) {
            smalls.push(s..(s + 1)); // leave a gap of 1 to avoid merging
            s += 2;
        }

        let tail = b.extend_from_range_slice(rls(&smalls));
        assert!(tail.is_empty());

        let seg = b.build(None);
        assert_eq!(seg.span(), span);
        assert_eq!(seg.kind(), SegmentKind::Bits);
    }

    #[test]
    fn extend_ranges_adjacent_merge_in_ranges_mode() {
        let span = 0..Segment::SPAN;
        let mut b = SegmentBuilder::new(span.clone());

        // Force ranges with first slice
        let long_len = (Segment::MAX_LIST_LEN + 10) as u64;
        let start = 5000u64;
        let mid = start + long_len;
        let _ = b.extend_from_range_slice(rls(&[start..mid]));
        assert_eq!(b.min_next_pos(), mid);

        // Adjacent second range
        let _ = b.extend_from_range_slice(rls(&[mid..(mid + 20)]));
        assert_eq!(b.min_next_pos(), mid + 20);

        let seg = b.build(None);
        assert_eq!(seg.kind(), SegmentKind::Ranges);
        let got: Vec<Range<u64>> = seg.ranges().collect();
        assert_eq!(got, vec![start..(mid + 20)]);
    }

    #[test]
    fn extend_ranges_full_span_results_in_full_segment() {
        let span = 0..200;
        let mut b = SegmentBuilder::new(span.clone());

        let s = [span.clone()];
        let tail = b.extend_from_range_slice(rls(&s));
        assert!(tail.is_empty());
        assert_eq!(b.min_next_pos(), span.end);

        let seg = b.build(None);
        assert_eq!(seg.span(), span);
        assert_eq!(seg.kind(), SegmentKind::Full);
        assert_eq!(seg.count_positions(), (span.end - span.start) as usize);
    }

    #[test]
    fn extend_ranges_with_offset_span() {
        let span = super::span_offset();
        let mut b = SegmentBuilder::new(span.clone());

        let a = span.start + 123;
        let c = span.start + 700;
        let d = c + 50;
        let s = [a..(a + 10), c..d];
        let tail = b.extend_from_range_slice(rls(&s));
        assert!(tail.is_empty());
        assert_eq!(b.min_next_pos(), d);

        let seg = b.build(None);
        assert_eq!(seg.span(), span);
        assert!(seg.contains(a));
        assert!(seg.contains(a + 9));
        assert!(seg.contains(c));
        assert!(seg.contains(d - 1));
        assert!(!seg.contains(a - 1));
        assert!(!seg.contains(d));
    }

    #[test]
    fn extend_ranges_split_inside_single_range() {
        let span = 0..110;
        let mut b = SegmentBuilder::new(span.clone());

        let slice = rls(&[90..120]); // Splits at 110 into [90..110] + tail [110..120]
        let tail = b.extend_from_range_slice(slice);
        assert_eq!(tail.bounds(), 110..120);
        assert_eq!(tail.to_vec(), vec![110..120]);
        assert_eq!(b.min_next_pos(), 110);

        let seg = b.build(None);
        assert_eq!(seg.span(), span);
        let got: Vec<Range<u64>> = seg.ranges().collect();
        assert_eq!(got, vec![90..110]);
    }
}
