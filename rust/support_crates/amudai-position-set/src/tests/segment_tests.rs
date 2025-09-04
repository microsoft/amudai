use crate::segment::{
    Segment, SegmentKind, bits::BitSegment, list::ListSegment, ranges::RangeSegment,
};

#[test]
fn infer_empty_from_list() {
    let span = 0u64..64u64;
    let list = ListSegment::empty(span.clone());
    let seg = Segment::List(list);
    assert_eq!(seg.infer_optimal_kind(), SegmentKind::Empty);
}

#[test]
fn infer_full_from_bits() {
    let span = 0u64..64u64;
    let bits = BitSegment::full(span.clone());
    let seg = Segment::Bits(bits);
    assert_eq!(seg.infer_optimal_kind(), SegmentKind::Full);
}

#[test]
fn prefers_list_for_sparse_singleton_from_bits() {
    let span = 0u64..64u64; // sizes: bits=8, list=4, ranges=8 -> List
    let bits = BitSegment::from_positions(span.clone(), [5u64].into_iter());
    let seg = Segment::Bits(bits);
    assert_eq!(seg.infer_optimal_kind(), SegmentKind::List);
}

#[test]
fn prefers_ranges_for_long_run_from_bits() {
    let span = 0u64..128u64; // sizes: bits=16, list=80 (20 pts), ranges=8 -> Ranges
    let run = 10u64..30u64;
    let seg_bits = Segment::Bits(BitSegment::from_ranges(
        span.clone(),
        std::iter::once(run.clone()),
    ));
    assert_eq!(seg_bits.infer_optimal_kind(), SegmentKind::Ranges);

    // Also stays Ranges if already in ranges form
    let seg_ranges = Segment::Ranges(RangeSegment::from_ranges(
        span.clone(),
        std::iter::once(run),
    ));
    assert_eq!(seg_ranges.infer_optimal_kind(), SegmentKind::Ranges);
}

#[test]
fn prefers_bits_for_dense_scattered() {
    let span = 0u64..1024u64; // even positions: 32 pts, 32 runs -> list=128, ranges=256, bits=8 -> Bits
    let positions: Vec<u64> = (0u64..1024u64).step_by(2).collect();

    let seg_ranges = Segment::Ranges(RangeSegment::from_positions(
        span.clone(),
        positions.clone().into_iter(),
    ));
    assert_eq!(seg_ranges.infer_optimal_kind(), SegmentKind::Bits);

    let seg_list = Segment::List(ListSegment::from_positions(
        span.clone(),
        positions.clone().into_iter(),
    ));
    assert_eq!(seg_list.infer_optimal_kind(), SegmentKind::Bits);

    let seg_bits = Segment::Bits(BitSegment::from_positions(
        span.clone(),
        positions.into_iter(),
    ));
    assert_eq!(seg_bits.infer_optimal_kind(), SegmentKind::Bits);
}

// Helper to collect positions from any Segment variant
fn positions_of(seg: &Segment) -> Vec<u64> {
    match seg {
        Segment::Empty(s) => s.positions().collect(),
        Segment::Full(s) => s.positions().collect(),
        Segment::List(s) => s.positions().collect(),
        Segment::Bits(s) => s.positions().collect(),
        Segment::Ranges(s) => s.positions().collect(),
    }
}

#[test]
fn optimize_list_empty_to_empty() {
    let span = 0u64..64u64;
    let list = ListSegment::empty(span.clone());
    let mut seg = Segment::List(list);

    let before = positions_of(&seg);
    let kind = seg.optimize();
    assert_eq!(kind, SegmentKind::Empty);
    assert!(matches!(seg, Segment::Empty(_)));
    let after = positions_of(&seg);
    assert_eq!(after, before);
}

#[test]
fn optimize_bits_full_to_full() {
    let span = 0u64..64u64;
    let bits = BitSegment::full(span.clone());
    let mut seg = Segment::Bits(bits);

    let before = positions_of(&seg);
    let kind = seg.optimize();
    assert_eq!(kind, SegmentKind::Full);
    assert!(matches!(seg, Segment::Full(_)));
    let after = positions_of(&seg);
    assert_eq!(after, before);
}

#[test]
fn optimize_sparse_singleton_bits_to_list() {
    let span = 0u64..64u64; // sizes: bits=8, list=4, ranges=8 -> List
    let bits = BitSegment::from_positions(span.clone(), [5u64].into_iter());
    let mut seg = Segment::Bits(bits);

    let before = positions_of(&seg);
    let kind = seg.optimize();
    assert_eq!(kind, SegmentKind::List);
    assert!(matches!(seg, Segment::List(_)));
    let after = positions_of(&seg);
    assert_eq!(after, before);
}

#[test]
fn optimize_long_run_bits_to_ranges_and_noop_for_ranges() {
    let span = 0u64..128u64; // sizes: bits=16, list=80 (20 pts), ranges=8 -> Ranges
    let run = 10u64..30u64;

    // Bits -> Ranges
    let bits = BitSegment::from_ranges(span.clone(), std::iter::once(run.clone()));
    let mut seg_bits = Segment::Bits(bits);
    let before_bits = positions_of(&seg_bits);
    let kind = seg_bits.optimize();
    assert_eq!(kind, SegmentKind::Ranges);
    assert!(matches!(seg_bits, Segment::Ranges(_)));
    let after_bits = positions_of(&seg_bits);
    assert_eq!(after_bits, before_bits);

    // Already Ranges -> no-op
    let ranges = RangeSegment::from_ranges(span.clone(), std::iter::once(run));
    let mut seg_ranges = Segment::Ranges(ranges);
    let before_ranges = positions_of(&seg_ranges);
    let kind2 = seg_ranges.optimize();
    assert_eq!(kind2, SegmentKind::Ranges);
    assert!(matches!(seg_ranges, Segment::Ranges(_)));
    let after_ranges = positions_of(&seg_ranges);
    assert_eq!(after_ranges, before_ranges);
}

#[test]
fn optimize_dense_scattered_to_bits_from_each_variant() {
    let span = 0u64..1024u64; // even positions -> Bits preferred
    let positions: Vec<u64> = (0u64..1024u64).step_by(2).collect();

    // Ranges -> Bits
    let ranges = RangeSegment::from_positions(span.clone(), positions.clone().into_iter());
    let mut seg_ranges = Segment::Ranges(ranges);
    let before_ranges = positions_of(&seg_ranges);
    assert_eq!(seg_ranges.optimize(), SegmentKind::Bits);
    assert!(matches!(seg_ranges, Segment::Bits(_)));
    let after_ranges = positions_of(&seg_ranges);
    assert_eq!(after_ranges, before_ranges);

    // List -> Bits
    let list = ListSegment::from_positions(span.clone(), positions.clone().into_iter());
    let mut seg_list = Segment::List(list);
    let before_list = positions_of(&seg_list);
    assert_eq!(seg_list.optimize(), SegmentKind::Bits);
    assert!(matches!(seg_list, Segment::Bits(_)));
    let after_list = positions_of(&seg_list);
    assert_eq!(after_list, before_list);

    // Bits already optimal -> no-op
    let bits = BitSegment::from_positions(span.clone(), positions.into_iter());
    let mut seg_bits = Segment::Bits(bits);
    let before_bits = positions_of(&seg_bits);
    assert_eq!(seg_bits.optimize(), SegmentKind::Bits);
    assert!(matches!(seg_bits, Segment::Bits(_)));
    let after_bits = positions_of(&seg_bits);
    assert_eq!(after_bits, before_bits);
}

#[test]
fn from_positions_empty() {
    let span = 0u64..64u64;
    let seg = Segment::from_positions(span.clone(), std::iter::empty());
    assert!(matches!(seg, Segment::Empty(_)));
    assert_eq!(positions_of(&seg), Vec::<u64>::new());
}

#[test]
fn from_positions_full() {
    let span = 0u64..64u64;
    let seg = Segment::from_positions(span.clone(), span.start..span.end);
    assert!(matches!(seg, Segment::Full(_)));
    assert_eq!(
        positions_of(&seg),
        (span.start..span.end).collect::<Vec<_>>()
    );
}

#[test]
fn from_positions_prefers_list_for_sparse_singleton() {
    let span = 0u64..64u64; // sizes: bits=8, list=4, ranges=8 -> List
    let seg = Segment::from_positions(span.clone(), [5u64].into_iter());
    assert!(matches!(seg, Segment::List(_)));
    assert_eq!(positions_of(&seg), vec![5]);
}

#[test]
fn from_positions_prefers_ranges_for_long_run() {
    let span = 0u64..1024u64;
    let run = 100u64..300u64;
    let seg = Segment::from_positions(span.clone(), run.clone());
    dbg!(seg.kind());
    assert!(matches!(seg, Segment::Ranges(_)));
    assert_eq!(positions_of(&seg), run.collect::<Vec<_>>());
}

#[test]
fn from_positions_prefers_bits_for_dense_scattered() {
    let span = 0u64..1024u64;
    let positions: Vec<u64> = (span.start..span.end).step_by(2).collect();
    let seg = Segment::from_positions(span.clone(), positions.clone().into_iter());
    assert!(matches!(seg, Segment::Bits(_)));
    assert_eq!(positions_of(&seg), positions);
}

#[test]
fn from_ranges_empty() {
    let span = 0u64..64u64;
    let seg = Segment::from_ranges(span.clone(), std::iter::empty());
    assert!(matches!(seg, Segment::Empty(_)));
    assert_eq!(positions_of(&seg), Vec::<u64>::new());
}

#[test]
fn from_ranges_full() {
    let span = 0u64..64u64;
    let seg = Segment::from_ranges(span.clone(), std::iter::once(span.clone()));
    assert!(matches!(seg, Segment::Full(_)));
    assert_eq!(
        positions_of(&seg),
        (span.start..span.end).collect::<Vec<_>>()
    );
}

#[test]
fn from_ranges_prefers_list_for_sparse_points() {
    let span = 0u64..1024u64;
    let ranges = [5u64..6u64, 55u64..56u64, 1000..1001];
    let seg = Segment::from_ranges(span.clone(), ranges.into_iter());
    assert!(matches!(seg, Segment::List(_)));
    assert_eq!(positions_of(&seg), vec![5, 55, 1000]);
}

#[test]
fn from_ranges_prefers_ranges_for_long_run() {
    let span = 0u64..128u64; // long run -> Ranges
    let run = 10u64..30u64;
    let seg = Segment::from_ranges(span.clone(), std::iter::once(run.clone()));
    assert!(matches!(seg, Segment::Ranges(_)));
    assert_eq!(positions_of(&seg), run.collect::<Vec<_>>());
}

#[test]
fn from_ranges_prefers_bits_for_dense_scattered() {
    let span = 0u64..1024u64;
    // Encode even positions as many unit ranges.
    let ranges = (span.start..span.end).step_by(2).map(|i| i..i + 1);
    let expected: Vec<u64> = (span.start..span.end).step_by(2).collect();
    let seg = Segment::from_ranges(span.clone(), ranges);
    assert!(matches!(seg, Segment::Bits(_)));
    assert_eq!(positions_of(&seg), expected);
}

// ==================== is_equal_to tests ====================

#[test]
fn is_equal_to_empty_segments() {
    let span = 0u64..64u64;
    let empty1 = Segment::empty(span.clone());
    let empty2 = Segment::empty(span.clone());
    let empty_list = Segment::List(ListSegment::empty(span.clone()));

    assert!(empty1.is_equal_to(&empty2));
    assert!(empty2.is_equal_to(&empty1));
    assert!(empty1.is_equal_to(&empty_list));
    assert!(empty_list.is_equal_to(&empty1));
}

#[test]
fn is_equal_to_full_segments() {
    let span = 0u64..64u64;
    let full1 = Segment::full(span.clone());
    let full2 = Segment::full(span.clone());
    let full_bits = Segment::Bits(BitSegment::full(span.clone()));

    assert!(full1.is_equal_to(&full2));
    assert!(full2.is_equal_to(&full1));
    assert!(full1.is_equal_to(&full_bits));
    assert!(full_bits.is_equal_to(&full1));
}

#[test]
fn is_equal_to_same_positions_different_representations() {
    let span = 0u64..1024u64;
    let positions = vec![5u64, 10u64, 100u64, 500u64];

    // Create same logical content in different representations
    let list_seg = Segment::List(ListSegment::from_positions(
        span.clone(),
        positions.clone().into_iter(),
    ));
    let bits_seg = Segment::Bits(BitSegment::from_positions(
        span.clone(),
        positions.clone().into_iter(),
    ));
    let ranges_seg = Segment::Ranges(RangeSegment::from_positions(
        span.clone(),
        positions.into_iter(),
    ));

    // All should be equal regardless of internal representation
    assert!(list_seg.is_equal_to(&bits_seg));
    assert!(bits_seg.is_equal_to(&list_seg));
    assert!(list_seg.is_equal_to(&ranges_seg));
    assert!(ranges_seg.is_equal_to(&list_seg));
    assert!(bits_seg.is_equal_to(&ranges_seg));
    assert!(ranges_seg.is_equal_to(&bits_seg));
}

#[test]
fn is_equal_to_different_positions() {
    let span = 0u64..64u64;
    let seg1 = Segment::List(ListSegment::from_positions(
        span.clone(),
        [5u64, 10u64].into_iter(),
    ));
    let seg2 = Segment::List(ListSegment::from_positions(
        span.clone(),
        [5u64, 11u64].into_iter(),
    ));
    let seg3 = Segment::List(ListSegment::from_positions(
        span.clone(),
        [5u64, 10u64, 15u64].into_iter(),
    ));

    assert!(!seg1.is_equal_to(&seg2)); // Different positions
    assert!(!seg2.is_equal_to(&seg1));
    assert!(!seg1.is_equal_to(&seg3)); // Different position count
    assert!(!seg3.is_equal_to(&seg1));
}

#[test]
fn is_equal_to_empty_vs_non_empty() {
    let span = 0u64..64u64;
    let empty = Segment::empty(span.clone());
    let non_empty = Segment::List(ListSegment::from_positions(
        span.clone(),
        [10u64].into_iter(),
    ));

    assert!(!empty.is_equal_to(&non_empty));
    assert!(!non_empty.is_equal_to(&empty));
}

#[test]
fn is_equal_to_full_vs_non_full() {
    let span = 0u64..64u64;
    let full = Segment::full(span.clone());
    let almost_full = Segment::Bits(BitSegment::from_positions(
        span.clone(),
        (span.start..span.end - 1).collect::<Vec<_>>().into_iter(), // Missing last position
    ));

    assert!(!full.is_equal_to(&almost_full));
    assert!(!almost_full.is_equal_to(&full));
}

#[test]
fn is_equal_to_ranges_with_same_content() {
    let span = 0u64..1024u64;
    let ranges1 = vec![10u64..20u64, 30u64..40u64, 100u64..200u64];
    let ranges2 = ranges1.clone();

    let seg1 = Segment::Ranges(RangeSegment::from_ranges(span.clone(), ranges1.into_iter()));
    let seg2 = Segment::Ranges(RangeSegment::from_ranges(span.clone(), ranges2.into_iter()));

    assert!(seg1.is_equal_to(&seg2));
    assert!(seg2.is_equal_to(&seg1));
}

#[test]
fn is_equal_to_mixed_segment_types() {
    let span = 0u64..128u64;

    // Create a pattern that could be represented efficiently by different segment types
    let run = 10u64..30u64; // A single long run

    let bits_seg = Segment::Bits(BitSegment::from_ranges(
        span.clone(),
        std::iter::once(run.clone()),
    ));
    let ranges_seg = Segment::Ranges(RangeSegment::from_ranges(
        span.clone(),
        std::iter::once(run.clone()),
    ));
    let list_seg = Segment::List(ListSegment::from_positions(span.clone(), run.clone()));

    // All should be equal since they represent the same logical content
    assert!(bits_seg.is_equal_to(&ranges_seg));
    assert!(ranges_seg.is_equal_to(&bits_seg));
    assert!(bits_seg.is_equal_to(&list_seg));
    assert!(list_seg.is_equal_to(&bits_seg));
    assert!(ranges_seg.is_equal_to(&list_seg));
    assert!(list_seg.is_equal_to(&ranges_seg));
}

#[test]
fn is_equal_to_reflexive() {
    let span = 0u64..64u64;
    let positions = vec![1u64, 5u64, 10u64, 20u64];

    let empty = Segment::empty(span.clone());
    let full = Segment::full(span.clone());
    let list = Segment::List(ListSegment::from_positions(
        span.clone(),
        positions.clone().into_iter(),
    ));
    let bits = Segment::Bits(BitSegment::from_positions(
        span.clone(),
        positions.clone().into_iter(),
    ));
    let ranges = Segment::Ranges(RangeSegment::from_positions(
        span.clone(),
        positions.into_iter(),
    ));

    // Every segment should be equal to itself
    assert!(empty.is_equal_to(&empty));
    assert!(full.is_equal_to(&full));
    assert!(list.is_equal_to(&list));
    assert!(bits.is_equal_to(&bits));
    assert!(ranges.is_equal_to(&ranges));
}

#[test]
fn is_equal_to_different_spans_mixed_types() {
    // Test span checking when comparison falls through to bit conversion
    let span1 = 0u64..64u64;
    let span2 = Segment::SPAN..(Segment::SPAN + 64u64);
    let positions = vec![5u64, 10u64, 20u64];

    let list1 = Segment::List(ListSegment::from_positions(
        span1.clone(),
        positions.clone().into_iter(),
    ));
    // Adjust positions for the different span
    let positions2: Vec<u64> = positions.iter().map(|&p| p + Segment::SPAN).collect();
    let bits2 = Segment::Bits(BitSegment::from_positions(
        span2.clone(),
        positions2.into_iter(),
    ));

    // These should not be equal due to different spans
    // This will convert both to BitSegment and compare, which checks spans
    assert!(!list1.is_equal_to(&bits2));
    assert!(!bits2.is_equal_to(&list1));
}

#[test]
fn is_equal_to_symmetric() {
    let span = 0u64..64u64;
    let positions = vec![1u64, 5u64, 10u64];

    let seg1 = Segment::List(ListSegment::from_positions(
        span.clone(),
        positions.clone().into_iter(),
    ));
    let seg2 = Segment::Bits(BitSegment::from_positions(
        span.clone(),
        positions.into_iter(),
    ));

    // If seg1.is_equal_to(&seg2), then seg2.is_equal_to(&seg1) should also be true
    let eq1 = seg1.is_equal_to(&seg2);
    let eq2 = seg2.is_equal_to(&seg1);
    assert_eq!(eq1, eq2);
    assert!(eq1); // They should actually be equal
}

#[test]
fn is_equal_to_transitive() {
    let span = 0u64..64u64;
    let positions = vec![1u64, 5u64, 10u64];

    let seg1 = Segment::List(ListSegment::from_positions(
        span.clone(),
        positions.clone().into_iter(),
    ));
    let seg2 = Segment::Bits(BitSegment::from_positions(
        span.clone(),
        positions.clone().into_iter(),
    ));
    let seg3 = Segment::Ranges(RangeSegment::from_positions(
        span.clone(),
        positions.into_iter(),
    ));

    // If seg1 == seg2 and seg2 == seg3, then seg1 == seg3
    assert!(seg1.is_equal_to(&seg2));
    assert!(seg2.is_equal_to(&seg3));
    assert!(seg1.is_equal_to(&seg3));
}
