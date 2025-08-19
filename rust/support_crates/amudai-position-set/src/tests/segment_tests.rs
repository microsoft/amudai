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
