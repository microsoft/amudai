use crate::{PositionSet, segment::Segment};

#[test]
fn test_from_positions_basic() {
    let set = PositionSet::from_positions(55500000, [100u64, 12000100, 30000001].into_iter());
    assert_eq!(set.count_positions(), 3);
    let stats = set.compute_stats();
    assert_eq!(
        stats.total_segments,
        stats.span.div_ceil(Segment::SPAN) as usize
    );
    assert_eq!(stats.list_segments, 3);
    assert_eq!(
        stats.empty_segments,
        stats.total_segments - stats.list_segments
    );

    let span = 37000000u64;
    let positions = (0..span).step_by(7);
    let set = PositionSet::from_positions(span, positions.clone());

    let stats = set.compute_stats();
    assert_eq!(stats.position_count, positions.clone().count() as u64);
    assert_eq!(stats.bit_segments, stats.total_segments);

    for pos in positions {
        assert!(set.contains(pos), "contains {pos}");
    }
}

#[test]
fn test_from_positions_empty_iter() {
    let span = 10u64;
    let set = PositionSet::from_positions(span, std::iter::empty::<u64>());
    let stats = set.compute_stats();
    assert_eq!(stats.position_count, 0);
    assert_eq!(stats.empty_segments, stats.total_segments);
    assert!(!set.contains(0));
}

#[test]
fn test_from_positions_ignores_out_of_span() {
    let span = 3_000_000u64;
    let positions = [0u64, 1, 999_999, 1_000_000, 2_000_000, 2_500_000, 4_000_000];
    let set = PositionSet::from_positions(span, positions.into_iter());

    assert_eq!(set.count_positions(), 6);
    assert!(set.contains(0));
    assert!(set.contains(1));
    assert!(set.contains(999_999));
    assert!(set.contains(1_000_000));
    assert!(set.contains(2_000_000));
    assert!(set.contains(2_500_000));
    assert!(!set.contains(2)); // within span but not present
    dbg!(set.compute_stats());
}

#[test]
fn test_from_ranges_basic() {
    let span = 10_000u64;
    let ranges = vec![5u64..10, 100u64..200, 9000u64..9500];

    let set = PositionSet::from_ranges(span, ranges.clone().into_iter());
    let expected: u64 = ranges.iter().map(|r| r.end - r.start).sum();

    assert_eq!(set.count_positions(), expected);

    for r in &ranges {
        assert!(set.contains(r.start));
        assert!(set.contains(r.start + 1));
        assert!(set.contains(r.end - 1));
        if r.start > 0 {
            assert!(!set.contains(r.start - 1));
        }
        if r.end < span {
            assert!(!set.contains(r.end));
        }
    }
}

#[test]
fn test_from_ranges_empty_iter() {
    let span = 12_345u64;
    let set = PositionSet::from_ranges(span, std::iter::empty::<std::ops::Range<u64>>());
    let stats = set.compute_stats();
    assert_eq!(stats.position_count, 0);
    assert_eq!(stats.empty_segments, stats.total_segments);
    assert!(!set.contains(0));
}

#[test]
fn test_union_with_basic() {
    let span = Segment::SPAN * 3;
    let positions_a = vec![0, 5, Segment::SPAN + 1, Segment::SPAN * 2 + 10];
    let positions_b = vec![
        1,
        5,
        Segment::SPAN,
        Segment::SPAN * 2 + 10,
        Segment::SPAN * 2 + 11,
    ];

    let a = PositionSet::from_positions(span, positions_a.iter().copied());
    let b = PositionSet::from_positions(span, positions_b.iter().copied());

    let expected = a.union(&b);

    let mut inplace = PositionSet::from_positions(span, positions_a.iter().copied());
    inplace.union_with(&b);

    let mut positions_union = positions_a.clone();
    positions_union.extend(positions_b.iter().copied());
    positions_union.sort_unstable();
    positions_union.dedup();

    assert_eq!(inplace.count_positions(), expected.count_positions());
    for p in positions_union {
        assert!(inplace.contains(p), "union_with result missing {p}");
    }

    // original b unchanged
    for p in positions_b {
        assert!(b.contains(p));
    }
}

#[test]
fn test_union_with_empty_other() {
    let span = Segment::SPAN * 2;
    let positions_a = vec![0, 7, Segment::SPAN + 3];
    let mut a = PositionSet::from_positions(span, positions_a.iter().copied());
    let empty = PositionSet::empty(span);
    let before = a.count_positions();
    a.union_with(&empty);
    assert_eq!(a.count_positions(), before);
    for p in positions_a {
        assert!(a.contains(p));
    }
}

#[test]
fn test_intersect_with_basic() {
    let span = Segment::SPAN * 3;
    let positions_a = vec![
        0,
        5,
        Segment::SPAN,
        Segment::SPAN + 1,
        Segment::SPAN * 2 + 10,
    ];
    let positions_b = [
        5,
        Segment::SPAN,
        Segment::SPAN * 2 + 9,
        Segment::SPAN * 2 + 10,
        Segment::SPAN * 2 + 11,
    ];
    let a = PositionSet::from_positions(span, positions_a.iter().copied());
    let b = PositionSet::from_positions(span, positions_b.iter().copied());

    let expected = a.intersect(&b);

    let mut inplace = PositionSet::from_positions(span, positions_a.iter().copied());
    inplace.intersect_with(&b);

    let intersection: Vec<u64> = positions_a.into_iter().filter(|p| b.contains(*p)).collect();

    assert_eq!(inplace.count_positions(), expected.count_positions());
    assert_eq!(inplace.count_positions() as usize, intersection.len());
    for p in intersection {
        assert!(inplace.contains(p));
    }
}

#[test]
fn test_intersect_with_full_other() {
    let span = Segment::SPAN * 2;
    let positions_a = (0..100).map(|i| i * 2).collect::<Vec<_>>();
    let mut a = PositionSet::from_positions(span, positions_a.iter().copied());
    let full = PositionSet::full(span);
    let before = a.count_positions();
    a.intersect_with(&full);
    assert_eq!(a.count_positions(), before);
    for p in positions_a {
        assert!(a.contains(p));
    }
}

#[test]
fn test_merge_with_basic() {
    let span = Segment::SPAN * 3;
    let positions_a = vec![0, Segment::SPAN - 1, Segment::SPAN + 5];
    let positions_b = [1, Segment::SPAN, Segment::SPAN * 2 + 3];
    let a = PositionSet::from_positions(span, positions_a.iter().copied());
    let b = PositionSet::from_positions(span, positions_b.iter().copied());

    let expected = a.union(&b);

    // Perform merge_with consuming b
    let mut a_merge = PositionSet::from_positions(span, positions_a.iter().copied());
    let b_owned = PositionSet::from_positions(span, positions_b.iter().copied());
    a_merge.merge_with(b_owned);

    assert_eq!(a_merge.count_positions(), expected.count_positions());

    let mut union_positions = positions_a.clone();
    union_positions.extend(positions_b.iter().copied());
    union_positions.sort_unstable();
    union_positions.dedup();
    for p in union_positions {
        assert!(a_merge.contains(p), "merge_with missing {p}");
    }
}

#[test]
fn test_merge_with_empty_left() {
    let span = Segment::SPAN * 2;
    let positions_b = vec![0, 3, Segment::SPAN + 1];
    let mut empty_left = PositionSet::empty(span);
    let b_owned = PositionSet::from_positions(span, positions_b.iter().copied());
    empty_left.merge_with(b_owned);
    assert_eq!(empty_left.count_positions(), 3);
    for p in positions_b {
        assert!(empty_left.contains(p));
    }
}

#[test]
fn test_invert_in_place_basic() {
    let span = Segment::SPAN + 50;
    let positions = (0..span).step_by(7).collect::<Vec<_>>();
    let mut set = PositionSet::from_positions(span, positions.iter().copied());
    let expected = set.invert();

    set.invert_in_place();

    assert_eq!(set.count_positions(), expected.count_positions());
    for p in 0..span {
        assert_eq!(set.contains(p), expected.contains(p), "mismatch at {p}");
    }
}

#[test]
fn test_invert_in_place_full_and_empty() {
    let span = Segment::SPAN + 13;

    // Full -> Empty
    let mut full = PositionSet::full(span);
    full.invert_in_place();
    assert_eq!(full.count_positions(), 0);
    if span > 0 {
        assert!(!full.contains(0));
    }

    // Empty -> Full
    let mut empty = PositionSet::empty(span);
    empty.invert_in_place();
    assert_eq!(empty.count_positions(), span);
    if span > 0 {
        assert!(empty.contains(span - 1));
    }
}

#[test]
#[should_panic]
fn test_union_with_span_mismatch_panics() {
    let mut a = PositionSet::empty(10);
    let b = PositionSet::empty(11);
    a.union_with(&b);
}

#[test]
#[should_panic]
fn test_intersect_with_span_mismatch_panics() {
    let mut a = PositionSet::full(32);
    let b = PositionSet::full(64);
    a.intersect_with(&b);
}

#[test]
fn test_invert_in_place_empty_span() {
    let mut set = PositionSet::empty(0);
    set.invert_in_place(); // should be a no-op logically
    assert_eq!(set.count_positions(), 0);
}

#[test]
fn test_is_equal_to_identical_sets() {
    let span = Segment::SPAN * 2 + 100;
    let positions = vec![0, 5, 100, Segment::SPAN, Segment::SPAN + 50, span - 1];
    let set1 = PositionSet::from_positions(span, positions.iter().copied());
    let set2 = PositionSet::from_positions(span, positions.iter().copied());

    assert!(set1.is_equal_to(&set2));
    assert!(set2.is_equal_to(&set1));
}

#[test]
fn test_is_equal_to_self() {
    let span = Segment::SPAN + 50;
    let positions = (0..span).step_by(7).collect::<Vec<_>>();
    let set = PositionSet::from_positions(span, positions.iter().copied());

    assert!(set.is_equal_to(&set));
}

#[test]
fn test_is_equal_to_different_positions() {
    let span = 1000;
    let set1 = PositionSet::from_positions(span, [1, 5, 10, 50].iter().copied());
    let set2 = PositionSet::from_positions(span, [1, 5, 10, 51].iter().copied());

    assert!(!set1.is_equal_to(&set2));
    assert!(!set2.is_equal_to(&set1));
}

#[test]
fn test_is_equal_to_different_spans() {
    let positions = vec![1, 5, 10, 50];
    let set1 = PositionSet::from_positions(100, positions.iter().copied());
    let set2 = PositionSet::from_positions(200, positions.iter().copied());

    assert!(!set1.is_equal_to(&set2));
    assert!(!set2.is_equal_to(&set1));
}

#[test]
fn test_is_equal_to_empty_sets() {
    let span1 = 1000;
    let span2 = 1000;
    let empty1 = PositionSet::empty(span1);
    let empty2 = PositionSet::empty(span2);

    assert!(empty1.is_equal_to(&empty2));
    assert!(empty2.is_equal_to(&empty1));
}

#[test]
fn test_is_equal_to_empty_sets_different_spans() {
    let empty1 = PositionSet::empty(1000);
    let empty2 = PositionSet::empty(2000);

    assert!(!empty1.is_equal_to(&empty2));
    assert!(!empty2.is_equal_to(&empty1));
}

#[test]
fn test_is_equal_to_full_sets() {
    let span = 1000;
    let full1 = PositionSet::full(span);
    let full2 = PositionSet::full(span);

    assert!(full1.is_equal_to(&full2));
    assert!(full2.is_equal_to(&full1));
}

#[test]
fn test_is_equal_to_full_sets_different_spans() {
    let full1 = PositionSet::full(1000);
    let full2 = PositionSet::full(2000);

    assert!(!full1.is_equal_to(&full2));
    assert!(!full2.is_equal_to(&full1));
}

#[test]
fn test_is_equal_to_empty_vs_non_empty() {
    let span = 1000;
    let empty = PositionSet::empty(span);
    let non_empty = PositionSet::from_positions(span, [100].iter().copied());

    assert!(!empty.is_equal_to(&non_empty));
    assert!(!non_empty.is_equal_to(&empty));
}

#[test]
fn test_is_equal_to_full_vs_non_full() {
    let span = 100;
    let full = PositionSet::full(span);
    let non_full =
        PositionSet::from_positions(span, (0..span - 1).collect::<Vec<_>>().iter().copied());

    assert!(!full.is_equal_to(&non_full));
    assert!(!non_full.is_equal_to(&full));
}

#[test]
fn test_is_equal_to_different_segment_representations() {
    // Create sets with the same logical content but potentially different internal representations
    let span = Segment::SPAN * 3;

    // Create via positions (likely to use List segments for sparse data)
    let positions = vec![100, 200, Segment::SPAN + 100, Segment::SPAN * 2 + 50];
    let set1 = PositionSet::from_positions(span, positions.iter().copied());

    // Create via ranges (likely to use Range segments)
    let ranges = vec![
        100..101,
        200..201,
        (Segment::SPAN + 100)..(Segment::SPAN + 101),
        (Segment::SPAN * 2 + 50)..(Segment::SPAN * 2 + 51),
    ];
    let set2 = PositionSet::from_ranges(span, ranges.into_iter());

    // Despite potentially different internal representations, they should be equal
    assert!(set1.is_equal_to(&set2));
    assert!(set2.is_equal_to(&set1));
}

#[test]
fn test_is_equal_to_after_operations() {
    let span = Segment::SPAN * 2;
    let positions1 = vec![0, 100, Segment::SPAN + 50];
    let positions2 = vec![50, 200, Segment::SPAN + 100];

    let set1 = PositionSet::from_positions(span, positions1.iter().copied());
    let set2 = PositionSet::from_positions(span, positions2.iter().copied());

    // Create union in two different ways
    let union1 = set1.union(&set2);
    let union2 = set2.union(&set1);

    // Results should be equal
    assert!(union1.is_equal_to(&union2));
    assert!(union2.is_equal_to(&union1));
}

#[test]
fn test_is_equal_to_large_sets() {
    let span = Segment::SPAN * 5;
    // Create large sets with the same positions
    let positions: Vec<u64> = (0..span).step_by(1000).collect();

    let set1 = PositionSet::from_positions(span, positions.iter().copied());
    let set2 = PositionSet::from_positions(span, positions.iter().copied());

    assert!(set1.is_equal_to(&set2));

    // Modify one position
    let mut modified_positions = positions;
    modified_positions.push(span - 1);
    let set3 = PositionSet::from_positions(span, modified_positions.iter().copied());

    assert!(!set1.is_equal_to(&set3));
}

#[test]
fn test_is_equal_to_zero_span() {
    let empty1 = PositionSet::empty(0);
    let empty2 = PositionSet::empty(0);

    assert!(empty1.is_equal_to(&empty2));
}

#[test]
fn test_is_equal_to_inverted_sets() {
    let span = 1000;
    let positions = vec![10, 50, 100, 500, 900];
    let set = PositionSet::from_positions(span, positions.iter().copied());

    let inverted1 = set.invert();
    let inverted2 = set.invert();

    assert!(inverted1.is_equal_to(&inverted2));
    assert!(!set.is_equal_to(&inverted1));
}
