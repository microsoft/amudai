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
    let expected: u64 = ranges.iter().map(|r| (r.end - r.start)).sum();

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
