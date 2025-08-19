use crate::{bit_array::BitArray, position_set_builder::PositionSetBuilder, segment::Segment};

#[test]
fn test_build_empty() {
    let builder = PositionSetBuilder::new();
    let set = builder.build(0);
    assert_eq!(set.span(), 0);
    let stats = set.compute_stats();
    assert_eq!(stats.position_count, 0);
    assert_eq!(stats.total_segments, 0);

    let builder = PositionSetBuilder::new();
    let set = builder.build(10000);
    assert_eq!(set.span(), 10000);
    let stats = set.compute_stats();
    assert_eq!(stats.position_count, 0);
    assert_eq!(stats.total_segments, 1);

    let builder = PositionSetBuilder::new();
    let set = builder.build(100000000);
    assert_eq!(set.span(), 100000000);
    assert!(!set.contains(0));
    assert!(!set.contains(1));
    assert!(!set.contains(99999999));
    let stats = set.compute_stats();
    assert_eq!(stats.position_count, 0);
    let expected_segments = 100000000usize.div_ceil(Segment::SPAN as usize);
    assert_eq!(stats.total_segments, expected_segments);
    assert_eq!(stats.empty_segments, expected_segments);
}

#[test]
fn test_push_positions() {
    let mut builder = PositionSetBuilder::new();
    let positions = (0u64..100000000).step_by(56128);
    for pos in positions.clone() {
        builder.push_position(pos);
    }
    let next_pos = builder.next_pos();
    let set = builder.build(next_pos);
    let stats = set.compute_stats();
    assert_eq!(stats.total_segments, stats.list_segments);
    assert_eq!(stats.position_count, positions.clone().count() as u64);

    for pos in positions {
        assert!(set.contains(pos));
    }

    let mut builder = PositionSetBuilder::new();
    builder.push_position(10000000);
    builder.push_position(10000001);
    builder.push_position(20000000);
    builder.push_position(91000000);
    let set = builder.build(200000000);
    let stats = set.compute_stats();
    assert!(stats.empty_segments > 180);
    assert_eq!(stats.position_count, 4);
    assert_eq!(stats.list_segments, 3);
}

#[test]
fn test_push_ranges() {
    let mut builder = PositionSetBuilder::new();
    let s = Segment::SPAN;

    // Three ranges spanning three segments, with one crossing a boundary.
    builder.push_range(10..110); // 100 positions in segment 0
    builder.push_range(s + 20..s + 120); // 100 positions in segment 1
    builder.push_range(2 * s - 50..2 * s + 50); // 50 in segment 1, 50 in segment 2

    let span = 3 * s;
    let set = builder.build(span);
    set.check_basic_invariants();

    // Membership checks
    assert!(set.contains(10));
    assert!(set.contains(109));
    assert!(!set.contains(110));
    assert!(set.contains(s + 20));
    assert!(set.contains(s + 119));
    assert!(!set.contains(s + 120));
    assert!(set.contains(2 * s - 1));
    assert!(set.contains(2 * s + 49));
    assert!(!set.contains(2 * s + 50));
    assert!(!set.contains(s));

    let stats = set.compute_stats();
    let expected_segments = (span as usize).div_ceil(Segment::SPAN as usize);
    assert_eq!(stats.total_segments, expected_segments);
    assert_eq!(stats.empty_segments, 0);
    assert_eq!(stats.position_count, 300);
    // At least two segments should be represented as ranges.
    assert!(stats.range_segments >= 2);
}

#[test]
fn test_push_positions_and_ranges() {
    let mut builder = PositionSetBuilder::new();
    let s = Segment::SPAN;

    // First segment: positions and a small range
    builder.push_position(10);
    builder.push_position(20);
    builder.push_range(30..35); // 5 positions

    // Second segment: a range and a position
    builder.push_range(s + 5..s + 15); // 10 positions
    builder.push_position(s + 40);

    let span = 2 * s;
    let set = builder.build(span);
    set.check_basic_invariants();

    // Membership spot checks
    assert!(set.contains(10));
    assert!(set.contains(20));
    assert!(set.contains(34));
    assert!(!set.contains(35));
    assert!(set.contains(s + 5));
    assert!(set.contains(s + 14));
    assert!(!set.contains(s + 15));
    assert!(set.contains(s + 40));

    let stats = set.compute_stats();
    let expected_segments = (span as usize).div_ceil(Segment::SPAN as usize);
    assert_eq!(stats.total_segments, expected_segments);
    assert_eq!(stats.empty_segments, 0);
    assert_eq!(
        stats.position_count,
        2 /*10,20*/ + 5 /*30..35*/ + 10 /*s+5..s+15*/ + 1 /*s+40*/
    );
    assert!(stats.range_segments >= 1);
}

#[test]
fn test_push_slices() {
    let mut builder = PositionSetBuilder::new();
    let s = Segment::SPAN;

    // Sorted unique slice, spread across 3 segments and avoiding consecutive positions
    let positions = [1, 10, 100, s + 1, s + 3, 2 * s + 5];
    builder.extend_from_position_slice(&positions);

    let span = builder.next_pos(); // tight span up to last+1
    let set = builder.build(span);
    set.check_basic_invariants();

    for &p in &positions {
        assert!(set.contains(p));
    }
    assert!(!set.contains(2));

    let stats = set.compute_stats();
    let expected_segments = (span as usize).div_ceil(Segment::SPAN as usize);
    assert_eq!(stats.total_segments, expected_segments);
    assert_eq!(stats.empty_segments, 0);
    assert_eq!(stats.position_count, positions.len() as u64);
    // Likely list representation due to sparsity
    assert!(stats.list_segments >= 2);
}

#[test]
fn test_push_positions_ranges_and_slices() {
    let mut builder = PositionSetBuilder::new();
    let s = Segment::SPAN;

    // Segment 0: positions, slice, and a range
    builder.push_position(2);
    builder.push_position(4);
    builder.push_position(6);
    builder.extend_from_position_slice(&[8, 12, 14]);
    builder.push_range(20..30); // 10 positions

    // Segment 1: range, slice, and a position
    builder.push_range(s + 5..s + 15); // 10 positions
    builder.extend_from_position_slice(&[s + 30, s + 33]);
    builder.push_position(s + 40);

    let span = builder.next_pos();
    let set = builder.build(span);
    set.check_basic_invariants();

    // Spot checks
    for &p in &[
        2,
        4,
        6,
        8,
        12,
        14,
        20,
        29,
        s + 5,
        s + 14,
        s + 30,
        s + 33,
        s + 40,
    ] {
        assert!(set.contains(p));
    }
    assert!(!set.contains(30));
    assert!(!set.contains(s + 15));

    let stats = set.compute_stats();
    let expected_segments = (span as usize).div_ceil(Segment::SPAN as usize);
    assert_eq!(stats.total_segments, expected_segments);
    assert_eq!(stats.empty_segments, 0);
    assert_eq!(
        stats.position_count,
        3 /*positions*/ + 3 /*slice*/ + 10 /*range*/ + 10 /*range*/ + 2 /*slice*/ + 1 /*pos*/
    );
    assert!(stats.range_segments >= 1);
}

#[test]
fn test_extend_from_range_slice() {
    let mut builder = PositionSetBuilder::new();
    let s = Segment::SPAN;

    // Disjoint, sorted ranges covering multiple segments, including a boundary-crossing one.
    let ranges = vec![
        5..25,                  // seg 0: 20 positions
        s - 10..s + 10,         // crosses boundary: 20 positions (10 in seg0, 10 in seg1)
        s + 50..s + 80,         // seg 1: 30 positions
        2 * s..2 * s + 100,     // seg 2: 100 positions
        3 * s + 30..3 * s + 50, // seg 3: 20 positions
    ];

    builder.extend_from_range_slice(&ranges);

    // Build with a tight span (up to last end)
    let span = builder.next_pos();
    let set = builder.build(span);
    set.check_basic_invariants();

    // Membership checks for each range
    assert!(set.contains(5));
    assert!(set.contains(24));
    assert!(!set.contains(25));

    assert!(set.contains(s - 10));
    assert!(set.contains(s - 1));
    assert!(set.contains(s));
    assert!(set.contains(s + 9));
    assert!(!set.contains(s + 10));

    assert!(set.contains(s + 50));
    assert!(set.contains(s + 79));
    assert!(!set.contains(s + 80));

    assert!(set.contains(2 * s));
    assert!(set.contains(2 * s + 99));
    assert!(!set.contains(2 * s + 100));

    assert!(set.contains(3 * s + 30));
    assert!(set.contains(3 * s + 49));

    // Stats checks
    let stats = set.compute_stats();
    let expected_segments = (span as usize).div_ceil(Segment::SPAN as usize);
    let expected_count = ranges.iter().map(|r| r.end - r.start).sum::<u64>();

    assert_eq!(stats.total_segments, expected_segments);
    assert_eq!(stats.empty_segments, 0);
    assert_eq!(stats.position_count, expected_count);
}

#[test]
fn test_push_dense_mix() {
    const SPAN: usize = 30000000;
    let mut memo = BitArray::empty(SPAN + 100000);
    let mut builder = PositionSetBuilder::new();

    fastrand::seed(297135646);
    let mut pos = 0;
    while pos < SPAN {
        match fastrand::u8(0..4) {
            0 => {
                memo.set(pos);
                builder.push_position(pos as u64);

                pos += fastrand::usize(1..15);
            }
            1 => {
                let end = pos + fastrand::usize(1..40);

                memo.set_range(pos..end);
                builder.push_range(pos as u64..end as u64);

                pos = end;
                pos += fastrand::usize(1..50);
            }
            2 => {
                let count = fastrand::usize(1..100);
                let slice = (0..count)
                    .map(|_| fastrand::u64(1..20))
                    .scan(pos as u64, |curr, step| {
                        *curr += step;
                        Some(*curr)
                    })
                    .collect::<Vec<_>>();

                pos = *slice.last().unwrap() as usize + fastrand::usize(1..20);

                for &p in &slice {
                    memo.set(p as usize);
                }

                builder.extend_from_position_slice(&slice);
            }
            3 => {
                pos = std::cmp::min(pos + fastrand::usize(10..100), SPAN);
            }
            _ => panic!(),
        }
    }

    let set = builder.build(pos as u64);
    set.check_full_invariants();

    assert_eq!(set.count_positions(), memo.count_ones() as u64);

    for pos in memo.iter() {
        assert!(set.contains(pos as u64));
    }

    let stats = set.compute_stats();
    assert_eq!(stats.bit_segments, stats.total_segments);
}
