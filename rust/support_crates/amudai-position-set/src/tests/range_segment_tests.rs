use std::ops::Range;

use crate::segment::{Segment, bits::BitSegment, ranges::RangeSegment};

fn mk(span: Range<u64>, ranges: &[(u64, u64)]) -> RangeSegment {
    RangeSegment::from_ranges(span, ranges.iter().copied().map(|(s, e)| s..e))
}

fn abs_ranges(seg: &RangeSegment) -> Vec<Range<u64>> {
    let span = seg.span();
    seg.runs().iter().map(|r| r.rebase(span.start)).collect()
}

#[test]
fn test_contains() {
    let s = RangeSegment::empty(0..1000);
    assert!(!s.contains(0));
    assert!(!s.contains(999));

    let s = RangeSegment::full(0..1000);
    assert!(s.contains(0));
    assert!(s.contains(999));

    let s = RangeSegment::from_positions(
        0..Segment::SPAN,
        [1u64, 10, 100, 1000, u16::MAX as u64].into_iter(),
    );
    assert_eq!(s.count_positions(), 5);
    assert!(s.contains(100));
    assert!(s.contains(1000));
    assert!(s.contains(u16::MAX as u64));
    assert!(!s.contains(0));
    assert!(!s.contains(2));
    assert!(!s.contains(9999));

    let ranges = (10u64..100).map(|i| i * 10..i * 10 + 7);
    let s = RangeSegment::from_ranges(0..Segment::SPAN, ranges.clone());
    assert_eq!(s.count_positions(), ranges.clone().flatten().count());
    assert_eq!(s.count_runs(), 90);
    for pos in ranges.flatten() {
        assert!(s.contains(pos));
    }

    let ranges = (10u64..100).map(|i| i * 10..i * 10 + 10);
    let s = RangeSegment::from_ranges(0..Segment::SPAN, ranges.clone());
    assert_eq!(s.count_positions(), ranges.clone().flatten().count());
    assert_eq!(s.count_runs(), 1);
    for pos in ranges.flatten() {
        assert!(s.contains(pos));
    }
}

#[test]
fn union_disjoint_and_adjacent() {
    let span = 0..120u64;
    let a = mk(span.clone(), &[(100, 105), (108, 110)]);
    let b = mk(span.clone(), &[(102, 103), (105, 108), (110, 115)]);

    // All pieces connect into a single run [100,115)
    let u = a.union(&b);
    assert_eq!(u.span(), span.clone());
    assert_eq!(abs_ranges(&u), vec![100..115]);

    // Operator form
    let u2 = &a | &b;
    assert_eq!(abs_ranges(&u2), vec![100..115]);
}

#[test]
fn union_with_empty() {
    let span = 0..60u64;
    let a = mk(span.clone(), &[(52, 55)]);
    let empty = RangeSegment::empty(span.clone());

    let u1 = a.union(&empty);
    assert_eq!(abs_ranges(&u1), vec![52..55]);

    let u2 = empty.union(&a);
    assert_eq!(abs_ranges(&u2), vec![52..55]);

    // Operator form
    let u3 = &a | &empty;
    assert_eq!(abs_ranges(&u3), vec![52..55]);
}

#[test]
#[should_panic(expected = "RangeSegment spans must match for union")]
fn union_mismatched_spans_panics() {
    let a = mk(0..10, &[(0, 3)]);
    let b = mk(0..20, &[(12, 15)]);
    let _ = a.union(&b);
}

#[test]
fn intersect_overlap_complex() {
    let span = 0..120u64;
    let a = mk(span.clone(), &[(100, 105), (107, 112), (115, 118)]);
    let b = mk(span.clone(), &[(102, 108), (110, 116)]);

    let i = a.intersect(&b);
    assert_eq!(i.span(), span.clone());
    assert_eq!(abs_ranges(&i), vec![102..105, 107..108, 110..112, 115..116]);

    // Operator form
    let i2 = &a & &b;
    assert_eq!(
        abs_ranges(&i2),
        vec![102..105, 107..108, 110..112, 115..116]
    );
}

#[test]
fn intersect_disjoint_and_adjacent() {
    let span = 0u64..10u64;
    let a = mk(span.clone(), &[(0, 2), (4, 5)]);
    let b = mk(span.clone(), &[(2, 3), (5, 8)]); // only adjacent, no overlap

    let i = a.intersect(&b);
    assert!(abs_ranges(&i).is_empty());

    let i2 = &a & &b;
    assert!(abs_ranges(&i2).is_empty());
}

#[test]
fn intersect_identical() {
    let span = 0..20u64;
    let a = mk(span.clone(), &[(11, 12), (15, 19)]);
    let b = mk(span.clone(), &[(11, 12), (15, 19)]);

    let i = a.intersect(&b);
    assert_eq!(abs_ranges(&i), vec![11..12, 15..19]);
}

#[test]
fn complement_of_empty_is_full_span() {
    let span = 0..105u64; // five positions: 100..=104
    let s = RangeSegment::empty(span.clone());

    let c = s.complement();
    assert_eq!(c.span(), span.clone());
    assert_eq!(abs_ranges(&c), vec![0..105]);

    // Operator form
    let c2 = !&s;
    assert_eq!(abs_ranges(&c2), vec![0..105]);
}

#[test]
fn complement_of_full_is_empty() {
    let span = 0u64..10u64;
    let s = RangeSegment::from_ranges(span.clone(), std::iter::once(span.clone()));

    let c = s.complement();
    assert!(abs_ranges(&c).is_empty());
}

#[test]
fn complement_partial() {
    let span = 0..120u64;
    let s = mk(span.clone(), &[(100, 102), (104, 106), (109, 110)]);

    let c = s.complement();
    assert_eq!(abs_ranges(&c), vec![0..100, 102..104, 106..109, 110..120]);

    // Operator form
    let c2 = !&s;
    assert_eq!(abs_ranges(&c2), vec![0..100, 102..104, 106..109, 110..120]);
}

fn abs_ranges_within(seg: &RangeSegment, range: Range<u64>) -> Vec<Range<u64>> {
    seg.ranges_within(range).collect()
}

fn verify_ranges_within(ranges: &[Range<usize>], clip: Range<usize>) {
    let start =
        (ranges.first().map(|r| r.start as u64).unwrap_or(0) / Segment::SPAN) * Segment::SPAN;
    let end = ranges
        .last()
        .map(|r| r.end.max(clip.end) as u64)
        .unwrap_or(Segment::SPAN);
    let span = start..end;

    let segment = RangeSegment::from_ranges(
        span.clone(),
        ranges.iter().map(|r| r.start as u64..r.end as u64),
    );
    segment.check_full_invariants();

    let clipped = segment
        .ranges_within(clip.start as u64..clip.end as u64)
        .collect::<Vec<_>>();

    let bits = BitSegment::from_ranges(span, ranges.iter().map(|r| r.start as u64..r.end as u64));
    bits.check_full_invariants();
    let expected_positions = clip
        .map(|pos| pos as u64)
        .filter(|&pos| bits.contains(pos))
        .collect::<Vec<_>>();
    let actual_positions = clipped.iter().cloned().flatten().collect::<Vec<_>>();

    assert_eq!(&actual_positions, &expected_positions);
}

#[test]
fn test_ranges_within() {
    verify_ranges_within(&[], 0..1);

    verify_ranges_within(&[10..100], 5..15);

    verify_ranges_within(&[10..100, 101..120], 95..110);

    // Empty ranges with a non-empty clip
    verify_ranges_within(&[], 10..20);

    // Single range: clip completely before/after
    verify_ranges_within(&[50..60], 0..10);
    verify_ranges_within(&[50..60], 60..70);

    // Single range: exact match and partial overlaps
    verify_ranges_within(&[50..60], 50..60);
    verify_ranges_within(&[50..60], 45..55);
    verify_ranges_within(&[50..60], 55..65);

    // Adjacent ranges that merge; clip spanning the adjacency
    verify_ranges_within(&[10..20, 20..30], 15..25);

    // Zero-length clip on adjacency boundary
    verify_ranges_within(&[10..20, 20..30], 20..20);

    // Overlapping input ranges that coalesce
    verify_ranges_within(&[10..20, 15..30], 18..22);

    // Multiple disjoint ranges; clip matching one exactly
    verify_ranges_within(&[5..10, 15..20, 25..30], 15..20);

    // Clip entirely within a gap (touching boundaries only)
    verify_ranges_within(&[5..10, 15..20, 25..30], 20..25);

    // Many single-position ranges; clip selects a subset
    verify_ranges_within(&[1..2, 3..4, 5..6, 7..8], 2..7);

    // Range starting at 0
    verify_ranges_within(&[0..1, 2..4, 10..12], 0..1);

    // Wide clip fully covering all stored ranges
    verify_ranges_within(&[10..12, 14..16], 0..100);

    // Zero-length clip inside a stored range
    verify_ranges_within(&[100..110], 105..105);

    // Mixed adjacent and disjoint; clip spanning both groups
    verify_ranges_within(&[0..5, 5..10, 12..20, 20..25], 3..22);

    // Clip ending at stored range start
    verify_ranges_within(&[30..40], 20..30);

    // Clip starting at stored range end
    verify_ranges_within(&[30..40], 40..50);

    // Far away clip with no overlap
    verify_ranges_within(&[10..12, 90..100], 50..60);
}

#[test]
fn ranges_within_basic_clipping() {
    let span = 0..120u64;
    let s = mk(
        span.clone(),
        &[(100, 102), (105, 108), (110, 115), (117, 119)],
    );

    // Completely before span end
    assert!(abs_ranges_within(&s, 95..100).is_empty());
    // Partial overlap at the start
    assert_eq!(abs_ranges_within(&s, 95..101), vec![100..101]);
    // Ends exactly at a stored range boundary
    assert_eq!(abs_ranges_within(&s, 101..105), vec![101..102]);
    // Overlaps a middle range only
    assert_eq!(abs_ranges_within(&s, 103..110), vec![105..108]);
    // Partial overlap at the end of span
    assert_eq!(abs_ranges_within(&s, 115..130), vec![117..119]);
    // Empty query range
    assert!(abs_ranges_within(&s, 117..117).is_empty());
}

#[test]
fn ranges_within_multiple_and_inside() {
    let span = 0u64..20u64;
    let s = mk(span.clone(), &[(2, 6), (8, 10), (12, 18)]);

    // Covers parts of multiple stored ranges, with clipping on the last one
    assert_eq!(abs_ranges_within(&s, 3..15), vec![3..6, 8..10, 12..15]);
    // Whole span should return the stored absolute ranges as-is
    assert_eq!(abs_ranges_within(&s, 0..20), abs_ranges(&s));
    // Strictly inside a single stored range
    assert_eq!(abs_ranges_within(&s, 4..5), vec![4..5]);
    // Completely outside after span
    assert!(abs_ranges_within(&s, 20..25).is_empty());
}
