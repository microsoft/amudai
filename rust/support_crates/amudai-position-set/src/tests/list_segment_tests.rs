use std::ops::Range;

use crate::segment::{Segment, list::ListSegment};

#[test]
fn test_from_positions_empty() {
    let span = 0..Segment::SPAN;
    let positions = std::iter::empty();
    let segment = ListSegment::from_positions(span.clone(), positions);

    assert_eq!(segment.span(), span);
    assert_eq!(segment.relative_positions(), &[]);
    assert_eq!(segment.positions().collect::<Vec<_>>(), vec![]);
}

#[test]
fn test_from_positions_single_position() {
    let span = 0..Segment::SPAN;
    let positions = std::iter::once(42);
    let segment = ListSegment::from_positions(span.clone(), positions);

    assert_eq!(segment.span(), span);
    assert_eq!(segment.relative_positions(), &[42]);
    assert_eq!(segment.positions().collect::<Vec<_>>(), vec![42]);
}

#[test]
fn test_from_positions_multiple_sorted_positions() {
    let span = 100..200;
    let positions = vec![105, 110, 150, 199].into_iter();
    let segment = ListSegment::from_positions(span.clone(), positions);

    assert_eq!(segment.span(), span);
    assert_eq!(segment.relative_positions(), &[5, 10, 50, 99]);
    assert_eq!(
        segment.positions().collect::<Vec<_>>(),
        vec![105, 110, 150, 199]
    );
}

#[test]
fn test_from_positions_at_span_boundaries() {
    let span = 1000..2000;
    let positions = vec![1000, 1999].into_iter(); // First and last valid positions
    let segment = ListSegment::from_positions(span.clone(), positions);

    assert_eq!(segment.span(), span);
    assert_eq!(segment.relative_positions(), &[0, 999]);
    assert_eq!(segment.positions().collect::<Vec<_>>(), vec![1000, 1999]);
}

#[test]
fn test_from_positions_with_capacity_hint() {
    let span = 0..1000;
    let positions = vec![10, 20, 30, 40, 50];
    let segment = ListSegment::from_positions(span.clone(), positions.into_iter());

    assert_eq!(segment.span(), span);
    assert_eq!(segment.relative_positions(), &[10, 20, 30, 40, 50]);
    assert_eq!(
        segment.positions().collect::<Vec<_>>(),
        vec![10, 20, 30, 40, 50]
    );
}

#[test]
fn test_from_positions_large_span() {
    let span = 0..Segment::SPAN;
    let positions = vec![0, Segment::SPAN / 2, Segment::SPAN - 1].into_iter();
    let segment = ListSegment::from_positions(span.clone(), positions);

    assert_eq!(segment.span(), span);
    assert_eq!(
        segment.relative_positions(),
        &[0, (Segment::SPAN / 2) as u16, (Segment::SPAN - 1) as u16]
    );
    assert_eq!(
        segment.positions().collect::<Vec<_>>(),
        vec![0, Segment::SPAN / 2, Segment::SPAN - 1]
    );
}

#[test]
fn test_from_positions_offset_span() {
    let span_start = Segment::SPAN * 5; // Start at 5th segment
    let span = span_start..(span_start + Segment::SPAN);
    let positions = vec![span_start + 100, span_start + 500, span_start + 1000].into_iter();
    let segment = ListSegment::from_positions(span.clone(), positions);

    assert_eq!(segment.span(), span);
    assert_eq!(segment.relative_positions(), &[100, 500, 1000]);
    assert_eq!(
        segment.positions().collect::<Vec<_>>(),
        vec![span_start + 100, span_start + 500, span_start + 1000]
    );
}

#[test]
fn test_from_positions_many_positions() {
    let span = 0..10000;
    let positions = (0..1000).map(|i| i * 10).collect::<Vec<_>>();
    let segment = ListSegment::from_positions(span.clone(), positions.clone().into_iter());

    assert_eq!(segment.span(), span);
    assert_eq!(segment.relative_positions().len(), 1000);
    assert_eq!(segment.positions().collect::<Vec<_>>(), positions);

    // Verify positions are still sorted
    let relative_positions = segment.relative_positions();
    for window in relative_positions.windows(2) {
        assert!(window[0] < window[1], "Relative positions should be sorted");
    }
}

#[test]
#[should_panic]
fn test_from_positions_position_before_span() {
    let span = 100..200;
    let positions = vec![50].into_iter(); // Position before span start
    ListSegment::from_positions(span, positions);
}

#[test]
#[should_panic]
fn test_from_positions_position_after_span() {
    let span = 100..200;
    let positions = vec![200].into_iter(); // Position at span end (exclusive)
    ListSegment::from_positions(span, positions);
}

#[test]
#[should_panic]
fn test_from_positions_position_beyond_span() {
    let span = 100..200;
    let positions = vec![150, 250].into_iter(); // Second position beyond span
    ListSegment::from_positions(span, positions);
}

#[cfg(debug_assertions)]
#[test]
#[should_panic]
fn test_from_positions_unsorted_debug() {
    let span = 0..100;
    let positions = vec![10, 5, 20].into_iter(); // Unsorted positions
    ListSegment::from_positions(span, positions);
}

#[test]
fn test_from_positions_iterator_size_hint() {
    let span = 0..1000;
    let positions = vec![10, 20, 30];

    // Test with exact size hint
    let segment = ListSegment::from_positions(span.clone(), positions.clone().into_iter());
    assert_eq!(segment.relative_positions(), &[10, 20, 30]);

    // Test with iterator that has no upper bound in size hint
    let iter_no_upper = (0..100).filter(|x| x % 33 == 0);
    let collected: Vec<_> = iter_no_upper.clone().collect();
    let segment = ListSegment::from_positions(span.clone(), iter_no_upper);
    let expected_relative: Vec<u16> = collected.iter().map(|&x| x as u16).collect();
    assert_eq!(segment.relative_positions(), expected_relative.as_slice());
}

#[test]
fn test_from_positions_empty_with_capacity() {
    let span = 0..1000;
    let positions = std::iter::empty();
    let segment = ListSegment::from_positions(span.clone(), positions);

    assert_eq!(segment.span(), span);
    assert_eq!(segment.relative_positions(), &[]);
    assert_eq!(segment.positions().count(), 0);
}

#[test]
fn test_from_positions_preserves_relative_to_absolute_mapping() {
    let span_start = 12345;
    let span = span_start..(span_start + 1000);
    let absolute_positions = vec![span_start + 10, span_start + 500, span_start + 999];
    let segment = ListSegment::from_positions(span.clone(), absolute_positions.clone().into_iter());

    // Verify round-trip: absolute -> relative -> absolute
    let reconstructed: Vec<u64> = segment.positions().collect();
    assert_eq!(reconstructed, absolute_positions);

    // Verify relative positions are correct
    let expected_relative = vec![10u16, 500u16, 999u16];
    assert_eq!(segment.relative_positions(), expected_relative.as_slice());
}

fn mk(span: Range<u64>, vals: &[u64]) -> ListSegment {
    ListSegment::from_positions(span, vals.iter().copied())
}

#[test]
fn positions_within_returns_all_for_full_span() {
    let span = 100..120;
    let seg = mk(span.clone(), &[102, 105, 110, 115, 119]);

    // Exact span
    let got: Vec<u64> = seg.positions_within(span.clone()).collect();
    assert_eq!(got, vec![102, 105, 110, 115, 119]);

    // Superset of span (should clamp to span)
    let got2: Vec<u64> = seg.positions_within(0..u64::MAX).collect();
    assert_eq!(got2, vec![102, 105, 110, 115, 119]);
}

#[test]
fn positions_within_respects_inclusive_start_exclusive_end() {
    let span = 100..120;
    let seg = mk(span.clone(), &[100, 102, 105, 110, 115, 119]);

    // Start inclusive: 100 included
    let got: Vec<u64> = seg.positions_within(100..101).collect();
    assert_eq!(got, vec![100]);

    // End exclusive: 102 at end should be excluded
    let got2: Vec<u64> = seg.positions_within(100..102).collect();
    assert_eq!(got2, vec![100]);

    // Range [102, 115): includes 102,105,110; 115 excluded
    let got3: Vec<u64> = seg.positions_within(102..115).collect();
    assert_eq!(got3, vec![102, 105, 110]);

    // Range [119, 120): includes 119
    let got4: Vec<u64> = seg.positions_within(119..120).collect();
    assert_eq!(got4, vec![119]);
}

#[test]
fn positions_within_outside_or_empty_range_returns_empty() {
    let span = 100..120;
    let seg = mk(span.clone(), &[102, 105, 110, 115, 119]);

    // Entirely before
    assert!(seg.positions_within(80..90).next().is_none());
    // Entirely after
    assert!(seg.positions_within(120..130).next().is_none());
    // Empty range
    assert!(seg.positions_within(110..110).next().is_none());
}

#[test]
fn positions_within_clamps_partial_overlap() {
    let span = 100..120;
    let seg = mk(span.clone(), &[102, 105, 110, 115, 119]);

    // Overlaps the start: clamps to [100, 106)
    let got_start: Vec<u64> = seg.positions_within(90..106).collect();
    assert_eq!(got_start, vec![102, 105]);

    // Overlaps the end: clamps to [118, 120)
    let got_end: Vec<u64> = seg.positions_within(118..130).collect();
    assert_eq!(got_end, vec![119]);
}

#[test]
fn positions_within_empty_segment_is_empty() {
    let span = 200..210;
    let seg = mk(span.clone(), &[]);
    assert!(seg.positions_within(span.clone()).next().is_none());
    assert!(seg.positions_within(0..u64::MAX).next().is_none());
}

#[test]
fn positions_within_supports_double_ended_iteration() {
    let span = 100..120;
    let seg = mk(span.clone(), &[102, 105, 110, 115, 119]);

    // Range [103, 116): expect 105, 110, 115
    let mut it = seg.positions_within(103..116);
    assert_eq!(it.next(), Some(105));
    assert_eq!(it.next_back(), Some(115));
    assert_eq!(it.next(), Some(110));
    assert_eq!(it.next_back(), None);
    assert_eq!(it.next(), None);
}

fn positions(seg: &ListSegment) -> Vec<u64> {
    seg.positions().collect()
}

#[test]
fn union_disjoint() {
    let span = 0u64..110u64;
    let a = ListSegment::from_positions(span.clone(), [100u64, 102, 105].into_iter());
    let b = ListSegment::from_positions(span.clone(), [101u64, 103, 106, 109].into_iter());

    let u = a.union(&b);
    assert_eq!(u.span(), span.clone());
    assert_eq!(positions(&u), vec![100, 101, 102, 103, 105, 106, 109]);

    // Operator form
    let u2 = &a | &b;
    assert_eq!(positions(&u2), vec![100, 101, 102, 103, 105, 106, 109]);
}

#[test]
fn union_with_overlap_and_duplicates() {
    let span = 0u64..10u64;
    let a = ListSegment::from_positions(span.clone(), [0u64, 1, 5, 7].into_iter());
    let b = ListSegment::from_positions(span.clone(), [1u64, 3, 7, 8].into_iter());

    let u = a.union(&b);
    assert_eq!(positions(&u), vec![0, 1, 3, 5, 7, 8]);

    // Operator form
    let u2 = &a | &b;
    assert_eq!(positions(&u2), vec![0, 1, 3, 5, 7, 8]);
}

#[test]
fn union_with_empty() {
    let span = 50u64..55u64;
    let a = ListSegment::from_positions(span.clone(), [50u64, 52, 54].into_iter());
    let empty = ListSegment::empty(span.clone());

    let u1 = a.union(&empty);
    assert_eq!(positions(&u1), vec![50, 52, 54]);

    let u2 = empty.union(&a);
    assert_eq!(positions(&u2), vec![50, 52, 54]);

    // Operator form
    let u3 = &a | &empty;
    assert_eq!(positions(&u3), vec![50, 52, 54]);
}

#[test]
fn intersect_overlap() {
    let span = 0u64..10u64;
    let a = ListSegment::from_positions(span.clone(), [0u64, 1, 5, 7].into_iter());
    let b = ListSegment::from_positions(span.clone(), [1u64, 3, 7, 8].into_iter());

    let i = a.intersect(&b);
    assert_eq!(positions(&i), vec![1, 7]);

    // Operator form
    let i2 = &a & &b;
    assert_eq!(positions(&i2), vec![1, 7]);
}

#[test]
fn intersect_disjoint() {
    let span = 0u64..10u64;
    let a = ListSegment::from_positions(span.clone(), [0u64, 2, 4].into_iter());
    let b = ListSegment::from_positions(span.clone(), [1u64, 3, 5].into_iter());

    let i = a.intersect(&b);
    assert!(positions(&i).is_empty());

    // Operator form
    let i2 = &a & &b;
    assert!(positions(&i2).is_empty());
}

#[test]
fn intersect_identical() {
    let span = 10u64..20u64;
    let a = ListSegment::from_positions(span.clone(), [11u64, 12, 15, 19].into_iter());
    let b = ListSegment::from_positions(span.clone(), [11u64, 12, 15, 19].into_iter());

    let i = a.intersect(&b);
    assert_eq!(positions(&i), vec![11, 12, 15, 19]);
}

#[test]
fn complement_of_empty_is_full_span() {
    let span = 100u64..105u64; // five positions: 100..=104
    let s = ListSegment::empty(span.clone());

    let c = s.complement();
    assert_eq!(c.span(), span.clone());
    assert_eq!(positions(&c), vec![100, 101, 102, 103, 104]);

    // Operator form
    let c2 = !&s;
    assert_eq!(positions(&c2), vec![100, 101, 102, 103, 104]);
}

#[test]
fn complement_of_full_is_empty() {
    let span = 0u64..5u64; // positions: 0..=4
    let s = ListSegment::from_ranges(span.clone(), std::iter::once(span.clone()));

    let c = s.complement();
    assert!(positions(&c).is_empty());
}

#[test]
fn complement_partial() {
    let span = 100u64..110u64; // positions: 100..=109
    let s = ListSegment::from_positions(span.clone(), [100u64, 102, 109].into_iter());

    let c = s.complement();
    assert_eq!(positions(&c), vec![101, 103, 104, 105, 106, 107, 108]);

    // Operator form
    let c2 = !&s;
    assert_eq!(positions(&c2), vec![101, 103, 104, 105, 106, 107, 108]);
}
