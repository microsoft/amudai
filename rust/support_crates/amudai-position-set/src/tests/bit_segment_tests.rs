use crate::segment::bits::BitSegment;

#[test]
fn bit_segment_ranges_within_empty() {
    let span = 0u64..20u64;
    let b = BitSegment::empty(span.clone());

    assert!(b.ranges_within(0u64..20u64).next().is_none());
    assert!(b.ranges_within(5u64..15u64).next().is_none());
    assert!(b.ranges_within(10u64..10u64).next().is_none());
    assert!(b.ranges_within(15u64..5u64).next().is_none());
}

#[test]
fn bit_segment_ranges_within_single_run_clipping() {
    let span = 0u64..30u64;
    let run = 5u64..10u64; // set bits [5,10)
    let b = BitSegment::from_ranges(span.clone(), std::iter::once(run.clone()));

    // Full coverage
    assert_eq!(
        b.ranges_within(0u64..30u64).collect::<Vec<_>>(),
        vec![5u64..10u64]
    );
    assert_eq!(
        b.ranges_within(5u64..10u64).collect::<Vec<_>>(),
        vec![5u64..10u64]
    );

    // Clipped on both sides
    assert_eq!(
        b.ranges_within(3u64..8u64).collect::<Vec<_>>(),
        vec![5u64..8u64]
    );
    assert_eq!(
        b.ranges_within(7u64..12u64).collect::<Vec<_>>(),
        vec![7u64..10u64]
    );

    // Outside the run
    assert!(b.ranges_within(0u64..5u64).next().is_none());
    assert!(b.ranges_within(10u64..15u64).next().is_none());

    // Empty or inverted ranges
    assert!(b.ranges_within(8u64..8u64).next().is_none());
    assert!(b.ranges_within(9u64..7u64).next().is_none());
}

#[test]
fn bit_segment_ranges_within_multiple_runs_and_span_clamp() {
    // Non-zero start span to verify absolute offset logic
    let span = 10u64..30u64;
    let runs = vec![12u64..14u64, 16u64..19u64, 22u64..25u64];
    let b = BitSegment::from_ranges(span.clone(), runs.clone().into_iter());

    // Full coverage returns all runs (absolute)
    assert_eq!(b.ranges_within(10u64..30u64).collect::<Vec<_>>(), runs);

    // Clip across runs
    assert_eq!(
        b.ranges_within(13u64..23u64).collect::<Vec<_>>(),
        vec![13u64..14u64, 16u64..19u64, 22u64..23u64]
    );

    // Between runs (no overlap)
    assert!(b.ranges_within(14u64..16u64).next().is_none());
    assert!(b.ranges_within(19u64..22u64).next().is_none());

    // Span clamping: query extends beyond span on both sides
    assert_eq!(
        b.ranges_within(0u64..40u64).collect::<Vec<_>>(),
        vec![12u64..14u64, 16u64..19u64, 22u64..25u64]
    );

    // Span clamping: partial overlap at start
    assert_eq!(
        b.ranges_within(0u64..13u64).collect::<Vec<_>>(),
        vec![12u64..13u64]
    );

    // Span clamping: partial overlap at end
    assert!(b.ranges_within(25u64..40u64).next().is_none());
    assert_eq!(
        b.ranges_within(24u64..40u64).collect::<Vec<_>>(),
        vec![24u64..25u64]
    );
}
