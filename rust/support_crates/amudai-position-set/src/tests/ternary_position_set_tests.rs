use crate::{PositionSet, TernaryPositionSet, segment::Segment};

#[test]
fn test_all_unknown() {
    let span = Segment::SPAN * 2 + 7;
    let t = TernaryPositionSet::all_unknown(span);
    assert_eq!(t.span(), span);
    assert_eq!(t.yes().count_positions(), 0);
    assert_eq!(t.no().count_positions(), 0);
    let unknown = t.unknown();
    assert_eq!(unknown.count_positions(), span);
    if span > 0 {
        assert!(unknown.contains(0));
        assert!(unknown.contains(span - 1));
    }
}

#[test]
fn test_all_yes_no_unknown() {
    let span = Segment::SPAN + 3;
    let t = TernaryPositionSet::all_yes(span);
    assert_eq!(t.span(), span);
    assert_eq!(t.yes().count_positions(), span);
    assert_eq!(t.no().count_positions(), 0);
    assert_eq!(t.unknown().count_positions(), 0);

    let t2 = TernaryPositionSet::all_no(span);
    assert_eq!(t2.span(), span);
    assert_eq!(t2.yes().count_positions(), 0);
    assert_eq!(t2.no().count_positions(), span);
    assert_eq!(t2.unknown().count_positions(), 0);
}

#[test]
fn test_exact_and_into_exact() {
    let span = Segment::SPAN * 2 + 1;
    let yes = PositionSet::from_positions(span, (0..span).step_by(5));
    let t = TernaryPositionSet::exact(yes.clone());
    assert_eq!(t.span(), span);
    assert_eq!(t.yes().count_positions(), yes.count_positions());
    assert_eq!(t.no().count_positions(), span - yes.count_positions());
    assert_eq!(t.unknown().count_positions(), 0);

    let exact = t.clone().into_exact_if_no_unknown();
    assert!(exact.is_some());
    assert_eq!(exact.unwrap().count_positions(), yes.count_positions());
}

#[test]
fn test_from_yes_no_basic() {
    let span = Segment::SPAN + 17;
    let yes_positions: Vec<u64> = (0..span).step_by(7).collect();
    let no_positions: Vec<u64> = (1..span).step_by(11).collect();
    let yes = PositionSet::from_positions(span, yes_positions.iter().copied());
    let no = PositionSet::from_positions(span, no_positions.iter().copied());

    let t = TernaryPositionSet::from_yes_no(yes.clone(), no.clone());
    t.check_basic_invariants();

    // Unknown = rest of the domain
    let unknown = t.unknown();
    // Disjointness is intended but not enforced; derive expected unknown by set ops
    let union = yes.union(&no);
    let known_count = union.count_positions();
    assert_eq!(unknown.count_positions(), span - known_count);

    // Spot checks for membership against union/complement semantics
    for p in (0..span).step_by(13) {
        let is_yes = yes.contains(p);
        let is_no = no.contains(p);
        let is_unknown = unknown.contains(p);
        // Unknown must be disjoint from both yes and no
        assert!(!(is_unknown && (is_yes || is_no)), "unknown overlap at {p}");
        // Union membership must match yes || no
        assert_eq!(union.contains(p), is_yes || is_no, "union mismatch at {p}");
        // Unknown must be the complement of union within the domain
        assert_eq!(!union.contains(p), is_unknown, "complement mismatch at {p}");
    }
}

#[test]
#[should_panic]
fn test_from_yes_no_span_mismatch_panics() {
    let yes = PositionSet::empty(10);
    let no = PositionSet::empty(11);
    let _ = TernaryPositionSet::from_yes_no(yes, no);
}

#[test]
fn test_invert_and_invert_in_place() {
    let span = Segment::SPAN + 5;
    let yes = PositionSet::from_positions(span, (0..span).step_by(3));
    let no = PositionSet::from_positions(span, (1..span).step_by(5));
    let mut t = TernaryPositionSet::from_yes_no(yes.clone(), no.clone());

    let inv = t.invert();
    // Yes/No swap
    assert_eq!(inv.yes().count_positions(), no.count_positions());
    assert_eq!(inv.no().count_positions(), yes.count_positions());
    // Double invert returns to original
    let inv2 = inv.invert();
    assert_eq!(inv2.yes().count_positions(), t.yes().count_positions());
    assert_eq!(inv2.no().count_positions(), t.no().count_positions());

    // In-place variant
    let original_yes = t.yes().count_positions();
    let original_no = t.no().count_positions();
    t.invert_in_place();
    assert_eq!(t.yes().count_positions(), original_no);
    assert_eq!(t.no().count_positions(), original_yes);
}

#[test]
fn test_union_basic() {
    let span = Segment::SPAN * 2 + 9;
    let a_yes = PositionSet::from_positions(span, (0..span).step_by(7));
    let a_no = PositionSet::from_positions(span, (2..span).step_by(11));
    let b_yes = PositionSet::from_positions(span, (1..span).step_by(5));
    let b_no = PositionSet::from_positions(span, (3..span).step_by(13));

    let a = TernaryPositionSet::from_yes_no(a_yes, a_no);
    let b = TernaryPositionSet::from_yes_no(b_yes, b_no);

    let u = a.union(&b);

    // Per-position truth table checks (spot check stride)
    for p in (0..span).step_by(17) {
        let ay = a.yes().contains(p);
        let an = a.no().contains(p);
        let by = b.yes().contains(p);
        let bn = b.no().contains(p);
        let uy = u.yes().contains(p);
        let un = u.no().contains(p);
        // Yes if any yes
        assert_eq!(uy, ay || by, "union yes mismatch at {p}");
        // No if both no and not yes
        assert_eq!(un, (an && bn) && !uy, "union no mismatch at {p}");
        // Unknown is complement of yes/no union
        let union_known = uy || un;
        let unknown = u.unknown().contains(p);
        assert_eq!(unknown, !union_known, "union unknown mismatch at {p}");
    }
}

#[test]
fn test_union_with_in_place() {
    let span = Segment::SPAN + 23;
    let mut a = TernaryPositionSet::from_yes_no(
        PositionSet::from_positions(span, (0..span).step_by(4)),
        PositionSet::from_positions(span, (1..span).step_by(9)),
    );
    let b = TernaryPositionSet::from_yes_no(
        PositionSet::from_positions(span, (2..span).step_by(5)),
        PositionSet::from_positions(span, (3..span).step_by(7)),
    );
    let expected = a.clone().union(&b);
    a.union_with(&b);
    // Compare by sampling due to different internal encodings
    for p in (0..span).step_by(19) {
        assert_eq!(a.yes().contains(p), expected.yes().contains(p));
        assert_eq!(a.no().contains(p), expected.no().contains(p));
    }
}

#[test]
fn test_intersect_basic() {
    let span = Segment::SPAN * 2 + 31;
    let a_yes = PositionSet::from_positions(span, (0..span).step_by(3));
    let a_no = PositionSet::from_positions(span, (1..span).step_by(7));
    let b_yes = PositionSet::from_positions(span, (0..span).step_by(5));
    let b_no = PositionSet::from_positions(span, (2..span).step_by(11));

    let a = TernaryPositionSet::from_yes_no(a_yes, a_no);
    let b = TernaryPositionSet::from_yes_no(b_yes, b_no);

    let i = a.intersect(&b);

    for p in (0..span).step_by(23) {
        let ay = a.yes().contains(p);
        let an = a.no().contains(p);
        let by = b.yes().contains(p);
        let bn = b.no().contains(p);
        let iy = i.yes().contains(p);
        let inn = i.no().contains(p);
        // No if any no
        assert_eq!(inn, an || bn, "intersect no mismatch at {p}");
        // Yes if both yes and not no
        assert_eq!(iy, (ay && by) && !inn, "intersect yes mismatch at {p}");
        // Unknown is complement of yes/no union
        let known = iy || inn;
        let unknown = i.unknown().contains(p);
        assert_eq!(unknown, !known, "intersect unknown mismatch at {p}");
    }
}

#[test]
fn test_intersect_with_in_place() {
    let span = Segment::SPAN + 37;
    let mut a = TernaryPositionSet::from_yes_no(
        PositionSet::from_positions(span, (0..span).step_by(2)),
        PositionSet::from_positions(span, (1..span).step_by(5)),
    );
    let b = TernaryPositionSet::from_yes_no(
        PositionSet::from_positions(span, (0..span).step_by(3)),
        PositionSet::from_positions(span, (2..span).step_by(7)),
    );
    let expected = a.clone().intersect(&b);
    a.intersect_with(&b);
    for p in (0..span).step_by(29) {
        assert_eq!(a.yes().contains(p), expected.yes().contains(p));
        assert_eq!(a.no().contains(p), expected.no().contains(p));
    }
}
