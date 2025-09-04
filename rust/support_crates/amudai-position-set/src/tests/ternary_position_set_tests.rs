use crate::{PositionSet, TernaryPositionSet};

// Helper: collect positions of a PositionSet into a Vec<u64>
fn positions(ps: &PositionSet) -> Vec<u64> {
    ps.positions().collect::<Vec<_>>()
}

fn set(span: u64, elems: &[u64]) -> PositionSet {
    PositionSet::from_positions(span, elems.iter().copied())
}

#[test]
fn test_new_canonicalization_definite_when_equal() {
    let span = 32;
    let t = set(span, &[1, 3, 5]);
    let u = set(span, &[1, 3, 5]);
    match TernaryPositionSet::new(t.clone(), u.clone()) {
        TernaryPositionSet::Definite(d) => assert!(d.is_equal_to(&t)),
        other => panic!("expected Definite, got {:?}", variant_name(&other)),
    }
}

#[test]
fn test_new_canonicalization_unknown_when_true_empty() {
    let span = 48;
    let t = PositionSet::empty(span);
    let u = set(span, &[0, 2, 4, 6]);
    match TernaryPositionSet::new(t, u.clone()) {
        TernaryPositionSet::Unknown(uu) => assert!(uu.is_equal_to(&u)),
        other => panic!("expected Unknown, got {:?}", variant_name(&other)),
    }
}

#[test]
fn test_new_canonicalization_definite_when_both_empty() {
    let span = 10;
    let t = PositionSet::empty(span);
    let u = PositionSet::empty(span);
    match TernaryPositionSet::new(t.clone(), u) {
        TernaryPositionSet::Definite(d) => assert!(d.is_equal_to(&t)),
        other => panic!("expected Definite empty, got {:?}", variant_name(&other)),
    }
}

#[test]
fn test_new_mixed() {
    let span = 64;
    let t = set(span, &[1, 10]);
    let u = set(span, &[2, 3, 20]);
    match TernaryPositionSet::new(t.clone(), u.clone()) {
        TernaryPositionSet::Mixed(tt, uu) => {
            assert!(tt.is_equal_to(&t));
            assert!(uu.is_equal_to(&u));
        }
        other => panic!("expected Mixed, got {:?}", variant_name(&other)),
    }
}

#[test]
fn test_is_methods_overlap_masking() {
    // Overlapping true & unknown bits; is_unknown must mask overlap out.
    let span = 32;
    let t = set(span, &[1, 5, 9]);
    let u = set(span, &[1, 2, 6, 9, 15]); // overlaps at 1 and 9
    let tern = TernaryPositionSet::new(t.clone(), u.clone());
    // Ensure we indeed have Mixed (not collapsed): t != u and t not empty
    match tern {
        TernaryPositionSet::Mixed(_, _) => {}
        _ => panic!("expected Mixed variant"),
    }
    let true_ps = tern.is_true();
    let unknown_ps = tern.is_unknown();
    let false_ps = tern.is_false();

    // True positions exactly t
    assert_eq!(positions(&true_ps), positions(&t));
    // Unknown positions must exclude overlap (1,9) => remaining {2,6,15}
    assert_eq!(positions(&unknown_ps), vec![2, 6, 15]);

    // Sanity: no position classified as both true & unknown
    for p in positions(&true_ps) {
        assert!(!unknown_ps.contains(p), "position {p} leaked into unknown");
    }

    // false = complement of (true ∪ unknown)
    for p in 0..span {
        let is_true = true_ps.contains(p);
        let is_unknown = unknown_ps.contains(p);
        let is_false = false_ps.contains(p);
        assert!(!(is_true && is_unknown));
        if is_true || is_unknown {
            assert!(!is_false, "position {p} incorrectly false");
        } else {
            assert!(is_false, "position {p} should be false");
        }
    }
}

#[test]
fn test_union_semantics_and_variant_collapse() {
    let span = 64;
    // Definite A
    let a_true = set(span, &[1, 2, 3]);
    let a = TernaryPositionSet::new(a_true.clone(), a_true.clone()); // Definite
    // Unknown B (only unknowns)
    let b_unknown = set(span, &[2]); // subset of a_true so union should collapse
    let b = TernaryPositionSet::new(PositionSet::empty(span), b_unknown.clone()); // Unknown
    // Union where unknowns are subset of definite trues -> collapse to Definite
    let ab = a.union(&b);
    match &ab {
        TernaryPositionSet::Definite(d) => assert!(d.is_equal_to(&a_true)),
        _ => panic!("union should canonicalize to Definite"),
    }

    // Mixed + Unknown producing Mixed
    let c_true = set(span, &[5, 6]);
    let c_unknown = set(span, &[8, 9]);
    let c = TernaryPositionSet::new(c_true.clone(), c_unknown.clone()); // Mixed
    let bu = b.union(&c);
    // Semantic expectations:
    // true = c_true (since b has no trues)
    // unknown = b_unknown ∪ c_unknown minus true bits = {2,8,9,10}
    let true_expected = c_true;
    let unknown_expected = set(span, &[2, 8, 9]);
    assert!(bu.is_true().is_equal_to(&true_expected));
    assert!(bu.is_unknown().is_equal_to(&unknown_expected));
}

#[test]
fn test_intersect_semantics() {
    let span = 64;
    // A: Definite trues at {1,2,3}
    let a = TernaryPositionSet::new(set(span, &[1, 2, 3]), set(span, &[1, 2, 3]));
    // B: Unknowns at {2,3,4}
    let b = TernaryPositionSet::new(PositionSet::empty(span), set(span, &[2, 3, 4]));
    let i = a.intersect(&b);
    // Rules: true only if both true -> none (b has no trues)
    // unknown if at least one unknown and neither true -> {2,3}
    assert_eq!(positions(&i.is_true()), Vec::<u64>::new());
    assert_eq!(positions(&i.is_unknown()), vec![2, 3]);

    // Mixed intersection Mixed
    let c = TernaryPositionSet::new(set(span, &[10, 11]), set(span, &[12, 13])); // Mixed
    let d = TernaryPositionSet::new(set(span, &[11, 15]), set(span, &[13, 14, 16])); // Mixed
    let cd = c.intersect(&d);
    // true = {11}
    assert_eq!(positions(&cd.is_true()), vec![11]);
    // Implementation forms unknown by intersecting unknown bitmaps then masking trues.
    // So unknown should be intersection({12,13},{13,14,16}) = {13}
    assert_eq!(positions(&cd.is_unknown()), vec![13]);
}

#[test]
fn test_negate_semantics_and_canonicalization() {
    let span = 32;
    // Definite -> Definite (invert)
    let def = TernaryPositionSet::make_true(span); // all true
    let not_def = def.negate();
    match not_def {
        TernaryPositionSet::Definite(d) => {
            assert_eq!(d.count_positions(), 0); // inverted full => empty
        }
        _ => panic!("negating full definite should yield definite empty"),
    }

    // Unknown(full) -> after negate: new(empty, full) => Unknown(full)
    let unk = TernaryPositionSet::make_unknown(span);
    let not_unk = unk.negate();
    match not_unk {
        TernaryPositionSet::Unknown(u) => {
            assert_eq!(u.count_positions(), span); // still all unknown
        }
        _ => panic!("negating all-unknown should remain unknown"),
    }

    // Unknown(empty) (non-canonical) -> represented by constructing directly then negate.
    // Build via intersect that yields empty unknown to exercise path (may produce Unknown(empty)).
    let unknown_empty = TernaryPositionSet::new(PositionSet::empty(span), PositionSet::empty(span));
    match unknown_empty.negate() {
        // becomes Definite(full)
        TernaryPositionSet::Definite(d) => assert_eq!(d.count_positions(), span),
        other => panic!(
            "expected Definite(full) after negating empty, got {:?}",
            variant_name(&other)
        ),
    }

    // Mixed negate
    let t = set(span, &[1, 4]);
    let u = set(span, &[2, 3, 10]);
    let mixed = TernaryPositionSet::new(t.clone(), u.clone());
    let neg = mixed.negate();
    // For Mixed(t,u): true' = ¬t ∧ ¬u; unknown' = ¬t ∨ ¬u;
    // Check a sample of positions
    let orig_true_ps = mixed.is_true();
    let orig_unknown_ps = mixed.is_unknown();
    let neg_true_ps = neg.is_true();
    let neg_unknown_ps = neg.is_unknown();
    for p in 0..span {
        let orig_true = orig_true_ps.contains(p);
        let orig_unknown = orig_unknown_ps.contains(p);
        let orig_false = !orig_true && !orig_unknown;
        let neg_true = neg_true_ps.contains(p);
        let neg_unknown = neg_unknown_ps.contains(p);
        if orig_true {
            assert!(!neg_true, "p={p} true should not remain true after negate");
        }
        if orig_false {
            assert!(neg_true, "p={p} false should become true after negate");
        }
        if orig_unknown {
            assert!(
                neg_unknown,
                "p={p} unknown should remain unknown after negate"
            );
        }
    }
}

#[test]
fn test_make_constructors() {
    let span = 40;
    let all_true = TernaryPositionSet::make_true(span);
    assert_eq!(all_true.is_true().count_positions(), span);
    assert_eq!(all_true.is_unknown().count_positions(), 0);

    let all_false = TernaryPositionSet::make_false(span);
    assert_eq!(all_false.is_true().count_positions(), 0);
    assert_eq!(all_false.is_false().count_positions(), span);

    let all_unknown = TernaryPositionSet::make_unknown(span);
    assert_eq!(all_unknown.is_unknown().count_positions(), span);
    assert_eq!(all_unknown.is_true().count_positions(), 0);
}

fn variant_name(t: &TernaryPositionSet) -> &'static str {
    match t {
        TernaryPositionSet::Definite(_) => "Definite",
        TernaryPositionSet::Unknown(_) => "Unknown",
        TernaryPositionSet::Mixed(_, _) => "Mixed",
    }
}
