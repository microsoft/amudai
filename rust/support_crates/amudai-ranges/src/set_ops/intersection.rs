use std::{cmp, iter::Peekable, ops::Range};

/// Creates an iterator over the set‑intersection of two ordered (strictly ascending)
/// non‑duplicated streams of single values.
///
/// Each input iterable (`a` and `b`) must yield items in strictly ascending order
/// (i.e. no duplicates within the same input). The produced iterator:
/// * Yields only items present in BOTH inputs.
/// * Yields items in ascending order.
/// * Never yields duplicates.
/// * Performs a streaming merge without materializing intermediate collections.
///
/// Complexity: O(len(a) + len(b)) comparisons; only constant additional memory.
///
/// This is analogous to the mathematical set intersection of two already sorted,
/// de‑duplicated sequences. If you need an intersection over ranges (`Range<T>`),
/// use [`intersect_ranges`].
pub fn intersect<T, L, R>(a: L, b: R) -> Intersection<L::IntoIter, R::IntoIter, T>
where
    T: Ord + Clone,
    L: IntoIterator<Item = T>,
    R: IntoIterator<Item = T>,
{
    Intersection::new(a.into_iter(), b.into_iter())
}

/// Creates an iterator that yields the set‑intersection of two ordered, non‑overlapping
/// streams of `Range<T>`.
///
/// The two input iterables (`a` and `b`) must each yield ranges:
/// * In strictly ascending order by `start`.
/// * That never overlap within the same iterable (adjacent is allowed).
///
/// The returned iterator:
/// * Yields only the overlapping portions between ranges from the two sides.
/// * Yields ranges in ascending order by `start`.
/// * Never yields empty or zero‑length intersections.
/// * Output ranges are guaranteed non‑overlapping.
///
/// This is a streaming, O(len(a) + len(b)) operation holding only the current range from
/// each side.
pub fn intersect_ranges<T, L, R>(a: L, b: R) -> IntersectionRanges<L::IntoIter, R::IntoIter, T>
where
    T: Ord + Clone,
    L: IntoIterator<Item = Range<T>>,
    R: IntoIterator<Item = Range<T>>,
{
    IntersectionRanges::new(a.into_iter(), b.into_iter())
}

/// Iterator adapter yielding the set‑intersection of two ordered, non‑duplicated
/// streams of single elements.
pub struct Intersection<I, J, T>
where
    I: Iterator<Item = T>,
    J: Iterator<Item = T>,
{
    a: Peekable<I>,
    b: Peekable<J>,
}

impl<I, J, T> Intersection<I, J, T>
where
    I: Iterator<Item = T>,
    J: Iterator<Item = T>,
    T: Ord + Clone,
{
    pub fn new(a: I, b: J) -> Self {
        Self {
            a: a.peekable(),
            b: b.peekable(),
        }
    }
}

impl<I, J, T> Iterator for Intersection<I, J, T>
where
    I: Iterator<Item = T>,
    J: Iterator<Item = T>,
    T: Ord + Clone,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (pa, pb) = match (self.a.peek(), self.b.peek()) {
                (Some(a), Some(b)) => (a, b),
                _ => return None,
            };
            match pa.cmp(pb) {
                cmp::Ordering::Less => {
                    self.a.next();
                }
                cmp::Ordering::Greater => {
                    self.b.next();
                }
                cmp::Ordering::Equal => {
                    let v = self.a.next().unwrap();
                    self.b.next().unwrap();
                    return Some(v);
                }
            }
        }
    }
}

/// Iterator adapter yielding the set‑intersection of two ordered, non‑overlapping
/// (within each input) streams of ranges.
pub struct IntersectionRanges<I, J, T>
where
    I: Iterator<Item = Range<T>>,
    J: Iterator<Item = Range<T>>,
{
    a: I,
    b: J,
    cur_a: Option<Range<T>>,
    cur_b: Option<Range<T>>,
    _marker: std::marker::PhantomData<T>,
}

impl<I, J, T> IntersectionRanges<I, J, T>
where
    I: Iterator<Item = Range<T>>,
    J: Iterator<Item = Range<T>>,
    T: Ord + Clone,
{
    pub fn new(mut a: I, mut b: J) -> Self {
        let cur_a = a.next();
        let cur_b = b.next();
        Self {
            a,
            b,
            cur_a,
            cur_b,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<I, J, T> Iterator for IntersectionRanges<I, J, T>
where
    I: Iterator<Item = Range<T>>,
    J: Iterator<Item = Range<T>>,
    T: Ord + Clone,
{
    type Item = Range<T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (ra, rb) = match (&self.cur_a, &self.cur_b) {
                (Some(a), Some(b)) => (a, b),
                _ => return None,
            };

            // If a ends before (or exactly at) b starts: advance a.
            if ra.end <= rb.start {
                self.cur_a = self.a.next();
                continue;
            }

            // If b ends before (or exactly at) a starts: advance b.
            if rb.end <= ra.start {
                self.cur_b = self.b.next();
                continue;
            }

            // They overlap: compute intersection.
            let start = if ra.start >= rb.start {
                ra.start.clone()
            } else {
                rb.start.clone()
            };
            let end = if ra.end <= rb.end {
                ra.end.clone()
            } else {
                rb.end.clone()
            };

            // Advance whichever range(s) finished at 'end'.
            let a_ended = ra.end <= rb.end;
            let b_ended = rb.end <= ra.end;

            if a_ended {
                self.cur_a = self.a.next();
            }
            if b_ended {
                self.cur_b = self.b.next();
            }

            // Produce only non‑empty intersections (end > start).
            if start < end {
                return Some(start..end);
            }
            // If empty (should only occur with degenerate inputs), continue.
        }
    }
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod intersection_tests {
    use super::intersect;

    fn collect<T: Ord + Clone>(a: Vec<T>, b: Vec<T>) -> Vec<T> {
        intersect(a, b).collect()
    }

    #[test]
    fn empty_inputs() {
        let out: Vec<u32> = collect(vec![], vec![]);
        assert!(out.is_empty());
    }

    #[test]
    fn one_empty() {
        let out: Vec<u32> = collect(vec![1, 2, 3], vec![]);
        assert!(out.is_empty());

        let out: Vec<u32> = collect(vec![], vec![4, 5]);
        assert!(out.is_empty());
    }

    #[test]
    fn disjoint() {
        let out = collect(vec![1, 3, 5], vec![2, 4, 6]);
        assert!(out.is_empty());
    }

    #[test]
    fn partial_overlap() {
        let out = collect(vec![1, 3, 5, 7], vec![0, 3, 4, 7, 9]);
        assert_eq!(out, vec![3, 7]);
    }

    #[test]
    fn full_overlap_identical() {
        let out = collect(vec![1, 2, 3], vec![1, 2, 3]);
        assert_eq!(out, vec![1, 2, 3]);
    }

    #[test]
    fn strings() {
        let out = collect(
            vec!["a".to_string(), "c".to_string(), "d".to_string()],
            vec![
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string(),
            ],
        );
        assert_eq!(out, vec!["c".to_string(), "d".to_string()]);
    }
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod intersection_ranges_tests {
    use super::intersect_ranges;
    use std::ops::Range;

    fn collect<T: Ord + Clone>(a: Vec<Range<T>>, b: Vec<Range<T>>) -> Vec<Range<T>> {
        intersect_ranges(a, b).collect()
    }

    #[test]
    fn empty_inputs() {
        let out: Vec<Range<u32>> = collect(vec![], vec![]);
        assert!(out.is_empty());
    }

    #[test]
    fn one_empty() {
        let out: Vec<Range<u32>> = collect(vec![0..10], vec![]);
        assert!(out.is_empty());
    }

    #[test]
    fn no_overlap_disjoint() {
        let out = collect(vec![0..5], vec![5..10]);
        assert!(out.is_empty());

        let out = collect(vec![5..10], vec![0..5]);
        assert!(out.is_empty());
    }

    #[test]
    fn simple_overlap() {
        let out = collect(vec![0..10], vec![3..7]);
        assert_eq!(out, vec![3..7]);

        let out = collect(vec![3..7], vec![0..10]);
        assert_eq!(out, vec![3..7]);
    }

    #[test]
    fn partial_edges() {
        let out = collect(vec![0..5], vec![3..8]);
        assert_eq!(out, vec![3..5]);

        let out = collect(vec![3..8], vec![0..5]);
        assert_eq!(out, vec![3..5]);
    }

    #[test]
    fn nested_multi_segments() {
        let a = vec![0..5, 10..20, 30..40];
        let b = vec![3..12, 18..35];
        // overlaps: 3..5, 10..12, 18..20, 30..35
        let out = collect(a, b);
        assert_eq!(out, vec![3..5, 10..12, 18..20, 30..35]);
    }

    #[test]
    fn touching_not_included() {
        let out = collect(vec![0..5, 5..10], vec![10..15]);
        assert!(out.is_empty());

        let out = collect(vec![0..5], vec![5..10, 10..15]);
        assert!(out.is_empty());
    }

    #[test]
    fn complex_interleaving() {
        let a = vec![0..2, 4..6, 8..11, 15..18];
        let b = vec![1..5, 5..9, 10..16];
        let out = collect(a, b);
        assert_eq!(out, vec![1..2, 4..5, 5..6, 8..9, 10..11, 15..16]);
    }

    #[test]
    fn equal_ranges() {
        let out = collect(vec![0..5, 10..15], vec![0..5, 10..15]);
        assert_eq!(out, vec![0..5, 10..15]);
    }

    #[test]
    fn string_generic_type() {
        let a = vec!["a".to_string().."f".to_string()];
        let b = vec!["c".to_string().."z".to_string()];
        let out = collect(a, b);
        assert_eq!(out, vec!["c".to_string().."f".to_string()]);
    }
}
