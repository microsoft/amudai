use std::{iter::Peekable, ops::Range};

/// Creates an iterator over the set‑union of two ordered (strictly ascending)
/// non‑duplicated streams of single values.
///
/// Each input iterable (`a` and `b`) must yield items in strictly ascending order
/// (i.e. no duplicates within the same input). The produced iterator:
/// * Yields items in ascending order.
/// * Never yields the same item twice (a value present in both inputs is yielded once).
/// * Performs a streaming merge without materializing intermediate collections.
///
/// Complexity: O(len(a) + len(b)) comparisons; only constant additional memory (two `Peekable`s).
///
/// This is analogous to the mathematical set union of two already sorted, de‑duplicated
/// sequences. If you need a union over ranges (`Range<T>`), use [`union_ranges`].
///
/// # Type Parameters
/// * `T`: Item type; must implement `Ord + Clone`.
/// * `L`, `R`: Types convertible into iterators yielding `T`.
///
/// # Panics
/// Never panics under the stated preconditions.
pub fn union<T, L, R>(a: L, b: R) -> Union<L::IntoIter, R::IntoIter, T>
where
    T: Ord + Clone,
    L: IntoIterator<Item = T>,
    R: IntoIterator<Item = T>,
{
    Union::new(a.into_iter(), b.into_iter())
}

/// Creates an iterator that yields the set-union of two ordered, non-overlapping
/// streams of `Range<T>`.
///
/// The two input iterables (`a` and `b`) must each yield ranges:
/// * In strictly ascending order by `start`.
/// * That never overlap within the same iterable (adjacent is allowed).
///
/// The returned iterator:
/// * Yields ranges in ascending order by `start`.
/// * Never yields overlapping ranges.
/// * Merges any overlapping or adjacent (`next.start <= cur.end`) input
///   ranges (from either side) into a single output range.
///
/// This is a streaming, O(len(a) + len(b)) operation that only holds
/// minimal look‑ahead (each side is wrapped in a `Peekable`).
///
/// For more control (when you already have `Iterator`s) see [`UnionRanges`].
pub fn union_ranges<T, L, R>(a: L, b: R) -> UnionRanges<L::IntoIter, R::IntoIter, T>
where
    T: Ord + Clone,
    L: IntoIterator<Item = Range<T>>,
    R: IntoIterator<Item = Range<T>>,
{
    UnionRanges::new(a.into_iter(), b.into_iter())
}

/// Iterator adapter that yields the set-union of two ordered, non-overlapping
/// streams of ranges.
///
/// Ranges produced by the resulting iterator:
/// * Are yielded in ascending order by `start`.
/// * Never overlap.
/// * Have been merged so that any overlapping or adjacent (`next.start <= cur.end`)
///   input ranges become a single output range.
///
/// Requirements on the two input iterators (`a` and `b`):
/// * Each must yield `Range<T>` items strictly ordered by `start`.
/// * Within each input iterator, ranges must not overlap (adjacency is OK).
/// * `T` must be `Ord + Clone`.
///
/// Complexity: O(len(a) + len(b)) comparisons; only constant additional memory
/// (two `Peekable`s).
///
/// Prefer constructing via [`union_ranges`] unless you already have the
/// concrete iterators.
pub struct UnionRanges<I, J, T>
where
    I: Iterator<Item = Range<T>>,
    J: Iterator<Item = Range<T>>,
{
    a: Peekable<I>,
    b: Peekable<J>,
    // Only needed because the struct itself does not store a `T` directly
    _marker: std::marker::PhantomData<T>,
}

impl<I, J, T> UnionRanges<I, J, T>
where
    I: Iterator<Item = Range<T>>,
    J: Iterator<Item = Range<T>>,
{
    pub fn new(a: I, b: J) -> Self {
        Self {
            a: a.peekable(),
            b: b.peekable(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<I, J, T> Iterator for UnionRanges<I, J, T>
where
    I: Iterator<Item = Range<T>>,
    J: Iterator<Item = Range<T>>,
    T: Ord + Clone,
{
    type Item = Range<T>;

    fn next(&mut self) -> Option<Self::Item> {
        // Pick the next starting range
        let mut cur = match (self.a.peek(), self.b.peek()) {
            (None, None) => return None,
            (Some(_), None) => self.a.next().unwrap(),
            (None, Some(_)) => self.b.next().unwrap(),
            (Some(ra), Some(rb)) => {
                if ra.start <= rb.start {
                    self.a.next().unwrap()
                } else {
                    self.b.next().unwrap()
                }
            }
        };

        loop {
            let mut merged_any = false;

            if let Some(na) = self.a.peek() {
                if na.start <= cur.end {
                    let r = self.a.next().unwrap();
                    if r.end > cur.end {
                        cur.end = r.end;
                    }
                    merged_any = true;
                }
            }

            if let Some(nb) = self.b.peek() {
                if nb.start <= cur.end {
                    let r = self.b.next().unwrap();
                    if r.end > cur.end {
                        cur.end = r.end;
                    }
                    merged_any = true;
                }
            }

            if !merged_any {
                break;
            }
        }

        Some(cur)
    }
}

/// Iterator adapter that yields the set‑union of two ordered, non‑overlapping
/// (within each input) streams of single elements.
///
/// Requirements on inputs:
/// * Each input iterator must yield items in strictly ascending order (no duplicates inside one side).
///
/// Output guarantees:
/// * Items are yielded in ascending order.
/// * No duplicates are yielded (if an item appears in both inputs it is yielded once).
///
/// Complexity: O(len(a) + len(b)) comparisons; only constant additional memory.
pub struct Union<I, J, T>
where
    I: Iterator<Item = T>,
    J: Iterator<Item = T>,
{
    a: Peekable<I>,
    b: Peekable<J>,
}

impl<I, J, T> Union<I, J, T>
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

impl<I, J, T> Iterator for Union<I, J, T>
where
    I: Iterator<Item = T>,
    J: Iterator<Item = T>,
    T: Ord + Clone,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.a.peek(), self.b.peek()) {
            (None, None) => None,
            (Some(_), None) => self.a.next(),
            (None, Some(_)) => self.b.next(),
            (Some(va), Some(vb)) => {
                use std::cmp::Ordering::*;
                match va.cmp(vb) {
                    Less => self.a.next(),
                    Greater => self.b.next(),
                    Equal => {
                        // Advance both; yield one
                        let v = self.a.next().unwrap();
                        self.b.next().unwrap();
                        Some(v)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod union_ranges_tests {
    use super::union_ranges;
    use std::ops::Range;

    fn collect<T: Ord + Clone>(a: Vec<Range<T>>, b: Vec<Range<T>>) -> Vec<Range<T>> {
        union_ranges(a, b).collect()
    }

    #[test]
    fn empty_inputs() {
        let out: Vec<Range<u32>> = collect(vec![], vec![]);
        assert!(out.is_empty());
    }

    #[test]
    fn one_empty() {
        let out = collect(vec![0..5, 10..12], vec![]);
        assert_eq!(out, vec![0..5, 10..12]);

        let out = collect(vec![], vec![3..7]);
        assert_eq!(out, vec![3..7]);
    }

    #[test]
    fn disjoint_non_adjacent() {
        let out = collect(vec![0..5], vec![10..15]);
        assert_eq!(out, vec![0..5, 10..15]);

        // Order reversed
        let out = collect(vec![10..15], vec![0..5]);
        assert_eq!(out, vec![0..5, 10..15]);
    }

    #[test]
    fn adjacent_ranges_merge() {
        // Adjacent within same side
        let out = collect(vec![0..5, 5..10], vec![]);
        assert_eq!(out, vec![0..10]);

        // Adjacent across sides
        let out = collect(vec![0..5], vec![5..9]);
        assert_eq!(out, vec![0..9]);
    }

    #[test]
    fn overlapping_across_sides() {
        let out = collect(vec![0..5], vec![3..8]);
        assert_eq!(out, vec![0..8]);

        let out = collect(vec![3..8], vec![0..5]);
        assert_eq!(out, vec![0..8]);
    }

    #[test]
    fn nested_range() {
        let out = collect(vec![0..10], vec![3..5]);
        assert_eq!(out, vec![0..10]);

        let out = collect(vec![3..5], vec![0..10]);
        assert_eq!(out, vec![0..10]);
    }

    #[test]
    fn complex_interleaving() {
        // Interleaved, requiring multiple successive merges pulling from both sides
        let a = vec![0..2, 10..12, 12..15];
        let b = vec![1..5, 5..10, 20..25];
        let out = collect(a, b);
        assert_eq!(out, vec![0..15, 20..25]);
    }

    #[test]
    fn many_small_overlaps() {
        let a = vec![0..3, 6..9, 12..15];
        let b = vec![2..7, 8..13];
        // Merge chain: 0..3 with 2..7 -> 0..7; 6..9 merges -> 0..9; 8..13 merges -> 0..13; 12..15 merges -> 0..15
        let out = collect(a, b);
        assert_eq!(out, vec![0..15]);
    }

    #[test]
    fn string_generic_type() {
        let a = vec!["a".to_string().."c".to_string()];
        let b = vec!["c".to_string().."e".to_string()];
        let out = collect(a, b);
        assert_eq!(out, vec!["a".to_string().."e".to_string()]);
    }
}

#[cfg(test)]
mod union_tests {
    use super::union;

    fn collect<T: Ord + Clone>(a: Vec<T>, b: Vec<T>) -> Vec<T> {
        union(a, b).collect()
    }

    #[test]
    fn empty_inputs() {
        let out: Vec<u32> = collect(vec![], vec![]);
        assert!(out.is_empty());
    }

    #[test]
    fn one_empty() {
        let out = collect(vec![1, 3, 5], vec![]);
        assert_eq!(out, vec![1, 3, 5]);

        let out = collect(Vec::<u32>::new(), vec![2, 4]);
        assert_eq!(out, vec![2, 4]);
    }

    #[test]
    fn disjoint_interleaved() {
        let out = collect(vec![1, 3, 5], vec![2, 4, 6]);
        assert_eq!(out, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn duplicates_across_sides() {
        let out = collect(vec![1, 3, 5], vec![1, 2, 5]);
        assert_eq!(out, vec![1, 2, 3, 5]);
    }

    #[test]
    fn all_overlap() {
        let out = collect(vec![1, 2, 3], vec![1, 2, 3]);
        assert_eq!(out, vec![1, 2, 3]);
    }

    #[test]
    fn strings() {
        let out = collect(
            vec!["a".to_string(), "c".to_string()],
            vec!["b".to_string(), "c".to_string()],
        );
        assert_eq!(out, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    }
}
