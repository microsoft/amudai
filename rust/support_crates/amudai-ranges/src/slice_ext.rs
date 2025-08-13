//! Extensions for working with slices.
//!
//! This module provides the [`SliceExt`] trait, which adds a utility for
//! splitting a mutable slice (or a `Vec`) into a sequence of mutable
//! sub-slices based on an iterator of sizes.

/// Extension trait adding utilities for splitting collections into mutable chunks
/// according to an iterator of sizes.
pub trait SliceExt<T> {
    /// Splits this collection into contiguous mutable sub-slices according to `sizes`.
    ///
    /// Each item produced by `sizes` is interpreted as the length of the next chunk.
    /// Chunks are formed from the start of the slice in order. If `sizes` does not
    /// cover the entire slice, the remaining tail is ignored. If a requested chunk
    /// length exceeds the remaining length, this function panics.
    ///
    /// This operation does not allocate aside from the returned `Vec` of references,
    /// and it does not copy data. The returned sub-slices are borrowed views into the
    /// original buffer and are non-overlapping.
    ///
    /// Panics
    /// - If any size yielded by `sizes` is greater than the number of elements remaining
    ///   in the slice at that step.
    fn split_at_sizes_mut<'a>(&'a mut self, sizes: impl Iterator<Item = usize>)
    -> Vec<&'a mut [T]>;
}

impl<T> SliceExt<T> for [T] {
    fn split_at_sizes_mut(&mut self, sizes: impl Iterator<Item = usize>) -> Vec<&mut [T]> {
        let mut rest = self;
        let (lower, upper) = sizes.size_hint();
        let mut res: Vec<&mut [T]> = Vec::with_capacity(upper.unwrap_or(lower));
        for size in sizes {
            let (chunk, tail) = rest.split_at_mut(size);
            res.push(chunk);
            rest = tail;
        }
        res
    }
}

impl<T> SliceExt<T> for Vec<T> {
    fn split_at_sizes_mut(&mut self, sizes: impl Iterator<Item = usize>) -> Vec<&mut [T]> {
        self.as_mut_slice().split_at_sizes_mut(sizes)
    }
}
