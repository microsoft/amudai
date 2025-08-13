//! Standard-library implementations for the spill-data traits.
//!
//! This module provides implementations of [`Collection`] and [`Collector`] for
//! common std types used throughout the crate, enabling them to plug into the
//! dumping/loading and segmented collection facilities.
//!
//! Currently implemented:
//! - [`Vec<T>`] implements [`Collection`]
//! - [`Vec<T>`] implements [`Collector`] when `T: Clone + Default`
//!
//! Notes on semantics for `Vec<T>` as a [`Collector`]:
//! - Capacity methods that take “items” vs “bytes” map bytes to items via
//!   `capacity_bytes / size_of::<T>()` (floor division)
//! - [`set_len`](Collector::set_len) truncates when shrinking and extends with
//!   `T::default()` when growing
//! - [`push`](Collector::push) clones the provided value (`T: Clone`)

use amudai_io::{ReadAt, verify};

use crate::{Collection, Collector, Load, manifest::Manifest};

impl<T> Collection for Vec<T> {
    #[inline]
    fn len(&self) -> usize {
        Vec::len(self)
    }
}

impl<T: Clone + Default> Collector for Vec<T> {
    type Item = T;

    fn with_item_capacity(capacity: usize) -> Self {
        Vec::with_capacity(capacity)
    }

    fn with_data_capacity(capacity_bytes: usize) -> Self {
        Vec::with_capacity(capacity_bytes / std::mem::size_of::<T>())
    }

    fn reserve_items(&mut self, additional: usize) {
        self.reserve(additional);
    }

    fn reserve_data(&mut self, additional_bytes: usize) {
        self.reserve(additional_bytes / std::mem::size_of::<T>());
    }

    fn push(&mut self, value: &Self::Item) {
        Vec::push(self, value.clone());
    }

    fn set_len(&mut self, new_len: usize) {
        if new_len < self.len() {
            self.truncate(new_len);
        } else {
            self.resize(new_len, T::default());
        }
    }

    fn clear(&mut self) {
        Vec::clear(self);
    }
}

impl<T: bytemuck::Pod> Load for Vec<T> {
    fn load<R>(reader: amudai_io::SlicedFile<R>, manifest: &Manifest) -> std::io::Result<Self>
    where
        R: ReadAt + Clone,
    {
        let len = manifest.get::<u64>("len")?;
        let item_size = manifest.get::<u64>("item_size")? as usize;
        verify!(item_size == std::mem::size_of::<T>());
        let end_pos = len * (std::mem::size_of::<T>() as u64);
        let bytes = if len != 0 {
            reader.read_at(0..end_pos)?
        } else {
            return Ok(Vec::new());
        };
        verify!(bytes.len() == end_pos as usize);
        let vec = bytes.typed_data::<T>().to_vec();
        verify!(vec.len() == len as usize);
        Ok(vec)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use crate::segmented_vec::SegmentedVec;
    use crate::{Dump, Load};

    #[test]
    fn test_seg_vec_with_vec() {
        let mut svec = SegmentedVec::<Vec<Range<u64>>>::with_segment_size(1024);
        svec.set_len(10000);
        svec.push(&(10..100));
        assert_eq!(svec.len(), 10001);
        assert_eq!(svec[10000], 10..100);
        svec[5000] = 100..200;
        assert_eq!(svec[5000], 100..200);
        assert_eq!(svec.segments().len(), 10);

        let v = svec.iter().cloned().collect::<Vec<_>>();
        assert_eq!(v.len(), 10001);

        svec.iter_mut().for_each(|r| r.end = 100000);
        assert_eq!(svec[10].end, 100000);

        svec.clear();
        assert!(svec.is_empty());
    }

    #[test]
    fn test_seg_vec_with_vec_dump_load() {
        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(10000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();

        let mut svec = SegmentedVec::<Vec<u64>>::with_segment_size(1024);
        for i in 0u64..20000 {
            svec.push(&i);
        }

        svec.dump_and_seal(file.clone()).unwrap();

        let svec1: SegmentedVec<Vec<u64>> = Load::load_sealed(file.clone()).unwrap();
        assert_eq!(svec1, svec);
    }
}
