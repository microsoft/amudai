use amudai_io::verify;

use crate::{
    Collection, Collector, Dump, Load,
    manifest::Manifest,
    pod_vec::{PodCollector, PodView},
};

/// A collector for variable-length byte slices.
///
/// `BytesCollector` is used to build a collection of byte slices, which can then
/// be dumped to a file or converted into a `BytesView`. It stores all byte slices
/// concatenated in a single byte buffer (`data`) and uses an `offsets` vector
/// to store the end position of each slice.
pub struct BytesCollector {
    offsets: PodCollector<u64>,
    data: PodCollector<u8>,
}

impl BytesCollector {
    const BYTES_PER_ITEM_RESERVATION: usize = 8;

    /// Creates a new, empty `BytesCollector`.
    pub fn new() -> BytesCollector {
        Self::with_item_capacity(0)
    }

    /// Creates a new `BytesCollector` with a specified capacity for the number of items.
    ///
    /// The underlying data buffer is given a default capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - The number of byte slices to pre-allocate space for.
    pub fn with_item_capacity(capacity: usize) -> BytesCollector {
        BytesCollector {
            offsets: empty_offsets(capacity),
            data: empty_data(capacity * Self::BYTES_PER_ITEM_RESERVATION),
        }
    }

    /// Creates a new `BytesCollector` with a specified capacity for the data buffer.
    ///
    /// The offsets vector is given a default capacity.
    ///
    /// # Arguments
    ///
    /// * `data_capacity` - The number of bytes to pre-allocate space for in the data buffer.
    pub fn with_data_capacity(data_capacity: usize) -> BytesCollector {
        BytesCollector {
            offsets: empty_offsets(data_capacity / Self::BYTES_PER_ITEM_RESERVATION),
            data: empty_data(data_capacity),
        }
    }

    /// Returns the number of byte slices in the collector.
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets().len() - 1
    }

    /// Returns `true` if the collector contains no byte slices.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a slice containing the offsets of the byte slices.
    ///
    /// The offsets are the cumulative lengths of the byte slices. The length of this
    /// slice is `self.len() + 1`.
    #[inline]
    pub fn offsets(&self) -> &[u64] {
        &self.offsets
    }

    /// Returns a slice containing the raw concatenated data of all byte slices.
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Reserves capacity for at least `additional` more bytes to be added to the data buffer.
    ///
    /// # Arguments
    ///
    /// * `additional` - The number of additional bytes to reserve capacity for.
    pub fn reserve_items(&mut self, additional: usize) {
        self.offsets.reserve(additional);
        self.data
            .reserve(additional * Self::BYTES_PER_ITEM_RESERVATION);
    }

    /// Reserves capacity for at least `additional` more bytes to be added to the data buffer.
    ///
    /// # Arguments
    ///
    /// * `additional` - The number of additional bytes to reserve capacity for.
    pub fn reserve_data(&mut self, additional_bytes: usize) {
        self.offsets
            .reserve(additional_bytes / Self::BYTES_PER_ITEM_RESERVATION);
        self.data.reserve(additional_bytes);
    }

    /// Appends a byte slice to the back of the collection.
    ///
    /// # Arguments
    ///
    /// * `value` - The byte slice to push.
    pub fn push(&mut self, value: impl AsRef<[u8]>) {
        self.data.extend_from_slice(value.as_ref());
        self.offsets.push(self.data.len() as u64);
    }

    /// Appends a sequence of byte slices by concatenating them as a single item.
    ///
    /// # Arguments
    ///
    /// * `fragments` - An iterator over byte slices to concatenate and push
    ///   as a single item.
    pub fn push_concat(&mut self, fragments: impl IntoIterator<Item = impl AsRef<[u8]>> + Clone) {
        let len = fragments
            .clone()
            .into_iter()
            .map(|s| s.as_ref().len())
            .sum();
        self.data.reserve(len);
        for slice in fragments {
            self.data.extend_from_slice(slice.as_ref());
        }
        self.offsets.push(self.data.len() as u64);
    }

    /// Returns the byte slice at the given index.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    #[inline]
    pub fn get_at(&self, index: usize) -> &[u8] {
        let offsets = self.offsets();
        let start = offsets[index] as usize;
        let end = offsets[index + 1] as usize;
        &self.data.as_slice()[start..end]
    }

    /// Returns a mutable byte slice at the given index.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    #[inline]
    pub fn get_at_mut(&mut self, index: usize) -> &mut [u8] {
        let offsets = self.offsets();
        let start = offsets[index] as usize;
        let end = offsets[index + 1] as usize;
        &mut self.data.as_mut_slice()[start..end]
    }

    /// Resizes the `BytesCollector` in-place so that `len` is equal to `new_len`.
    ///
    /// If `new_len` is greater than `len`, the collector is extended by the
    /// difference, with each additional slot filled with `value`.
    /// If `new_len` is less than `len`, the collector is simply truncated.
    ///
    /// If you only need to resize to a smaller size, use [`BytesCollector::truncate`].
    pub fn resize(&mut self, new_len: usize, value: impl AsRef<[u8]>) {
        if new_len < self.len() {
            self.truncate(new_len);
        } else if new_len > self.len() {
            let additional = new_len - self.len();
            let value = value.as_ref();
            if value.is_empty() {
                self.offsets
                    .resize(new_len + 1, *self.offsets.last().unwrap());
            } else {
                self.offsets.reserve(additional);
                self.data.reserve(additional * value.len());
                for _ in 0..additional {
                    self.push(value);
                }
            }
        }
    }

    /// Sets the length of the `BytesCollector` to the specified value.
    ///
    /// This method modifies the `BytesCollector` length, either by truncating it if
    /// `new_len` is smaller than the current length, or by extending it with empty
    /// buffers if `new_len` is larger.
    ///
    /// # Parameters
    ///
    /// * `new_len` - The desired new length of the `BytesCollector`
    pub fn resize_with_empty(&mut self, new_len: usize) {
        self.resize(new_len, []);
    }

    /// Shortens the `BytesCollector`, keeping the first `len` elements and dropping
    /// the rest.
    ///
    /// If `len` is greater or equal to the collector's current length, this has
    /// no effect.
    ///
    /// Note that this method has no effect on the allocated capacity of the collector.
    pub fn truncate(&mut self, len: usize) {
        if len < self.len() {
            self.offsets.truncate(len + 1);
            let data_size = self.offsets[len] as usize;
            self.data.truncate(data_size);
        }
    }

    /// Clears the collector, removing all values.
    pub fn clear(&mut self) {
        self.offsets.truncate(1);
        self.data.clear();
    }

    /// Consumes the `BytesCollector` and returns a `BytesView`.
    pub fn into_view(self) -> BytesView {
        BytesView::new(self.offsets.into_view(), self.data.into_view())
    }
}

impl std::ops::Index<usize> for BytesCollector {
    type Output = [u8];

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        self.get_at(index)
    }
}

impl std::ops::IndexMut<usize> for BytesCollector {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.get_at_mut(index)
    }
}

impl Default for BytesCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl Collection for BytesCollector {
    #[inline]
    fn len(&self) -> usize {
        BytesCollector::len(self)
    }
}

impl Collector for BytesCollector {
    type Item = [u8];

    fn with_item_capacity(capacity: usize) -> Self {
        BytesCollector::with_item_capacity(capacity)
    }

    fn with_data_capacity(capacity_bytes: usize) -> Self {
        BytesCollector::with_data_capacity(capacity_bytes)
    }

    fn reserve_items(&mut self, additional: usize) {
        BytesCollector::reserve_items(self, additional);
    }

    fn reserve_data(&mut self, additional_bytes: usize) {
        BytesCollector::reserve_data(self, additional_bytes);
    }

    #[inline]
    fn push(&mut self, value: &Self::Item) {
        BytesCollector::push(self, value);
    }

    fn set_len(&mut self, new_len: usize) {
        BytesCollector::resize_with_empty(self, new_len);
    }

    fn clear(&mut self) {
        BytesCollector::clear(self);
    }
}

impl Dump for BytesCollector {
    fn compute_size(&self) -> u64 {
        self.offsets().compute_size() + self.data().compute_size()
    }

    fn dump<W>(
        &self,
        writer: amudai_io::SlicedFile<W>,
    ) -> std::io::Result<crate::manifest::Manifest>
    where
        W: amudai_io::WriteAt + Clone,
    {
        let offsets_range = 0..self.offsets.compute_size();
        let data_range = offsets_range.end..offsets_range.end + self.data.compute_size();
        verify!(data_range.end <= writer.slice_size());

        let offsets_manifest = self
            .offsets
            .dump(writer.slice(offsets_range.clone())?)?
            .into_nested(offsets_range);

        let data_manifest = self
            .data
            .dump(writer.slice(data_range.clone())?)?
            .into_nested(data_range);

        let mut manifest = Manifest::default();
        manifest.put("len", self.len() as u64);
        manifest.set_child("offsets", offsets_manifest);
        manifest.set_child("data", data_manifest);
        Ok(manifest)
    }
}

impl Load for BytesCollector {
    fn load<R>(reader: amudai_io::SlicedFile<R>, manifest: &Manifest) -> std::io::Result<Self>
    where
        R: amudai_io::ReadAt + Clone,
    {
        let len = manifest.get::<u64>("len")? as usize;
        let offsets_manifest = manifest.child("offsets")?;
        let data_manifest = manifest.child("data")?;

        let offsets = PodCollector::<u64>::load(
            reader.slice(offsets_manifest.range.clone())?,
            &offsets_manifest.manifest,
        )?;
        let data = PodCollector::<u8>::load(
            reader.slice(data_manifest.range.clone())?,
            &data_manifest.manifest,
        )?;
        verify!(offsets.len() == len + 1);
        verify!(offsets[0] <= data.len() as u64);
        verify!(*offsets.last().unwrap() <= data.len() as u64);
        Ok(BytesCollector { offsets, data })
    }
}

/// A read-only view of a collection of variable-length byte slices.
///
/// `BytesView` is typically created by loading data that was previously dumped
/// by a `BytesCollector`. It provides efficient read-only access to the byte
/// slices without copying the entire collection into mutable `BytesCollector`.
/// It consists of a view into the offsets and a view into the concatenated data.
#[derive(Clone, Default)]
pub struct BytesView {
    offsets: PodView<u64>,
    data: PodView<u8>,
}

impl BytesView {
    /// Creates a new `BytesView` from a view of offsets and a view of data.
    ///
    /// # Arguments
    ///
    /// * `offsets` - A `PodView` of `u64` values representing the end position
    ///   of each byte slice in the `data` view. The first offset is typically 0.
    /// * `data` - A `PodView` of `u8` containing the concatenated byte slices.
    ///
    /// # Panics
    ///
    /// Panics if `offsets` is empty, or if the offsets are inconsistent with
    /// the length of the `data` view.
    pub fn new(offsets: PodView<u64>, data: PodView<u8>) -> BytesView {
        assert!(!offsets.is_empty());
        let data_len = *offsets.last().unwrap() - offsets[0];
        assert!(data.len() >= data_len as usize);
        BytesView { offsets, data }
    }

    /// Returns the number of byte slices in the view.
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Returns `true` if the view contains no byte slices.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the byte slice at the given index.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    #[inline]
    pub fn get_at(&self, index: usize) -> &[u8] {
        let offsets = self.offsets.as_slice();
        let start = offsets[index] as usize;
        let end = offsets[index + 1] as usize;
        &self.data.as_slice()[start..end]
    }
}

impl Collection for BytesView {
    #[inline]
    fn len(&self) -> usize {
        BytesView::len(self)
    }
}

impl std::ops::Index<usize> for BytesView {
    type Output = [u8];

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        self.get_at(index)
    }
}

impl Load for BytesView {
    fn load<R>(reader: amudai_io::SlicedFile<R>, manifest: &Manifest) -> std::io::Result<Self>
    where
        R: amudai_io::ReadAt + Clone,
    {
        let len = manifest.get::<u64>("len")? as usize;
        let offsets_manifest = manifest.child("offsets")?;
        let data_manifest = manifest.child("data")?;

        let offsets = PodView::<u64>::load(
            reader.slice(offsets_manifest.range.clone())?,
            &offsets_manifest.manifest,
        )?;
        let data = PodView::<u8>::load(
            reader.slice(data_manifest.range.clone())?,
            &data_manifest.manifest,
        )?;
        verify!(offsets.len() == len + 1);
        verify!(offsets[0] <= data.len() as u64);
        verify!(*offsets.last().unwrap() <= data.len() as u64);
        Ok(BytesView::new(offsets, data))
    }
}

fn empty_offsets(capacity: usize) -> PodCollector<u64> {
    let mut offsets = PodCollector::with_capacity(capacity + 1);
    offsets.push(0u64);
    offsets
}

fn empty_data(capacity: usize) -> PodCollector<u8> {
    PodCollector::with_capacity(capacity)
}

#[cfg(test)]
mod tests {
    use crate::{
        Dump, Load,
        bytes_vec::{BytesCollector, BytesView},
    };

    #[test]
    fn test_bytes_collector_basics() {
        let mut bytes = BytesCollector::new();
        bytes.push(b"abcde");
        assert_eq!(&bytes[0], b"abcde");
    }

    #[test]
    fn test_bytes_vec_dump_and_load() {
        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        assert_eq!(file.size().unwrap(), 0);

        // Create a BytesCollector with various byte sequences
        let mut bytes_collector = BytesCollector::new();
        bytes_collector.push(b"hello");
        bytes_collector.push(b"world");
        bytes_collector.push(b"test");
        bytes_collector.push(b"data");
        bytes_collector.push(b""); // empty slice
        bytes_collector.push(b"final_entry");

        assert_eq!(bytes_collector.len(), 6);
        assert_eq!(&bytes_collector[0], b"hello");
        assert_eq!(&bytes_collector[1], b"world");
        assert_eq!(&bytes_collector[2], b"test");
        assert_eq!(&bytes_collector[3], b"data");
        assert_eq!(&bytes_collector[4], b"");
        assert_eq!(&bytes_collector[5], b"final_entry");

        // Dump and seal the bytes collector
        let end_pos = bytes_collector.dump_and_seal(file.clone()).unwrap();
        dbg!(&end_pos);

        // Load it back as ReadOnlyBytesVec
        let readonly_bytes = BytesView::load_sealed(file.clone()).unwrap();

        // Verify the loaded data matches the original
        assert_eq!(readonly_bytes.len(), 6);
        assert_eq!(&readonly_bytes[0], b"hello");
        assert_eq!(&readonly_bytes[1], b"world");
        assert_eq!(&readonly_bytes[2], b"test");
        assert_eq!(&readonly_bytes[3], b"data");
        assert_eq!(&readonly_bytes[4], b"");
        assert_eq!(&readonly_bytes[5], b"final_entry");
    }

    #[test]
    fn test_bytes_vec_dump_and_load_large_data() {
        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();

        // Create a BytesCollector with larger, varied data
        let mut bytes_collector = BytesCollector::with_item_capacity(1000);

        // Add various sizes of data
        for i in 0..100 {
            let data = format!(
                "entry_{:04}_with_some_longer_content_to_test_larger_offsets",
                i
            );
            bytes_collector.push(data.as_bytes());
        }

        // Add some very large entries
        let large_data = vec![b'X'; 10000];
        bytes_collector.push(&large_data);

        let very_large_data = vec![b'Y'; 50000];
        bytes_collector.push(&very_large_data);

        let original_len = bytes_collector.len();
        assert_eq!(original_len, 102);

        // Test some original data
        assert_eq!(
            &bytes_collector[0],
            b"entry_0000_with_some_longer_content_to_test_larger_offsets"
        );
        assert_eq!(
            &bytes_collector[99],
            b"entry_0099_with_some_longer_content_to_test_larger_offsets"
        );
        assert_eq!(bytes_collector[100].len(), 10000);
        assert_eq!(bytes_collector[101].len(), 50000);

        // Dump and seal
        let end_pos = bytes_collector.dump_and_seal(file.clone()).unwrap();
        dbg!(&end_pos);

        // Load back
        let readonly_bytes = BytesView::load_sealed(file.clone()).unwrap();

        // Verify loaded data
        assert_eq!(readonly_bytes.len(), original_len);
        assert_eq!(
            &readonly_bytes[0],
            b"entry_0000_with_some_longer_content_to_test_larger_offsets"
        );
        assert_eq!(
            &readonly_bytes[99],
            b"entry_0099_with_some_longer_content_to_test_larger_offsets"
        );
        assert_eq!(readonly_bytes[100].len(), 10000);
        assert_eq!(readonly_bytes[101].len(), 50000);

        // Verify the content of large entries
        assert!(readonly_bytes[100].iter().all(|&b| b == b'X'));
        assert!(readonly_bytes[101].iter().all(|&b| b == b'Y'));

        let mut bytes_collector2 = BytesCollector::load_sealed(file.clone()).unwrap();
        assert_eq!(bytes_collector2.len(), original_len);
        for i in 0..original_len {
            assert_eq!(&bytes_collector[i], &bytes_collector2[i]);
        }

        bytes_collector2.push(b"abcde");
        assert_eq!(&bytes_collector2[original_len], b"abcde");
    }

    #[test]
    fn test_bytes_vec_empty_dump_and_load() {
        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();

        // Create an empty BytesCollector
        let bytes_collector = BytesCollector::new();
        assert_eq!(bytes_collector.len(), 0);
        assert!(bytes_collector.is_empty());

        // Dump and seal the empty collector
        let end_pos = bytes_collector.dump_and_seal(file.clone()).unwrap();
        dbg!(&end_pos);

        // Load it back
        let readonly_bytes = BytesView::load_sealed(file.clone()).unwrap();

        // Verify the loaded empty data
        assert_eq!(readonly_bytes.len(), 0);
        assert!(readonly_bytes.is_empty());
    }
}
