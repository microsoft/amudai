//! A reader that prefetches data from a source `ReadAt` in the background

use std::{
    collections::VecDeque,
    iter::Peekable,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use amudai_bytes::Bytes;
use amudai_io::ReadAt;

use crate::io_pool::{IoPool, QueuedOp};

pub type BoxedIterator<T> = Box<dyn Iterator<Item = T> + Send + Sync + 'static>;
pub type BoxedRangeIterator<T> = BoxedIterator<Range<T>>;

/// A reader that prefetches data from a source `ReadAt` in the background according
/// to a predicted sequence of ranges.
///
/// `PrefetchReadAt` works by:
/// 1. Queuing reads in the background based on a predicted sequence of ranges
/// 2. Checking if requested ranges are already available in the prefetch cache
/// 3. Falling back to direct reads from the source if prefetched data isn't available
pub struct PrefetchReadAt {
    /// The underlying reader that provides the actual data
    reader: Arc<dyn ReadAt>,
    /// The prefetcher that manages background reads and caching
    prefetcher: Mutex<Prefetcher>,
    /// Counter for successful reads served from the prefetcher
    reads_from_prefetcher: AtomicUsize,
    /// Counter for reads that had to fall back to the source
    reads_from_source: AtomicUsize,
}

impl PrefetchReadAt {
    /// Creates a new `PrefetchReadAt` with the given reader and predicted range sequence.
    ///
    /// # Parameters
    /// * `reader` - The underlying `ReadAt` implementation to read from
    /// * `ranges` - Iterator of predicted ranges that will be prefetched in the background
    ///
    /// # Returns
    /// A new `PrefetchReadAt` instance
    pub fn new(reader: Arc<dyn ReadAt>, ranges: BoxedRangeIterator<u64>) -> PrefetchReadAt {
        PrefetchReadAt {
            reader: reader.clone(),
            prefetcher: Mutex::new(Prefetcher::new(reader, ranges)),
            reads_from_prefetcher: AtomicUsize::new(0),
            reads_from_source: AtomicUsize::new(0),
        }
    }

    /// Reads data from the specified range, potentially using the prefetcher.
    ///
    /// This variant takes a mutable reference, which allows direct access to the prefetcher
    /// without locking.
    ///
    /// # Parameters
    /// * `range` - The byte range to read
    ///
    /// # Returns
    /// The read data, or an error if the read fails
    pub fn read_at_mut(&mut self, range: Range<u64>) -> std::io::Result<Bytes> {
        let bytes = self.prefetcher.get_mut().unwrap().try_read(range.clone());
        if let Some(bytes) = bytes {
            self.reads_from_prefetcher.fetch_add(1, Ordering::Relaxed);
            return Ok(bytes);
        }
        self.reads_from_source.fetch_add(1, Ordering::Relaxed);
        self.reader.read_at(range)
    }

    /// Returns the number of reads that needed to be served directly from the source.
    ///
    /// Higher values may indicate that the prefetcher is not effectively predicting
    /// the access pattern.
    ///
    /// # Returns
    /// The count of reads served from the source
    pub fn count_reads_from_source(&self) -> usize {
        self.reads_from_source.load(Ordering::Relaxed)
    }

    /// Returns the number of reads that were successfully served from the prefetcher.
    ///
    /// Higher values indicate the prefetcher is effectively anticipating read patterns.
    ///
    /// # Returns
    /// The count of reads served from the prefetcher
    pub fn count_reads_from_prefetcher(&self) -> usize {
        self.reads_from_prefetcher.load(Ordering::Relaxed)
    }
}

impl ReadAt for PrefetchReadAt {
    fn size(&self) -> std::io::Result<u64> {
        self.reader.size()
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        let bytes = self.prefetcher.lock().unwrap().try_read(range.clone());
        if let Some(bytes) = bytes {
            self.reads_from_prefetcher.fetch_add(1, Ordering::Relaxed);
            return Ok(bytes);
        }
        self.reads_from_source.fetch_add(1, Ordering::Relaxed);
        self.reader.read_at(range)
    }

    fn storage_profile(&self) -> amudai_io::StorageProfile {
        self.reader.storage_profile()
    }
}

/// The engine behind `PrefetchReadAt` that manages background reads and caching.
///
/// `Prefetcher` maintains a queue of in-progress reads and tries to serve
/// requested ranges from this queue whenever possible. It also manages
/// discarding obsolete prefetch data and scheduling new prefetch operations.
pub struct Prefetcher {
    /// The underlying reader that provides the actual data
    reader: Arc<dyn ReadAt>,
    /// Queue of in-progress or completed read operations
    queue: VecDeque<Arc<QueuedOp>>,
    /// Iterator of predicted ranges to prefetch, with the ability to peek
    ranges: Peekable<BoxedRangeIterator<u64>>,
    /// Current size of all queued data in bytes
    queued_data_size: usize,
    /// The I/O pool used to execute background read operations
    pool: Arc<IoPool>,
}

impl Prefetcher {
    /// Maximum number of read operations (for a single prefetcher) that can be queued
    /// at once
    pub const MAX_QUEUED_READS: usize = 128;
    /// Maximum total size of queued data in bytes (8 MB)
    pub const MAX_QUEUED_DATA_SIZE: usize = 8 * 1024 * 1024;

    /// Creates a new `Prefetcher` with the given reader and predicted range sequence.
    ///
    /// # Parameters
    /// * `reader` - The underlying `ReadAt` implementation to read from
    /// * `ranges` - Iterator of predicted ranges that will be prefetched in the background
    ///
    /// # Returns
    /// A new `Prefetcher` instance
    pub fn new(reader: Arc<dyn ReadAt>, ranges: BoxedRangeIterator<u64>) -> Prefetcher {
        Prefetcher {
            reader,
            queue: Default::default(),
            ranges: ranges.peekable(),
            queued_data_size: 0,
            pool: IoPool::get(),
        }
    }

    /// Attempts to read the requested range from the prefetch queue.
    ///
    /// This method:
    /// 1. Skips any prefetch ranges that are completely before the requested range
    /// 2. Tries to enqueue new prefetch operations
    /// 3. Looks for the requested range in the queue of prefetched data
    ///
    /// # Parameters
    /// * `range` - The byte range to read
    ///
    /// # Returns
    /// The read data if available, or `None` if not yet available
    pub fn try_read(&mut self, range: Range<u64>) -> Option<Bytes> {
        // Skip prefetch ranges that are completely before the requested range
        while self
            .ranges
            .peek()
            .map(|prefetch_range| range.start >= prefetch_range.end)
            .unwrap_or(false)
        {
            self.ranges.next().expect("next after peek");
        }

        // Process and enqueue upcoming prefetch ranges until we hit the limits.
        while let Some(prefetch_range) = self.ranges.peek().cloned() {
            if !self.try_enqueue(prefetch_range) {
                break;
            }
            self.ranges.next().expect("next after peek");
        }

        while let Some(queued_op) = self.queue.front().cloned() {
            let queued_read = queued_op.as_read().unwrap();
            if range.start >= queued_read.range.end {
                self.discard_front();
                continue;
            }
            if !queued_read.contains(&range) {
                break;
            }
            if let Some(bytes) = queued_read.try_read(&range) {
                return Some(bytes);
            } else {
                self.discard_front();
                break;
            }
        }
        None
    }

    /// Attempts to enqueue a range for prefetching.
    ///
    /// Will not enqueue if:
    /// - Queue length or data size limits are reached
    /// - The range is not sequential with the previous range
    /// - The I/O pool cannot accept more operations
    ///
    /// # Parameters
    /// * `range` - The byte range to prefetch
    ///
    /// # Returns
    /// `true` if the range was enqueued or should be skipped, `false` if limits were reached
    fn try_enqueue(&mut self, range: Range<u64>) -> bool {
        if self.queue.len() >= Self::MAX_QUEUED_READS {
            return false;
        }
        if self.queued_data_size >= Self::MAX_QUEUED_DATA_SIZE {
            return false;
        }
        let is_valid = self
            .queue
            .back()
            .map(|op| op.as_read().unwrap().range.end <= range.start)
            .unwrap_or(true);
        if !is_valid {
            // Skip and continue
            return true;
        }

        let size = (range.end - range.start) as usize;
        let queued_read = QueuedOp::new_read(range, self.reader.clone());
        if !self.pool.try_enqueue(queued_read.clone()) {
            return false;
        }
        self.queued_data_size += size;
        self.queue.push_back(queued_read);
        true
    }

    /// Removes and discards the oldest entry in the prefetch queue.
    ///
    /// This also updates the `queued_data_size` to reflect the removal.
    fn discard_front(&mut self) {
        if let Some(queued_op) = self.queue.pop_front() {
            queued_op.discard();
            let size = queued_op.as_read().unwrap().size();
            self.queued_data_size -= std::cmp::min(self.queued_data_size, size);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use amudai_io::ReadAt;

    use crate::prefetch_read::PrefetchReadAt;

    #[test]
    fn test_prefetch_read_at_basics() {
        let temp_store = crate::temp_file_store::create_fs_based(16 * 1024 * 1024, None).unwrap();
        let mut writer = temp_store.allocate_stream(None).unwrap();
        for _ in 0..10 {
            writer.write_all(&vec![1u8; 4096]).unwrap();
        }

        let reader = writer.into_read_at().unwrap();
        assert_eq!(reader.size().unwrap(), 4096 * 10);

        let ranges = (0u64..30000).step_by(1000).map(|s| s..s + 200);
        let prefetcher = PrefetchReadAt::new(reader, Box::new(ranges.clone()));
        for range in ranges.step_by(4) {
            prefetcher.read_at(range.clone()).unwrap();
            prefetcher
                .read_at(range.start + 400..range.end + 400)
                .unwrap();
        }

        assert!(prefetcher.count_reads_from_source() > 3);
        assert!(prefetcher.count_reads_from_prefetcher() > 3);
    }

    #[test]
    fn test_mutable_read_access() {
        let temp_store = crate::temp_file_store::create_fs_based(1024 * 1024, None).unwrap();
        let mut writer = temp_store.allocate_stream(None).unwrap();
        writer.write_all(&vec![42u8; 4096]).unwrap();

        let reader = writer.into_read_at().unwrap();
        let ranges = (0u64..8192).step_by(1024).map(|s| s..s + 512);
        let mut prefetcher = PrefetchReadAt::new(reader, Box::new(ranges));

        // First read should come from source
        let bytes = prefetcher.read_at_mut(0..100).unwrap();
        assert_eq!(bytes.as_ref(), &vec![42u8; 100]);
        assert_eq!(prefetcher.count_reads_from_source(), 0);

        // Second read likely from prefetcher since in prefetch range
        let bytes = prefetcher.read_at_mut(0..100).unwrap();
        assert_eq!(bytes.as_ref(), &vec![42u8; 100]);
        assert!(prefetcher.count_reads_from_prefetcher() > 0);
    }

    #[test]
    fn test_sequential_access_pattern() {
        let temp_store = crate::temp_file_store::create_fs_based(1024 * 1024, None).unwrap();
        let mut writer = temp_store.allocate_stream(None).unwrap();
        for i in 0..100 {
            let value = (i % 256) as u8;
            writer.write_all(&vec![value; 1024]).unwrap();
        }

        let reader = writer.into_read_at().unwrap();

        // Create sequential access pattern
        let ranges = (0u64..102400).step_by(1024).map(|s| s..s + 1024);
        let prefetcher = PrefetchReadAt::new(reader, Box::new(ranges.clone()));

        // Read every chunk sequentially
        for (i, range) in ranges.enumerate() {
            let bytes = prefetcher.read_at(range).unwrap();
            let expected_value = (i % 256) as u8;
            assert_eq!(bytes.as_ref(), &vec![expected_value; 1024]);
        }

        // Should have many reads from prefetcher
        assert!(prefetcher.count_reads_from_prefetcher() > 50);
    }

    #[test]
    fn test_random_access_pattern() {
        let temp_store = crate::temp_file_store::create_fs_based(1024 * 1024, None).unwrap();
        let mut writer = temp_store.allocate_stream(None).unwrap();
        writer.write_all(&vec![255u8; 1024 * 100]).unwrap();

        let reader = writer.into_read_at().unwrap();

        // Prefetcher expects sequential access
        let ranges = (0u64..102400).step_by(1024).map(|s| s..s + 1024);
        let prefetcher = PrefetchReadAt::new(reader, Box::new(ranges));

        // But we read in random order
        let mut random_ranges = vec![];
        for i in 0..100 {
            random_ranges.push((i * 7) % 100 * 1024..((i * 7) % 100 + 1) * 1024);
        }

        for range in random_ranges {
            let bytes = prefetcher.read_at(range).unwrap();
            assert_eq!(bytes.as_ref(), &vec![255u8; 1024]);
        }

        // More reads should come from source due to misprediction
        assert!(prefetcher.count_reads_from_source() > prefetcher.count_reads_from_prefetcher());
    }

    #[test]
    fn test_overlapping_reads() {
        let temp_store = crate::temp_file_store::create_fs_based(1024 * 1024, None).unwrap();
        let mut writer = temp_store.allocate_stream(None).unwrap();
        writer.write_all(&vec![123u8; 4096]).unwrap();

        let reader = writer.into_read_at().unwrap();

        // Prefetch large chunks
        let ranges = (0u64..4096).step_by(1024).map(|s| s..s + 1024);
        let prefetcher = PrefetchReadAt::new(reader, Box::new(ranges));

        // Read overlapping smaller ranges
        for i in 0..32 {
            let start = i * 128;
            let bytes = prefetcher.read_at(start..start + 128).unwrap();
            assert_eq!(bytes.as_ref(), &vec![123u8; 128]);
        }

        // Should have more prefetcher hits than source reads
        assert!(prefetcher.count_reads_from_prefetcher() > prefetcher.count_reads_from_source());
    }

    #[test]
    fn test_empty_range_iterator() {
        let temp_store = crate::temp_file_store::create_fs_based(1024 * 1024, None).unwrap();
        let mut writer = temp_store.allocate_stream(None).unwrap();
        writer.write_all(&vec![1u8; 1024]).unwrap();

        let reader = writer.into_read_at().unwrap();

        // Empty range iterator
        let empty_ranges: Vec<Range<u64>> = vec![];
        let prefetcher = PrefetchReadAt::new(reader, Box::new(empty_ranges.into_iter()));

        // Should fall back to direct reads
        let bytes = prefetcher.read_at(0..100).unwrap();
        assert_eq!(bytes.as_ref(), &vec![1u8; 100]);
        assert_eq!(prefetcher.count_reads_from_source(), 1);
        assert_eq!(prefetcher.count_reads_from_prefetcher(), 0);
    }

    #[test]
    fn test_reads_beyond_prefetched_range() {
        let temp_store = crate::temp_file_store::create_fs_based(1024 * 1024, None).unwrap();
        let mut writer = temp_store.allocate_stream(None).unwrap();
        writer.write_all(&vec![42u8; 10240]).unwrap();

        let reader = writer.into_read_at().unwrap();

        // Prefetch only first 5KB
        let ranges = (0u64..5120).step_by(1024).map(|s| s..s + 1024);
        let prefetcher = PrefetchReadAt::new(reader, Box::new(ranges));

        // Reads within prefetched range
        for i in 0..5 {
            prefetcher.read_at(i * 1024..i * 1024 + 512).unwrap();
        }

        // Reads beyond prefetched range
        for i in 5..10 {
            let bytes = prefetcher.read_at(i * 1024..i * 1024 + 512).unwrap();
            assert_eq!(bytes.as_ref(), &vec![42u8; 512]);
        }

        assert!(prefetcher.count_reads_from_source() >= 5);
        assert!(prefetcher.count_reads_from_prefetcher() > 0);
    }
}
