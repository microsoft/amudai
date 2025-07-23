//! Statistics collection for container types (Struct and List).
//!
//! Container statistics track metadata about the container structure itself,
//! such as lengths, null counts, and raw data size.

/// Statistics for struct-like containers (Struct, Map).
///
/// Struct-like containers have simple structural overhead with just null bitmaps.
#[derive(Debug, Clone, PartialEq)]
pub struct StructStats {
    /// Total number of struct containers processed
    pub count: u64,
    /// Number of null struct containers
    pub null_count: u64,
    /// Raw data size in bytes (null bitmap overhead)
    pub raw_data_size: u64,
}

impl StructStats {
    /// Creates a new StructStats for containers with no data.
    pub fn empty() -> Self {
        Self {
            count: 0,
            null_count: 0,
            raw_data_size: 0,
        }
    }
}

/// Statistics for list-like containers (List, FixedSizeList).
///
/// List-like containers have structural overhead including offsets arrays and null bitmaps.
#[derive(Debug, Clone, PartialEq)]
pub struct ListStats {
    /// Total number of list containers processed
    pub count: u64,
    /// Number of null list containers
    pub null_count: u64,
    /// Raw data size in bytes (offsets arrays and null bitmaps)
    pub raw_data_size: u64,
    /// Minimum length among all non-null lists
    pub min_length: Option<u64>,
    /// Maximum length among all non-null lists
    pub max_length: Option<u64>,
    /// Minimum length among all non-empty, non-null lists
    pub min_non_empty_length: Option<u64>,
}

impl ListStats {
    /// Creates a new ListStats for containers with no data.
    pub fn empty() -> Self {
        Self {
            count: 0,
            null_count: 0,
            raw_data_size: 0,
            min_length: None,
            max_length: None,
            min_non_empty_length: None,
        }
    }
}

/// Statistics collector for struct-like container types.
///
/// Collects statistics about struct container structure and null bitmap overhead.
#[derive(Debug, Default)]
pub struct StructStatsCollector {
    count: u64,
    null_count: u64,
}

impl StructStatsCollector {
    /// Creates a new struct statistics collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Processes struct containers with their null bitmap.
    ///
    /// # Arguments
    /// * `count` - Number of struct containers to process
    /// * `null_count` - Number of the nulls
    pub fn process_structs(&mut self, count: usize, null_count: usize) {
        self.count += count as u64;
        self.null_count += null_count as u64;
    }

    /// Processes multiple null struct containers at once for better performance.
    ///
    /// # Arguments
    /// * `count` - Number of null struct containers to process
    pub fn process_nulls(&mut self, count: u64) {
        self.count += count;
        self.null_count += count;
    }

    /// Finalizes the statistics collection and returns the final stats.
    pub fn finalize(self) -> StructStats {
        // Only need a null bitmap when we have mixed null and non-null data
        let raw_data_size =
            if self.count > 0 && self.null_count > 0 && self.null_count != self.count {
                self.count.div_ceil(8)
            } else {
                0
            };

        StructStats {
            count: self.count,
            null_count: self.null_count,
            raw_data_size,
        }
    }
}

/// Statistics collector for list-like container types.
///
/// Collects statistics about list container structure, metadata overhead, and length statistics.
#[derive(Debug, Default)]
pub struct ListStatsCollector {
    count: u64,
    null_count: u64,
    min_length: Option<u64>,
    max_length: Option<u64>,
    min_non_empty_length: Option<u64>,
}

impl ListStatsCollector {
    /// Creates a new list statistics collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Processes list containers with their offsets and null bitmap.
    ///
    /// # Arguments
    /// * `offsets` - Array of offsets for the list containers
    /// * `base_offset` - The base offset to calculate the first list length
    /// * `nulls` - Optional null bitmap indicating which lists are null
    pub fn process_lists(
        &mut self,
        offsets: &dyn arrow_array::Array,
        base_offset: u64,
        nulls: Option<&arrow_buffer::NullBuffer>,
    ) {
        use arrow_array::cast::AsArray;

        let offsets_array = offsets.as_primitive::<arrow_array::types::UInt64Type>();
        let offsets_values = offsets_array.values();

        if offsets_values.is_empty() {
            return;
        }

        // Process lists by calculating lengths on-the-fly
        let mut prev_offset = base_offset;

        for (i, &current_offset) in offsets_values.iter().enumerate() {
            let length = current_offset - prev_offset;
            let is_null = nulls.is_some_and(|null_buffer| null_buffer.is_null(i));

            if is_null {
                self.process_nulls(1);
            } else {
                self.process_list(length);
            }

            prev_offset = current_offset;
        }
    }

    /// Processes a single non-null list container with its length.
    ///
    /// # Arguments
    /// * `length` - Length of the list container
    fn process_list(&mut self, length: u64) {
        self.count += 1;

        // Update min/max length
        self.min_length = Some(match self.min_length {
            None => length,
            Some(current_min) => current_min.min(length),
        });

        self.max_length = Some(match self.max_length {
            None => length,
            Some(current_max) => current_max.max(length),
        });

        // Update min non-empty length
        if length > 0 {
            self.min_non_empty_length = Some(match self.min_non_empty_length {
                None => length,
                Some(current_min) => current_min.min(length),
            });
        }
    }

    /// Processes multiple null list containers at once for better performance.
    ///
    /// # Arguments
    /// * `count` - Number of null list containers to process
    pub fn process_nulls(&mut self, count: u64) {
        self.count += count;
        self.null_count += count;
    }

    /// Finalizes the statistics collection and returns the final stats.
    pub fn finalize(self) -> ListStats {
        let total_offset_bytes = self.count * 8; // 8 bytes per offset

        let raw_data_size = match (self.count, self.null_count) {
            (0, _) => 0,                                      // Empty dataset
            (_, 0) => total_offset_bytes,                     // No nulls
            (count, null_count) if count == null_count => 0,  // All null containers
            _ => total_offset_bytes + self.count.div_ceil(8), // Mixed (offset bytes + null bitmap)
        };

        ListStats {
            count: self.count,
            null_count: self.null_count,
            raw_data_size,
            min_length: self.min_length,
            max_length: self.max_length,
            min_non_empty_length: self.min_non_empty_length,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::UInt64Array;
    use arrow_buffer::NullBuffer;

    /// Helper function to create a UInt64Array for testing offsets
    fn create_offsets_array(offsets: Vec<u64>) -> UInt64Array {
        UInt64Array::from(offsets)
    }

    /// Helper function to create a null buffer from a boolean vector
    fn create_null_buffer(nulls: Vec<bool>) -> NullBuffer {
        NullBuffer::from(nulls)
    }

    #[test]
    fn test_struct_stats_empty() {
        let stats = StructStats::empty();
        assert_eq!(stats.count, 0);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.raw_data_size, 0);
    }

    #[test]
    fn test_list_stats_empty() {
        let stats = ListStats::empty();
        assert_eq!(stats.count, 0);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.raw_data_size, 0);
        assert_eq!(stats.min_length, None);
        assert_eq!(stats.max_length, None);
        assert_eq!(stats.min_non_empty_length, None);
    }

    #[test]
    fn test_struct_stats_collector_empty() {
        let collector = StructStatsCollector::new();
        let stats = collector.finalize();
        assert_eq!(stats.count, 0);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.raw_data_size, 0);
    }

    #[test]
    fn test_struct_stats_collector_no_nulls() {
        let mut collector = StructStatsCollector::new();
        collector.process_structs(5, 0);
        let stats = collector.finalize();

        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.raw_data_size, 0); // No null bitmap needed
    }

    #[test]
    fn test_struct_stats_collector_all_nulls() {
        let mut collector = StructStatsCollector::new();
        collector.process_nulls(3);
        let stats = collector.finalize();

        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 3);
        assert_eq!(stats.raw_data_size, 0); // No null bitmap needed for all nulls
    }

    #[test]
    fn test_struct_stats_collector_mixed_nulls() {
        let mut collector = StructStatsCollector::new();
        collector.process_structs(5, 3);
        let stats = collector.finalize();

        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 3); // Three false values (nulls)
        assert_eq!(stats.raw_data_size, 1); // ceil(5/8) = 1 byte for null bitmap
    }

    #[test]
    fn test_struct_stats_collector_multiple_batches() {
        let mut collector = StructStatsCollector::new();

        // First batch: mixed nulls and non-nulls
        collector.process_structs(3, 2);
        // Second batch: no nulls
        collector.process_structs(2, 0);
        // Third batch: all nulls
        collector.process_nulls(2);

        let stats = collector.finalize();
        assert_eq!(stats.count, 7);
        assert_eq!(stats.null_count, 4); // 2 from first batch + 2 from third batch
        assert_eq!(stats.raw_data_size, 1); // ceil(7/8) = 1 byte for null bitmap
    }

    #[test]
    fn test_list_stats_collector_empty() {
        let collector = ListStatsCollector::new();
        let stats = collector.finalize();

        assert_eq!(stats.count, 0);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.raw_data_size, 0);
        assert_eq!(stats.min_length, None);
        assert_eq!(stats.max_length, None);
        assert_eq!(stats.min_non_empty_length, None);
    }

    #[test]
    fn test_list_stats_collector_simple_lists() {
        let mut collector = ListStatsCollector::new();

        // Offsets: [0, 3, 7, 10] representing lists of lengths [3, 4, 3]
        let offsets = create_offsets_array(vec![3, 7, 10]);
        collector.process_lists(&offsets, 0, None);

        let stats = collector.finalize();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.raw_data_size, 24); // 3 lists * 8 bytes per offset
        assert_eq!(stats.min_length, Some(3));
        assert_eq!(stats.max_length, Some(4));
        assert_eq!(stats.min_non_empty_length, Some(3));
    }

    #[test]
    fn test_list_stats_collector_with_empty_lists() {
        let mut collector = ListStatsCollector::new();

        // Offsets: [0, 0, 5, 5, 8] representing lists of lengths [0, 5, 0, 3]
        let offsets = create_offsets_array(vec![0, 5, 5, 8]);
        collector.process_lists(&offsets, 0, None);

        let stats = collector.finalize();
        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.raw_data_size, 32); // 4 lists * 8 bytes per offset
        assert_eq!(stats.min_length, Some(0));
        assert_eq!(stats.max_length, Some(5));
        assert_eq!(stats.min_non_empty_length, Some(3)); // Excludes empty lists
    }

    #[test]
    fn test_list_stats_collector_with_nulls() {
        let mut collector = ListStatsCollector::new();

        // Offsets: [0, 3, 7, 10] representing potential lists of lengths [3, 4, 3]
        // But second list is null
        let offsets = create_offsets_array(vec![3, 7, 10]);
        let null_buffer = create_null_buffer(vec![true, false, true]); // Middle one is null
        collector.process_lists(&offsets, 0, Some(&null_buffer));

        let stats = collector.finalize();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.raw_data_size, 25); // 24 bytes for offsets + 1 byte for null bitmap
        assert_eq!(stats.min_length, Some(3)); // Only considers non-null lists
        assert_eq!(stats.max_length, Some(3));
        assert_eq!(stats.min_non_empty_length, Some(3));
    }

    #[test]
    fn test_list_stats_collector_all_nulls() {
        let mut collector = ListStatsCollector::new();

        let offsets = create_offsets_array(vec![3, 7, 10]);
        let null_buffer = create_null_buffer(vec![false, false, false]); // All nulls
        collector.process_lists(&offsets, 0, Some(&null_buffer));

        let stats = collector.finalize();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 3);
        assert_eq!(stats.raw_data_size, 0); // All nulls, no data needed
        assert_eq!(stats.min_length, None);
        assert_eq!(stats.max_length, None);
        assert_eq!(stats.min_non_empty_length, None);
    }

    #[test]
    fn test_list_stats_collector_single_element() {
        let mut collector = ListStatsCollector::new();

        // Single list of length 5
        let offsets = create_offsets_array(vec![5]);
        collector.process_lists(&offsets, 0, None);

        let stats = collector.finalize();
        assert_eq!(stats.count, 1);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.raw_data_size, 8); // 1 list * 8 bytes per offset
        assert_eq!(stats.min_length, Some(5));
        assert_eq!(stats.max_length, Some(5));
        assert_eq!(stats.min_non_empty_length, Some(5));
    }

    #[test]
    fn test_list_stats_collector_with_base_offset() {
        let mut collector = ListStatsCollector::new();

        // Offsets: [10, 15, 20] with base_offset=10 representing lists of lengths [5, 5]
        let offsets = create_offsets_array(vec![15, 20]);
        collector.process_lists(&offsets, 10, None);

        let stats = collector.finalize();
        assert_eq!(stats.count, 2);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.raw_data_size, 16); // 2 lists * 8 bytes per offset
        assert_eq!(stats.min_length, Some(5));
        assert_eq!(stats.max_length, Some(5));
        assert_eq!(stats.min_non_empty_length, Some(5));
    }

    #[test]
    fn test_list_stats_collector_large_lengths() {
        let mut collector = ListStatsCollector::new();

        // Test with large list lengths
        let offsets = create_offsets_array(vec![1000, 5000, 10000]);
        collector.process_lists(&offsets, 0, None);

        let stats = collector.finalize();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.raw_data_size, 24); // 3 lists * 8 bytes per offset
        assert_eq!(stats.min_length, Some(1000));
        assert_eq!(stats.max_length, Some(5000));
        assert_eq!(stats.min_non_empty_length, Some(1000));
    }

    #[test]
    fn test_list_stats_collector_multiple_process_calls() {
        let mut collector = ListStatsCollector::new();

        // First batch: [0, 3, 7] -> lengths [3, 4]
        let offsets1 = create_offsets_array(vec![3, 7]);
        collector.process_lists(&offsets1, 0, None);

        // Second batch: [7, 7, 12] -> lengths [0, 5]
        let offsets2 = create_offsets_array(vec![7, 12]);
        collector.process_lists(&offsets2, 7, None);

        let stats = collector.finalize();
        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.raw_data_size, 32); // 4 lists * 8 bytes per offset
        assert_eq!(stats.min_length, Some(0));
        assert_eq!(stats.max_length, Some(5));
        assert_eq!(stats.min_non_empty_length, Some(3));
    }

    #[test]
    fn test_list_stats_collector_mixed_nulls_and_process_nulls() {
        let mut collector = ListStatsCollector::new();

        // First: regular lists with some nulls
        let offsets = create_offsets_array(vec![2, 5]);
        let null_buffer = create_null_buffer(vec![true, false]); // Second is null
        collector.process_lists(&offsets, 0, Some(&null_buffer));

        // Second: direct null processing
        collector.process_nulls(3);

        let stats = collector.finalize();
        assert_eq!(stats.count, 5);
        assert_eq!(stats.null_count, 4); // 1 from first batch + 3 from second
        assert_eq!(stats.raw_data_size, 41); // 40 bytes for offsets (5*8) + 1 byte for null bitmap
        assert_eq!(stats.min_length, Some(2)); // Only from the non-null list
        assert_eq!(stats.max_length, Some(2));
        assert_eq!(stats.min_non_empty_length, Some(2));
    }

    #[test]
    fn test_list_stats_collector_only_empty_lists() {
        let mut collector = ListStatsCollector::new();

        // All lists are empty: [0, 0, 0, 0] -> lengths [0, 0, 0]
        let offsets = create_offsets_array(vec![0, 0, 0]);
        collector.process_lists(&offsets, 0, None);

        let stats = collector.finalize();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.raw_data_size, 24); // 3 lists * 8 bytes per offset
        assert_eq!(stats.min_length, Some(0));
        assert_eq!(stats.max_length, Some(0));
        assert_eq!(stats.min_non_empty_length, None); // No non-empty lists
    }

    #[test]
    fn test_list_stats_collector_edge_case_empty_offsets() {
        let mut collector = ListStatsCollector::new();

        // Empty offsets array
        let offsets = create_offsets_array(vec![]);
        collector.process_lists(&offsets, 0, None);

        let stats = collector.finalize();
        assert_eq!(stats.count, 0);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.raw_data_size, 0);
        assert_eq!(stats.min_length, None);
        assert_eq!(stats.max_length, None);
        assert_eq!(stats.min_non_empty_length, None);
    }

    #[test]
    fn test_struct_stats_collector_large_batch() {
        let mut collector = StructStatsCollector::new();

        // Large batch with mixed nulls
        let nulls: Vec<bool> = (0..1000).map(|i| i % 3 != 0).collect(); // Every 3rd is null
        let null_buffer = create_null_buffer(nulls);
        collector.process_structs(1000, null_buffer.null_count());

        let stats = collector.finalize();
        assert_eq!(stats.count, 1000);
        assert_eq!(stats.null_count, 334); // ~1000/3 nulls
        assert_eq!(stats.raw_data_size, 125); // ceil(1000/8) = 125 bytes for null bitmap
    }

    #[test]
    fn test_list_stats_increasing_offsets_validation() {
        let mut collector = ListStatsCollector::new();

        // Test strictly increasing offsets as required
        let offsets = create_offsets_array(vec![0, 1, 3, 6, 10, 15, 21, 28, 36, 45]);
        collector.process_lists(&offsets, 0, None);

        let stats = collector.finalize();
        assert_eq!(stats.count, 10);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_length, Some(0)); // First list length (0-0)
        assert_eq!(stats.max_length, Some(9)); // Last list length (45-36)
        assert_eq!(stats.min_non_empty_length, Some(1)); // Second list length (1-0)
    }
}
