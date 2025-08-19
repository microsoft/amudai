use std::sync::Arc;

use amudai_arrow::builder::ArrowReaderBuilder;
use amudai_common::Result;
use amudai_format::defs::shard::IndexDescriptor;
use amudai_objectstore::{ObjectStore, ReferenceResolver, url::ObjectUrl};
use amudai_ranges::SharedRangeList;
use amudai_shard::read::{shard::Shard, shard::ShardOptions};
use arrow_array::{Array, LargeListArray, StructArray, UInt64Array};

use crate::write::builder::{get_entry_bucket_index, get_entry_partition_index};

/// The type identifier for the hashmap index in Amudai.
pub const HASHMAP_INDEX_TYPE: &str = "hashmap-index";

/// A reader interface for hashmap indexes stored in Amudai shards.
///
/// HashmapIndex provides fast O(1) lookup operations for hash-to-positions mappings
/// by leveraging the partitioned structure created by HashmapIndexBuilder.
///
/// # Index Structure
///
/// The index is distributed across multiple partitions (shards), where:
/// - Each partition contains buckets organized as a single stripe
/// - Buckets contain hash-position entries for efficient lookup
/// - Partition selection uses high-order bits of the hash
/// - Bucket selection uses low-order bits of the hash
///
/// # Usage
///
/// ```rust,ignore
/// let index = HashmapIndex::open_from_descriptor(
///     object_store,
///     &index_descriptor,
///     None
/// )?;
///
/// match index.lookup(hash_value)? {
///     LookupResult::Found { positions } => {
///         // Process the found positions
///         println!("Found {} positions", positions.len());
///     }
///     LookupResult::NotFound => {
///         println!("Hash not found in index");
///     }
/// }
/// ```
pub struct HashmapIndex {
    /// The partition shards that make up this index
    partitions: Vec<HashmapIndexPartition>,
    /// Number of bits used for partition selection
    partition_bits: u8,
    /// The object store used for reading data
    object_store: Arc<dyn ObjectStore>,
    /// Optional reference resolver for resolving shard references.
    reference_resolver: Option<Arc<dyn ReferenceResolver>>,
}

/// A single partition within a hashmap index
struct HashmapIndexPartition {
    /// The shard containing this partition's data
    shard: Shard,
    /// The number of buckets in this partition
    buckets_count: usize,
}

/// Options for opening a hashmap index
pub struct HashmapIndexOptions {
    object_store: Arc<dyn ObjectStore>,
    reference_resolver: Option<Arc<dyn ReferenceResolver>>,
}

/// Result of a hash lookup operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupResult {
    /// The hash was found and these are the associated positions
    Found { positions: Vec<u64> },
    /// The hash was not found in the index
    NotFound,
}

impl HashmapIndexOptions {
    /// Creates new options with the specified object store.
    ///
    /// # Arguments
    /// * `object_store` - The object store to use for reading index data
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            reference_resolver: None,
        }
    }

    /// Sets the reference resolver for resolving shard references.
    ///
    /// # Arguments
    /// * `reference_resolver` - The resolver to use for shard references
    pub fn reference_resolver(mut self, reference_resolver: Arc<dyn ReferenceResolver>) -> Self {
        self.reference_resolver = Some(reference_resolver);
        self
    }

    /// Opens a hashmap index from an IndexDescriptor.
    ///
    /// # Arguments
    /// * `descriptor` - The index descriptor containing partition artifact URLs
    ///
    /// # Errors
    /// Returns an error if any partition shard cannot be opened or if the descriptor is invalid
    pub fn open_from_descriptor(self, descriptor: &IndexDescriptor) -> Result<HashmapIndex> {
        if descriptor.index_type != HASHMAP_INDEX_TYPE {
            return Err(amudai_common::error::Error::invalid_format(format!(
                "Expected hashmap-index type, got: {}",
                descriptor.index_type
            )));
        }

        let partitions_count = descriptor.artifacts.len();
        if !partitions_count.is_power_of_two() {
            return Err(amudai_common::error::Error::invalid_format(format!(
                "Partition count must be power of 2, got: {partitions_count}"
            )));
        }

        let partition_bits = partitions_count.trailing_zeros() as u8;

        let mut partitions = Vec::with_capacity(partitions_count);
        for artifact in &descriptor.artifacts {
            let data_ref = artifact.data_ref.as_ref().ok_or_else(|| {
                amudai_common::error::Error::invalid_format("index artifact data_ref")
            })?;
            let shard_url = ObjectUrl::parse(&data_ref.url)?;
            let mut shard_options = ShardOptions::new(self.object_store.clone());
            if let Some(ref resolver) = self.reference_resolver {
                shard_options = shard_options.reference_resolver(resolver.clone());
            }
            let shard = shard_options.open(shard_url)?;
            let buckets_count = shard.directory().total_record_count as usize;

            partitions.push(HashmapIndexPartition {
                shard,
                buckets_count,
            });
        }

        Ok(HashmapIndex {
            partitions,
            partition_bits,
            object_store: self.object_store,
            reference_resolver: self.reference_resolver,
        })
    }
}

impl HashmapIndex {
    /// Looks up positions for a given hash value.
    ///
    /// # Arguments
    /// * `hash` - The hash value to look up
    ///
    /// # Returns
    /// `LookupResult::Found` containing the positions associated with the hash,
    /// or `LookupResult::NotFound` if the hash is not found.
    ///
    /// # Errors
    /// Returns an error if the lookup operation fails due to I/O or parsing issues
    pub fn lookup(&self, hash: u64) -> Result<LookupResult> {
        // Determine which partition contains this hash
        let partition_index = get_entry_partition_index(hash, self.partition_bits);
        let partition = &self.partitions[partition_index];

        // Determine which bucket within the partition contains this hash
        let bucket_index = get_entry_bucket_index(hash, partition.buckets_count);

        // Read the specific bucket from the shard using amudai-arrow
        let position_ranges =
            SharedRangeList::from_elem(bucket_index as u64..(bucket_index as u64 + 1));

        // Use the existing amudai-arrow infrastructure to read the data
        let mut reader_builder = ArrowReaderBuilder::try_new(partition.shard.url().as_str())
            .map_err(|e| {
                amudai_common::error::Error::invalid_operation(format!(
                    "Failed to create reader: {e}"
                ))
            })?
            .with_object_store(self.object_store.clone())
            .with_batch_size(1)
            .with_position_ranges(position_ranges);
        if let Some(resolver) = &self.reference_resolver {
            reader_builder = reader_builder.with_reference_resolver(resolver.clone());
        }

        let mut reader = reader_builder.build().map_err(|e| {
            amudai_common::error::Error::invalid_operation(format!("Failed to build reader: {e}"))
        })?;

        if let Some(batch) = reader.next().transpose().map_err(|e| {
            amudai_common::error::Error::invalid_operation(format!("Failed to read batch: {e}"))
        })? {
            return self.extract_positions_from_batch(&batch, hash);
        }

        // Hash not found
        Ok(LookupResult::NotFound)
    }

    /// Extracts positions for a specific hash from an Arrow RecordBatch.
    fn extract_positions_from_batch(
        &self,
        batch: &arrow_array::RecordBatch,
        target_hash: u64,
    ) -> Result<LookupResult> {
        let bucket_column = batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeListArray>()
            .ok_or_else(|| {
                amudai_common::error::Error::invalid_format("Expected LargeListArray for bucket")
            })?;

        // The bucket should contain exactly one list (our target bucket)
        if bucket_column.len() != 1 {
            return Err(amudai_common::error::Error::invalid_format(
                "Expected exactly one bucket",
            ));
        }

        let bucket_entries = bucket_column.value(0);
        let entries_array = bucket_entries
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                amudai_common::error::Error::invalid_format("Expected StructArray for entries")
            })?;

        let hash_column = entries_array
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                amudai_common::error::Error::invalid_format("Expected UInt64Array for hashes")
            })?;

        let positions_column = entries_array
            .column(1)
            .as_any()
            .downcast_ref::<LargeListArray>()
            .ok_or_else(|| {
                amudai_common::error::Error::invalid_format("Expected LargeListArray for positions")
            })?;

        // Search through the entries in this bucket for our target hash
        for i in 0..entries_array.len() {
            if hash_column.value(i) == target_hash {
                let positions_list = positions_column.value(i);
                let positions_array = positions_list
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        amudai_common::error::Error::invalid_format(
                            "Expected UInt64Array for position values",
                        )
                    })?;

                let positions = positions_array.values().to_vec();
                return Ok(LookupResult::Found { positions });
            }
        }

        Ok(LookupResult::NotFound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::builder::{
        HashmapIndexBuilder, HashmapIndexBuilderParams, tests::create_sample_stream,
    };
    use amudai_io_impl::temp_file_store;
    use amudai_shard::tests::shard_store::ShardStore;

    #[test]
    fn test_hashmap_index_lookup() {
        let shard_store = ShardStore::new();
        let params = HashmapIndexBuilderParams {
            max_memory_usage: Some(32 * 1024 * 1024),
            object_store: Arc::clone(&shard_store.object_store),
            temp_store: temp_file_store::create_in_memory(32 * 1024 * 1024).unwrap(),
        };
        let mut builder = HashmapIndexBuilder::new(params.clone()).unwrap();

        let entries = create_sample_stream(200000, 100000);
        let test_hash = entries[0].hash;
        let mut test_positions = entries
            .iter()
            .filter(|e| e.hash == test_hash)
            .map(|e| e.position)
            .collect::<Vec<_>>();
        test_positions.sort_unstable();
        entries
            .chunks(10000)
            .for_each(|c| builder.push_entries(c).unwrap());

        // Finalize the index
        let prepared_index = builder.finish().unwrap();
        let sealed_index = prepared_index
            .seal("null:///tmp/test_hashmap_index")
            .unwrap();
        let index_descriptor = sealed_index.into_index_descriptor();

        // Verify lookup for a specific hash
        let options = HashmapIndexOptions::new(shard_store.object_store.clone());
        let index = options.open_from_descriptor(&index_descriptor).unwrap();
        let result = index.lookup(test_hash).unwrap();
        match result {
            LookupResult::Found { mut positions } => {
                positions.sort_unstable();
                assert_eq!(positions, test_positions);
            }
            LookupResult::NotFound => {
                panic!("Expected to find positions for hash {test_hash}");
            }
        }

        // Verify lookup for a hash that does not exist
        let non_existent_hash = 999999;
        if !entries.iter().any(|e| e.hash == non_existent_hash) {
            let result = index.lookup(non_existent_hash).unwrap();
            assert_eq!(result, LookupResult::NotFound);
        }
    }
}
