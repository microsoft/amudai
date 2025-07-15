use arrow_array::{
    RecordBatch,
    builder::{ArrayBuilder, GenericListBuilder, LargeListBuilder, StructBuilder, UInt64Builder},
};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields, Schema as ArrowSchema,
};
use rayon::prelude::*;
use std::sync::{Arc, Mutex};

use amudai_common::Result;
use amudai_format::{
    defs::shard::IndexDescriptor,
    schema::{BasicType as AmudaiBasicType, SchemaMessage as AmudaiSchema},
    schema_builder::{DataTypeBuilder, SchemaBuilder},
};
use amudai_io::temp_file_store::TemporaryFileStore;
use amudai_objectstore::ObjectStore;
use amudai_shard::write::{
    shard_builder::{PreparedShard, SealedShard, ShardBuilder, ShardBuilderParams},
    stripe_builder::StripeBuilder,
};

use crate::{read::hashmap_index::HASHMAP_INDEX_TYPE, write::aggregator::EntriesAggregator};

/// A single index entry that maps a key hash to a payload location.
///
/// This structure represents one entry in the hashmap index, linking a hashed key to its
/// corresponding payload stored in an external Amudai shard. The payload position serves
/// as a reference that can be used to retrieve the actual data from the shard.
#[derive(Debug, Copy, Clone)]
pub struct IndexedEntry {
    /// The computed hash value of the indexed key.
    pub hash: u64,
    /// The position where the corresponding payload is stored in the shard.
    pub position: u64,
}

/// Configuration parameters for building a hashmap index.
///
/// This structure contains all necessary configuration for creating a new hashmap index,
/// including partitioning settings, storage configuration, and memory management options.
#[derive(Clone)]
pub struct HashmapIndexBuilderParams {
    /// The number of partitions in the index.
    /// If this number is not a power of two, it will be rounded up to the next power of two.
    pub partitions_count: usize,

    /// A hint for memory budget in bytes during index building.
    /// This value is used to configure the number of sorters running in parallel.
    /// By default, this parameter is set to 1GB.
    pub max_memory_usage: Option<usize>,

    /// The persistent object store where the index files will be written.
    ///
    /// # Note
    ///
    /// Wrapped in `Arc` for shared ownership to enable concurrent index building
    /// operations.
    pub object_store: Arc<dyn ObjectStore>,

    /// The temporary file store used for intermediate files during index construction.
    ///
    /// The index builder may create temporary files to store intermediate results
    /// before finalizing the index. This store is used to manage those temporary files.
    ///
    /// # Performance Considerations
    ///
    /// For optimal performance, use fast storage (e.g., in-memory or SSD-based) for
    /// the temporary store, especially when processing large datasets.
    pub temp_store: Arc<dyn TemporaryFileStore>,
}

impl HashmapIndexBuilderParams {
    /// Maximum allowed number of partitions to prevent excessive overhead.
    const MAX_PARTITIONS_COUNT: usize = 1024;

    /// Default memory usage limit for the index builder.
    const DEFAULT_MEM_USAGE_LIMIT: usize = 1024 * 1024 * 1024; // 1 GB

    /// Calculates the effective number of partitions for the index.
    /// The resulted number is rounded to next power of two if necessary
    /// and capped at `MAX_PARTITIONS_COUNT`.
    pub fn partitions_count(&self) -> usize {
        self.partitions_count
            .next_power_of_two()
            .min(Self::MAX_PARTITIONS_COUNT)
    }

    /// Returns the effective maximum memory usage for the index builder.
    /// Default is 1GB, but can be overridden by setting `max_memory_usage`.
    pub fn max_memory_usage(&self) -> usize {
        self.max_memory_usage
            .unwrap_or(Self::DEFAULT_MEM_USAGE_LIMIT)
    }

    pub fn max_memory_usage_per_partition(&self) -> usize {
        self.max_memory_usage() / self.partitions_count()
    }
}

/// A builder for constructing hashmap indexes with efficient parallel processing.
///
/// This builder creates a distributed hashmap index by partitioning entries across multiple
/// shards based on hash values, enabling fast lookups and parallel construction. The resulting
/// index provides O(1) lookup performance for key-to-position mappings.
///
/// # Index Structure
///
/// The hashmap index is organized hierarchically:
///
/// - **Partitions**: Top-level divisions based on hash value prefixes, implemented as Amudai shards,
///   where each shard consists of a single stripe.
/// - **Buckets**: Hash-based groupings within stripe for fast retrieval.
///
/// More formal definition of the index structure:
///
/// ```text
/// type ShardRecord = Bucket;
/// type Bucket = List<Entry>;
/// type Entry = Struct<Hash, PositionsList>;
/// type Hash = UInt64;
/// type PositionsList = List<UInt64>;
/// ```
///
/// # Construction Process
///
/// 1. **Partitioning**: Entries are distributed across partitions using the most significant
///    bits of their hash values, ensuring balanced load distribution.
///
/// 2. **Aggregation**: Within each partition, entries with identical hashes are aggregated,
///    collecting all positions for each unique hash.
///
/// 3. **Bucketing**: Aggregated entries are organized into buckets for efficient range queries
///    within the shard.
///
/// 4. **Serialization**: Data is written to stripes within Amudai shards.
///
/// ## Entry Lookup Process
///
/// To locate a hash within the index, follow these steps:
///
/// 1. **Determine the partition**: Use the most significant bits of the hash to identify
///    which shard contains the target hash.
///    ```text
///    partition_index = get_entry_partition_index(hash, partition_bits)
///    ```
/// 2. **Calculate the bucket index**: Use lower bits of the hash to determine which
///    bucket contains it.
///    ```text
///    bucket_index = get_entry_bucket_index(hash, buckets_count)
///    ```
/// 3. **Read the bucket**: Use logical position `bucket_index..bucket_index + 1` to read
///    the bucket record from the shard.
///
/// 4. **Scan the bucket**: Iterate on bucket entries. Since bucket size is
///    limited by some constant, this scan has O(1) complexity.
pub struct HashmapIndexBuilder {
    /// Number of most significant bits used to determine the partition index
    /// based on the hash value.
    partition_bits: u8,
    partition_builders: Vec<Arc<Mutex<HashmapIndexPartitionBuilder>>>,
}

impl HashmapIndexBuilder {
    /// Creates a new hashmap index builder with the given parameters.
    ///
    /// The builder creates partitions based on [HashmapIndexBuilderParams::effective_partitions_count].
    /// Each partition has its own builder for parallel processing.
    ///
    /// # Errors
    /// Returns an error if the builder cannot be initialized due to storage or schema issues
    pub fn new(params: HashmapIndexBuilderParams) -> Result<Self> {
        let partitions_count = params.partitions_count();
        let partition_bits = partitions_count.trailing_zeros() as u8;

        let shard_params = ShardBuilderParams {
            schema: create_amudai_schema(),
            object_store: Arc::clone(&params.object_store),
            temp_store: Arc::clone(&params.temp_store),
            encoding_profile: Default::default(),
            file_organization: Default::default(),
        };

        let partition_builders = (0..partitions_count)
            .map(|partition_index| {
                let partition_builder = Arc::new(Mutex::new(HashmapIndexPartitionBuilder::new(
                    partition_index,
                    params.clone(),
                    shard_params.clone(),
                )?));
                Ok(partition_builder)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(HashmapIndexBuilder {
            partition_builders,
            partition_bits,
        })
    }

    /// Add entries to the index for processing.
    ///
    /// Distributes entries across partitions based on hash values and processes
    /// them in parallel for optimal performance.
    ///
    /// # Errors
    /// Returns an error if any partition fails to process its entries
    pub fn push_entries(&mut self, entries: &[IndexedEntry]) -> Result<()> {
        let mut partitioned_entries = vec![vec![]; self.partition_builders.len()];
        for &entry in entries {
            let partition_index = get_entry_partition_index(entry.hash, self.partition_bits);
            partitioned_entries[partition_index].push(entry);
        }

        partitioned_entries
            .into_par_iter()
            .enumerate()
            .filter(|(_, entries)| !entries.is_empty())
            .try_for_each(|(partition_index, entries)| {
                let mut partition_builder =
                    self.partition_builders[partition_index].lock().unwrap();
                partition_builder.push_entries(entries.as_slice())
            })?;
        Ok(())
    }

    /// Finalizes the index building process and returns a prepared index.
    ///
    /// Processes all accumulated entries across partitions in parallel.
    ///
    /// # Errors
    /// Returns an error if any partition fails to finish or during aggregation
    pub fn finish(self) -> Result<PreparedHashmapIndex> {
        let partitions = self
            .partition_builders
            .into_par_iter()
            .map(|builder| {
                Arc::try_unwrap(builder)
                    .unwrap_or_else(|_| panic!("Expected single reference to partition builder"))
                    .into_inner()
                    .unwrap()
                    .finish()
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(PreparedHashmapIndex { partitions })
    }
}

/// Builder for constructing a single partition of a hashmap index.
///
/// Aggregates entries within a partition and prepares them for storage in an Amudai shard.
/// Uses disk-based aggregation to handle large datasets efficiently.
struct HashmapIndexPartitionBuilder {
    partition_index: usize,
    shard_params: ShardBuilderParams,
    aggregator: EntriesAggregator,
}

impl HashmapIndexPartitionBuilder {
    /// Creates a new partition builder for the specified partition index.
    ///
    /// # Errors
    /// Returns an error if the aggregator cannot be initialized, typically due to
    /// storage issues when using disk-based aggregation
    pub fn new(
        partition_index: usize,
        params: HashmapIndexBuilderParams,
        shard_params: ShardBuilderParams,
    ) -> Result<Self> {
        let aggregator = EntriesAggregator::new(
            Arc::clone(&params.temp_store),
            params.max_memory_usage_per_partition(),
        )?;
        Ok(HashmapIndexPartitionBuilder {
            partition_index,
            shard_params,
            aggregator,
        })
    }

    /// Adds a batch of indexed entries to this partition.
    ///
    /// Entries are processed and aggregated by hash value, with identical hashes
    /// having their positions collected together for efficient storage.
    ///
    /// # Errors
    /// Returns an error if the underlying aggregator fails, typically due to
    /// storage issues in disk-based aggregation
    pub fn push_entries(&mut self, entries: &[IndexedEntry]) -> Result<()> {
        for entry in entries {
            self.aggregator.push_entry(entry.hash, entry.position)?;
        }
        Ok(())
    }

    /// Finalizes the partition building process and prepares it for sealing.
    ///
    /// This method:
    /// 1. Completes aggregation and calculates optimal bucket configuration
    /// 2. Sorts entries by bucket index for efficient storage layout
    /// 3. Builds the shard structure with proper bucketing
    ///
    /// # Returns
    /// A `PreparedHashmapIndexPartition` containing the organized data and metadata
    ///
    /// # Errors
    /// Returns an error if aggregation, bucket organization, or shard building fails
    pub fn finish(mut self) -> Result<PreparedHashmapIndexPartition> {
        let entries_count = self.aggregator.finish()?;
        let buckets_count = get_buckets_count(entries_count);

        let mut sorted_entries = self.aggregator.sorted_entries(buckets_count)?;

        let mut shard_builder =
            HashmapIndexShardBuilder::new(self.partition_index, self.shard_params, buckets_count)?;

        while let Some((hash, positions)) = sorted_entries.next_entry()? {
            let bucket_idx = get_entry_bucket_index(hash, buckets_count);
            shard_builder.push_entry(bucket_idx, hash, &positions)?;
        }
        shard_builder.finish()
    }
}

/// Builder for creating a single Amudai shard representing one partition of the hashmap index.
///
/// Manages conversion of aggregated hash-to-positions mappings into Amudai's columnar
/// storage format organized in buckets into a single stripe.
struct HashmapIndexShardBuilder {
    partition_index: usize,
    shard: ShardBuilder,
    stripe: StripeBuilder,
    collector: RecordsCollector,
}

impl HashmapIndexShardBuilder {
    /// Creates a new shard builder for a single partition.
    ///
    /// # Parameters
    /// * `partition_index` - The index of the partition this shard belongs to
    /// * `shard_params` - Configuration parameters for the underlying shard
    /// * `buckets_count` - The total number of buckets in the partition
    ///
    /// # Errors
    /// Returns an error if the underlying shard or stripe builder cannot be initialized.
    pub fn new(
        partition_index: usize,
        shard_params: ShardBuilderParams,
        buckets_count: usize,
    ) -> Result<Self> {
        let collector = RecordsCollector::new(buckets_count)?;
        let shard = ShardBuilder::new(shard_params)?;
        let stripe = shard.build_stripe()?;
        Ok(HashmapIndexShardBuilder {
            partition_index,
            shard,
            stripe,
            collector,
        })
    }

    /// Adds a hash-to-positions entry to the shard.
    ///
    /// Entries must be provided in bucket index order for optimal performance.
    /// This method handles bucket padding, batch flushing, and stripe management automatically.
    ///
    /// # Parameters
    /// * `bucket_idx` - The index of the bucket where this hash belongs.
    /// * `hash` - The hash value of the indexed key.
    /// * `positions` - A slice containing all positions where this hash appears in the payload shard.
    ///
    /// # Errors
    /// Returns an error if batching or flushing operations fail.
    pub fn push_entry(&mut self, bucket_idx: usize, hash: u64, positions: &[u64]) -> Result<()> {
        if let Some(batch) = self.collector.add_entry(bucket_idx, hash, positions)? {
            self.stripe.push_batch(&batch)?;
        }
        Ok(())
    }

    /// Finalizes the shard building process and returns the completed partition.
    ///
    /// This method:
    /// 1. Pads any remaining incomplete buckets to maintain consistent sizing
    /// 2. Flushes the final batch of collected records
    /// 3. Seals the stripe and adds it to the shard
    /// 4. Completes the shard building process
    ///
    /// # Errors
    /// Returns an error if padding, flushing, stripe finalization, or shard building fails
    pub fn finish(mut self) -> Result<PreparedHashmapIndexPartition> {
        if let Some(batch) = self.collector.finish()? {
            self.stripe.push_batch(&batch)?;
        }
        self.shard.add_stripe(self.stripe.finish()?)?;

        let prepared_shard = self.shard.finish()?;
        Ok(PreparedHashmapIndexPartition {
            index: self.partition_index,
            shard: prepared_shard,
        })
    }
}

/// A utility for constructing record batches in Arrow format for Amudai shard storage.
///
/// Collects hash-position pairs and null padding entries, then converts them into
/// Arrow `RecordBatch` objects that can be written to Amudai stripes.
struct RecordsCollector {
    schema: Arc<ArrowSchema>,
    buckets: GenericListBuilder<i64, StructBuilder>,
    buckets_count: usize,
    current_bucket: usize,
}

impl RecordsCollector {
    /// The batch size for in-memory aggregation before writing to stripe.
    const MAX_BATCH_LEN: usize = 128;

    /// Creates a new collector with the specified Arrow schema.
    fn new(buckets_count: usize) -> Result<Self> {
        let struct_fields = vec![
            ArrowField::new("hash", ArrowDataType::UInt64, false),
            ArrowField::new(
                "positions",
                ArrowDataType::LargeList(Arc::new(ArrowField::new(
                    "item",
                    ArrowDataType::UInt64,
                    false,
                ))),
                false,
            ),
        ];
        let struct_item_field = Arc::new(ArrowField::new(
            "item",
            ArrowDataType::Struct(ArrowFields::from(struct_fields.clone())),
            false,
        ));
        let schema = ArrowSchema::new(ArrowFields::from(vec![ArrowField::new(
            "bucket",
            ArrowDataType::LargeList(Arc::clone(&struct_item_field)),
            false,
        )]));

        let buckets =
            LargeListBuilder::new(StructBuilder::from_fields(struct_fields, buckets_count))
                .with_field(struct_item_field);

        Ok(RecordsCollector {
            schema: Arc::new(schema),
            buckets,
            buckets_count,
            current_bucket: 0,
        })
    }

    /// Adds a hash-position entry to the collector.
    /// If the collector reaches the maximum batch size, it returns the collected batch
    /// for further processing.
    fn add_entry(
        &mut self,
        bucket_idx: usize,
        hash: u64,
        positions: &[u64],
    ) -> Result<Option<RecordBatch>> {
        self.complete_buckets(bucket_idx);

        let mut batch = None;
        if self.buckets.len() >= Self::MAX_BATCH_LEN {
            batch = Some(self.get_batch()?);
        }

        // The following magic adds a new entry to the Arrow record builder.

        // Get the reference to the current bucket.
        let items = self.buckets.values();

        // Populate entry fields: hash and position lists.
        items
            .field_builder::<UInt64Builder>(0)
            .expect("hash builder")
            .append_value(hash);
        let positions_list = items
            .field_builder::<LargeListBuilder<Box<dyn ArrayBuilder>>>(1)
            .expect("positions list builder");
        let positions_array = positions_list
            .values()
            .as_any_mut()
            .downcast_mut::<UInt64Builder>()
            .expect("positions builder");
        positions_array.append_slice(positions);
        positions_list.append(true);

        // Add the item to the bucket.
        items.append(true);

        Ok(batch)
    }

    /// Completes current and all subsequent buckets up to the specified index.
    /// Empty buckets are represented as empty lists in the Arrow record batch.
    #[inline]
    fn complete_buckets(&mut self, next_bucket: usize) {
        if next_bucket != self.current_bucket {
            assert!(next_bucket > self.current_bucket);
            while self.current_bucket < next_bucket {
                self.buckets.append(true);
                self.current_bucket += 1;
            }
        }
    }

    /// Creates a new Arrow record batch from the collected data and resets the collector.
    fn get_batch(&mut self) -> Result<RecordBatch> {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(self.buckets.finish())],
        )
        .map_err(|e| amudai_common::error::Error::arrow("new record batch", e))
    }

    /// Finalizes the collector, ensuring all buckets are completed and returns the final batch.
    /// If no records were collected in the last batch, returns `None`.
    fn finish(mut self) -> Result<Option<RecordBatch>> {
        self.complete_buckets(self.buckets_count);
        if self.buckets.len() == 0 {
            Ok(None)
        } else {
            Ok(Some(self.get_batch()?))
        }
    }
}

/// A prepared index ready to be sealed and written to storage.
pub struct PreparedHashmapIndex {
    /// The individual partitions that make up this index.
    pub partitions: Vec<PreparedHashmapIndexPartition>,
}

/// A single partition within a prepared hashmap index.
pub struct PreparedHashmapIndexPartition {
    /// Index of the partition within the overall index.
    pub index: usize,
    /// The prepared shard containing the indexed data for this partition.
    pub shard: PreparedShard,
}

impl PreparedHashmapIndexPartition {
    /// Seals the partition, finalizing its storage.
    ///
    /// Writes the partition's shard to its final storage location at
    /// `{index_url}/part{index}.shard`.
    ///
    /// # Errors
    /// Returns an error if sealing fails or storage issues occur
    pub fn seal(self, index_url: &str) -> Result<SealedHashmapIndexPartition> {
        let partition_url = format!("{}/part{}.shard", index_url, self.index);
        let shard = self.shard.seal(&partition_url)?;
        Ok(SealedHashmapIndexPartition {
            index: self.index as u64,
            shard,
        })
    }
}

/// A sealed index that has been written to persistent storage and is ready for querying.
#[derive(Debug, Clone)]
pub struct SealedHashmapIndex {
    /// The individual sealed partitions that make up this index.
    pub partitions: Vec<SealedHashmapIndexPartition>,
}

impl SealedHashmapIndex {
    /// Converts the sealed index into an Amudai `IndexDescriptor` for metadata storage.
    pub fn into_index_descriptor(self) -> IndexDescriptor {
        let artifacts = self
            .partitions
            .iter()
            .map(|partition| partition.shard.directory_blob.clone())
            .collect::<Vec<_>>();
        IndexDescriptor {
            index_type: HASHMAP_INDEX_TYPE.to_owned(),
            artifacts,
            ..Default::default()
        }
    }
}

impl PreparedHashmapIndex {
    /// Seals the index, finalizing its storage.
    ///
    /// Writes all partitions to their final storage locations as separate shard files
    /// with the naming pattern `part{N}.shard` (e.g., `part0.shard`, `part1.shard`).
    ///
    /// # Errors
    /// Returns an error if any partition fails to seal or storage issues occur
    pub fn seal(self, index_url: &str) -> Result<SealedHashmapIndex> {
        let partitions = self
            .partitions
            .into_par_iter()
            .map(|partition| partition.seal(index_url))
            .collect::<Result<Vec<_>>>()?;
        Ok(SealedHashmapIndex { partitions })
    }
}

/// A sealed partition within a hashmap index, stored as a finalized shard file.
#[derive(Debug, Clone)]
pub struct SealedHashmapIndexPartition {
    /// Index of the partition within the overall index.
    pub index: u64,
    /// The sealed shard containing the indexed data for this partition.
    pub shard: SealedShard,
}

/// Creates the Amudai schema for the hashmap index data structure.
///
/// The schema represents buckets containing hash-position entries organized as:
/// bucket: List<entry: Struct<hash: Int64, positions: List<Int64>>>
///
fn create_amudai_schema() -> AmudaiSchema {
    let schema = SchemaBuilder::new(vec![
        DataTypeBuilder::new(
            "bucket",
            AmudaiBasicType::List,
            false,
            None,
            vec![DataTypeBuilder::new(
                "item",
                AmudaiBasicType::Struct,
                false,
                None,
                vec![
                    DataTypeBuilder::new("hash", AmudaiBasicType::Int64, false, None, vec![]),
                    DataTypeBuilder::new(
                        "positions",
                        AmudaiBasicType::List,
                        false,
                        None,
                        vec![DataTypeBuilder::new(
                            "item",
                            AmudaiBasicType::Int64,
                            false,
                            None,
                            vec![],
                        )],
                    ),
                ],
            )],
        )
        .into(),
    ]);
    schema.finish_and_seal()
}

/// Calculates the partition index for a given hash value.
///
/// Uses the most significant bits of the hash to determine partition placement,
/// enabling balanced distribution across partitions.
///
/// # Parameters
/// * `hash` - The hash value to determine the partition for
/// * `partition_bits` - The number of most significant bits to use for partitioning
#[inline]
pub fn get_entry_partition_index(hash: u64, partition_bits: u8) -> usize {
    if partition_bits == 0 {
        return 0;
    }
    (hash >> (64 - partition_bits)) as usize
}

/// Calculates the bucket index for a given hash value within a partition.
///
/// Uses the lower bits of the hash to determine bucket placement within a partition
/// for efficient retrieval. Buckets count must be a power of 2 for this to work correctly.
///
/// # Parameters
/// * `hash` - The hash value to determine the bucket for
/// * `buckets_count` - The total number of buckets in the partition (must be power of 2)
#[inline]
pub fn get_entry_bucket_index(hash: u64, buckets_count: usize) -> usize {
    (hash & (buckets_count as u64 - 1)) as usize
}

/// Calculates the optimal number of buckets for a given number of unique entries.
fn get_buckets_count(entries_count: usize) -> usize {
    (entries_count / 2).next_power_of_two().max(2)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use amudai_io_impl::temp_file_store;
    use amudai_objectstore::null_store::NullObjectStore;

    #[test]
    fn test_hashmap_index_build() {
        let params = HashmapIndexBuilderParams {
            partitions_count: 8,
            max_memory_usage: Some(32 * 1024 * 1024),
            object_store: Arc::new(NullObjectStore),
            temp_store: temp_file_store::create_in_memory(32 * 1024 * 1024).unwrap(),
        };
        let mut builder = HashmapIndexBuilder::new(params.clone()).unwrap();

        let entries = create_sample_stream(200000, 100000);
        let unique_entries = entries
            .iter()
            .map(|e| e.hash)
            .collect::<std::collections::HashSet<_>>()
            .len();
        entries
            .chunks(50000)
            .for_each(|c| builder.push_entries(c).unwrap());

        let prepared_index = builder.finish().unwrap();
        let sealed_index = prepared_index
            .seal("null:///tmp/test_hashmap_index")
            .unwrap();

        assert_eq!(params.partitions_count, sealed_index.partitions.len());
        assert_eq!(
            get_buckets_count(unique_entries) as u64,
            sealed_index
                .partitions
                .iter()
                .map(|p| p.shard.directory.total_record_count)
                .sum::<u64>()
        );
    }

    pub fn create_sample_stream(pos_count: u64, key_count: u64) -> Vec<IndexedEntry> {
        let mut v = Vec::new();
        fastrand::seed(2985745485);
        let keys: Vec<_> = (0..key_count).map(|_| fastrand::u64(..)).collect();
        for pos in 0..pos_count {
            for _ in 0..fastrand::usize(1..4) {
                let key = keys[fastrand::usize(0..keys.len())];
                v.push(IndexedEntry {
                    hash: key,
                    position: pos,
                });
            }
        }
        v
    }
}
