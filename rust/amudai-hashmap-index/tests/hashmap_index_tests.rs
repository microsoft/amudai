use std::sync::Arc;

use amudai_hashmap_index::{
    HashmapIndexBuilder, HashmapIndexBuilderParams, HashmapIndexOptions, IndexedEntry, LookupResult,
};
use amudai_io_impl::temp_file_store;
use amudai_objectstore::null_store::NullObjectStore;
use amudai_shard::tests::shard_store::ShardStore;

fn create_sample_stream(pos_count: u64, key_count: u64) -> Vec<IndexedEntry> {
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

#[test]
fn test_hashmap_index_build() {
    let params = HashmapIndexBuilderParams {
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
    // Builder decides partition count: ensure it's power-of-two and non-zero
    assert!(sealed_index.partitions.len().is_power_of_two());
    assert!(!sealed_index.partitions.is_empty());
    // The following calculation is based on get_buckets_count() function logic.
    let expected_buckets_count = (unique_entries / 2).next_power_of_two().max(2) as u64;
    assert_eq!(
        expected_buckets_count,
        sealed_index
            .partitions
            .iter()
            .map(|p| p.shard.directory.total_record_count)
            .sum::<u64>()
    );
}

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
        .collect::<Vec<u64>>();
    test_positions.sort_unstable();
    entries
        .chunks(10000)
        .for_each(|c| builder.push_entries(c).unwrap());

    // Finalize the index
    let prepared_index = builder.finish().unwrap();
    let sealed_index = prepared_index
        .seal("null:///tmp/test_hashmap_index")
        .unwrap();
    let index_descriptor = sealed_index.into_index_descriptor("hashmap-index");

    // Verify lookup for a specific hash
    let options = HashmapIndexOptions::new(shard_store.object_store.clone());
    let index = options.open_from_descriptor(&index_descriptor).unwrap();
    let result = index.lookup(test_hash, 200001).unwrap();
    match result {
        LookupResult::Found { positions } => {
            let positions = positions.positions().collect::<Vec<_>>();
            assert_eq!(test_positions, positions);
        }
        LookupResult::NotFound => {
            panic!("Expected to find positions for hash {test_hash}");
        }
    }

    // Verify lookup for a hash that does not exist
    let non_existent_hash = 999999;
    if !entries.iter().any(|e| e.hash == non_existent_hash) {
        let result = index.lookup(non_existent_hash, 200001).unwrap();
        match result {
            LookupResult::Found { .. } => {
                panic!("Did not expect to find positions for hash {non_existent_hash}");
            }
            LookupResult::NotFound => {
                // Expected outcome
            }
        }
    }
}
