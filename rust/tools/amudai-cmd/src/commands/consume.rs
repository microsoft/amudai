//! Consume command implementation

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use std::sync::Arc;
use std::time::Instant;

use amudai_arrow::builder::ArrowReaderBuilder;
use amudai_objectstore::local_store::LocalFsObjectStore;

use crate::commands::file_path_to_object_url;
use crate::utils;

/// Run the consume command
pub fn run(count: Option<u64>, iterations: Option<u64>, shard_path: String) -> Result<()> {
    let iterations = iterations.unwrap_or(1);
    let shard_path = file_path_to_object_url(&shard_path)?;
    for _ in 0..iterations {
        run_single(count, shard_path.as_str())?;
    }
    Ok(())
}

pub fn run_single(count: Option<u64>, shard_path: &str) -> Result<()> {
    println!("Consuming shard: {shard_path}");

    // Start timing
    let start_time = Instant::now();

    // Create object store for local filesystem
    let object_store = Arc::new(LocalFsObjectStore::new_unscoped());

    // Create the arrow reader builder
    let mut builder = ArrowReaderBuilder::try_new(shard_path)
        .with_context(|| format!("Failed to create reader for shard: {shard_path}"))?
        .with_object_store(object_store)
        .with_batch_size(1024);

    // If count is specified, limit the position ranges
    if let Some(max_records) = count {
        let ranges = amudai_collections::range_list::RangeList::from_elem(0..max_records);
        builder = builder.with_position_ranges(Some(ranges));
    }

    // Build the reader
    let reader = builder
        .build()
        .with_context(|| "Failed to build shard reader")?;

    // Consume all batches and accumulate data size
    let mut total_data_size = 0u64;
    let mut total_records = 0u64;
    let mut batch_count = 0u32;

    for batch_result in reader {
        let batch = batch_result.with_context(|| "Failed to read batch from shard")?;

        batch_count += 1;
        total_records += batch.num_rows() as u64;

        let data_size = get_batch_data_size(&batch);
        total_data_size += data_size as u64;
    }

    // Stop timing
    let elapsed = start_time.elapsed();

    // Print statistics
    println!("Consumption completed:");
    println!("  Total time: {:.3} seconds", elapsed.as_secs_f64());
    println!("  Total batches: {batch_count}");
    println!("  Total records: {total_records}");
    println!("  Total data size: {}", utils::format_size(total_data_size));

    if elapsed.as_millis() > 0 {
        let throughput_mb_per_sec =
            (total_data_size as f64) / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        println!("  Throughput: {throughput_mb_per_sec:.2} MB/s");

        let records_per_sec = (total_records as f64) / elapsed.as_secs_f64();
        println!("  Records/sec: {records_per_sec:.0}");
    }

    Ok(())
}

fn get_batch_data_size(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
}
