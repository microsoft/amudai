use anyhow::{Context, Result};
use arrow::util::pretty::print_batches;
use std::sync::Arc;

use amudai_arrow::builder::ArrowReaderBuilder;
use amudai_objectstore::local_store::LocalFsObjectStore;

use crate::commands::file_path_to_object_url;

pub fn run(count: Option<u64>, shard_path: String) -> Result<()> {
    let shard_url = file_path_to_object_url(&shard_path)?;
    let object_store = Arc::new(LocalFsObjectStore::new_unscoped());

    let builder = ArrowReaderBuilder::try_new(shard_url.as_str())
        .with_context(|| format!("Failed to create reader for shard: {:?}", shard_url))?
        .with_object_store(object_store);

    let reader = builder.build().with_context(|| "Failed to build reader")?;

    let mut batches = Vec::new();
    let mut rows_collected = 0;
    let limit = count.unwrap_or(1).try_into().unwrap();

    for batch in reader {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }

        let rows_needed = limit - rows_collected;
        if batch.num_rows() > rows_needed {
            batches.push(batch.slice(0, rows_needed));
            rows_collected += rows_needed;
        } else {
            rows_collected += batch.num_rows();
            batches.push(batch);
        }

        if rows_collected >= limit {
            break;
        }
    }

    if !batches.is_empty() {
        print_batches(&batches)?;
    } else {
        println!("No data found in shard.");
    }

    Ok(())
}
