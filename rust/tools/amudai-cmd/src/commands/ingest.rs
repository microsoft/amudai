//! Ingest command implementation

use anyhow::{Context, Result};
use arrow::csv::ReaderBuilder;
use arrow_schema::Schema;
use std::{fs, sync::Arc};

use amudai_arrow_compat::arrow_to_amudai_schema::FromArrowSchema;
use amudai_format::schema_builder::SchemaBuilder;
use amudai_io_impl::temp_file_store;
use amudai_objectstore::local_store::LocalFsObjectStore;
use amudai_shard::write::shard_builder::{ShardBuilder, ShardBuilderParams};

use crate::{schema_parser, utils};

/// Run the ingest command
pub fn run(
    schema_path: Option<String>,
    schema_string: Option<String>,
    source_files: Vec<String>,
    shard_path: String,
) -> Result<()> {
    println!("Ingesting files into shard: {}", shard_path);

    // Validate input files exist
    for file in &source_files {
        utils::validate_file_exists(file)
            .with_context(|| format!("Invalid source file: {}", file))?;
        println!("  Source file: {}", file);
    }

    // Validate schema file if provided
    if let Some(ref schema) = schema_path {
        utils::validate_file_exists(schema)
            .with_context(|| format!("Invalid schema file: {}", schema))?;
        println!("  Schema file: {}", schema);
    }

    // Handle schema string if provided
    if let Some(ref schema_str) = schema_string {
        println!("  Schema string: {}", schema_str);
    }

    let arrow_schema = if let Some(schema_str) = schema_string {
        // Priority 1: Parse schema from string
        let schema = schema_parser::parse_schema_string(&schema_str)
            .with_context(|| format!("Failed to parse schema string: {}", schema_str))?;
        println!("Schema parsed from string successfully");
        schema
    } else if let Some(schema_path) = schema_path {
        // Priority 2: Load schema from file
        let schema_content = fs::read_to_string(&schema_path)
            .with_context(|| format!("Failed to read schema file: {}", schema_path))?;

        let schema = serde_json::from_str::<Schema>(&schema_content)
            .with_context(|| format!("Failed to deserialize schema from {schema_path}"))?;
        println!("Schema loaded from file successfully");
        schema
    } else {
        // Priority 3: Infer schema from source files
        println!("No schema provided, inferring from source files...");
        super::inferschema::infer_schema(source_files.iter().map(String::as_str))?
    };

    // Filter CSV files and skip others
    let csv_files: Vec<&String> = source_files
        .iter()
        .filter(|file| {
            let is_csv = file.to_lowercase().ends_with(".csv");
            if !is_csv {
                println!("Skipping non-CSV file: {}", file);
            }
            is_csv
        })
        .collect();

    if csv_files.is_empty() {
        println!("No CSV files found to ingest.");
        return Ok(());
    }

    // Convert Arrow schema to Amudai schema
    let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
        .with_context(|| "Failed to convert Arrow schema to Amudai schema")?;

    // Create local filesystem object store (unscoped for full path access)
    let object_store = Arc::new(LocalFsObjectStore::new_unscoped());

    // Create temporary store with 10GB budget
    let temp_store = temp_file_store::create_file_based(10 * 1024 * 1024 * 1024, None)
        .with_context(|| "Failed to create temporary file store")?;

    // Create shard builder
    let mut shard_builder = ShardBuilder::new(ShardBuilderParams {
        schema: shard_schema.into(),
        object_store,
        temp_store,
        encoding_profile: Default::default(),
    })
    .with_context(|| "Failed to create shard builder")?;

    println!("Created shard builder with schema");

    // Process each CSV file
    for file_path in csv_files {
        println!("Processing CSV file: {}", file_path);

        // Create stripe builder for this file
        let mut stripe_builder = shard_builder
            .build_stripe()
            .with_context(|| format!("Failed to create stripe builder for {}", file_path))?;

        // Open and read CSV file
        let file = fs::File::open(file_path)
            .with_context(|| format!("Failed to open CSV file: {}", file_path))?;

        let csv_reader = ReaderBuilder::new(Arc::new(arrow_schema.clone()))
            .with_header(false)
            .build(file)
            .with_context(|| format!("Failed to create CSV reader for {}", file_path))?;

        let mut batch_count = 0;
        for batch_result in csv_reader {
            let batch =
                batch_result.with_context(|| format!("Failed to read batch from {}", file_path))?;

            stripe_builder
                .push_batch(&batch)
                .with_context(|| format!("Failed to push batch to stripe for {}", file_path))?;

            batch_count += 1;
            if batch_count % 100 == 0 {
                println!("  Processed {} batches from {}", batch_count, file_path);
            }
        }

        println!(
            "  Finished processing {} batches from {}",
            batch_count, file_path
        );

        // Finish the stripe and add it to the shard
        let stripe = stripe_builder
            .finish()
            .with_context(|| format!("Failed to finish stripe for {}", file_path))?;

        shard_builder
            .add_stripe(stripe)
            .with_context(|| format!("Failed to add stripe to shard for {}", file_path))?;

        println!("  Added stripe to shard for {}", file_path);
    }

    // Finish and seal the shard
    println!("Finalizing shard...");
    let prepared_shard = shard_builder
        .finish()
        .with_context(|| "Failed to finish shard building")?;

    let sealed_shard = prepared_shard
        .seal(&shard_path)
        .with_context(|| format!("Failed to seal shard at {}", shard_path))?;

    println!("Shard successfully sealed at: {}", shard_path);
    println!("Sealed shard: {:?}", sealed_shard);

    // let mut total_size = 0u64;
    // for file in &source_files {
    //     let metadata = fs::metadata(file)
    //         .with_context(|| format!("Failed to get metadata for file: {}", file))?;
    //     total_size += metadata.len();
    // }

    // println!("Total input size: {}", utils::format_size(total_size));
    // println!("Ingestion completed successfully!");

    Ok(())
}
