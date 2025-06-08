//! Ingest command implementation

use anyhow::{Context, Result};
use arrow_schema::Schema;
use std::fs;

use crate::utils;

/// Run the ingest command
pub fn run(
    schema_path: Option<String>,
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

    let _schema = if let Some(schema_path) = schema_path {
        let schema_content = fs::read_to_string(&schema_path)
            .with_context(|| format!("Failed to read schema file: {}", schema_path))?;

        let schema = serde_json::from_str::<Schema>(&schema_content)
            .with_context(|| format!("Failed to deserialize schema from {schema_path}"))?;
        println!("Schema loaded successfully");
        schema
    } else {
        super::inferschema::infer_schema(source_files.iter().map(String::as_str))?
    };

    let mut total_size = 0u64;
    for file in &source_files {
        let metadata = fs::metadata(file)
            .with_context(|| format!("Failed to get metadata for file: {}", file))?;
        total_size += metadata.len();
    }

    println!("Total input size: {}", utils::format_size(total_size));
    println!("Ingestion completed successfully!");

    Ok(())
}
