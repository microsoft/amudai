//! Ingest command implementation

use anyhow::{Context, Result};
use arrow::{
    array::{Array, FixedSizeBinaryArray, NullBufferBuilder, RecordBatch, StringArray},
    buffer::Buffer,
    csv::ReaderBuilder,
};
use arrow_json::ReaderBuilder as JsonReaderBuilder;
use arrow_schema::{DataType, Field, Schema};
use serde_json;
use std::{
    fs,
    io::{BufRead, BufReader},
    sync::Arc,
};

use amudai_arrow_compat::arrow_to_amudai_schema::FromArrowSchema;
use amudai_format::schema_builder::SchemaBuilder;
use amudai_io_impl::temp_file_store;
use amudai_objectstore::local_store::LocalFsObjectStore;
use amudai_shard::write::shard_builder::{self, ShardBuilder, ShardBuilderParams};

use crate::{ShardFileOrganization, commands::file_path_to_object_url, schema_parser, utils};

fn convert_file_organization_param(
    value: ShardFileOrganization,
) -> shard_builder::ShardFileOrganization {
    match value {
        ShardFileOrganization::Single => shard_builder::ShardFileOrganization::SingleFile,
        ShardFileOrganization::Twolevel => shard_builder::ShardFileOrganization::TwoLevel,
    }
}

/// Run the ingest command
pub fn run(
    schema_path: Option<String>,
    schema_string: Option<String>,
    source_files: Vec<String>,
    shard_file_organization: ShardFileOrganization,
    shard_path: String,
) -> Result<()> {
    let shard_url = file_path_to_object_url(&shard_path)?;
    println!("Ingesting files into shard: {}", shard_url.as_str());

    // Validate all inputs
    validate_inputs(&source_files, &schema_path, &schema_string)?; // Establish the schema to use
    let arrow_schema = establish_schema(schema_string, schema_path, &source_files)?;

    // Convert Arrow schema to Amudai schema
    let shard_schema = SchemaBuilder::from_arrow_schema(&arrow_schema)
        .with_context(|| "Failed to convert Arrow schema to Amudai schema")?;

    // Create local filesystem object store (unscoped for full path access)
    let object_store = Arc::new(LocalFsObjectStore::new_unscoped());

    // Create temporary store with 10GB budget
    let temp_store = temp_file_store::create_file_based_cached(10 * 1024 * 1024 * 1024, None)
        .with_context(|| "Failed to create temporary file store")?;

    // Create shard builder
    let mut shard_builder = ShardBuilder::new(ShardBuilderParams {
        schema: shard_schema.into(),
        object_store,
        temp_store,
        encoding_profile: Default::default(),
        file_organization: convert_file_organization_param(shard_file_organization),
    })
    .with_context(|| "Failed to create shard builder")?;

    println!("Created shard builder with schema");

    // Process all files
    let mut processed_count = 0;
    for file_path in &source_files {
        let file_extension = file_path.to_lowercase();
        if file_extension.ends_with(".csv") {
            ingest_csv_file(file_path, &arrow_schema, &mut shard_builder)?;
            processed_count += 1;
        } else if file_extension.ends_with(".json")
            || file_extension.ends_with(".mdjson")
            || file_extension.ends_with(".ndjson")
            || file_extension.ends_with(".jsonl")
        {
            ingest_json_file(file_path, &arrow_schema, &mut shard_builder)?;
            processed_count += 1;
        } else {
            println!(
                "Skipping unsupported file: {} (only .csv, .json, .mdjson, .ndjson, .jsonl are supported)",
                file_path
            );
        }
    }

    if processed_count == 0 {
        println!("No supported files found to ingest.");
        return Ok(());
    }

    // Finish and seal the shard
    println!("Finalizing shard...");
    let prepared_shard = shard_builder
        .finish()
        .with_context(|| "Failed to finish shard building")?;

    let sealed_shard = prepared_shard
        .seal(shard_url.as_str())
        .with_context(|| format!("Failed to seal shard at {}", shard_url.as_str()))?;

    println!(
        "Shard successfully sealed at: {}",
        sealed_shard.directory_blob.url
    );
    Ok(())
}

/// Ingest a single CSV file into the shard builder
fn ingest_csv_file(
    file_path: &str,
    arrow_schema: &Schema,
    shard_builder: &mut ShardBuilder,
) -> Result<()> {
    println!("Processing CSV file: {}", file_path);

    // Create stripe builder for this file
    let mut stripe_builder = shard_builder
        .build_stripe()
        .with_context(|| format!("Failed to create stripe builder for {}", file_path))?;

    // Create a modified schema for CSV reading (convert GUID fields to strings)
    let (csv_schema, guid_field_indices) = create_arrow_compatible_schema(arrow_schema)?;

    // Open and read CSV file
    let file = fs::File::open(file_path)
        .with_context(|| format!("Failed to open CSV file: {}", file_path))?;

    let csv_reader = ReaderBuilder::new(Arc::new(csv_schema))
        .with_header(true)
        .build(file)
        .with_context(|| format!("Failed to create CSV reader for {}", file_path))?;

    let mut batch_count = 0;
    for batch_result in csv_reader {
        let mut batch =
            batch_result.with_context(|| format!("Failed to read batch from {}", file_path))?;

        // Convert GUID string fields to binary fields
        if !guid_field_indices.is_empty() {
            batch = convert_guid_fields_to_binary(&batch, &guid_field_indices, arrow_schema)?;
        }

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

    Ok(())
}

/// Validate all input parameters
fn validate_inputs(
    source_files: &[String],
    schema_path: &Option<String>,
    schema_string: &Option<String>,
) -> Result<()> {
    // Validate input files exist
    for file in source_files {
        utils::validate_file_exists(file)
            .with_context(|| format!("Invalid source file: {}", file))?;
        println!("  Source file: {}", file);
    }

    // Validate schema file if provided
    if let Some(schema) = schema_path {
        utils::validate_file_exists(schema)
            .with_context(|| format!("Invalid schema file: {}", schema))?;
        println!("  Schema file: {}", schema);
    }

    // Handle schema string if provided
    if let Some(schema_str) = schema_string {
        println!("  Schema string: {}", schema_str);
    }

    Ok(())
}

/// Establish the Arrow schema to use for ingestion
/// Priority: 1) schema_string, 2) schema_path, 3) infer from source files
fn establish_schema(
    schema_string: Option<String>,
    schema_path: Option<String>,
    source_files: &[String],
) -> Result<Schema> {
    if let Some(schema_str) = schema_string {
        // Priority 1: Parse schema from string
        let schema = schema_parser::parse_schema_string(&schema_str)
            .with_context(|| format!("Failed to parse schema string: {}", schema_str))?;
        println!("Schema parsed from string successfully");
        Ok(schema)
    } else if let Some(schema_path) = schema_path {
        // Priority 2: Load schema from file
        let schema_content = fs::read_to_string(&schema_path)
            .with_context(|| format!("Failed to read schema file: {}", schema_path))?;

        let schema = serde_json::from_str::<Schema>(&schema_content)
            .with_context(|| format!("Failed to deserialize schema from {schema_path}"))?;
        println!("Schema loaded from file successfully");
        Ok(schema)
    } else {
        // Priority 3: Infer schema from source files
        println!("No schema provided, inferring from source files...");
        super::inferschema::infer_schema(source_files.iter().map(String::as_str))
    }
}

/// Check if a file contains valid multi-line JSON by sampling the first few lines
fn validate_multiline_json(file_path: &str) -> Result<()> {
    println!("Validating multi-line JSON format for: {}", file_path);

    let file = fs::File::open(file_path)
        .with_context(|| format!("Failed to open file for validation: {}", file_path))?;

    let reader = BufReader::new(file);
    let mut line_count = 0;
    let max_lines_to_check = 5; // Sample first 5 lines

    for line_result in reader.lines() {
        let line =
            line_result.with_context(|| format!("Failed to read line from {}", file_path))?;

        // Skip empty lines
        if line.trim().is_empty() {
            continue;
        }

        // Try to parse as JSON
        serde_json::from_str::<serde_json::Value>(&line).with_context(|| {
            format!(
                "Line {} is not valid JSON in {}: {}",
                line_count + 1,
                file_path,
                line
            )
        })?;

        line_count += 1;
        if line_count >= max_lines_to_check {
            break;
        }
    }

    if line_count == 0 {
        return Err(anyhow::anyhow!(
            "File {} appears to be empty or contains no valid JSON lines",
            file_path
        ));
    }

    println!("  Validated {} lines as valid JSON", line_count);
    Ok(())
}

/// Ingest a single multi-line JSON file into the shard builder
fn ingest_json_file(
    file_path: &str,
    arrow_schema: &Schema,
    shard_builder: &mut ShardBuilder,
) -> Result<()> {
    println!("Processing multi-line JSON file: {}", file_path);

    // First validate that it's actually multi-line JSON
    validate_multiline_json(file_path)?;

    // Create stripe builder for this file
    let mut stripe_builder = shard_builder
        .build_stripe()
        .with_context(|| format!("Failed to create stripe builder for {}", file_path))?;

    // Create a modified schema for JSON reading (convert GUID fields to strings)
    let (json_schema, guid_field_indices) = create_arrow_compatible_schema(arrow_schema)?;

    // Open and read JSON file
    let file = fs::File::open(file_path)
        .with_context(|| format!("Failed to open JSON file: {}", file_path))?;

    let json_reader = JsonReaderBuilder::new(Arc::new(json_schema))
        .with_coerce_primitive(true)
        .build(BufReader::new(file))
        .with_context(|| format!("Failed to create JSON reader for {}", file_path))?;

    let mut batch_count = 0;
    for batch_result in json_reader {
        let mut batch =
            batch_result.with_context(|| format!("Failed to read batch from {}", file_path))?;

        // Convert GUID string fields to binary fields
        if !guid_field_indices.is_empty() {
            batch = convert_guid_fields_to_binary(&batch, &guid_field_indices, arrow_schema)?;
        }

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

    Ok(())
}

/// Create an Arrow-compatible schema by converting GUID fields to string fields
/// This allows reading GUID data as strings from JSON/CSV files before converting to binary
fn create_arrow_compatible_schema(schema: &Schema) -> Result<(Schema, Vec<usize>)> {
    let mut compatible_fields = Vec::new();
    let mut guid_field_indices = Vec::new();

    for (i, field) in schema.fields().iter().enumerate() {
        if is_guid_field(field) {
            // Convert GUID field to string for reading from text formats
            let string_field = Field::new(field.name(), DataType::Utf8, field.is_nullable());
            compatible_fields.push(string_field);
            guid_field_indices.push(i);
        } else {
            compatible_fields.push(field.as_ref().clone());
        }
    }

    Ok((Schema::new(compatible_fields), guid_field_indices))
}

/// Check if a field is a GUID field
fn is_guid_field(field: &Field) -> bool {
    matches!(field.data_type(), DataType::FixedSizeBinary(16))
        && field.metadata().get("ARROW:extension:name") == Some(&"arrow.uuid".to_string())
}

/// Convert GUID string fields to binary fields in a record batch
fn convert_guid_fields_to_binary(
    batch: &RecordBatch,
    guid_field_indices: &[usize],
    target_schema: &Schema,
) -> Result<RecordBatch> {
    let mut new_columns = Vec::new();

    for (i, column) in batch.columns().iter().enumerate() {
        if guid_field_indices.contains(&i) {
            // Convert string array to FixedSizeBinary array
            let string_array = column
                .as_any()
                .downcast_ref::<StringArray>()
                .with_context(|| format!("Expected string array for GUID field at index {}", i))?;

            let mut binary_data = Vec::new();
            let mut null_buffer = NullBufferBuilder::new(string_array.len());

            for j in 0..string_array.len() {
                if string_array.is_null(j) {
                    null_buffer.append_null();
                    binary_data.extend_from_slice(&[0u8; 16]); // Zero bytes for null values
                } else {
                    let guid_str = string_array.value(j);
                    match parse_guid_string(guid_str) {
                        Ok(guid_bytes) => {
                            null_buffer.append_non_null();
                            binary_data.extend_from_slice(&guid_bytes);
                        }
                        Err(_) => {
                            return Err(anyhow::anyhow!(
                                "Invalid GUID format in field {}: {}",
                                target_schema.field(i).name(),
                                guid_str
                            ));
                        }
                    }
                }
            }

            let binary_array =
                FixedSizeBinaryArray::new(16, Buffer::from_vec(binary_data), null_buffer.finish());
            new_columns.push(Arc::new(binary_array) as Arc<dyn Array>);
        } else {
            new_columns.push(column.clone());
        }
    }

    Ok(RecordBatch::try_new(
        Arc::new(target_schema.clone()),
        new_columns,
    )?)
}

/// Parse a GUID string into a 16-byte array
fn parse_guid_string(guid_str: &str) -> Result<[u8; 16]> {
    // Remove braces and hyphens
    let clean_guid = guid_str
        .trim()
        .trim_start_matches('{')
        .trim_end_matches('}')
        .replace('-', "");

    if clean_guid.len() != 32 {
        return Err(anyhow::anyhow!("GUID string must be 32 hex characters"));
    }

    let mut bytes = [0u8; 16];
    for (i, chunk) in clean_guid.as_bytes().chunks(2).enumerate() {
        if i >= 16 {
            break;
        }
        let hex_str = std::str::from_utf8(chunk)?;
        bytes[i] = u8::from_str_radix(hex_str, 16)
            .with_context(|| format!("Invalid hex in GUID: {}", hex_str))?;
    }

    Ok(bytes)
}
