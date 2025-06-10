//! Infer schema command implementation

use anyhow::{Context, Result, bail};
use arrow_json::reader::infer_json_schema_from_seekable;
use arrow_schema::Schema;
use serde_json::Value;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;

use crate::utils;

pub fn run(source_files: Vec<String>, output_path: Option<String>) -> Result<()> {
    println!("Analyzing files for schema inference...");

    let final_schema = infer_schema(source_files.iter().map(String::as_str))?;

    let schema_json = serde_json::to_string_pretty(&final_schema)
        .context("Failed to serialize schema to JSON")?;

    match output_path {
        Some(output_file) => {
            fs::write(&output_file, &schema_json)
                .with_context(|| format!("Failed to write schema to file: {}", output_file))?;
            println!("Schema written to: {}", output_file);
        }
        None => {
            println!("{}", schema_json);
        }
    }
    Ok(())
}

pub fn infer_schema<'a>(source_files: impl Iterator<Item = &'a str> + Clone) -> Result<Schema> {
    for file in source_files.clone() {
        utils::validate_file_exists(file)
            .with_context(|| format!("Invalid source file: {}", file))?;
        println!("  Analyzing file: {}", file);
    }

    let mut inferred_schemas = Vec::new();

    for file in source_files {
        let schema = infer_file_schema(file)
            .with_context(|| format!("Failed to infer schema for file: {}", file))?;
        inferred_schemas.push(schema);
    }

    let final_schema = if inferred_schemas.len() == 1 {
        inferred_schemas.into_iter().next().unwrap()
    } else {
        combine_schemas(inferred_schemas)?
    };
    Ok(final_schema)
}

fn infer_file_schema(file_path: &str) -> Result<Schema> {
    let path = Path::new(file_path);
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_lowercase();

    match extension.as_str() {
        "json" | "ndjson" => infer_json_schema(file_path),
        "csv" => infer_csv_schema(file_path),
        _ => {
            let content = fs::read_to_string(file_path).context("Failed to read file as text")?;
            if serde_json::from_str::<Value>(&content).is_ok() {
                infer_json_schema(file_path)
            } else {
                bail!("Unsupported content type in {file_path}");
            }
        }
    }
}

fn infer_json_schema(file_path: &str) -> Result<Schema> {
    let file = File::open(file_path)
        .with_context(|| format!("Failed to open JSON file: {}", file_path))?;

    let mut buf_reader = BufReader::new(file);

    let (schema, _records_read) = infer_json_schema_from_seekable(&mut buf_reader, Some(100))
        .with_context(|| format!("Failed to infer JSON schema from file: {}", file_path))?;

    Ok(schema)
}

fn infer_csv_schema(_file_path: &str) -> Result<Schema> {
    todo!()
}

fn combine_schemas(schemas: Vec<Schema>) -> Result<Schema> {
    let schema = Schema::try_merge(schemas.into_iter())?;
    Ok(schema)
}
