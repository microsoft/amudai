//! Data generation utilities for testing.
//!
//! This module provides functions to generate synthetic test data.

use std::{
    io::{BufReader, Seek, SeekFrom},
    process::Stdio,
    sync::Arc,
};

use arrow_array::{RecordBatchIterator, RecordBatchReader};

/// Generates a specified number of "qlog" entries for testing.
///
/// This function executes a Python script to generate synthetic qlog entries
/// and returns them as a temporary file. The entries are JSON-formatted
/// records suitable for testing Amudai shard writer/reader functionality.
///
/// The generated entries contain realistic data patterns including:
/// - Record IDs for tracking individual entries
/// - Timestamp information
/// - Nested fields (lists, structs)
/// - Name-value property bags
///
/// # Arguments
///
/// * `count` - The number of qlog entries to generate. Must be greater than 0.
///
/// # Returns
///
/// Returns a `NamedTempFile` containing the generated entries, positioned
/// at the start of the file. Each line in the file contains a JSON object
/// representing a "qlog" entry (i.e. the file format is `ndjson`).
pub fn generate_qlog_entries(count: usize) -> anyhow::Result<tempfile::NamedTempFile> {
    assert_ne!(count, 0);
    let script = crate::dirs::get_qlog_generator_script_path()?;
    let mut cmd = crate::py::get_cmd()?;
    let child = cmd
        .arg(script)
        .arg(count.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()?;
    let mut output = child.stdout.expect("stdout");
    let mut file = tempfile::NamedTempFile::new()?;
    std::io::copy(&mut output, &mut file)?;
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

/// Loads the "qlog" record schema from the json-serialized Arrow Schema file.
pub fn load_qlog_schema() -> anyhow::Result<arrow_schema::Schema> {
    let path = crate::dirs::get_qlog_schema_path()?;
    let schema_json = std::fs::read_to_string(path)?;
    let schema = serde_json::from_str(&schema_json)?;
    Ok(schema)
}

pub fn create_qlog_batch_iterator(
    record_count: usize,
    batch_size: usize,
) -> anyhow::Result<Box<dyn RecordBatchReader>> {
    assert_ne!(batch_size, 0);
    let schema = Arc::new(load_qlog_schema()?);
    if record_count == 0 {
        Ok(Box::new(RecordBatchIterator::new(
            std::iter::empty(),
            schema,
        )))
    } else {
        let data = generate_qlog_entries(record_count)?;
        let reader = arrow_json::ReaderBuilder::new(schema)
            .with_batch_size(batch_size)
            .build(BufReader::with_capacity(128 * 1024, data))?;
        Ok(Box::new(reader))
    }
}

/// Generates a specified number of "inventory" entries for testing.
///
/// This function executes a Python script to generate synthetic inventory entries
/// and returns them as a temporary file. The entries are CSV-formatted
/// records suitable for testing Amudai shard writer/reader functionality.
///
/// The generated entries contain simple data patterns including:
/// - Record IDs for tracking individual entries
/// - Random numeric values
/// - Random text fields with lorem ipsum style words
///
/// # Arguments
///
/// * `count` - The number of inventory entries to generate. Must be greater than 0.
///
/// # Returns
///
/// Returns a `NamedTempFile` containing the generated entries, positioned
/// at the start of the file. Each line in the file contains a CSV record
/// representing an "inventory" entry.
pub fn generate_inventory_entries(count: usize) -> anyhow::Result<tempfile::NamedTempFile> {
    assert_ne!(count, 0);
    let script = crate::dirs::get_inventory_generator_script_path()?;
    let mut cmd = crate::py::get_cmd()?;
    let child = cmd
        .arg(script)
        .arg(count.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()?;
    let mut output = child.stdout.expect("stdout");
    let mut file = tempfile::NamedTempFile::new()?;
    std::io::copy(&mut output, &mut file)?;
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

/// Loads the "inventory" record schema from the json-serialized Arrow Schema file.
pub fn load_inventory_schema() -> anyhow::Result<arrow_schema::Schema> {
    let path = crate::dirs::get_inventory_schema_path()?;
    let schema_json = std::fs::read_to_string(path)?;
    let schema = serde_json::from_str(&schema_json)?;
    Ok(schema)
}

pub fn create_inventory_batch_iterator(
    record_count: usize,
    batch_size: usize,
) -> anyhow::Result<Box<dyn RecordBatchReader>> {
    assert_ne!(batch_size, 0);
    let schema = Arc::new(load_inventory_schema()?);
    if record_count == 0 {
        Ok(Box::new(RecordBatchIterator::new(
            std::iter::empty(),
            schema,
        )))
    } else {
        let data = generate_inventory_entries(record_count)?;
        let reader = arrow_csv::ReaderBuilder::new(schema)
            .with_batch_size(batch_size)
            .with_header(false)
            .build(BufReader::with_capacity(128 * 1024, data))?;
        Ok(Box::new(reader))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufRead, BufReader, Cursor, Seek, SeekFrom},
        str::FromStr,
        sync::Arc,
    };

    use arrow_json::ReaderBuilder;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn test_schema_timestamp() {
        let data_type =
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(Arc::from("+00")));
        println!("{}", serde_json::to_string(&data_type).unwrap());
        let field = Field::new("timestamp", data_type, false);
        let schema = Schema::new(vec![field]);

        let data = r#"{"timestamp":"2025-06-04T00:18:52.123456Z"}
{"timestamp":"2025-06-04T10:20:51.123Z"}
{"timestamp":"2025-06-04T20:33:00Z"}
"#;
        let cursor = Cursor::new(data);
        let mut reader = ReaderBuilder::new(Arc::new(schema.clone()))
            .build(cursor)
            .unwrap();

        let batch = reader.next().unwrap().unwrap();
        dbg!(&batch);
    }

    #[test]
    fn test_generate_qlog_entries() {
        let schema = super::load_qlog_schema().unwrap();

        let mut entries = super::generate_qlog_entries(100).unwrap();
        let reader = BufReader::with_capacity(256 * 1024, &mut entries);
        for line in reader.lines() {
            let line = line.unwrap();
            let value = serde_json::Value::from_str(&line).unwrap();
            assert!(value["recordId"].is_i64());
        }

        entries.seek(SeekFrom::Start(0)).unwrap();

        let mut reader = ReaderBuilder::new(Arc::new(schema.clone()))
            .with_batch_size(1024)
            .build(BufReader::with_capacity(128 * 1024, entries))
            .unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 100);
    }

    #[test]
    fn test_qlog_batch_iter() {
        let iter = super::create_qlog_batch_iterator(1000, 64).unwrap();
        let total = iter.map(|batch| batch.unwrap().num_rows()).sum::<usize>();
        assert_eq!(total, 1000);

        let iter = super::create_qlog_batch_iterator(0, 100).unwrap();
        let total = iter.map(|batch| batch.unwrap().num_rows()).sum::<usize>();
        assert_eq!(total, 0);
    }

    #[test]
    fn test_generate_inventory_entries() {
        let schema = super::load_inventory_schema().unwrap();

        let mut entries = super::generate_inventory_entries(100).unwrap();
        entries.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
            .with_batch_size(1024)
            .with_header(false)
            .build(BufReader::with_capacity(128 * 1024, entries))
            .unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 100);
    }

    #[test]
    fn test_inventory_batch_iter() {
        let iter = super::create_inventory_batch_iterator(1000, 64).unwrap();
        let total = iter.map(|batch| batch.unwrap().num_rows()).sum::<usize>();
        assert_eq!(total, 1000);

        let iter = super::create_inventory_batch_iterator(0, 100).unwrap();
        let total = iter.map(|batch| batch.unwrap().num_rows()).sum::<usize>();
        assert_eq!(total, 0);
    }
}
