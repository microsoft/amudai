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

/// Procedurally generated data sets
pub mod procedural {
    use std::sync::Arc;

    use amudai_arrow_builders::RecordBatchBuilder;
    use amudai_arrow_builders_macros::struct_builder;
    use arrow_array::{RecordBatch, RecordBatchIterator};
    use arrow_schema::{ArrowError, Schema};

    struct_builder!(
        struct Point {
            x: f32,
            y: f32,
        }
    );

    struct_builder!(
        struct Amudatum {
            ordinal: u64,
            name: String,
            timestamp_arrow: Timestamp,
            #[metadata("ARROW:extension:name", "arrow.uuid")]
            guid: FixedSizeBinary<16>,
            #[metadata("ARROW:extension:name", "KustoDateTime")]
            timestamp_ticks: u64,
            num_u8: u8,
            num_u16: u16,
            num_u32: u32,
            num_u64: u64,
            num_i8: i8,
            num_i16: i16,
            num_i32: i32,
            num_i64: i64,
            num_f32: f32,
            num_f64: f64,
            num_list: List<i32>,
            data_binary: Binary,
            data_string: String,
            point: Point,
            point_list: List<Point>,
            property_bag: Map<String, String>,
            #[metadata("ARROW:extension:name", "KustoDecimal")]
            num_decimal: FixedSizeBinary<16>,
            is_active: bool,
        }
    );

    pub type AmudatumBatchBuilder = RecordBatchBuilder<AmudatumFields>;

    pub fn generate_amudatum_record_batches(
        record_count: u64,
        batch_size: usize,
    ) -> RecordBatchIterator<Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>>>> {
        let schema = get_amudatum_schema();

        let iter = (0..record_count).step_by(batch_size).map(move |pos| {
            let size = std::cmp::min(batch_size, (record_count - pos) as usize);
            Ok(generate_amudatum_record_batch(pos, size))
        });

        RecordBatchIterator::new(Box::new(iter), Arc::new(schema))
    }

    pub fn generate_amudatum_record_batch(start_pos: u64, batch_size: usize) -> RecordBatch {
        let mut builder = create_amudatum_batch_builder();
        let end_pos = start_pos + batch_size as u64;
        for pos in start_pos..end_pos {
            populate_amudatum_record(pos, &mut builder);
        }
        builder.build()
    }

    pub fn populate_amudatum_record(pos: u64, builder: &mut AmudatumBatchBuilder) {
        // Basic fields
        builder.ordinal_field().push(pos);
        builder.name_field().push(format!("record_{pos}"));

        // Timestamp fields
        let base_timestamp_ns = 1640995200_000_000_000i64; // 2022-01-01 00:00:00 UTC
        let timestamp_ns = base_timestamp_ns + (pos as i64 * 1_000_000_000); // Add seconds
        builder.timestamp_arrow_field().push(timestamp_ns);

        // GUID field (16 bytes) - generate deterministic UUID-like bytes based on pos
        let guid_bytes = {
            let mut bytes = [0u8; 16];
            let pos_bytes = pos.to_le_bytes();
            // Fill first 8 bytes with pos, repeat for next 8 bytes with slight variation
            bytes[0..8].copy_from_slice(&pos_bytes);
            let varied_pos = pos.wrapping_mul(0x1234567890ABCDEF);
            let varied_bytes = varied_pos.to_le_bytes();
            bytes[8..16].copy_from_slice(&varied_bytes);
            bytes
        };
        builder.guid_field().push(&guid_bytes);

        // KustoDateTime as ticks (100ns intervals since 0001-01-01)
        let base_ticks = 621355968000000000i64; // Ticks from 0001-01-01 to 1970-01-01
        let timestamp_ticks = base_ticks + (timestamp_ns / 100); // Convert ns to 100ns ticks
        builder.timestamp_ticks_field().push(timestamp_ticks as u64);

        // Numeric fields - generate deterministic values based on pos
        builder.num_u8_field().push((pos % 256) as u8);
        builder.num_u16_field().push((pos % 65536) as u16);
        builder.num_u32_field().push((pos % 4294967296) as u32);
        builder.num_u64_field().push(pos);

        // For signed integers, use wrapping arithmetic to get full range
        builder.num_i8_field().push((pos % 256) as u8 as i8);
        builder.num_i16_field().push((pos % 65536) as u16 as i16);
        builder.num_i32_field().push((pos % 4294967296) as i32);
        builder.num_i64_field().push(pos as i64);

        builder.num_f32_field().push((pos as f32) * 0.1);
        builder.num_f64_field().push((pos as f64) * 0.01);

        let num_list_size = (pos % 10) as usize;
        for i in 0..num_list_size {
            builder.num_list_field().item().push(i as i32);
        }
        builder.num_list_field().finish_list();

        // Binary data - generate deterministic bytes based on pos
        let binary_data = format!("binary_data_{pos}").into_bytes();
        builder.data_binary_field().push(&binary_data);

        // String data
        builder
            .data_string_field()
            .push(format!("string_data_{pos}"));

        // Point struct
        builder.point_field().x_field().push((pos as f32) * 0.1);
        builder.point_field().y_field().push((pos as f32) * 0.2);
        builder.point_field().finish_struct();

        // List of Point structs - size based on pos % 3 (0 to 2 elements)
        let point_list_size = (pos % 5) as usize;
        for i in 0..point_list_size {
            let point_x = (pos as f32) * 0.1 + (i as f32) * 0.001;
            let point_y = (pos as f32) * 0.01 + (i as f32) * 0.001;
            builder.point_list_field().item().x_field().push(point_x);
            builder.point_list_field().item().y_field().push(point_y);
            builder.point_list_field().item().finish_struct();
        }
        builder.point_list_field().finish_list();

        let property_bag_size = (pos % 4) as usize;
        for i in 0..property_bag_size {
            let key = format!("key_{pos}_{i}");
            let value = format!("value_{pos}_{i}");
            builder.property_bag_field().key().push(key);
            builder.property_bag_field().value().push(value);
        }
        builder.property_bag_field().finish_map();

        // TODO: KustoDecimal as 16-byte binary

        match pos % 3 {
            0 => builder.is_active_field().push(true),
            1 => builder.is_active_field().push(false),
            2 => builder.is_active_field().push_null(),
            _ => unreachable!(),
        }

        builder.finish_record();
    }

    pub fn get_amudatum_schema() -> Schema {
        create_amudatum_batch_builder().schema()
    }

    fn create_amudatum_batch_builder() -> AmudatumBatchBuilder {
        AmudatumBatchBuilder::default()
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
    fn test_amudatum_schema() {
        use super::procedural::*;

        let schema = get_amudatum_schema();
        assert!(
            schema
                .field_with_name("num_decimal")
                .unwrap()
                .metadata()
                .contains_key("ARROW:extension:name")
        );
        // println!("{}", serde_json::to_string_pretty(&schema).unwrap());
    }

    #[test]
    fn test_amudatum_batch_gen() {
        use super::procedural::*;

        let batch = generate_amudatum_record_batch(10, 3);
        let batch_json =
            amudai_arrow_processing::array_to_json::record_batch_to_ndjson(&batch).unwrap();
        assert!(batch_json.contains("num_i32"));
        // println!("{batch_json}");
    }

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
