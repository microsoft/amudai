use std::time::Instant;

use amudai_sequence::values::Values;
use arrow::{
    array::{
        Array, BinaryArray, FixedSizeBinaryArray, Float64Array, Int64Array, ListArray, RecordBatch,
        StringArray,
    },
    datatypes::{DataType, Field, Schema},
};

fn main() {
    let t0 = Instant::now();
    test_values_push(1024 * 1024 * 1024 + std::env::args().count());
    let elapsed = t0.elapsed();
    println!("elapsed: {elapsed:?}");
}

#[inline(never)]
#[unsafe(no_mangle)]
#[allow(dead_code)]
fn test_values_push(count: usize) {
    let mut values = Values::with_capacity::<i64>(1024);
    let mut c = 0;
    while c < count {
        values.write::<i64, _>(1024, |s| {
            s.iter_mut().enumerate().for_each(|(i, x)| *x = i as i64);
        });
        c += values.len::<i64>();
        // values.clear();
    }
}

#[allow(dead_code)]
fn test_arrow_ipc() {
    let t0 = Instant::now();
    let frame = build_frame(50000);
    println!(
        "Generated frame with {} rows and {} columns ({:?})",
        frame.num_rows(),
        frame.num_columns(),
        t0.elapsed()
    );

    let t0 = Instant::now();
    let output = Vec::with_capacity(200 * 1024 * 1024);
    let mut writer = arrow_ipc::writer::FileWriter::try_new(output, &frame.schema()).unwrap();
    for _ in 0..20 {
        writer.write(&frame).unwrap();
    }
    writer.flush().unwrap();
    writer.finish().unwrap();
    let output = writer.into_inner().unwrap();
    let time = t0.elapsed();
    println!("{} bytes ({:?})", output.len(), time);

    let t0 = Instant::now();
    let reader =
        arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(&output), None).unwrap();
    println!("batches: {}", reader.num_batches());
    for batch in reader {
        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 50000);
    }
    println!("read all ({:?})", t0.elapsed());
}

fn build_frame(records: usize) -> RecordBatch {
    // Define the schema
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new(
            "props",
            DataType::List(std::sync::Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            ))),
            false,
        ),
        Field::new("metric", DataType::Float64, false),
        Field::new("data", DataType::Binary, false),
        Field::new("rowid", DataType::FixedSizeBinary(16), false),
    ]);

    // Generate data for each column
    let mut names = Vec::with_capacity(records);
    let mut values = Vec::with_capacity(records);
    let mut props_values = Vec::new();
    let mut props_offsets = vec![0i32];
    let mut metrics = Vec::with_capacity(records);
    let mut data_values = Vec::new();
    let mut data_offsets = vec![0i32];
    let mut rowids = Vec::with_capacity(records * 16);

    for _ in 0..records {
        // Generate name: random alphanumeric string 5-15 chars
        let name_len = fastrand::usize(5..=15);
        let name: String = (0..name_len)
            .map(|_| {
                let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
                chars[fastrand::usize(..chars.len())] as char
            })
            .collect();
        names.push(name);

        // Generate value: random int from 0 to 100
        values.push(fastrand::i64(0..=100));

        // Generate props: 0..20 random alphanumeric strings with 5-10 chars
        let props_count = fastrand::usize(0..=20);
        for _ in 0..props_count {
            let prop_len = fastrand::usize(5..=10);
            let prop: String = (0..prop_len)
                .map(|_| {
                    let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
                    chars[fastrand::usize(..chars.len())] as char
                })
                .collect();
            props_values.push(Some(prop));
        }
        props_offsets.push(props_values.len() as i32);

        // Generate metric: random f64 from 0.0 to 1.0
        metrics.push(fastrand::f64());

        // Generate data: random binary 10-20 bytes
        let data_len = fastrand::usize(10..=20);
        let mut data_bytes = vec![0u8; data_len];
        for byte in &mut data_bytes {
            *byte = fastrand::u8(..);
        }
        data_values.extend_from_slice(&data_bytes);
        data_offsets.push(data_values.len() as i32);

        // Generate rowid: random 16-byte binary
        for _ in 0..16 {
            rowids.push(fastrand::u8(..));
        }
    }

    // Create arrays
    let name_array = StringArray::from(names);
    let value_array = Int64Array::from(values);

    let props_string_array = StringArray::from(props_values);
    let props_array = ListArray::new(
        std::sync::Arc::new(Field::new("item", DataType::Utf8, true)),
        arrow::buffer::OffsetBuffer::new(props_offsets.into()),
        std::sync::Arc::new(props_string_array),
        None,
    );

    let metric_array = Float64Array::from(metrics);

    let data_array = BinaryArray::new(
        arrow::buffer::OffsetBuffer::new(data_offsets.into()),
        data_values.into(),
        None,
    );

    let rowid_array = FixedSizeBinaryArray::new(16, rowids.into(), None);

    // Create the RecordBatch
    RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![
            std::sync::Arc::new(name_array),
            std::sync::Arc::new(value_array),
            std::sync::Arc::new(props_array),
            std::sync::Arc::new(metric_array),
            std::sync::Arc::new(data_array),
            std::sync::Arc::new(rowid_array),
        ],
    )
    .unwrap()
}

pub fn frame_to_csv(frame: &RecordBatch) -> String {
    use csv::WriterBuilder;

    let schema = frame.schema();
    let mut csv_buffer = Vec::new();
    {
        let mut writer = WriterBuilder::new()
            .has_headers(true)
            .from_writer(&mut csv_buffer);

        // Write header row
        let headers: Vec<&str> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        writer.write_record(&headers).unwrap();

        // Write data rows
        for row_idx in 0..frame.num_rows() {
            let mut record = Vec::new();

            for col_idx in 0..frame.num_columns() {
                let column = frame.column(col_idx);
                let field = schema.field(col_idx);
                let value_str =
                    convert_array_value_to_string(column.as_ref(), row_idx, field.data_type());
                record.push(value_str);
            }

            writer.write_record(&record).unwrap();
        }

        writer.flush().unwrap();
    }

    String::from_utf8(csv_buffer).unwrap()
}

fn convert_array_value_to_string(
    array: &dyn Array,
    row_idx: usize,
    data_type: &DataType,
) -> String {
    if array.is_null(row_idx) {
        return String::new();
    }

    match data_type {
        DataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            string_array.value(row_idx).to_string()
        }
        DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            int_array.value(row_idx).to_string()
        }
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            float_array.value(row_idx).to_string()
        }
        DataType::Binary => {
            let binary_array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let bytes = binary_array.value(row_idx);
            // Convert binary to JSON array of integers
            let json_array: Vec<u8> = bytes.to_vec();
            serde_json::to_string(&json_array).unwrap()
        }
        DataType::FixedSizeBinary(_) => {
            let fixed_binary_array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let bytes = fixed_binary_array.value(row_idx);
            // Convert binary to JSON array of integers
            let json_array: Vec<u8> = bytes.to_vec();
            serde_json::to_string(&json_array).unwrap()
        }
        DataType::List(field) => {
            let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
            let list_value = list_array.value(row_idx);

            // Convert list elements to JSON array
            let mut json_values = Vec::new();
            for i in 0..list_value.len() {
                let element_value =
                    convert_array_value_to_string(list_value.as_ref(), i, field.data_type());
                // If the element is a string, we want it as a JSON string, otherwise parse it as JSON value
                if matches!(field.data_type(), DataType::Utf8) {
                    json_values.push(serde_json::Value::String(element_value));
                } else {
                    // For non-string types, try to parse the string representation back to JSON
                    match serde_json::from_str::<serde_json::Value>(&element_value) {
                        Ok(val) => json_values.push(val),
                        Err(_) => json_values.push(serde_json::Value::String(element_value)),
                    }
                }
            }
            serde_json::to_string(&json_values).unwrap()
        }
        _ => {
            // For other data types, convert to string representation
            format!("unsupported_type_{}", data_type)
        }
    }
}
