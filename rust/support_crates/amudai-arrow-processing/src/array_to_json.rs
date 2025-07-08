use arrow_array::{Array, RecordBatch, make_array};
use arrow_json::LineDelimitedWriter;
use arrow_schema::{ArrowError, Field, Schema};
use std::sync::Arc;

pub fn array_to_ndjson(array: &dyn Array) -> Result<String, ArrowError> {
    let field = Field::new("value", array.data_type().clone(), array.null_count() > 0);
    let schema = Schema::new(vec![field]);
    let array_data = array.to_data();
    let array_arc = make_array(array_data);
    let record_batch = RecordBatch::try_new(Arc::new(schema), vec![array_arc])?;
    record_batch_to_ndjson(&record_batch)
}

pub fn record_batch_to_ndjson(record_batch: &RecordBatch) -> Result<String, ArrowError> {
    let buf = Vec::new();
    let mut writer = LineDelimitedWriter::new(buf);
    writer.write_batches(&[record_batch])?;
    writer.finish()?;

    let output = String::from_utf8(writer.into_inner())
        .map_err(|e| ArrowError::ComputeError(format!("Failed to convert to UTF-8: {e}")))?;

    Ok(output)
}
