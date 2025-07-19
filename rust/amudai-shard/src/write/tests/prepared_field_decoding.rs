use crate::tests::data_generator::{self, create_nested_test_schema};
use crate::write::stripe_builder::{StripeBuilder, StripeBuilderParams};
use amudai_arrow_compat::arrow_to_amudai_schema::FromArrowSchema;
use amudai_common::Result;
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::schema_builder::SchemaBuilder;
use amudai_io_impl::temp_file_store;

#[test]
fn test_prepared_fields_decoding() -> Result<()> {
    // Create a complex nested test schema
    let arrow_schema = create_nested_test_schema();

    // Convert to Amudai schema
    let schema_builder = SchemaBuilder::from_arrow_schema(&arrow_schema)?;
    let schema_message = schema_builder.finish_and_seal();
    let schema = schema_message.schema()?;

    // Create stripe builder and ingest data
    let temp_store = temp_file_store::create_in_memory(64 * 1024 * 1024)?;
    let stripe_params = StripeBuilderParams {
        schema: schema.clone(),
        temp_store: temp_store.clone(),
        encoding_profile: BlockEncodingProfile::default(),
    };

    let mut stripe_builder = StripeBuilder::new(stripe_params)?;

    let record_count = 20000;
    // Push all batches to the stripe builder
    for batch in data_generator::generate_batches(arrow_schema.clone(), 1000..1001, record_count) {
        stripe_builder.push_batch(&batch)?;
    }

    // Finish building the stripe to get PreparedStripe
    let prepared_stripe = stripe_builder.finish()?;

    // Verify we processed the expected number of records
    assert_eq!(
        prepared_stripe.record_count, record_count as u64,
        "Expected to process some records"
    );

    // Track statistics about the decoding
    let mut total_fields_processed = 0u64;
    let mut total_values_read = 0u64;

    for schema_id in prepared_stripe
        .fields
        .iter()
        .map(|(schema_id, _)| schema_id)
    {
        let prepared_field = prepared_stripe.fields.get(schema_id).expect("field");
        let decoder = prepared_stripe.decode_field(schema_id)?;
        // Create a reader for the decoder
        let position_count = prepared_field.descriptor.position_count;

        if position_count > 0 {
            // Read data in 1000-value chunks
            const CHUNK_SIZE: u64 = 500;
            let mut start_pos = 0u64;
            let mut reader =
                decoder.create_reader_with_ranges(std::iter::once(0..position_count))?;
            while start_pos < position_count {
                let end_pos = std::cmp::min(start_pos + CHUNK_SIZE, position_count);
                let range = start_pos..end_pos;

                // Read the chunk
                let sequence = reader.read_range(range.clone())?;
                let chunk_size = end_pos - start_pos;

                // Verify the sequence has the expected number of positions
                assert_eq!(sequence.len(), chunk_size as usize);
                total_values_read += chunk_size;
                start_pos = end_pos;
            }
        }
        total_fields_processed += 1;
    }
    dbg!(total_fields_processed);
    dbg!(total_values_read);
    Ok(())
}
