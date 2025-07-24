use std::sync::{Arc, Mutex};

use amudai_arrow_compat::arrow_to_amudai_schema::FromArrowSchema;
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::{defs::common::DataRef, schema_builder::SchemaBuilder};
use amudai_io_impl::temp_file_store;
use amudai_objectstore::{
    ObjectStore,
    local_store::{LocalFsMode, LocalFsObjectStore},
};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::Schema;
use tempfile::TempDir;

use crate::{
    tests::data_generator::{create_nested_test_schema, generate_batches},
    write::shard_builder::{ShardBuilder, ShardBuilderParams, ShardFileOrganization},
};

pub struct ShardStore {
    pub object_store: Arc<dyn ObjectStore>,
    encoding_profile: Mutex<BlockEncodingProfile>,
    file_organization: Mutex<ShardFileOrganization>,
    _temp_dir: TempDir,
}

impl Default for ShardStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardStore {
    pub fn new() -> ShardStore {
        let dir = TempDir::new().unwrap();
        let fs = LocalFsObjectStore::new(dir.path(), LocalFsMode::VirtualRoot).unwrap();
        ShardStore {
            object_store: Arc::new(fs),
            encoding_profile: Default::default(),
            file_organization: Default::default(),
            _temp_dir: dir,
        }
    }

    pub fn set_shard_encoding_profile(&self, encoding_profile: BlockEncodingProfile) {
        *self.encoding_profile.lock().unwrap() = encoding_profile;
    }

    pub fn set_shard_file_organization(&self, file_organization: ShardFileOrganization) {
        *self.file_organization.lock().unwrap() = file_organization;
    }

    pub fn open_shard(&self, url: impl AsRef<str>) -> crate::read::shard::Shard {
        crate::read::shard::ShardOptions::new(self.object_store.clone())
            .open(url.as_ref())
            .unwrap()
    }

    pub fn open_shard_ref(&self, shard_ref: &DataRef) -> crate::read::shard::Shard {
        crate::read::shard::ShardOptions::new(self.object_store.clone())
            .open_data_ref(shard_ref)
            .unwrap()
    }

    pub fn ingest_shard_with_nested_schema(&self, record_count: usize) -> DataRef {
        self.ingest_shard_with_schema(&create_nested_test_schema(), record_count)
    }

    pub fn ingest_shard_with_schema(&self, schema: &Arc<Schema>, record_count: usize) -> DataRef {
        let batches = RecordBatchIterator::new(
            generate_batches(schema.clone(), 100..200, record_count).map(Ok),
            schema.clone(),
        );
        self.ingest_shard_from_record_batches(batches)
    }

    pub fn ingest_shard_from_record_batches<I>(&self, batches: RecordBatchIterator<I>) -> DataRef
    where
        I: IntoIterator<Item = Result<RecordBatch, arrow_schema::ArrowError>>,
    {
        let schema = batches.schema();
        let mut shard_builder = self.create_shard_builder(&schema);
        let mut stripe_builder = shard_builder.build_stripe().unwrap();

        for batch in batches {
            let batch = batch.unwrap();
            stripe_builder.push_batch(&batch).unwrap();
        }
        let stripe = stripe_builder.finish().unwrap();
        shard_builder.add_stripe(stripe).unwrap();
        self.seal_shard_builder(shard_builder)
    }

    pub fn create_shard_builder(&self, schema: &Arc<Schema>) -> ShardBuilder {
        let shard_schema = SchemaBuilder::from_arrow_schema(schema).unwrap();
        ShardBuilder::new(ShardBuilderParams {
            schema: shard_schema.into(),
            object_store: self.object_store.clone(),
            temp_store: temp_file_store::create_in_memory(256 * 1024 * 1024).unwrap(),
            encoding_profile: *self.encoding_profile.lock().unwrap(),
            file_organization: *self.file_organization.lock().unwrap(),
        })
        .unwrap()
    }

    pub fn seal_shard_builder(&self, shard_builder: ShardBuilder) -> DataRef {
        let shard = shard_builder
            .finish()
            .unwrap()
            .seal(&self.generate_shard_url())
            .unwrap();
        shard.directory_ref
    }

    /// Generates a unique test shard URL.
    ///
    /// This method creates a file-based URL with a random component to ensure
    /// unique shard identifiers during testing. Each call returns a different URL.
    ///
    /// # Returns
    ///
    /// A unique file URL string suitable for test shard identification.
    pub fn generate_shard_url(&self) -> String {
        format!("file:///{}/test.amudai.shard", fastrand::u32(..))
    }

    pub fn verify_shard(&self, shard: crate::read::shard::Shard) {
        // Get the schema for the shard
        let schema = shard.fetch_schema().expect("Failed to fetch schema");

        // Iterate through all stripes in the shard
        for stripe_index in 0..shard.stripe_count() {
            let stripe = shard
                .open_stripe(stripe_index)
                .expect("Failed to open stripe");

            // Verify all fields in the schema (including nested fields)
            for field_index in 0..schema.len().expect("Failed to get schema length") {
                let field = schema.field_at(field_index).expect("Failed to get field");
                let data_type = field.data_type().expect("Failed to get data type");

                // Recursively verify this field and all its nested children
                Self::verify_field_recursive(&stripe, &data_type);
            }
        }
    }

    fn verify_field_recursive(
        stripe: &crate::read::stripe::Stripe,
        data_type: &amudai_format::schema::DataType,
    ) {
        let field = stripe.open_field(data_type.clone()).unwrap_or_else(|_| {
            panic!(
                "Failed to open field '{}' (schema_id: {:?})",
                data_type.name().unwrap_or("<unknown>"),
                data_type
                    .schema_id()
                    .unwrap_or(amudai_format::schema::SchemaId::invalid())
            )
        });

        let decoder = field.create_decoder().unwrap_or_else(|_| {
            panic!(
                "Failed to create decoder for field '{}' (schema_id: {:?})",
                data_type.name().unwrap_or("<unknown>"),
                data_type
                    .schema_id()
                    .unwrap_or(amudai_format::schema::SchemaId::invalid())
            )
        });

        let total_positions = field.position_count();
        let mut reader = decoder
            .create_reader(std::iter::once(0..total_positions))
            .unwrap_or_else(|_| {
                panic!(
                    "Failed to create reader for field '{}' (schema_id: {:?})",
                    data_type.name().unwrap_or("<unknown>"),
                    data_type
                        .schema_id()
                        .unwrap_or(amudai_format::schema::SchemaId::invalid())
                )
            });

        let batch_size = 1024u64;
        let mut position = 0u64;

        while position < total_positions {
            let end_position = (position + batch_size).min(total_positions);
            reader
                .read_range(position..end_position)
                .unwrap_or_else(|_| {
                    panic!(
                        "Failed to read data from field '{}' (schema_id: {:?}) at positions {}..{}",
                        data_type.name().unwrap_or("<unknown>"),
                        data_type
                            .schema_id()
                            .unwrap_or(amudai_format::schema::SchemaId::invalid()),
                        position,
                        end_position
                    )
                });
            position = end_position;
        }

        let child_count = data_type.child_count().expect("Failed to get child count");
        for child_index in 0..child_count {
            let child_data_type = data_type
                .child_at(child_index)
                .expect("Failed to get child data type");
            Self::verify_field_recursive(stripe, &child_data_type);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_shard() {
        let shard_store = ShardStore::new();
        let shard_ref = shard_store.ingest_shard_with_nested_schema(1000);
        let shard = shard_store.open_shard(&shard_ref.url);
        shard_store.verify_shard(shard);
    }
}
