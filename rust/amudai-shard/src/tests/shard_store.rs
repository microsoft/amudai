use std::sync::Arc;

use amudai_arrow_compat::arrow_to_amudai_schema::FromArrowSchema;
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
    write::shard_builder::{ShardBuilder, ShardBuilderParams},
};

pub struct ShardStore {
    pub object_store: Arc<dyn ObjectStore>,
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
            _temp_dir: dir,
        }
    }

    pub fn open_shard(&self, url: impl AsRef<str>) -> crate::read::shard::Shard {
        crate::read::shard::ShardOptions::new(self.object_store.clone())
            .open(url.as_ref())
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
        let shard_schema = SchemaBuilder::from_arrow_schema(&schema).unwrap();

        let mut shard_builder = ShardBuilder::new(ShardBuilderParams {
            schema: shard_schema.into(),
            object_store: self.object_store.clone(),
            temp_store: temp_file_store::create_in_memory(32 * 1024 * 1024).unwrap(),
            encoding_profile: Default::default(),
        })
        .unwrap();

        let mut stripe_builder = shard_builder.build_stripe().unwrap();

        for batch in batches {
            let batch = batch.unwrap();
            stripe_builder.push_batch(&batch).unwrap();
        }
        let stripe = stripe_builder.finish().unwrap();
        shard_builder.add_stripe(stripe).unwrap();
        let shard = shard_builder
            .finish()
            .unwrap()
            .seal(&format!("file:///{}/test.amudai.shard", fastrand::u32(..)))
            .unwrap();
        shard.directory_blob
    }
}
