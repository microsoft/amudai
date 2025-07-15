//! Arrow Reader Builder Module
//!
//! Provides a builder pattern for configuring and constructing Arrow readers
//! from Amudai shards.
//! Allows customization of object store, schema projection, batch size,
//! and position ranges.

use std::sync::{Arc, OnceLock};

use amudai_arrow_compat::{
    amudai_to_arrow_error::ToArrowResult, amudai_to_arrow_schema::ToArrowSchema,
};
use amudai_collections::range_list::RangeList;
use amudai_objectstore::{
    ObjectStore, ReferenceResolver, local_store::LocalFsObjectStore, url::ObjectUrl,
};
use amudai_shard::read::shard::{Shard, ShardOptions};
use arrow::error::ArrowError;

use crate::shard_reader::ShardRecordBatchReader;

/// Builder for constructing Arrow readers from Amudai shards.
/// Allows configuration of object store, reference resolver, schema projection,
/// batch size, and record position ranges.
#[derive(Clone)]
pub struct ArrowReaderBuilder {
    shard_url: ObjectUrl,
    object_store: Option<Arc<dyn ObjectStore>>,
    reference_resolver: Option<Arc<dyn ReferenceResolver>>,
    projection: Option<arrow::datatypes::SchemaRef>,
    batch_size: u64,
    pos_ranges: Option<RangeList<u64>>,
    shard: OnceLock<Shard>,
}

impl ArrowReaderBuilder {
    /// Creates a new `ArrowReaderBuilder` for the given shard URL.
    ///
    /// # Arguments
    /// * `shard_url` - The URL of the Amudai shard to read.
    ///
    /// # Errors
    /// Returns an error if the URL is invalid.
    pub fn try_new(shard_url: impl AsRef<str>) -> Result<ArrowReaderBuilder, ArrowError> {
        let shard_url = ObjectUrl::parse(shard_url.as_ref()).to_arrow_res()?;
        Ok(ArrowReaderBuilder {
            shard_url,
            object_store: None,
            reference_resolver: None,
            projection: None,
            batch_size: 1024,
            pos_ranges: None,
            shard: OnceLock::new(),
        })
    }

    /// Sets the object store to use for reading the shard.
    /// If not set, the default (local FS) store implementation will be used.
    pub fn with_object_store(self, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store: Some(object_store),
            ..self
        }
    }

    /// Sets the reference resolver that will be used to resolve internal references to
    /// the shard sub-artifacts.
    /// If not set, the default (container-scoped) resolver will be used.
    pub fn with_reference_resolver(self, reference_resolver: Arc<dyn ReferenceResolver>) -> Self {
        Self {
            reference_resolver: Some(reference_resolver),
            ..self
        }
    }

    /// Sets the projected Arrow schema for reading.
    pub fn with_projected_schema(self, arrow_schema: arrow::datatypes::SchemaRef) -> Self {
        Self {
            projection: Some(arrow_schema),
            ..self
        }
    }

    /// Sets the record batch size for reading.
    ///
    /// # Panics
    /// Panics if `batch_size` is zero.
    pub fn with_batch_size(self, batch_size: u64) -> Self {
        assert_ne!(batch_size, 0);
        Self { batch_size, ..self }
    }

    /// Sets the record position ranges to read from the shard.
    /// If not set, the entire shard will be scanned.
    ///
    /// **Note**: the desired batch size is still in effect on top of these ranges. E.g.
    /// if the ranges to be read from the shard are `[1000..3000, 5000..5500]` and the
    /// `batch_size` is 1000, the iterator would yield the record batches
    /// `[1000..2000, 2000..3000, 5000..5500]`.
    pub fn with_position_ranges(self, pos_ranges: impl Into<Option<RangeList<u64>>>) -> Self {
        Self {
            pos_ranges: pos_ranges.into(),
            ..self
        }
    }

    /// Fetches the Amudai schema from the shard.
    ///
    /// # Errors
    /// Returns an error if the schema cannot be fetched.
    pub fn fetch_amudai_schema(&self) -> Result<amudai_format::schema::Schema, ArrowError> {
        Ok(self
            .establish_shard()?
            .fetch_schema()
            .to_arrow_res()?
            .clone())
    }

    /// Returns a reference to the underlying Amudai shard.
    ///
    /// # Errors
    /// Returns an error if the shard cannot be established.
    pub fn get_amudai_shard(&self) -> Result<&Shard, ArrowError> {
        self.establish_shard()
    }

    /// Fetches the Arrow schema for the shard.
    ///
    /// # Errors
    /// Returns an error if the schema cannot be converted.
    pub fn fetch_arrow_schema(&self) -> Result<arrow::datatypes::SchemaRef, ArrowError> {
        // TODO: decide whether we want to remove "internal fields" from this schema.
        self.fetch_amudai_schema()?
            .to_arrow_schema()
            .map(Arc::new)
            .to_arrow_res()
    }

    /// Builds a `ShardRecordBatchReader` for reading record batches from the shard.
    ///
    /// # Errors
    /// Returns an error if the shard contains deleted records or if construction fails.
    pub fn build(&self) -> Result<ShardRecordBatchReader, ArrowError> {
        self.establish_shard()?;
        let projection = self
            .projection
            .clone()
            .map(Ok)
            .unwrap_or_else(|| self.fetch_arrow_schema())?;

        let shard = self.shard.get().expect("established shard").clone();

        if shard.directory().deleted_record_count != 0 {
            return Err(ArrowError::NotYetImplemented(
                "Reading shard with deleted records".into(),
            ));
        }

        ShardRecordBatchReader::new(shard, projection, self.batch_size, self.pos_ranges.clone())
    }
}

impl ArrowReaderBuilder {
    /// Returns a reference to the underlying Amudai `Shard`, initializing it
    /// if necessary.
    ///
    /// This method will open the shard using the configured object store and reference
    /// resolver if it has not already been established. The shard is cached for
    /// subsequent calls.
    ///
    /// # Errors
    /// Returns an error if the shard cannot be opened or initialized.
    fn establish_shard(&self) -> Result<&Shard, ArrowError> {
        if let Some(shard) = self.shard.get() {
            return Ok(shard);
        }

        let object_store = self
            .object_store
            .clone()
            .unwrap_or_else(|| Arc::new(LocalFsObjectStore::new_unscoped()));

        let mut options = ShardOptions::new(object_store);

        if let Some(reference_resolver) = self.reference_resolver.clone() {
            options = options.reference_resolver(reference_resolver);
        }

        let shard = options.open(self.shard_url.clone()).to_arrow_res()?;
        Ok(self.shard.get_or_init(|| shard))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use amudai_format::defs::common::DataRef;
    use amudai_shard::tests::{data_generator::create_nested_test_schema, shard_store::ShardStore};

    #[test]
    fn test_arrow_reader_builder_fetch_schema() {
        let schema = create_nested_test_schema();

        let shard_store = ShardStore::new();

        let shard_ref: DataRef = shard_store.ingest_shard_with_schema(&schema, 50);

        let reader_builder = ArrowReaderBuilder::try_new(&shard_ref.url)
            .expect("Failed to create ArrowReaderBuilder")
            .with_object_store(shard_store.object_store.clone());

        let fetched_schema = reader_builder
            .fetch_arrow_schema()
            .expect("fetch arrow schema");
        assert_eq!(fetched_schema.field(0).name(), "Id");
        assert_eq!(fetched_schema.field(1).name(), "Text");
    }
}
