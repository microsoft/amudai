use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_64;

use amudai_common::Result;
use amudai_common::error::Error;
use amudai_format::property_bag::PropertyBagBuilder;
use amudai_format::schema::BasicType;
use amudai_index_core::builder::Parameters;
use amudai_index_core::metadata::Scope;
use amudai_index_core::shard_markers::DynPreparedShard;
use amudai_index_core::{
    IndexBuilder, IndexType,
    builder::{ArtifactData, PreparedArtifact, PreparedIndex},
};
use amudai_objectstore::ObjectStore;
use amudai_sequence::{
    frame::{Frame, FrameLocation},
    sequence::{AsSequence, Sequence},
};

use crate::field_hashing_index::FieldHashingIndexType;
use crate::write::hashmap_index_builder::{
    HashmapIndexBuilder, HashmapIndexBuilderParams, IndexedEntry,
};

/// Builds hash-based indexes by extracting field paths and values from structured data,
/// then creating key-position mappings for efficient lookup operations.
///
/// The indexer processes frames of records by recursively traversing their structure,
/// building composite keys from field paths (e.g., "person.name", "items[2].price")
/// and their actual values. These keys are hashed using xxHash3 for consistent
/// distribution across hash buckets, enabling fast equality searches.
///
/// Entries are batched in memory before being flushed to the underlying partitioned
/// hashmap builder to optimize write performance and reduce system call overhead.
pub struct FieldHashingIndexBuilder {
    /// Reference to the index type that created this indexer
    index_type: Arc<dyn IndexType>,
    /// Underlying hashmap builder that manages partitioned storage
    builder: HashmapIndexBuilder,
    /// Scope configuration defining the range and context of this index
    scope: Scope,
    /// Builder for collecting metadata properties associated with this index
    properties: PropertyBagBuilder,
    /// Reusable buffer for building field path keys to avoid repeated allocations
    scratch: Vec<u8>,
    /// Buffered entries awaiting flush to reduce builder interaction overhead
    pending_entries: Vec<IndexedEntry>,
}

impl FieldHashingIndexBuilder {
    /// The number of entries to buffer before flushing to the underlying builder.
    const PENDING_COUNT: usize = 1024;

    /// Creates a new FieldHashingIndexBuilder from core parameters and index type.
    ///
    /// Validates that required object stores are available since the underlying
    /// hashmap builder requires persistent storage for partition management
    /// and temporary storage for intermediate data during construction.
    pub fn new(params: Parameters, index_type: Arc<FieldHashingIndexType>) -> Result<Self> {
        // Require object_store & temp_store for the underlying shard builder.
        let object_store: Arc<dyn ObjectStore> =
            params.object_store.as_ref().cloned().ok_or_else(|| {
                Error::invalid_arg(
                    "object_store",
                    "FieldHashingIndexBuilder requires object_store in Parameters",
                )
            })?;

        let builder_params = HashmapIndexBuilderParams {
            max_memory_usage: None,
            object_store,
            temp_store: Arc::clone(&params.temp_store),
        };
        let builder = HashmapIndexBuilder::new(builder_params)?;
        Ok(Self {
            index_type,
            builder,
            scope: params.scope,
            properties: params.properties,
            scratch: Vec::new(),
            pending_entries: Vec::with_capacity(1024),
        })
    }

    /// Conditionally flushes pending entries when the batch size threshold is reached.
    ///
    /// This batching strategy reduces the overhead of frequent builder calls while
    /// keeping memory usage bounded. The threshold balances memory efficiency with
    /// write performance by avoiding excessive syscalls or memory pressure.
    #[inline]
    fn flush_if_needed(&mut self) -> Result<()> {
        if self.pending_entries.len() >= Self::PENDING_COUNT {
            self.flush_pending()?;
        }
        Ok(())
    }

    /// Transfers all buffered entries to the underlying builder and clears the buffer.
    ///
    /// Uses memory swapping to avoid copying the entry vector, then immediately
    /// submits the batch to the builder. This approach minimizes allocation overhead
    /// while ensuring prompt delivery of accumulated index data.
    #[inline]
    fn flush_pending(&mut self) -> Result<()> {
        if self.pending_entries.is_empty() {
            return Ok(());
        }
        let mut chunk = vec![];
        std::mem::swap(&mut chunk, &mut self.pending_entries);
        self.builder.push_entries(&chunk)?;
        Ok(())
    }
}

impl IndexBuilder for FieldHashingIndexBuilder {
    /// Returns a reference to the index type that created this builder.
    ///
    /// This reference is used to identify the type of index being built
    /// and to access type-specific metadata and capabilities.
    fn index_type(&self) -> &Arc<dyn IndexType> {
        &self.index_type
    }

    /// Processes a frame of records by extracting field-value pairs and generating index entries.
    ///
    /// Uses pre-calculated value paths from leaf paths to directly navigate to leaf values only.
    /// For each leaf field in the schema, constructs the appropriate path string and extracts
    /// the corresponding value from the record, creating indexed key-value pairs.
    ///
    /// The leaf paths are converted to value paths that can efficiently navigate nested structures
    /// without recursive traversal. List elements are represented as "item" in leaf paths and
    /// are handled by calculating appropriate element indices during traversal.
    ///
    /// Each complete key (path + value) gets hashed using xxHash3 and paired with the record's
    /// absolute position within the shard. These hash-position pairs accumulate in the pending
    /// buffer until the batch size triggers a flush to the underlying builder.
    fn process_frame(&mut self, frame: &Frame, location: &FrameLocation) -> Result<()> {
        if frame.is_empty() {
            return Ok(());
        }
        let Some(schema) = frame.schema.as_ref() else {
            return Err(Error::invalid_arg(
                "frame",
                "FieldHashingIndexBuilder requires schema in Frame for field path resolution",
            ));
        };

        let base = location.shard_range.start;
        let mut collector = FieldValuesCollector;
        let mut scratch = std::mem::take(&mut self.scratch);

        let leaf_paths = schema.leaf_paths();
        assert!(
            leaf_paths.len() == frame.fields.len(),
            "Schema leaf paths count must match frame fields count"
        );

        // Pre-concatenate field paths to avoid repeated joins during processing
        let concatenated_paths = leaf_paths.iter().map(|p| p.join(".")).collect::<Vec<_>>();

        // Pre-calculate value paths for each leaf path to enable direct navigation
        let value_lookup_paths = leaf_paths
            .iter()
            .map(|path| ValuePath::new(path, schema.fields()))
            .collect::<Result<Vec<_>>>()?;

        for record_idx in 0..frame.len() {
            let position = base + record_idx as u64;

            // Process each leaf field directly using its value paths
            for field_idx in 0..frame.fields.len() {
                scratch.clear();
                scratch.extend_from_slice(concatenated_paths[field_idx].as_bytes());
                scratch.push(b':');

                // Collects values of each leaf and sends hashed entries to the hashmap indexer.
                let mut collect = |key: &[u8]| -> Result<()> {
                    let hash = xxh3_64(key);
                    self.pending_entries.push(IndexedEntry { hash, position });
                    self.flush_if_needed()?;
                    Ok(())
                };

                collector.collect_leaf_values(
                    frame.fields[field_idx].as_ref(),
                    record_idx,
                    &value_lookup_paths[field_idx].segments,
                    &mut scratch,
                    &mut collect,
                )?;
            }
        }

        let _ = std::mem::replace(&mut self.scratch, scratch);

        Ok(())
    }

    /// Finalizes index construction by flushing remaining data and preparing artifacts.
    ///
    /// Ensures all buffered entries are written to the builder before sealing, then
    /// transforms the builder's partitioned output into generic index artifacts.
    /// Each partition becomes a separate artifact with sequential naming (part0, part1...).
    ///
    /// The conversion to PreparedIndex abstracts away the specific hashmap implementation
    /// details, allowing the index to integrate with the broader indexing infrastructure
    /// through standard interfaces.
    fn seal(mut self: Box<Self>) -> Result<PreparedIndex> {
        // Flush any remaining buffered entries
        self.flush_pending()?;

        let prepared = self.builder.finish()?;
        // Convert to generic PreparedIndex
        let mut artifacts = Vec::with_capacity(prepared.partitions.len());
        for partition in prepared.partitions {
            let name = format!("part{}", partition.index);
            let shard: Box<dyn DynPreparedShard> = partition.shard.into();
            artifacts.push(PreparedArtifact {
                name,
                properties: Default::default(),
                data: ArtifactData::PreparedShard(shard),
            });
        }

        Ok(PreparedIndex {
            index_type: self.index_type.name().to_string(),
            artifacts,
            scope: self.scope,
            properties: self.properties,
            size: None,
        })
    }
}

/// Optimized field path and value extractor that collects only leaf values using pre-calculated value paths.
///
/// Instead of recursively traversing data structures and collecting intermediate paths,
/// this collector uses pre-calculated value paths derived from leaf paths to navigate
/// directly to leaf values only. This approach is more efficient and reduces the
/// number of keys generated for indexing.
///
/// The value paths represent the navigation path through nested structures:
/// - For struct fields: ordinal is the field index
/// - For list items: ordinal is the special marker indicating list traversal
/// - List elements are denoted as "item" in leaf paths and handled specially
///
/// IMPORTANT: The key construction logic here must remain in sync with the query-time
/// key generation logic in `amudai-hashmap-index/src/read/field_hashing_index_reader.rs` to ensure
/// that lookups match the indexed keys correctly.
struct FieldValuesCollector;

impl FieldValuesCollector {
    /// Collects leaf values from a sequence by navigating through the data structure
    /// using pre-calculated value path segments and invoking a collector function
    /// for each leaf value found.
    ///
    /// This function serves as the entry point for leaf value collection, delegating
    /// the actual navigation to `navigate_to_leaf` with an initial segment index of 0.
    /// It efficiently extracts only leaf values (terminal nodes in the data structure)
    /// without collecting intermediate path values.
    ///
    /// # Parameters
    ///
    /// * `seq` - The root sequence containing the data to traverse. This represents
    ///           the starting point for navigation and contains the record data.
    /// * `record_idx` - The zero-based index of the specific record within the sequence
    ///                  to process. This identifies which record's data should be extracted.
    /// * `segments` - Pre-calculated navigation path segments that define how to traverse
    ///                from the root sequence to the target leaf values. Each segment
    ///                represents one navigation step (struct field access or list traversal).
    /// * `target` - Mutable buffer used to construct the binary key representation.
    ///              The field path prefix is already present, and leaf values are appended.
    ///              The buffer is automatically truncated after each value collection.
    /// * `collector` - Mutable closure that processes each complete key (path + value).
    ///                 Called once for each leaf value found, receiving the binary key
    ///                 representation for hashing and indexing.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful collection of all leaf values, or an error
    /// if navigation fails, value serialization encounters issues, or the collector
    /// function returns an error.
    ///
    pub fn collect_leaf_values<F>(
        &mut self,
        seq: &dyn Sequence,
        record_idx: usize,
        segments: &[ValuePathSegment],
        target: &mut Vec<u8>,
        collector: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.navigate_to_leaf(seq, record_idx, segments, 0, target, collector)
    }

    /// Recursively navigates through the data structure using value paths to reach leaf values.
    ///
    /// This is the core navigation logic that follows the ordinal path to reach leaf values.
    /// When a ListItem is encountered, it iterates through all list elements.
    /// When a StructField is encountered, it navigates to the specific field by index.
    fn navigate_to_leaf<F>(
        &mut self,
        seq: &dyn Sequence,
        record_index: usize,
        segments: &[ValuePathSegment],
        segment_idx: usize,
        target: &mut Vec<u8>,
        collector: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        assert!(segment_idx <= segments.len());

        // If we've consumed all value paths, we're at a leaf - extract the value
        if segment_idx == segments.len() {
            let prev_len = target.len();
            Self::write_leaf_value(seq, record_index, target);
            collector(target.as_slice())?;
            target.truncate(prev_len); // Restore target to original length
            return Ok(());
        }

        match &segments[segment_idx] {
            ValuePathSegment::StructField(field_idx) => {
                let s = seq.as_struct();
                if s.presence.is_null(record_index) {
                    return Ok(()); // Skip null structs
                }

                if let Some(child) = s.fields.get(*field_idx) {
                    self.navigate_to_leaf(
                        child.as_ref(),
                        record_index,
                        segments,
                        segment_idx + 1,
                        target,
                        collector,
                    )?;
                }
            }
            ValuePathSegment::ListItem => {
                match seq.basic_type().basic_type {
                    BasicType::List => {
                        let l = seq.as_list();
                        if l.presence.is_null(record_index) {
                            return Ok(()); // Skip null lists
                        }

                        let range = l.offsets.range_at(record_index);
                        if let Some(item) = &l.item {
                            for idx in range.start..range.end {
                                self.navigate_to_leaf(
                                    item.as_ref(),
                                    idx as usize,
                                    segments,
                                    segment_idx + 1,
                                    target,
                                    collector,
                                )?;
                            }
                        }
                    }
                    BasicType::FixedSizeList => {
                        let fl = seq.as_fixed_list();
                        let start = record_index * fl.list_size;
                        let end = start + fl.list_size;

                        if let Some(item) = &fl.item {
                            for idx in start..end {
                                self.navigate_to_leaf(
                                    item.as_ref(),
                                    idx,
                                    segments,
                                    segment_idx + 1,
                                    target,
                                    collector,
                                )?;
                            }
                        }
                    }
                    _ => {
                        return Err(Error::invalid_arg(
                            "sequence",
                            "ListItem navigation step requires a list sequence",
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Writes the binary representation of a leaf value to the target buffer.
    ///
    /// Converts primitive data types to their canonical binary representation for hashing.
    /// The serialization format must remain consistent across indexing and query operations
    /// to ensure hash values match correctly.
    ///
    /// # Arguments
    ///
    /// * `seq` - The sequence containing the value to extract
    /// * `index` - The record index within the sequence  
    /// * `target` - Buffer to write the binary representation to
    ///
    /// # Behavior
    ///
    /// * Null values result in no bytes written to the target buffer
    /// * Numeric types are serialized in little-endian format for consistency
    /// * Boolean values are represented as single bytes: '0' or '1'  
    /// * Strings are written as UTF-8 bytes without null termination
    /// * Binary data types (GUID, Binary, FixedSizeBinary) are written as-is
    /// * DateTime values are serialized as i64 timestamps in little-endian format
    #[inline]
    fn write_leaf_value(seq: &dyn Sequence, index: usize, target: &mut Vec<u8>) {
        let vs = seq.as_value();
        if vs.presence.is_null(index) {
            return; // Skip null values - don't write anything to target
        }
        let td = seq.basic_type();
        match td.basic_type {
            BasicType::Boolean => {
                if vs.as_slice::<u8>()[index] == 0 {
                    target.extend_from_slice(b"0");
                } else {
                    target.extend_from_slice(b"1");
                }
            }
            BasicType::Int8 => {
                if td.signed {
                    target.extend_from_slice(&vs.as_slice::<i8>()[index].to_le_bytes());
                } else {
                    target.extend_from_slice(&vs.as_slice::<u8>()[index].to_le_bytes());
                }
            }
            BasicType::Int16 => {
                if td.signed {
                    target.extend_from_slice(&vs.as_slice::<i16>()[index].to_le_bytes());
                } else {
                    target.extend_from_slice(&vs.as_slice::<u16>()[index].to_le_bytes());
                }
            }
            BasicType::Int32 => {
                if td.signed {
                    target.extend_from_slice(&vs.as_slice::<i32>()[index].to_le_bytes());
                } else {
                    target.extend_from_slice(&vs.as_slice::<u32>()[index].to_le_bytes());
                }
            }
            BasicType::Int64 => {
                if td.signed {
                    target.extend_from_slice(&vs.as_slice::<i64>()[index].to_le_bytes());
                } else {
                    target.extend_from_slice(&vs.as_slice::<u64>()[index].to_le_bytes());
                }
            }
            BasicType::String => target.extend_from_slice(vs.string_at(index).as_bytes()),
            BasicType::Guid => target.extend_from_slice(vs.fixed_binary_at(index, 16)),
            BasicType::Binary => target.extend_from_slice(vs.binary_at(index)),
            BasicType::FixedSizeBinary => {
                target.extend_from_slice(vs.fixed_binary_at(index, td.fixed_size as usize))
            }
            BasicType::DateTime => {
                target.extend_from_slice(&vs.as_slice::<i64>()[index].to_le_bytes())
            }
            BasicType::Float32
            | BasicType::Float64
            | BasicType::Unit
            | BasicType::List
            | BasicType::FixedSizeList
            | BasicType::Struct
            | BasicType::Map
            | BasicType::Union => {
                unimplemented!("Unsupported type for field hashing: {:?}", td.basic_type)
            }
        }
    }
}

/// Represents a single segment in a path used to navigate through structured data.
///
/// Each segment corresponds to one step in traversing from the root of a data structure
/// to a leaf value. This enum enables efficient navigation without string-based lookups
/// by using pre-calculated indices and markers.
#[derive(Debug, Clone, PartialEq)]
enum ValuePathSegment {
    /// Navigate to a specific field within a struct by its zero-based index
    StructField(usize),
    /// Navigate into list items (used when processing array/list elements)
    ListItem,
}

/// A sequence of navigation segments that define how to reach a specific leaf value.
///
/// This structure pre-calculates the navigation path from schema field paths, converting
/// string-based paths into efficient ordinal-based navigation sequences. This avoids
/// repeated string lookups during data processing and enables direct access to leaf values.
///
/// The segments are computed once during initialization and reused for all records
/// in a frame, providing significant performance benefits for large datasets.
#[derive(Debug, Clone, PartialEq)]
struct ValuePath {
    /// Ordered sequence of navigation steps to reach the target leaf value
    segments: Vec<ValuePathSegment>,
}

impl ValuePath {
    /// Constructs a ValuePath from a schema field path and root field definitions.
    ///
    /// Translates string-based field paths (like ["person", "address", "street"]) into
    /// efficient ordinal-based navigation segments. This method validates that all path
    /// components exist in the schema and builds the corresponding navigation sequence.
    ///
    /// # Arguments
    ///
    /// * `path` - Array of field names representing the path to a leaf value
    /// * `root_fields` - Schema field definitions at the root level
    ///
    /// # Returns
    ///
    /// A ValuePath containing the navigation segments, or an error if the path is invalid
    ///
    /// # Errors
    ///
    /// Returns an error if any field name in the path cannot be found in the schema
    pub fn new(
        path: &[Arc<str>],
        root_fields: &[amudai_format::projection::TypeProjectionRef],
    ) -> Result<Self> {
        let mut segments = Vec::new();
        let mut current_fields = root_fields;
        let mut current_field: Option<&amudai_format::projection::TypeProjectionRef> = None;

        for segment in path {
            if segment.as_ref() == "item" {
                // This is a list item reference
                segments.push(ValuePathSegment::ListItem);
                // For list items, the type information comes from the list's item type
                if let Some(field) = current_field {
                    if let Some(first_child) = field.children().first() {
                        current_field = Some(first_child);
                        current_fields = field.children();
                    }
                }
            } else {
                // Find the field by name in the current level
                let field_index = current_fields
                    .iter()
                    .position(|f| f.name().as_ref() == segment.as_ref())
                    .ok_or_else(|| {
                        Error::invalid_arg(
                            "leaf_path",
                            format!("Field '{segment}' not found in schema"),
                        )
                    })?;

                segments.push(ValuePathSegment::StructField(field_index));
                current_field = Some(&current_fields[field_index]);
                current_fields = current_field.unwrap().children();
            }
        }

        Ok(Self { segments })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use amudai_format::projection::SchemaProjection;
    use amudai_format::schema::{BasicType, BasicTypeDescriptor};
    use amudai_format::schema_builder::{DataTypeBuilder, FieldBuilder, SchemaBuilder};
    use amudai_sequence::list_sequence::ListSequence;
    use amudai_sequence::offsets::Offsets;
    use amudai_sequence::presence::Presence;
    use amudai_sequence::sequence::Sequence;
    use amudai_sequence::struct_sequence::StructSequence;
    use amudai_sequence::value_sequence::ValueSequence;

    use crate::write::field_hashing_index_builder::{
        FieldValuesCollector, ValuePath, ValuePathSegment,
    };

    fn td_int32() -> BasicTypeDescriptor {
        BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            ..Default::default()
        }
    }

    fn td_string() -> BasicTypeDescriptor {
        BasicTypeDescriptor {
            basic_type: BasicType::String,
            ..Default::default()
        }
    }

    fn collect_leaf_values(
        seq: &dyn Sequence,
        record_index: usize,
        segments: &[ValuePathSegment],
    ) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        let mut collector = FieldValuesCollector;
        let mut target = vec![];
        collector
            .collect_leaf_values(seq, record_index, segments, &mut target, &mut |k| {
                keys.push(k.to_vec());
                Ok(())
            })
            .unwrap();
        keys
    }

    #[test]
    fn test_value_paths() {
        // Build a simple schema: root: Struct { name: String, items: List<String> }
        let mut root = FieldBuilder::new_struct().with_name("root");
        root.data_type_mut()
            .add_child(DataTypeBuilder::new_str().with_field_name("name"));

        let mut items_list = DataTypeBuilder::new("items", BasicType::List, None, None, vec![]);
        items_list.add_child(DataTypeBuilder::new_str().with_field_name("item"));
        root.data_type_mut().add_child(items_list);

        let schema = SchemaBuilder::new(vec![root]).into_schema().unwrap();
        let proj = SchemaProjection::full(schema);

        // Test simple field path: root.name
        let name_path: Vec<Arc<str>> = vec!["root".into(), "name".into()];
        let value_path = ValuePath::new(&name_path, proj.fields()).unwrap();
        assert_eq!(
            value_path,
            ValuePath {
                segments: vec![
                    ValuePathSegment::StructField(0), // Navigate to "root" struct
                    ValuePathSegment::StructField(0), // Navigate to "name" field within root
                ]
            }
        );

        // Test list item path: root.items.item
        let item_path: Vec<Arc<str>> = vec!["root".into(), "items".into(), "item".into()];
        let value_path = ValuePath::new(&item_path, proj.fields()).unwrap();
        assert_eq!(
            value_path,
            ValuePath {
                segments: vec![
                    ValuePathSegment::StructField(0), // Navigate to "root" struct
                    ValuePathSegment::StructField(1), // Navigate to "items" field
                    ValuePathSegment::ListItem,       // Navigate into list items
                ]
            }
        );
    }

    #[test]
    fn test_collect_leaf_values() {
        // Create an Int32 value sequence with one non-null and one null
        let mut vs = ValueSequence::with_capacity(td_int32(), 2);
        vs.push_value::<i32>(42);
        vs.push_null();

        // Non-null at index 0 - should contain the actual value 42 in little-endian format
        let keys0 = collect_leaf_values(&vs, 0, &[]);
        assert_eq!(keys0.len(), 1);
        assert_eq!(keys0[0], 42i32.to_le_bytes().to_vec());

        let keys1 = collect_leaf_values(&vs, 1, &[]);
        assert_eq!(keys1.len(), 1);
        assert!(keys1[0].is_empty()); // Null value should yield empty key

        // Test String type
        let mut string_vs = ValueSequence::with_capacity(td_string(), 2);
        string_vs.push_str("hello");
        string_vs.push_str("");

        let string_keys0 = collect_leaf_values(&string_vs, 0, &[]);
        assert_eq!(string_keys0.len(), 1);
        assert_eq!(string_keys0[0], b"hello");

        let string_keys1 = collect_leaf_values(&string_vs, 1, &[]);
        assert_eq!(string_keys1.len(), 1);
        assert_eq!(string_keys1[0], b"");

        // Test Boolean type (ValueSequence doesn't support push_value for Boolean; build manually)
        let mut bool_vs = ValueSequence::with_capacity(
            BasicTypeDescriptor {
                basic_type: BasicType::Boolean,
                ..Default::default()
            },
            2,
        );
        // Write two boolean bytes and mark them present
        bool_vs.values.extend_from_slice::<u8>(&[1, 0]);
        bool_vs.presence.extend_with_non_nulls(2);

        let bool_keys0 = collect_leaf_values(&bool_vs, 0, &[]);
        assert_eq!(bool_keys0.len(), 1);
        assert_eq!(bool_keys0[0], b"1");

        let bool_keys1 = collect_leaf_values(&bool_vs, 1, &[]);
        assert_eq!(bool_keys1.len(), 1);
        assert_eq!(bool_keys1[0], b"0");

        // Test Int64 type
        let mut int64_vs = ValueSequence::with_capacity(
            BasicTypeDescriptor {
                basic_type: BasicType::Int64,
                signed: true,
                ..Default::default()
            },
            2,
        );
        int64_vs.push_value::<i64>(-9223372036854775807i64);
        int64_vs.push_value::<i64>(9223372036854775807i64);

        let int64_keys0 = collect_leaf_values(&int64_vs, 0, &[]);
        assert_eq!(int64_keys0.len(), 1);
        assert_eq!(
            int64_keys0[0],
            (-9223372036854775807i64).to_le_bytes().to_vec()
        );

        let int64_keys1 = collect_leaf_values(&int64_vs, 1, &[]);
        assert_eq!(int64_keys1.len(), 1);
        assert_eq!(
            int64_keys1[0],
            9223372036854775807i64.to_le_bytes().to_vec()
        );

        // Create child sequences
        let mut field_a = ValueSequence::with_capacity(td_int32(), 1);
        field_a.push_value::<i32>(7);
        let mut field_b = ValueSequence::with_capacity(td_string(), 1);
        field_b.push_str("hello");

        let s = StructSequence::new(
            None,
            vec![Box::new(field_a), Box::new(field_b)],
            Presence::Trivial(1),
        );

        // Test navigating to first field (ordinal 0) - should get the actual int32 value
        let keys_a = collect_leaf_values(&s, 0, &[ValuePathSegment::StructField(0)]);
        assert_eq!(keys_a.len(), 1);
        assert_eq!(keys_a[0], 7i32.to_le_bytes().to_vec());

        // Test navigating to second field (ordinal 1) - should get the actual string value
        let keys_b = collect_leaf_values(&s, 0, &[ValuePathSegment::StructField(1)]);
        assert_eq!(keys_b.len(), 1);
        assert_eq!(keys_b[0], b"hello");

        // Create a nested structure: root { inner: { value: int32, name: string } }
        let mut value_field = ValueSequence::with_capacity(td_int32(), 1);
        value_field.push_value::<i32>(123);
        let mut name_field = ValueSequence::with_capacity(td_string(), 1);
        name_field.push_str("nested");

        let inner_struct = StructSequence::new(
            None,
            vec![Box::new(value_field), Box::new(name_field)],
            Presence::Trivial(1),
        );

        let outer_struct =
            StructSequence::new(None, vec![Box::new(inner_struct)], Presence::Trivial(1));

        // Navigate to root.inner.value (ordinals: [0, 0])
        let keys_value = collect_leaf_values(
            &outer_struct,
            0,
            &[
                ValuePathSegment::StructField(0),
                ValuePathSegment::StructField(0),
            ],
        );
        assert_eq!(keys_value.len(), 1);
        assert_eq!(keys_value[0], 123i32.to_le_bytes().to_vec());

        // Navigate to root.inner.name (ordinals: [0, 1])
        let keys_name = collect_leaf_values(
            &outer_struct,
            0,
            &[
                ValuePathSegment::StructField(0),
                ValuePathSegment::StructField(1),
            ],
        );
        assert_eq!(keys_name.len(), 1);
        assert_eq!(keys_name[0], b"nested");

        // List with 2 lists: [2 items], [1 item]
        let mut offsets = Offsets::new();
        offsets.push_length(2); // list[0] has 2 items  
        offsets.push_length(1); // list[1] has 1 item
        let mut items = ValueSequence::with_capacity(td_int32(), 3);
        items.extend_from_slice::<i32>(&[10, 20, 30]);
        let l = ListSequence::new(None, Some(Box::new(items)), offsets, Presence::Trivial(2));

        // Collect for record_index 0 -> should get 2 leaf values (10 and 20)
        let keys0 = collect_leaf_values(&l, 0, &[ValuePathSegment::ListItem]);
        assert_eq!(keys0.len(), 2);
        assert_eq!(keys0[0], 10i32.to_le_bytes().to_vec());
        assert_eq!(keys0[1], 20i32.to_le_bytes().to_vec());

        // Collect for record_index 1 -> should get 1 leaf value (30)
        let keys1 = collect_leaf_values(&l, 1, &[ValuePathSegment::ListItem]);
        assert_eq!(keys1.len(), 1);
        assert_eq!(keys1[0], 30i32.to_le_bytes().to_vec());

        // Test list of strings to ensure string handling works in lists
        let mut offsets = Offsets::new();
        offsets.push_length(3); // list[0] has 3 string items
        offsets.push_length(1); // list[1] has 1 string item

        let mut items = ValueSequence::with_capacity(td_string(), 4);
        items.push_str("first");
        items.push_str("second");
        items.push_str(""); // empty string
        items.push_str("last");

        let l = ListSequence::new(None, Some(Box::new(items)), offsets, Presence::Trivial(2));

        // Collect for record_index 0 -> should get 3 string values
        let keys0 = collect_leaf_values(&l, 0, &[ValuePathSegment::ListItem]);
        assert_eq!(keys0.len(), 3);
        assert_eq!(keys0[0], b"first");
        assert_eq!(keys0[1], b"second");
        assert_eq!(keys0[2], b""); // empty string should be handled correctly

        // Collect for record_index 1 -> should get 1 string value
        let keys1 = collect_leaf_values(&l, 1, &[ValuePathSegment::ListItem]);
        assert_eq!(keys1.len(), 1);
        assert_eq!(keys1[0], b"last");

        // Test a complex structure: root { items: List<Struct{ id: int32, name: string }> }

        // Create inner struct items for the list
        let mut id_field1 = ValueSequence::with_capacity(td_int32(), 3);
        id_field1.extend_from_slice::<i32>(&[1, 2, 3]);
        let mut name_field1 = ValueSequence::with_capacity(td_string(), 3);
        name_field1.push_str("first");
        name_field1.push_str("second");
        name_field1.push_str("third");

        let item_struct = StructSequence::new(
            None,
            vec![Box::new(id_field1), Box::new(name_field1)],
            Presence::Trivial(3),
        );

        // Create the list containing the structs
        let mut offsets = Offsets::new();
        offsets.push_length(2); // list[0] has 2 struct items (indices 0,1)
        offsets.push_length(1); // list[1] has 1 struct item (index 2)

        let l = ListSequence::new(
            None,
            Some(Box::new(item_struct)),
            offsets,
            Presence::Trivial(2),
        );

        // Navigate to list items -> struct field 0 (id field)
        let id_keys0 = collect_leaf_values(
            &l,
            0,
            &[ValuePathSegment::ListItem, ValuePathSegment::StructField(0)],
        );
        assert_eq!(id_keys0.len(), 2);
        assert_eq!(id_keys0[0], 1i32.to_le_bytes().to_vec());
        assert_eq!(id_keys0[1], 2i32.to_le_bytes().to_vec());

        // Navigate to list items -> struct field 1 (name field)
        let name_keys0 = collect_leaf_values(
            &l,
            0,
            &[ValuePathSegment::ListItem, ValuePathSegment::StructField(1)],
        );
        assert_eq!(name_keys0.len(), 2);
        assert_eq!(name_keys0[0], b"first");
        assert_eq!(name_keys0[1], b"second");

        // Test second list record
        let id_keys1 = collect_leaf_values(
            &l,
            1,
            &[ValuePathSegment::ListItem, ValuePathSegment::StructField(0)],
        );
        assert_eq!(id_keys1.len(), 1);
        assert_eq!(id_keys1[0], 3i32.to_le_bytes().to_vec());

        let name_keys1 = collect_leaf_values(
            &l,
            1,
            &[ValuePathSegment::ListItem, ValuePathSegment::StructField(1)],
        );
        assert_eq!(name_keys1.len(), 1);
        assert_eq!(name_keys1[0], b"third");

        // Test handling of null values in struct fields
        let mut field_a = ValueSequence::with_capacity(td_int32(), 2);
        field_a.push_value::<i32>(42);
        field_a.push_null();

        let mut field_b = ValueSequence::with_capacity(td_string(), 2);
        field_b.push_str("valid");
        field_b.push_null();

        let s = StructSequence::new(
            None,
            vec![Box::new(field_a), Box::new(field_b)],
            Presence::Trivial(2),
        );

        // Record 0: both fields have values
        let keys_a0 = collect_leaf_values(&s, 0, &[ValuePathSegment::StructField(0)]);
        assert_eq!(keys_a0.len(), 1);
        assert_eq!(keys_a0[0], 42i32.to_le_bytes().to_vec());

        let keys_b0 = collect_leaf_values(&s, 0, &[ValuePathSegment::StructField(1)]);
        assert_eq!(keys_b0.len(), 1);
        assert_eq!(keys_b0[0], b"valid");

        // Record 1: both fields are null - should produce no values
        let keys_a1 = collect_leaf_values(&s, 1, &[ValuePathSegment::StructField(0)]);
        assert_eq!(keys_a1.len(), 1);

        let keys_b1 = collect_leaf_values(&s, 1, &[ValuePathSegment::StructField(1)]);
        assert_eq!(keys_b1.len(), 1);

        // Test empty list handling: one record with zero-length list
        let mut offsets = Offsets::new();
        offsets.push_length(0); // list[0] has 0 items
        let items = ValueSequence::with_capacity(td_int32(), 0);
        let l = ListSequence::new(None, Some(Box::new(items)), offsets, Presence::Trivial(1));

        // Should handle empty list gracefully for record 0
        let keys = collect_leaf_values(&l, 0, &[ValuePathSegment::ListItem]);
        assert_eq!(keys.len(), 0);

        // Test list where some items are null
        let mut offsets = Offsets::new();
        offsets.push_length(2); // list[0] has 2 items (one null, one valid)

        let mut items = ValueSequence::with_capacity(td_int32(), 2);
        items.push_null();
        items.push_value::<i32>(100);

        let l = ListSequence::new(None, Some(Box::new(items)), offsets, Presence::Trivial(1));

        let keys = collect_leaf_values(&l, 0, &[ValuePathSegment::ListItem]);
        assert_eq!(keys.len(), 2);
        assert!(keys[0].is_empty()); // null value should produce no bytes
        assert_eq!(keys[1], 100i32.to_le_bytes().to_vec());
    }
}
